"""IBGE 5,572 municipios → data_br.municipios via pyiceberg.

Seed/dim table loaded from `s3://data-br-raw/municipios-br/3.2.1/municipios.csv`
(produced by the upstream `municipios-br` package — slow-moving, only
re-released when IBGE updates the canonical municipalities list or
new socio-econômico enrichment lands).

Why pyiceberg direct (not dlt + s3tables_iceberg destination): this
table is a full-replace seed, not an incremental append. dlt's
destination is append-only by design (see destinations/s3tables.py
for context); replace would compound rows on every run. We use
pyiceberg's `overwrite()` which atomically swaps the table contents
in a single Iceberg snapshot.

Refresh contract:
- 1st run on empty table  → load 5,572 rows.
- Re-run on populated table → no-op (counts match).
- Re-run with --full-refresh → atomic overwrite (replace).

Schedule: yearly (catálogo IBGE muda raramente). Re-run manualmente
quando o package municipios-br publica nova versão.
"""
from __future__ import annotations

import argparse
import io
import os
import sys

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv

from lakebrasil.loaders.iceberg import catalog
from lakebrasil.common.args import add_common_args

RAW_BUCKET = os.environ.get("DATA_BR_RAW_BUCKET", "data-br-raw")
S3_KEY = "municipios-br/3.2.1/municipios.csv"
TABLE = "data_br.municipios"

# 0/1 in CSV → bool in Iceberg.
BOOL_COLS = ("capital", "has_flag", "has_icons", "sistema_costeiro")

# Required (NOT NULL) columns per the Iceberg schema in
# infra-cdk/lib/workloads/data-br/stacks/s3tables-stack.ts MUNICIPIOS_SCHEMA.
# pyarrow CSV reads everything as nullable; we flip required cols
# back to nullable=false before append (pyiceberg validates).
REQUIRED = frozenset({
    "ibge_code", "name", "slug", "uf", "uf_name", "region",
    "region_name", "capital", "has_flag", "has_icons",
})

# Final column order — matches MUNICIPIOS_SCHEMA field-id order.
COLS = (
    "ibge_code", "name", "slug", "uf", "uf_name", "region", "region_name",
    "capital", "microrregiao", "mesorregiao", "regiao_imediata",
    "regiao_intermediaria", "populacao_2022", "populacao_estimada_2025",
    "area_km2", "densidade_demo", "latitude", "longitude", "ddd",
    "cep_sede", "gentilico", "bioma", "sistema_costeiro", "fuso_horario",
    "codigo_siafi", "codigo_tse", "pib", "pib_per_capita", "idhm",
    "indice_gini", "veiculos", "veiculos_ano", "taxa_mortalidade_infantil",
    "estabelecimentos_saude", "ideb_anos_iniciais_2023",
    "ideb_anos_finais_2023", "matriculas_2024", "escolas_2024",
    "docentes_2024", "fundeb_2024", "fpm_2024", "receita_total_2023",
    "receita_iptu_2023", "receita_iss_2023", "despesa_total_2023",
    "despesa_pessoal_2023", "receita_per_capita_2023", "taxa_ocupacao",
    "rendimento_medio", "taxa_alfabetizacao", "populacao_urbana_pct",
    "agua_atendimento_pct", "esgoto_atendimento_pct",
    "esgoto_tratamento_pct", "perda_agua_pct", "prefeito",
    "prefeito_eleito_2024", "prefeito_nome_urna", "prefeito_partido",
    "prefeito_coligacao", "prefeito_genero", "prefeito_escolaridade",
    "prefeito_cor_raca", "vice_prefeito_2024", "vice_partido",
    "vereadores_eleitos", "vagas_vereadores", "has_flag", "has_icons",
    "flag_source",
)

# Explicit types — pyarrow CSV inference is unreliable on mixed-empty
# numeric columns, so we spell everything out.
TYPES: dict[str, pa.DataType] = {
    "ibge_code": pa.int64(),
    "name": pa.string(),
    "slug": pa.string(),
    "uf": pa.string(),
    "uf_name": pa.string(),
    "region": pa.string(),
    "region_name": pa.string(),
    "has_flag": pa.int8(),  # cast → bool below
    "has_icons": pa.int8(),
    "flag_source": pa.string(),
    "microrregiao": pa.string(),
    "mesorregiao": pa.string(),
    "regiao_imediata": pa.string(),
    "regiao_intermediaria": pa.string(),
    "populacao_2022": pa.int64(),
    "populacao_estimada_2025": pa.int64(),
    "area_km2": pa.float64(),
    "densidade_demo": pa.float64(),
    "latitude": pa.float64(),
    "longitude": pa.float64(),
    "ddd": pa.string(),
    "cep_sede": pa.string(),
    "gentilico": pa.string(),
    "bioma": pa.string(),
    "sistema_costeiro": pa.int8(),
    "prefeito": pa.string(),
    "pib": pa.float64(),
    "pib_per_capita": pa.float64(),
    "taxa_mortalidade_infantil": pa.float64(),
    "indice_gini": pa.float64(),
    "estabelecimentos_saude": pa.int64(),
    "codigo_siafi": pa.string(),
    "fuso_horario": pa.string(),
    "capital": pa.int8(),
    "idhm": pa.float64(),
    "veiculos": pa.int64(),
    "veiculos_ano": pa.string(),
    "codigo_tse": pa.string(),
    "prefeito_eleito_2024": pa.string(),
    "prefeito_nome_urna": pa.string(),
    "prefeito_partido": pa.string(),
    "prefeito_coligacao": pa.string(),
    "prefeito_genero": pa.string(),
    "prefeito_escolaridade": pa.string(),
    "prefeito_cor_raca": pa.string(),
    "vice_prefeito_2024": pa.string(),
    "vice_partido": pa.string(),
    "vereadores_eleitos": pa.int32(),
    "vagas_vereadores": pa.int32(),
    "fundeb_2024": pa.float64(),
    "fpm_2024": pa.float64(),
    "ideb_anos_iniciais_2023": pa.float64(),
    "ideb_anos_finais_2023": pa.float64(),
    "matriculas_2024": pa.int64(),
    "escolas_2024": pa.int64(),
    "docentes_2024": pa.int64(),
    "receita_total_2023": pa.float64(),
    "receita_iptu_2023": pa.float64(),
    "receita_iss_2023": pa.float64(),
    "despesa_total_2023": pa.float64(),
    "despesa_pessoal_2023": pa.float64(),
    "receita_per_capita_2023": pa.float64(),
    "taxa_ocupacao": pa.float64(),
    "rendimento_medio": pa.float64(),
    "taxa_alfabetizacao": pa.float64(),
    "populacao_urbana_pct": pa.float64(),
    "agua_atendimento_pct": pa.float64(),
    "esgoto_atendimento_pct": pa.float64(),
    "esgoto_tratamento_pct": pa.float64(),
    "perda_agua_pct": pa.float64(),
}


def _read_csv() -> pa.Table:
    raw = boto3.client("s3").get_object(Bucket=RAW_BUCKET, Key=S3_KEY)["Body"].read()
    convert = pa_csv.ConvertOptions(
        column_types=TYPES,
        strings_can_be_null=True,
        null_values=["", "null", "NULL"],
    )
    table = pa_csv.read_csv(io.BytesIO(raw), convert_options=convert)

    # Drop columns that exist in the upstream CSV but not in the
    # Iceberg schema (icons_json, geometry_geojson são lookup-side).
    for col in ("icons_json", "geometry_geojson"):
        if col in table.column_names:
            table = table.drop_columns([col])

    # int8 → bool for the 4 flag columns.
    for col in BOOL_COLS:
        idx = table.schema.get_field_index(col)
        as_bool = pc.equal(table.column(col), pa.scalar(1, pa.int8()))
        table = table.set_column(idx, col, as_bool)

    table = table.select(list(COLS))

    # Flip nullability for required columns to match the Iceberg schema
    # (pyiceberg's append rejects nullable→required mismatches).
    fields = [
        pa.field(f.name, f.type, nullable=(f.name not in REQUIRED))
        for f in table.schema
    ]
    return table.cast(pa.schema(fields))


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    add_common_args(p, table_default=None, include_table=False)
    args = p.parse_args()

    iceberg_table = catalog().load_table(TABLE)
    current_rows = iceberg_table.scan().to_arrow().num_rows
    print(f"  {TABLE}: {current_rows:,} rows currently")

    if current_rows > 0 and not args.full_refresh:
        print(f"  {TABLE} já populada — pass --full-refresh para overwrite")
        return 0

    print(f"  reading s3://{RAW_BUCKET}/{S3_KEY}")
    arrow = _read_csv()
    print(f"  parsed {arrow.num_rows:,} rows × {arrow.num_columns} cols")

    # Atomic replace via pyiceberg (single new snapshot, OVERWRITE op).
    iceberg_table.overwrite(arrow)
    iceberg_table.refresh()
    print(f"  ✓ overwrote {TABLE} → snapshot {iceberg_table.metadata.current_snapshot_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
