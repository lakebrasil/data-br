"""IBGE 5,572 municipios → data_br.municipios via pyiceberg.

Seed/dim table loaded directly from the `municipios-br` npm package's
bundled SQLite database (fetched from the npm registry, cached locally —
see `common/municipios_br_source.py`; no S3/AWS dependency at all).
Previously loaded from a manually-staged, untracked CSV export of the
same package at `s3://data-br-raw/municipios-br/3.2.1/municipios.csv` —
reading the SQLite source directly is more current and reproducible
(no separate manual export step, no untracked S3 object).

Loads via dlt + the shared `iceberg` destination (same as every other
pipeline), same as any other pipeline. This table is a full-replace
seed though, not an incremental append — the shared destination is
append-only by design (see destinations/iceberg.py), so `main()` does
the "replace" part itself: `iceberg_table.delete()` (clear all rows)
immediately before the dlt run, so the two steps together behave like
a replace. There's a brief window where the table reads empty between
those two steps — acceptable for a yearly-refresh reference table, not
something an append-heavy fact table should copy.

Refresh contract:
- 1st run (table doesn't exist yet) → dlt autocreates it, loads 5,572 rows.
- Re-run on populated table → no-op (counts match).
- Re-run with --full-refresh → delete then reload (replace).

Schedule: yearly (catálogo IBGE muda raramente). Re-run manualmente
quando o package municipios-br publica nova versão.
"""
from __future__ import annotations

import argparse
import sys
from typing import Iterator

import dlt
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.exceptions import NoSuchTableError

from lakebrasil.common.args import add_common_args
from lakebrasil.common.municipios_br_source import connect
from lakebrasil.loaders.iceberg import NAMESPACE, catalog
from lakebrasil.pipelines.destinations.iceberg import iceberg

TABLE = "municipios"

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


def _read_from_sqlite() -> pa.Table:
    conn = connect()
    try:
        rows = conn.execute(
            f"SELECT {', '.join(COLS)} FROM municipios ORDER BY ibge_code"
        ).fetchall()
    finally:
        conn.close()

    # Columnar dict-of-lists — same shape pa.table() wants, built straight
    # from sqlite3.Row (has_flag/has_icons/sistema_costeiro/capital come
    # back as INTEGER 0/1, same encoding the old CSV export used, so the
    # int8→bool cast below is unchanged).
    columnar: dict[str, list] = {col: [] for col in COLS}
    for row in rows:
        for col in COLS:
            columnar[col].append(row[col])

    table = pa.table(columnar, schema=pa.schema([pa.field(c, TYPES[c]) for c in COLS]))

    for col in BOOL_COLS:
        idx = table.schema.get_field_index(col)
        as_bool = pc.equal(table.column(col), pa.scalar(1, pa.int8()))
        table = table.set_column(idx, col, as_bool)

    # Flip nullability for required columns to match the Iceberg schema
    # (pyiceberg's append rejects nullable→required mismatches).
    fields = [
        pa.field(f.name, f.type, nullable=(f.name not in REQUIRED))
        for f in table.schema
    ]
    return table.cast(pa.schema(fields))


@dlt.resource(name="municipios", write_disposition="replace")
def municipios() -> Iterator[pa.Table]:
    print("  reading municipios-br SQLite database")
    arrow = _read_from_sqlite()
    print(f"  parsed {arrow.num_rows:,} rows × {arrow.num_columns} cols")
    yield arrow


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    add_common_args(p, table_default=None, include_table=False)
    args = p.parse_args()

    try:
        iceberg_table = catalog().load_table(f"{NAMESPACE}.{TABLE}")
        current_rows = iceberg_table.scan().to_arrow().num_rows
        print(f"  {NAMESPACE}.{TABLE}: {current_rows:,} rows currently")
        if current_rows > 0:
            if not args.full_refresh:
                print(f"  {NAMESPACE}.{TABLE} já populada — pass --full-refresh para overwrite")
                return 0
            # The shared `iceberg` destination is append-only by design —
            # do the "replace" half ourselves before dlt appends the fresh
            # rows. See module docstring for the atomicity tradeoff.
            iceberg_table.delete()
            print(f"  cleared {NAMESPACE}.{TABLE} — reloading")
    except NoSuchTableError:
        pass  # first run — the destination autocreates the table below.

    pipe = dlt.pipeline(pipeline_name="municipios", destination=iceberg)
    info = pipe.run(municipios())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
