"""Brazilian CEPs (~1.1M postal codes) → data_br.ceps via pyiceberg.

Seed/dim table loaded from `s3://data-br-raw/municipios-br/3.2.1/ceps.csv`
(produced by the upstream `municipios-br` package — Correios doesn't
publish a free bulk download, so the package merges multiple sources:
Kelvins, ViaCEP fallback, scraped CEP-by-CEP retries).

Same overwrite-only semantics as `municipios` (see that module's
docstring); pyiceberg direct because dlt's s3tables_iceberg
destination is append-only.

Schedule: yearly (CEPs change rarely; only when Correios opens new
postal districts). Re-run quando o package municipios-br publica novo
release.
"""
from __future__ import annotations

import argparse
import io
import os
import sys

import pyarrow as pa
import pyarrow.csv as pa_csv

from lakebrasil.common.args import add_common_args
from lakebrasil.common.s3 import s3_client
from lakebrasil.loaders.iceberg import catalog

RAW_BUCKET = os.environ.get("DATA_BR_RAW_BUCKET", "data-br-raw")
S3_KEY = "municipios-br/3.2.1/ceps.csv"
TABLE = "data_br.ceps"

# Column order matches CEPS_SCHEMA in s3tables-stack.ts (field-id order).
COLS = ("cep", "logradouro", "complemento", "bairro", "localidade",
        "uf", "ibge_code", "ddd", "source")

# Required (NOT NULL) columns per CEPS_SCHEMA.
REQUIRED = frozenset({"cep", "localidade", "uf", "source"})

# Explicit types — `ibge` column gets renamed to `ibge_code` after parse.
TYPES: dict[str, pa.DataType] = {
    "cep": pa.string(),
    "logradouro": pa.string(),
    "complemento": pa.string(),
    "bairro": pa.string(),
    "localidade": pa.string(),
    "uf": pa.string(),
    "ibge": pa.int64(),  # CSV column is `ibge`; renamed below.
    "ddd": pa.string(),
    "source": pa.string(),
    "synced_at": pa.string(),  # dropped after parse (not in Iceberg schema).
}


def _read_csv() -> pa.Table:
    raw = s3_client().get_object(Bucket=RAW_BUCKET, Key=S3_KEY)["Body"].read()
    convert = pa_csv.ConvertOptions(
        column_types=TYPES,
        strings_can_be_null=True,
        null_values=["", "null", "NULL"],
    )
    table = pa_csv.read_csv(io.BytesIO(raw), convert_options=convert)

    # Match Iceberg column name (CSV exports it as `ibge`).
    table = table.rename_columns([
        "ibge_code" if c == "ibge" else c for c in table.column_names
    ])
    if "synced_at" in table.column_names:
        table = table.drop_columns(["synced_at"])
    table = table.select(list(COLS))

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

    iceberg_table.overwrite(arrow)
    iceberg_table.refresh()
    print(f"  ✓ overwrote {TABLE} → snapshot {iceberg_table.metadata.current_snapshot_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
