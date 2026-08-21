"""Brazilian CEPs (~1.1M postal codes) → data_br.ceps via pyiceberg.

Seed/dim table loaded directly from the `municipios-br` npm package's
bundled SQLite database (fetched from the npm registry, cached locally —
see `common/municipios_br_source.py`; no S3/AWS dependency at all).
Correios doesn't publish a free bulk download, so the upstream package
merges multiple sources: Kelvins, ViaCEP fallback, scraped CEP-by-CEP
retries. Previously loaded from a manually-staged, untracked CSV export
of the same package — reading the SQLite source directly is more
current and reproducible.

Same replace-via-delete-then-dlt-append semantics as `municipios` (see
that module's docstring for why and the atomicity tradeoff) — loads via
dlt + the shared `iceberg` destination like every other pipeline,
~1.27M rows batched across multiple load jobs (destination's
batch_size=100_000), all landing after `main()`'s upfront delete.

Schedule: yearly (CEPs change rarely; only when Correios opens new
postal districts). Re-run quando o package municipios-br publica novo
release.
"""
from __future__ import annotations

import argparse
import sys
from typing import Iterator

import dlt
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError

from lakebrasil.common.args import add_common_args
from lakebrasil.common.municipios_br_source import connect
from lakebrasil.loaders.iceberg import NAMESPACE, catalog
from lakebrasil.pipelines.destinations.iceberg import iceberg

TABLE = "ceps"

# Column order matches CEPS_SCHEMA in s3tables-stack.ts (field-id order).
# `ibge_code` in the Iceberg schema maps to the sqlite table's `ibge` column.
COLS = ("cep", "logradouro", "complemento", "bairro", "localidade",
        "uf", "ibge_code", "ddd", "source")
_SQLITE_COLUMN_FOR = {"ibge_code": "ibge"}

# Required (NOT NULL) columns per CEPS_SCHEMA.
REQUIRED = frozenset({"cep", "localidade", "uf", "source"})

TYPES: dict[str, pa.DataType] = {
    "cep": pa.string(),
    "logradouro": pa.string(),
    "complemento": pa.string(),
    "bairro": pa.string(),
    "localidade": pa.string(),
    "uf": pa.string(),
    "ibge_code": pa.int64(),
    "ddd": pa.string(),
    "source": pa.string(),
}


def _read_from_sqlite() -> pa.Table:
    conn = connect()
    try:
        select_cols = ", ".join(_SQLITE_COLUMN_FOR.get(c, c) for c in COLS)
        rows = conn.execute(f"SELECT {select_cols} FROM ceps").fetchall()
    finally:
        conn.close()

    columnar: dict[str, list] = {col: [] for col in COLS}
    for row in rows:
        for i, col in enumerate(COLS):
            value = row[i]
            # sqlite's `ibge` column is TEXT ('1200401'), not INTEGER —
            # cast to match the Iceberg schema's ibge_code:int64.
            if col == "ibge_code" and value is not None:
                value = int(value) if value != "" else None
            columnar[col].append(value)

    table = pa.table(columnar, schema=pa.schema([pa.field(c, TYPES[c]) for c in COLS]))

    fields = [
        pa.field(f.name, f.type, nullable=(f.name not in REQUIRED))
        for f in table.schema
    ]
    return table.cast(pa.schema(fields))


@dlt.resource(name="ceps", write_disposition="replace")
def ceps() -> Iterator[pa.Table]:
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
            iceberg_table.delete()
            print(f"  cleared {NAMESPACE}.{TABLE} — reloading")
    except NoSuchTableError:
        pass  # first run — the destination autocreates the table below.

    pipe = dlt.pipeline(pipeline_name="ceps", destination=iceberg)
    info = pipe.run(ceps())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
