#!/usr/bin/env python3
"""Exports every table in the local Iceberg warehouse as a single flat
.parquet file, for the browser-side DuckDB-WASM data explorer on the docs
site — that reads Parquet directly over HTTP range requests, so it needs
one plain file per table (not an Iceberg table dir / not a tarball).

Usage:
    ICEBERG_WAREHOUSE=local:///path/to/warehouse python3 scripts/export_parquet_for_web.py [out_dir]
"""
from __future__ import annotations

import os
import sys

import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog

WAREHOUSE_ENV = os.environ.get("ICEBERG_WAREHOUSE", "")
if not WAREHOUSE_ENV.startswith("local://"):
    sys.exit("ICEBERG_WAREHOUSE must be a local:// warehouse")
WAREHOUSE_PATH = WAREHOUSE_ENV.removeprefix("local://")
OUT_DIR = sys.argv[1] if len(sys.argv) > 1 else "/tmp/lakebrasil-web-parquet"
os.makedirs(OUT_DIR, exist_ok=True)

catalog = SqlCatalog(
    "lakebrasil",
    **{
        "type": "sql",
        "uri": f"sqlite:///{WAREHOUSE_PATH}/catalog.db",
        "warehouse": f"file://{WAREHOUSE_PATH}",
    },
)

tables = sorted(catalog.list_tables("data_br"))
print(f"Exporting {len(tables)} tables to {OUT_DIR}/")
for namespace, name in tables:
    tbl = catalog.load_table((namespace, name))
    arrow_tbl = tbl.scan().to_arrow()
    out_path = os.path.join(OUT_DIR, f"{name}.parquet")
    pq.write_table(arrow_tbl, out_path, compression="zstd")
    size_mb = os.path.getsize(out_path) / 1e6
    print(f"  {name:35s} {arrow_tbl.num_rows:>12,} rows  {size_mb:>8.1f} MB")

print("done")
