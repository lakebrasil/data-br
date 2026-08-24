#!/usr/bin/env python3
"""Quick-view UI for the local lakebrasil Iceberg warehouse.

Opens DuckDB's built-in web UI (a real SQL editor + data grid in your
browser) with a view already registered for every table in the warehouse —
just click a table name or run SQL directly, no catalog wrangling needed.

Usage:
    ICEBERG_WAREHOUSE=local:///path/to/warehouse python3 scripts/browse_warehouse.py
"""
from __future__ import annotations

import os
import sys

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

WAREHOUSE_ENV = os.environ.get("ICEBERG_WAREHOUSE", "")
if not WAREHOUSE_ENV.startswith("local://"):
    sys.exit(
        "ICEBERG_WAREHOUSE must be a local:// warehouse, e.g.\n"
        "  export ICEBERG_WAREHOUSE=local:///Users/you/lakebrasil-warehouse"
    )
WAREHOUSE_PATH = WAREHOUSE_ENV.removeprefix("local://")

catalog = SqlCatalog(
    "lakebrasil",
    **{
        "type": "sql",
        "uri": f"sqlite:///{WAREHOUSE_PATH}/catalog.db",
        "warehouse": f"file://{WAREHOUSE_PATH}",
    },
)

con = duckdb.connect(os.path.join(WAREHOUSE_PATH, "browse.duckdb"))
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("INSTALL ui; LOAD ui;")

tables = sorted(catalog.list_tables("data_br"))
print(f"Registering {len(tables)} tables as DuckDB views...")
for namespace, name in tables:
    tbl = catalog.load_table((namespace, name))
    metadata_path = tbl.metadata_location
    con.execute(
        f'CREATE OR REPLACE VIEW "{name}" AS '
        f"SELECT * FROM iceberg_scan('{metadata_path}')"
    )
    print(f"  {name}")

(message,) = con.execute("CALL start_ui()").fetchone()
print(f"\n{message}")
print("(Ctrl+C to stop; this process must keep running while you browse)")

try:
    import time

    while True:
        time.sleep(3600)
except KeyboardInterrupt:
    pass
