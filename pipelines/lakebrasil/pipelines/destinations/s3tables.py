"""Custom dlt destination → S3 Tables Iceberg via pyiceberg.

dlt's built-in `filesystem + iceberg` destination assumes the writer
controls the underlying bucket. S3 Tables doesn't expose its
warehouse buckets directly — every data write must go through the
catalog's `update_table_metadata_location`, which pyiceberg handles
transparently when you call `iceberg_table.append(arrow_table)`.

This module exposes `s3tables_iceberg`, a `@dlt.destination` callable.
dlt batches data into Parquet files (controlled by `batch_size`) and
hands each path to the function; we read it as Arrow, cast to the
Iceberg schema, and append.

The Iceberg table must already exist (created pelo
DataBrS3TablesStack no infra-cdk — schemas vivem lá). Schema mismatch
raises pyiceberg.

Env (defaults match a placeholder; CDK injeta os valores reais):
    ICEBERG_WAREHOUSE   arn:aws:s3tables:...:bucket/data-br-tables
    AWS_REGION          us-east-1
    DATA_BR_NAMESPACE   data_br
"""
from __future__ import annotations

import os
from functools import lru_cache

import dlt
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog

REGION = os.environ.get("AWS_REGION", "us-east-1")
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE")
if not WAREHOUSE:
    raise RuntimeError(
        "ICEBERG_WAREHOUSE não setado. Exporte o ARN do S3 Tables bucket "
        "(ex: arn:aws:s3tables:us-east-1:<account>:bucket/data-br-tables)."
    )
NAMESPACE = os.environ.get("DATA_BR_NAMESPACE", "data_br")
ENDPOINT = os.environ.get(
    "ICEBERG_REST_ENDPOINT",
    f"https://s3tables.{REGION}.amazonaws.com/iceberg",
)


@lru_cache(maxsize=1)
def _catalog() -> RestCatalog:
    """Cached REST catalog. SigV4 signing is mandatory for the S3 Tables
    endpoint — the standard `iceberg-catalog-rest` client doesn't sign,
    which is why dlt's bundled iceberg destination won't work directly."""
    return RestCatalog(
        name="data_br",
        **{
            "uri": ENDPOINT,
            "warehouse": WAREHOUSE,
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": REGION,
        },
    )


@dlt.destination(
    batch_size=100_000,
    loader_file_format="parquet",
    name="s3tables_iceberg",
    naming_convention="snake_case",
    # CRUCIAL: Iceberg commits ao S3 Tables são single-writer-friendly —
    # dois appends concorrentes na mesma tabela disparam
    # CommitFailedException("branch main was created concurrently"),
    # dlt retry logic eventualmente roda mas pode perder a janela de
    # alguns appends se o batch worker abortar. Forçamos serial.
    max_parallel_load_jobs=1,
)
def s3tables_iceberg(items, table) -> None:
    """Append a batch to the matching Iceberg table.

    `items` is what dlt yields from its parquet load job — a single
    `pyarrow.RecordBatch` per call (see DestinationParquetLoadJob).
    `table` is the dlt schema dict — `table["name"]` is the Iceberg
    table name in `data_br.{name}`.

    No write_disposition handling here: pyiceberg.append always
    appends. Dedup happens upstream (dlt primary_key + merge) when the
    destination supports it; for append, post-load dedup via Iceberg
    row-level deletes is the escape hatch.
    """
    table_name = table["name"]

    # Normalise to pa.Table — dlt may pass a RecordBatch, a list of
    # RecordBatch, or already a Table depending on batch_size mode.
    if isinstance(items, pa.RecordBatch):
        arrow = pa.Table.from_batches([items])
    elif isinstance(items, list):
        arrow = pa.Table.from_batches(items)
    elif isinstance(items, pa.Table):
        arrow = items
    else:
        raise TypeError(f"unexpected items type {type(items).__name__}")

    iceberg_table = _catalog().load_table(f"{NAMESPACE}.{table_name}")
    target_schema = iceberg_table.schema().as_arrow()

    # Reorder columns to match the Iceberg schema's field order (cast()
    # alone matches by name but pyiceberg.append requires positional
    # ordering). Add missing nullable columns as null arrays.
    target_names = [f.name for f in target_schema]
    columns = []
    for name in target_names:
        if name in arrow.column_names:
            columns.append(arrow.column(name))
        else:
            field = target_schema.field(name)
            if not field.nullable:
                raise ValueError(
                    f"missing required column {name!r} for {table_name!r}"
                )
            null_arr = pa.nulls(arrow.num_rows, type=field.type)
            columns.append(null_arr)
    arrow = pa.Table.from_arrays(columns, names=target_names).cast(target_schema)

    iceberg_table.append(arrow)
