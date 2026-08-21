"""Custom dlt destination → Iceberg (pluggable: AWS S3 Tables OR REST OR local).

dlt's built-in `filesystem + iceberg` destination assume o writer
controla o bucket diretamente. S3 Tables (e qualquer REST catalog
gerenciado) NÃO expõe o bucket — toda escrita passa pelo catalog que
faz `update_table_metadata_location`, o que pyiceberg lida
transparentemente quando você chama `iceberg_table.append(arrow_table)`.

Este módulo expõe `iceberg`, um `@dlt.destination` callable. dlt
batcheia em Parquet e nos entrega Arrow; nós alinhamos schema e
appendamos via pyiceberg.

Backend é auto-detectado em `lakebrasil.loaders.iceberg.catalog()`
(env `ICEBERG_WAREHOUSE` — ver doc lá).

Env vars adicionais aqui:
    DATA_BR_NAMESPACE   override do namespace (default 'data_br')
    DATA_BR_AUTOCREATE  '1' (default) cria namespace + tabela do schema
                        do primeiro batch quando não existem. '0' exige
                        DDL prévio (AWS S3 Tables CDK stack).
"""
from __future__ import annotations

import os

import dlt
import pyarrow as pa
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
)

from lakebrasil.loaders.iceberg import catalog as _shared_catalog

NAMESPACE = os.environ.get("DATA_BR_NAMESPACE", "data_br")
AUTOCREATE = os.environ.get("DATA_BR_AUTOCREATE", "1") == "1"


def _ensure_table(table_name: str, arrow_table: pa.Table):
    """Load existing table or create from the arrow schema.

    Auto-create é tudo-ou-nada: se a tabela existe, schema validation
    rola normalmente no `iceberg()` (reorder + cast). Se não existe e
    AUTOCREATE=1, cria com schema EXATAMENTE igual ao do primeiro batch.
    """
    cat = _shared_catalog()
    try:
        return cat.load_table(f"{NAMESPACE}.{table_name}")
    except NoSuchTableError:
        if not AUTOCREATE:
            raise
    except NoSuchNamespaceError:
        if not AUTOCREATE:
            raise
        cat.create_namespace(NAMESPACE)

    # Bootstrap namespace just in case (idempotent if already exists) — only
    # swallow the "already there" case, not e.g. a permission/network error
    # that would otherwise surface as a much more confusing failure later.
    try:
        cat.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    from pyiceberg.io.pyarrow import pyarrow_to_schema
    from pyiceberg.table.name_mapping import MappedField, NameMapping

    # Build NameMapping com IDs sequenciais (1..N) a partir dos top-level
    # field names. pyarrow_to_schema usa esse mapping pra assinar field-ids
    # ao schema iceberg (arrow não carrega field-ids).
    name_mapping = NameMapping([
        MappedField(field_id=i + 1, names=[f.name])
        for i, f in enumerate(arrow_table.schema)
    ])
    iceberg_schema = pyarrow_to_schema(arrow_table.schema, name_mapping=name_mapping)
    # CRITICAL: dlt-written parquets não carregam Iceberg field-ids embedded.
    # Sem `schema.name-mapping.default` o append falha com:
    #   "Parquet file does not have field-ids and the Iceberg table does
    #    not have 'schema.name-mapping.default' defined"
    return cat.create_table(
        f"{NAMESPACE}.{table_name}",
        schema=iceberg_schema,
        properties={"schema.name-mapping.default": name_mapping.model_dump_json()},
    )


@dlt.destination(
    batch_size=100_000,
    loader_file_format="parquet",
    name="iceberg",
    naming_convention="snake_case",
    # CRUCIAL: Iceberg commits são single-writer-friendly — 2 appends
    # concorrentes na mesma tabela disparam CommitFailedException("branch
    # main was created concurrently"). dlt retry roda mas pode perder
    # janela. Forçamos serial.
    max_parallel_load_jobs=1,
)
def iceberg(items, table) -> None:
    """Append a batch to the matching Iceberg table.

    `items` is what dlt yields from its parquet load job — single
    `pyarrow.RecordBatch` per call (see DestinationParquetLoadJob).
    `table` is the dlt schema dict — `table["name"]` é o nome da
    Iceberg table em `{NAMESPACE}.{name}`.

    Sem write_disposition handling aqui: pyiceberg.append sempre
    appenda. Dedup é responsabilidade do caller (loaded_triples filter).
    """
    table_name = table["name"]

    # Normalise → pa.Table
    if isinstance(items, pa.RecordBatch):
        arrow = pa.Table.from_batches([items])
    elif isinstance(items, list):
        arrow = pa.Table.from_batches(items)
    elif isinstance(items, pa.Table):
        arrow = items
    else:
        raise TypeError(f"unexpected items type {type(items).__name__}")

    iceberg_table = _ensure_table(table_name, arrow)
    target_schema = iceberg_table.schema().as_arrow()

    # Reorder + add missing nullable columns
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
            columns.append(pa.nulls(arrow.num_rows, type=field.type))
    arrow = pa.Table.from_arrays(columns, names=target_names).cast(target_schema)

    iceberg_table.append(arrow)


# Backward-compat alias — old `s3tables_iceberg` name across pipelines.
s3tables_iceberg = iceberg
