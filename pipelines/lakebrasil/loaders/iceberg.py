"""S3 Tables (Iceberg REST) catalog helper.

Catálogo único para data_br.*. O ARN do warehouse vem da env
`ICEBERG_WAREHOUSE` — injetada pela task definition do Fargate (via
DataBrPipelinesStack) ou exportada manualmente em dev.
"""
from __future__ import annotations

import os
from functools import lru_cache

from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog

NAMESPACE = "data_br"
META_NAMESPACE = "data_br_meta"
WAREHOUSE_ARN = os.environ.get("ICEBERG_WAREHOUSE")
if not WAREHOUSE_ARN:
    raise RuntimeError(
        "ICEBERG_WAREHOUSE não setado. Exporte o ARN do S3 Tables bucket "
        "(ex: arn:aws:s3tables:us-east-1:<account>:bucket/data-br-tables)."
    )
REGION = os.environ.get("AWS_REGION", "us-east-1")
ENDPOINT = os.environ.get(
    "ICEBERG_REST_ENDPOINT", f"https://s3tables.{REGION}.amazonaws.com/iceberg"
)


@lru_cache(maxsize=1)
def catalog() -> RestCatalog:
    # Não força AWS_PROFILE — credential chain do boto3 já encontra:
    #   - local: ~/.aws/credentials (set AWS_PROFILE=<seu-perfil> no shell)
    #   - Fargate: task role via metadata service
    #   - EC2: instance profile
    # Forçar profile inexistente quebra o Fargate.
    return load_catalog(
        "data-br",
        **{
            "type": "rest",
            "warehouse": WAREHOUSE_ARN,
            "uri": ENDPOINT,
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": REGION,
        },
    )


def table(name: str):
    return catalog().load_table(f"{NAMESPACE}.{name}")


def fq(name: str) -> str:
    return f"{NAMESPACE}.{name}"
