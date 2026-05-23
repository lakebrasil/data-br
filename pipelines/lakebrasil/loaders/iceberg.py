"""Iceberg catalog helper — pluggable backend (AWS S3 Tables OR vanilla REST OR local).

Backend é auto-detectado pelo formato de `ICEBERG_WAREHOUSE`:

  arn:aws:s3tables:...:bucket/X  → AWS S3 Tables (SigV4 signing)
  http(s)://...                  → vanilla REST catalog
                                   (Nessie, Lakekeeper, Tabular, etc.)
  s3://bucket/path               → REST catalog c/ ICEBERG_REST_ENDPOINT
  local:///abs/path/warehouse    → SQLite catalog + local FS (zero-infra dev)

Env vars:
  ICEBERG_WAREHOUSE        (obrigatório) — ver formatos acima
  ICEBERG_REST_ENDPOINT    URL do catalog REST (Nessie/Lakekeeper)
  ICEBERG_REST_TOKEN       Bearer token opcional (auth REST)
  AWS_REGION               us-east-1 (só p/ S3 Tables)
  S3_ENDPOINT_URL          MinIO endpoint (ex: http://localhost:9000)
                           — propagado pro pyiceberg via s3.endpoint
"""
from __future__ import annotations

import os
from functools import lru_cache

from pyiceberg.catalog import load_catalog

NAMESPACE = "data_br"
META_NAMESPACE = "data_br_meta"


def _warehouse() -> str:
    w = os.environ.get("ICEBERG_WAREHOUSE")
    if not w:
        raise RuntimeError(
            "ICEBERG_WAREHOUSE não setado. Formatos suportados:\n"
            "  arn:aws:s3tables:<region>:<account>:bucket/<name>  (AWS S3 Tables)\n"
            "  http(s)://<host>:<port>/...                        (REST: Nessie, Lakekeeper)\n"
            "  s3://<bucket>/<path>                               (REST + S3-compat storage)\n"
            "  local:///abs/path/warehouse                        (SQLite + local FS)"
        )
    return w


def _build_s3tables_config(warehouse: str) -> dict:
    region = os.environ.get("AWS_REGION", "us-east-1")
    endpoint = os.environ.get(
        "ICEBERG_REST_ENDPOINT",
        f"https://s3tables.{region}.amazonaws.com/iceberg",
    )
    return {
        "type": "rest",
        "warehouse": warehouse,
        "uri": endpoint,
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "s3tables",
        "rest.signing-region": region,
    }


def _build_rest_config(warehouse: str) -> dict:
    endpoint = os.environ.get("ICEBERG_REST_ENDPOINT")
    if not endpoint:
        raise RuntimeError(
            "REST catalog detectado mas ICEBERG_REST_ENDPOINT não setado. "
            "Ex.: http://localhost:19120/api/v2/iceberg (Nessie) "
            "ou http://localhost:8181/catalog (Lakekeeper)."
        )
    cfg: dict = {
        "type": "rest",
        "warehouse": warehouse,
        "uri": endpoint,
    }
    if (token := os.environ.get("ICEBERG_REST_TOKEN")):
        cfg["token"] = token
    # MinIO / S3-compat
    if (s3_endpoint := os.environ.get("S3_ENDPOINT_URL")):
        cfg["s3.endpoint"] = s3_endpoint
        cfg["s3.access-key-id"] = os.environ.get(
            "AWS_ACCESS_KEY_ID", "minioadmin")
        cfg["s3.secret-access-key"] = os.environ.get(
            "AWS_SECRET_ACCESS_KEY", "minioadmin")
        cfg["s3.path-style-access"] = "true"
    return cfg


def _build_local_config(warehouse: str) -> dict:
    path = warehouse.replace("local://", "", 1)
    os.makedirs(path, exist_ok=True)
    return {
        "type": "sql",
        "uri": f"sqlite:///{path}/catalog.db",
        "warehouse": f"file://{path}",
    }


@lru_cache(maxsize=1)
def catalog():
    """Cached pyiceberg catalog. Auto-detects backend by ICEBERG_WAREHOUSE format.

    Credential chain (AWS): boto3 padrão — ~/.aws/credentials, IAM role,
    instance profile. Não força AWS_PROFILE (quebra Fargate).
    """
    warehouse = _warehouse()
    if warehouse.startswith("arn:aws:s3tables:"):
        cfg = _build_s3tables_config(warehouse)
    elif warehouse.startswith("local://"):
        cfg = _build_local_config(warehouse)
    else:
        cfg = _build_rest_config(warehouse)
    return load_catalog("lakebrasil", **cfg)


def table(name: str):
    return catalog().load_table(f"{NAMESPACE}.{name}")


def fq(name: str) -> str:
    return f"{NAMESPACE}.{name}"
