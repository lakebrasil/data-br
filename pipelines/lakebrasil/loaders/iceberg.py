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
    """The native S3 Tables Iceberg REST endpoint is `s3tables IAM
    authorization only` — it does NOT vend temporary per-table credentials
    the way the Glue-federated endpoint (with Lake Formation) does. Sending
    `header.X-Iceberg-Access-Delegation: vended-credentials` is a no-op here
    (confirmed empirically: `LoadTableResult` never carries
    `s3.access-key-id`/`secret`/`session-token`). Without explicit
    credentials, pyarrow's S3FileSystem fails data-plane writes against the
    S3 Tables backing bucket with a confusing "Please use Signature Version
    4" error instead of a clean 403 — so we resolve the caller's own
    (assumed-role-aware) boto3 credentials once here and hand them to
    pyiceberg explicitly. The caller's principal still needs `s3tables:*`
    (or at minimum `GetTableMetadataLocation` + `GetTableData`/`PutTableData`)
    IAM permissions — this doesn't bypass authorization, it just stops
    relying on vending that this endpoint doesn't do for standalone roles.
    """
    region = os.environ.get("AWS_REGION", "us-east-1")
    endpoint = os.environ.get(
        "ICEBERG_REST_ENDPOINT",
        f"https://s3tables.{region}.amazonaws.com/iceberg",
    )

    import boto3

    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"), region_name=region)
    creds = session.get_credentials()
    if creds is None:
        raise RuntimeError(
            "no AWS credentials resolvable (boto3 default chain) — set AWS_PROFILE "
            "or run somewhere with an instance/role credential source"
        )
    frozen = creds.get_frozen_credentials()

    config = {
        "type": "rest",
        "warehouse": warehouse,
        "uri": endpoint,
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "s3tables",
        "rest.signing-region": region,
        "s3.region": region,
        "s3.access-key-id": frozen.access_key,
        "s3.secret-access-key": frozen.secret_key,
    }
    if frozen.token:
        config["s3.session-token"] = frozen.token
    return config


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
