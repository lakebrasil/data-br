"""S3 / S3 Files helpers compartilhados pelos pipelines.

Centraliza acesso ao raw layer com **dual mode**:
  - Mount mode (Fargate, dev com mount-s3): `DATA_BR_RAW_MOUNT=/mnt/raw`
    → todas as funções resolvem pra POSIX file open. Latência ~1ms,
    sem GET cost por arquivo, paginação trivial via os.scandir.
  - Boto3 mode (default fallback, laptop sem mount): chama S3 API.

Pipelines não enxergam a diferença — usam `list_keys()`, `read_bytes()`,
`open_zip()`. Quando rodando em Fargate com S3 Files volume montado,
performance é equivalente a leitura local depois do prefetch.
"""
from __future__ import annotations

import io
import os
import zipfile
from functools import lru_cache
from pathlib import Path
from typing import Iterator

import boto3
import duckdb
from botocore.config import Config

RAW_BUCKET = os.environ.get("DATA_BR_RAW_BUCKET", "data-br-raw")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Quando set, todos os acessos resolvem pra `{MOUNT_ROOT}/{key}` em vez
# de chamar o S3 API. Path POSIX completo do bucket raw exposto via
# S3 Files (NFS) ou mountpoint-s3 (FUSE).
MOUNT_ROOT = os.environ.get("DATA_BR_RAW_MOUNT")

# S3-compatível alternativo (MinIO local, R2, Wasabi, etc).
# Quando set, boto3 client aponta pra esse endpoint + path-style addressing.
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")


def _is_mounted() -> bool:
    return MOUNT_ROOT is not None and Path(MOUNT_ROOT).is_dir()


@lru_cache(maxsize=1)
def s3_client():
    """Cached boto3 S3 client. Single chokepoint pra raw layer access.

    Auto-detecta MinIO / S3-compat via `S3_ENDPOINT_URL`. AWS default
    quando não setado (usa credential chain padrão — env, ~/.aws,
    IAM role, instance profile).
    """
    kwargs: dict = {"region_name": AWS_REGION}
    if S3_ENDPOINT_URL:
        kwargs["endpoint_url"] = S3_ENDPOINT_URL
        # MinIO e a maioria dos S3-compat exige path-style (sem
        # virtual-host subdomains tipo `bucket.endpoint`).
        kwargs["config"] = Config(s3={"addressing_style": "path"})
        # Quando MinIO, credenciais explícitas vêm via AWS_* envs (já
        # consumidas pela credential chain do boto3); reforço aqui pra
        # ambientes onde o usuário só setou S3_ENDPOINT_URL sem rodar
        # `aws configure` antes.
        if "AWS_ACCESS_KEY_ID" in os.environ:
            kwargs["aws_access_key_id"] = os.environ["AWS_ACCESS_KEY_ID"]
            kwargs["aws_secret_access_key"] = os.environ.get(
                "AWS_SECRET_ACCESS_KEY", "")
    return boto3.client("s3", **kwargs)


def s3_uri(key: str) -> str:
    """URI canônica — sempre s3://, mesmo em mount mode (logging clarity)."""
    return f"s3://{RAW_BUCKET}/{key}"


def local_path(key: str) -> Path | None:
    """Resolve key → caminho local se mount mode, senão None.

    Use em hot paths que se beneficiam de zero-copy file IO (DuckDB
    `read_csv('/mnt/raw/...')`, `zipfile.ZipFile('/mnt/raw/...')`)."""
    if not _is_mounted():
        return None
    return Path(MOUNT_ROOT) / key


def list_keys(prefix: str) -> Iterator[str]:
    """Lista keys sob `prefix`. Em mount mode usa os.scandir (zero RPC);
    fallback paginated S3 API."""
    if _is_mounted():
        root = Path(MOUNT_ROOT) / prefix.rstrip("/")
        if not root.exists():
            return
        # scandir não recursivo — emulamos S3's behavior de listar tudo
        # sob o prefix usando rglob. Atenção: prefix sempre tem `/raw/`
        # então não há sub-tree gigante.
        for path in sorted(root.rglob("*")):
            if path.is_file():
                yield str(path.relative_to(MOUNT_ROOT))
        return

    paginator = s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def read_bytes(key: str) -> bytes:
    """Read full object → bytes. Mount: file read; fallback: S3 GET.
    Use só pra arquivos pequenos (<100 MB) — pra zips grandes use
    `open_zip()` que stream-extrai."""
    p = local_path(key)
    if p is not None:
        return p.read_bytes()
    return s3_client().get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()


# Backward compat alias (era o nome antigo).
get_object_bytes = read_bytes


def open_zip(key: str) -> zipfile.ZipFile:
    """Abre zip pra leitura. Mount: zipfile direto no FS (random access
    via FUSE/NFS); fallback: baixa pra memória + BytesIO.

    Caller é responsável por fechar (`with open_zip(k) as zf:`)."""
    p = local_path(key)
    if p is not None:
        return zipfile.ZipFile(p)
    return zipfile.ZipFile(io.BytesIO(read_bytes(key)))


def stream_zip_csv(key: str, csv_filter=None) -> Iterator[tuple[str, io.BufferedReader]]:
    """Itera CSVs internos do zip. `csv_filter(name) -> bool` opcional."""
    with open_zip(key) as zf:
        for inner in zf.namelist():
            if csv_filter and not csv_filter(inner):
                continue
            with zf.open(inner) as fh:
                yield inner, fh


def configure_duckdb_s3(con: duckdb.DuckDBPyConnection) -> None:
    """Wire DuckDB pra ler s3:// via httpfs.

    Em mount mode esse setup é desnecessário — DuckDB lê `/mnt/raw/...`
    direto. Mantido pra usos que querem `read_parquet('s3://...')`
    explicitamente (ex: cross-bucket queries)."""
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        CREATE OR REPLACE SECRET s3_default (
            TYPE s3,
            PROVIDER credential_chain,
            REGION '{AWS_REGION}'
        );
    """)
