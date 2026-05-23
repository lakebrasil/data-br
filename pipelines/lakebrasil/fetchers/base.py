"""Base types + S3 helpers para fetchers cloud-native.

Mudança vs versão local-disk: raw artefatos vão direto pra S3 via
multipart upload streaming (sem `/tmp` intermediário). Manifests também
vivem em S3 ao lado do raw.

Layout S3:
    s3://{RAW_BUCKET}/{source_out}/{filename}
    s3://{RAW_BUCKET}/_manifest/{source}/{stem}.manifest.json

`source_out` vem do `out:` do catalog.yaml (ex: 'bacen/raw/').
"""
from __future__ import annotations

import hashlib
import io
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Optional

import boto3

RAW_BUCKET = os.environ.get("DATA_BR_RAW_BUCKET", "data-br-raw")
MANIFEST_PREFIX = "_manifest"


@dataclass(frozen=True)
class FetchResult:
    ok: bool
    s3_key: str
    bytes_written: int
    sha256: str
    url: str
    skipped: bool = False
    error: Optional[str] = None


@lru_cache(maxsize=1)
def s3_client():
    """Cached boto3 S3 client. Profile via env (AWS_PROFILE) or instance role."""
    return boto3.client("s3")


def s3_key_for_target(target) -> str:
    """Translate a FetchTarget's local out_path into the S3 key.
    `target.out_path` is `data-br-sources/<source>/raw/<file>` →
    `<source>/raw/<file>`."""
    parts = target.out_path.parts
    # Find the source dir (parent of 'raw') — anchor on the catalog `out:`.
    # Simplest: take the last 3 components: source, 'raw', filename.
    if len(parts) >= 3 and parts[-2] == "raw":
        return f"{parts[-3]}/raw/{parts[-1]}"
    return parts[-1]  # fallback


def manifest_key_for(source: str, s3_key: str) -> str:
    """Sibling manifest in S3, namespaced by source.
    `<source>/raw/433_ipca_mensal.json` →
    `_manifest/<source>/433_ipca_mensal.manifest.json`."""
    filename = s3_key.rsplit("/", 1)[-1]
    stem = filename.rsplit(".", 1)[0] if "." in filename else filename
    return f"{MANIFEST_PREFIX}/{source}/{stem}.manifest.json"


def read_manifest(manifest_key: str) -> Optional[dict]:
    try:
        resp = s3_client().get_object(Bucket=RAW_BUCKET, Key=manifest_key)
        return json.loads(resp["Body"].read())
    except s3_client().exceptions.NoSuchKey:
        return None
    except Exception:
        return None


def write_manifest(
    manifest_key: str,
    *,
    source: str,
    url: str,
    s3_key: str,
    sha256: str,
    bytes_written: int,
    content_type: Optional[str] = None,
    extra: Optional[dict] = None,
) -> None:
    payload = {
        "source": source,
        "url": url,
        "s3_key": s3_key,
        "s3_uri": f"s3://{RAW_BUCKET}/{s3_key}",
        "sha256": sha256,
        "bytes": bytes_written,
        "content_type": content_type,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        payload.update(extra)
    s3_client().put_object(
        Bucket=RAW_BUCKET,
        Key=manifest_key,
        Body=json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def upload_stream_to_s3(stream, s3_key: str, content_type: Optional[str] = None,
                        chunk_size: int = 8 << 20) -> tuple[int, str]:
    """Stream `stream` (file-like with `.read()`) → s3 multipart upload.
    Computes sha256 incrementally without loading the full payload.
    Returns (bytes_written, sha256_hex)."""
    s3 = s3_client()
    h = hashlib.sha256()
    total = 0

    # Use multipart for anything > 8 MB; below that, single PUT.
    head = stream.read(chunk_size)
    h.update(head)
    total = len(head)

    next_chunk = stream.read(chunk_size)

    if not next_chunk:
        # Single-part upload.
        s3.put_object(
            Bucket=RAW_BUCKET, Key=s3_key, Body=head,
            **({"ContentType": content_type} if content_type else {}),
        )
        return total, h.hexdigest()

    # Multipart upload.
    create = s3.create_multipart_upload(
        Bucket=RAW_BUCKET, Key=s3_key,
        **({"ContentType": content_type} if content_type else {}),
    )
    upload_id = create["UploadId"]
    parts = []
    part_no = 1
    try:
        # Upload accumulated head as part 1.
        resp = s3.upload_part(
            Bucket=RAW_BUCKET, Key=s3_key, PartNumber=part_no,
            UploadId=upload_id, Body=head,
        )
        parts.append({"ETag": resp["ETag"], "PartNumber": part_no})

        # Continue with the chunk we just read + remaining stream.
        chunk = next_chunk
        while chunk:
            part_no += 1
            h.update(chunk)
            total += len(chunk)
            resp = s3.upload_part(
                Bucket=RAW_BUCKET, Key=s3_key, PartNumber=part_no,
                UploadId=upload_id, Body=chunk,
            )
            parts.append({"ETag": resp["ETag"], "PartNumber": part_no})
            chunk = stream.read(chunk_size)

        s3.complete_multipart_upload(
            Bucket=RAW_BUCKET, Key=s3_key, UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        try:
            s3.abort_multipart_upload(Bucket=RAW_BUCKET, Key=s3_key, UploadId=upload_id)
        except Exception:
            pass
        raise

    return total, h.hexdigest()


def s3_object_exists(s3_key: str) -> bool:
    s3 = s3_client()
    try:
        s3.head_object(Bucket=RAW_BUCKET, Key=s3_key)
        return True
    except Exception:
        return False
