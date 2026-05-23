"""gov.br → S3 streaming (urllib UA bypass do F5 BIG-IP)."""
from __future__ import annotations

import urllib.error
import urllib.request

from .base import (
    FetchResult,
    manifest_key_for,
    s3_key_for_target,
    upload_stream_to_s3,
    write_manifest,
)


def fetch(target) -> FetchResult:
    s3_key = s3_key_for_target(target)
    req = urllib.request.Request(
        target.url, headers={"User-Agent": "Python-urllib/3.13"}
    )
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            ctype = resp.headers.get("Content-Type")
            bytes_w, digest = upload_stream_to_s3(resp, s3_key, content_type=ctype)
    except urllib.error.HTTPError as e:
        return FetchResult(False, s3_key, 0, "", target.url, error=f"HTTP {e.code}")
    except Exception as e:
        return FetchResult(False, s3_key, 0, "", target.url, error=str(e))

    write_manifest(
        manifest_key_for(target.source, s3_key),
        source=target.source,
        url=target.url,
        s3_key=s3_key,
        sha256=digest,
        bytes_written=bytes_w,
        content_type=ctype,
        extra={"params": target.extra},
    )
    return FetchResult(True, s3_key, bytes_w, digest, target.url)
