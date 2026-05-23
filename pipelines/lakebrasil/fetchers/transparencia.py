"""Portal da Transparência → S3 streaming.

URL pattern usa YYYY-MM mas o endpoint quer YYYYMM. Renomeia chave S3
de modo que o pipeline saiba achar (acrescenta .zip).
"""
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


def _yyyymm_from_url(url: str) -> str | None:
    tail = url.rstrip("/").rsplit("/", 1)[-1]
    digits = tail.replace("-", "")
    if len(digits) == 6 and digits.isdigit():
        return digits
    return None


def _normalize_url(url: str) -> str:
    ym = _yyyymm_from_url(url)
    if ym is not None and ym not in url:
        return url.rsplit("/", 1)[0] + "/" + ym
    return url


def fetch(target) -> FetchResult:
    url = _normalize_url(target.url)
    s3_key = s3_key_for_target(target)
    if "-" in s3_key.rsplit("/", 1)[-1] and not s3_key.endswith(".zip"):
        s3_key += ".zip"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "data-br-fetch/0.1"})
        with urllib.request.urlopen(req, timeout=600) as resp:
            ctype = resp.headers.get("Content-Type")
            bytes_w, digest = upload_stream_to_s3(resp, s3_key, content_type=ctype)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # Mês ainda não publicado — registra como sentinel pro driver pular.
            write_manifest(
                manifest_key_for(target.source, s3_key),
                source=target.source,
                url=url,
                s3_key=s3_key,
                sha256="",
                bytes_written=0,
                content_type=None,
                extra={"params": target.extra, "not_yet_published": True},
            )
            return FetchResult(False, s3_key, 0, "", url, error="404 not yet published")
        return FetchResult(False, s3_key, 0, "", url, error=f"HTTP {e.code}")
    except Exception as e:
        return FetchResult(False, s3_key, 0, "", url, error=str(e))

    write_manifest(
        manifest_key_for(target.source, s3_key),
        source=target.source,
        url=url,
        s3_key=s3_key,
        sha256=digest,
        bytes_written=bytes_w,
        content_type=ctype,
        extra={"params": target.extra},
    )
    return FetchResult(True, s3_key, bytes_w, digest, url)
