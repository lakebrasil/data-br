"""WebDAV fetcher (Receita Federal CNPJ) — streaming direto para S3.

Receita Federal publica o dump CNPJ via WebDAV em
    https://arquivos.receitafederal.gov.br/public.php/webdav/Dados/Cadastros/CNPJ/{YYYY-MM}/{File}.zip
com auth básica obrigatória (RFC 7617). Token público — exporte:

    export DATA_BR_WEBDAV_TOKEN='<token-publicado-pela-receita>'

Token atual em https://github.com/lakebrasil/data-br/issues/13.
Senha vai vazia (esquema `<token>:`).

Streaming: chunks de 8 MB direto pro S3 via multipart upload, sem
`/tmp` intermediário (Estabelecimentos0.zip = 2 GB, não cabe em /tmp
do Lambda).
"""
from __future__ import annotations

import base64
import os
import urllib.error
import urllib.request

from .base import (
    FetchResult,
    manifest_key_for,
    read_manifest,
    s3_key_for_target,
    s3_object_exists,
    upload_stream_to_s3,
    write_manifest,
)


def _auth_header() -> str:
    token = os.environ.get("DATA_BR_WEBDAV_TOKEN")
    if not token:
        raise RuntimeError(
            "DATA_BR_WEBDAV_TOKEN não setado — exporte o token público da "
            "Receita Federal (ver issue #13) antes de rodar o fetcher cnpj_*."
        )
    raw = f"{token}:".encode("ascii")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def fetch(target) -> FetchResult:
    s3_key = s3_key_for_target(target)
    manifest_key = manifest_key_for(target.source, s3_key)

    # Skip-if-fresh: manifest existe + objeto existe → assume válido.
    existing = read_manifest(manifest_key)
    if existing and existing.get("sha256") and s3_object_exists(s3_key):
        return FetchResult(
            ok=True, s3_key=s3_key,
            bytes_written=int(existing.get("bytes") or 0),
            sha256=str(existing.get("sha256")),
            url=target.url, skipped=True,
        )

    try:
        auth = _auth_header()
    except RuntimeError as e:
        return FetchResult(False, s3_key, 0, "", target.url, error=str(e))

    req = urllib.request.Request(
        target.url,
        headers={
            "User-Agent": "data-br-fetch/0.1",
            "Authorization": auth,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=1800) as resp:
            ctype = resp.headers.get("Content-Type")
            # `resp` é file-like com .read(n) — `upload_stream_to_s3`
            # consome em chunks e calcula sha256 incrementalmente.
            bytes_written, sha256 = upload_stream_to_s3(
                resp, s3_key, content_type=ctype,
            )
    except urllib.error.HTTPError as e:
        return FetchResult(False, s3_key, 0, "", target.url,
                           error=f"HTTP {e.code}")
    except Exception as e:  # noqa: BLE001
        return FetchResult(False, s3_key, 0, "", target.url, error=str(e))

    write_manifest(
        manifest_key,
        source=target.source,
        url=target.url,
        s3_key=s3_key,
        sha256=sha256,
        bytes_written=bytes_written,
        content_type=ctype,
        extra={"params": target.extra},
    )
    return FetchResult(True, s3_key, bytes_written, sha256, target.url)
