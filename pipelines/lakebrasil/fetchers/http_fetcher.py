"""Plain HTTP(S) fetcher → S3 streaming.

Sem disco intermediário: urllib stream → multipart upload pra S3.
Falla pra `fallback_url` se a primária 4xx-ar.
"""
from __future__ import annotations

import gzip
import ssl
import urllib.error
import urllib.request
from functools import lru_cache
from pathlib import Path

import certifi

from .base import (
    FetchResult,
    manifest_key_for,
    s3_key_for_target,
    upload_stream_to_s3,
    write_manifest,
)

# balanca.economia.gov.br (SERPRO) não envia a intermediate CA no TLS
# handshake — openssl s_client confirma "unable to verify the first
# certificate" mesmo pro cert raiz sendo público/confiável (Sectigo).
# dadosabertos.aneel.gov.br usa a MESMA CA e verifica ok (manda a chain
# completa), então é config de servidor, não CA desconhecida. Em vez de
# desabilitar verificação, carregamos a intermediate que falta junto do
# bundle padrão do certifi.
_EXTRA_CA_DIR = Path(__file__).parent / "certs"


@lru_cache(maxsize=1)
def _ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context(cafile=certifi.where())
    for pem in sorted(_EXTRA_CA_DIR.glob("*.pem")):
        ctx.load_verify_locations(str(pem))
    return ctx


def _stream_to_s3(url: str, s3_key: str) -> tuple[int, str, str | None]:
    req = urllib.request.Request(url, headers={"User-Agent": "data-br-fetch/0.1"})
    with urllib.request.urlopen(req, timeout=600, context=_ssl_context()) as resp:
        ctype = resp.headers.get("Content-Type")
        # Alguns servidores (ex: servicodados.ibge.gov.br) gzipam a
        # resposta mesmo sem Accept-Encoding explícito no request.
        # urllib não descomprime automaticamente — fazemos isso aqui
        # pra não gravar bytes gzip crus como se fossem o payload real.
        stream = resp
        if (resp.headers.get("Content-Encoding") or "").lower() == "gzip":
            stream = gzip.GzipFile(fileobj=resp)
        bytes_w, digest = upload_stream_to_s3(stream, s3_key, content_type=ctype)
    return bytes_w, digest, ctype


def fetch(target) -> FetchResult:
    s3_key = s3_key_for_target(target)
    try:
        bytes_w, digest, ctype = _stream_to_s3(target.url, s3_key)
    except urllib.error.HTTPError as e:
        if target.fallback_url and 400 <= e.code < 500:
            try:
                bytes_w, digest, ctype = _stream_to_s3(target.fallback_url, s3_key)
            except Exception as e2:
                return FetchResult(False, s3_key, 0, "", target.url, error=str(e2))
        else:
            return FetchResult(False, s3_key, 0, "", target.url, error=str(e))
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
