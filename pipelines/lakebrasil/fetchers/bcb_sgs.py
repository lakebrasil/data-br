"""BCB SGS → S3 streaming.

JSON do endpoint público é pequeno (sub-MB) — single PUT direto.
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

ENDPOINT = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series}/dados?formato=json"
ENDPOINT_RANGE = ENDPOINT + "&dataInicial={data_inicial}"

# Séries grandes (diárias desde 1986+) BCB devolve 406 quando full
# history é >250k pontos. Limita a janela inicial = 2020-01-01.
LARGE_SERIES_FROM = "01/01/2020"
LARGE_SERIES_IDS = {1, 11, 12}  # cambio_dolar, selic_diaria, cdi_diario


def fetch(target) -> FetchResult:
    series = (target.extra or {}).get("series")
    if series is None:
        return FetchResult(False, "", 0, "", target.url, error="missing series")

    if int(series) in LARGE_SERIES_IDS:
        real_url = ENDPOINT_RANGE.format(series=series, data_inicial=LARGE_SERIES_FROM)
    else:
        real_url = ENDPOINT.format(series=series)
    s3_key = s3_key_for_target(target)
    try:
        # BCB SGS começou a bloquear UAs custom (HTTP 406 a partir de
        # mai/2026). UA tipo browser passa.
        req = urllib.request.Request(real_url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json,*/*",
        })
        with urllib.request.urlopen(req, timeout=300) as resp:
            ctype = resp.headers.get("Content-Type")
            bytes_w, digest = upload_stream_to_s3(resp, s3_key, content_type=ctype)
    except urllib.error.HTTPError as e:
        return FetchResult(False, s3_key, 0, "", real_url, error=f"HTTP {e.code}")
    except Exception as e:
        return FetchResult(False, s3_key, 0, "", real_url, error=str(e))

    write_manifest(
        manifest_key_for(target.source, s3_key),
        source=target.source,
        url=real_url,
        s3_key=s3_key,
        sha256=digest,
        bytes_written=bytes_w,
        content_type=ctype,
        extra={"series": series},
    )
    return FetchResult(True, s3_key, bytes_w, digest, real_url)
