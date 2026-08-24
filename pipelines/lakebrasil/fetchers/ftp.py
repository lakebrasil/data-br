"""Plain FTP fetcher → S3 streaming (ex: ftp2.fnde.gov.br/dadosabertos).

Alguns servidores FTP legados (FNDE incluso) mandam o welcome banner em
latin-1 em vez de utf-8 — `ftplib.FTP()` default (utf-8) quebra com
UnicodeDecodeError antes mesmo de logar. Força `encoding="latin-1"`.

Streaming via `transfercmd` (retorna o socket de dados bruto) +
`.makefile("rb")` — evita bufferizar o arquivo inteiro em memória antes
do upload, igual ao padrão dos outros fetchers (urllib response já é
file-like; aqui replicamos o mesmo shape pra `upload_stream_to_s3`).
"""
from __future__ import annotations

import ftplib
from urllib.parse import unquote, urlsplit

from .base import (
    FetchResult,
    manifest_key_for,
    s3_key_for_target,
    upload_stream_to_s3,
    write_manifest,
)


def fetch(target) -> FetchResult:
    s3_key = s3_key_for_target(target)
    parsed = urlsplit(target.url)
    host = parsed.hostname
    port = parsed.port or 21
    path = unquote(parsed.path)

    ftp: ftplib.FTP | None = None
    try:
        ftp = ftplib.FTP(encoding="latin-1", timeout=600)
        ftp.connect(host, port)
        ftp.login()  # anonymous — sem credenciais no path público dadosabertos
        ftp.voidcmd("TYPE I")
        conn = ftp.transfercmd(f"RETR {path}")
        try:
            stream = conn.makefile("rb")
            bytes_w, digest = upload_stream_to_s3(stream, s3_key)
        finally:
            conn.close()
        ftp.voidresp()
    except Exception as e:
        return FetchResult(False, s3_key, 0, "", target.url, error=str(e))
    finally:
        if ftp is not None:
            try:
                ftp.quit()
            except Exception:
                pass

    write_manifest(
        manifest_key_for(target.source, s3_key),
        source=target.source,
        url=target.url,
        s3_key=s3_key,
        sha256=digest,
        bytes_written=bytes_w,
        content_type=None,
        extra={"params": target.extra},
    )
    return FetchResult(True, s3_key, bytes_w, digest, target.url)
