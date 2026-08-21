"""Fetches the `municipios-br` npm package's bundled SQLite database directly
from the npm registry — no S3 raw-layer staging, no AWS dependency. Used by
`municipios.py` and `ceps.py` as their source of truth (57-field município
reference + 1.27M CEPs), replacing a previously untracked, manually-staged
CSV export of the same package.

https://github.com/nataliasm23/municipios-br (MIT, npm package `municipios-br`)
"""
from __future__ import annotations

import io
import sqlite3
import tarfile
from pathlib import Path

import requests

_PACKAGE_VERSION = "3.2.1"
_TARBALL_URL = f"https://registry.npmjs.org/municipios-br/-/municipios-br-{_PACKAGE_VERSION}.tgz"
_TARBALL_MEMBER = "package/database/municipios-br.sqlite"

_CACHE_DIR = Path.home() / ".cache" / "lakebrasil"
_DB_PATH = _CACHE_DIR / f"municipios-br-{_PACKAGE_VERSION}.sqlite"


def _download_and_extract() -> Path:
    if _DB_PATH.exists():
        return _DB_PATH
    print(f"  downloading {_TARBALL_URL}")
    resp = requests.get(_TARBALL_URL, timeout=120)
    resp.raise_for_status()
    with tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:gz") as tar:
        member = tar.getmember(_TARBALL_MEMBER)
        extracted = tar.extractfile(member)
        if extracted is None:
            raise RuntimeError(f"{_TARBALL_MEMBER} is not a regular file in the tarball")
        data = extracted.read()
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = _DB_PATH.with_suffix(".sqlite.tmp")
    tmp_path.write_bytes(data)
    tmp_path.replace(_DB_PATH)
    print(f"  extracted {len(data):,} bytes -> {_DB_PATH}")
    return _DB_PATH


def connect() -> sqlite3.Connection:
    """Read-only connection to the (cached, downloaded-once) SQLite database."""
    db_path = _download_and_extract()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn
