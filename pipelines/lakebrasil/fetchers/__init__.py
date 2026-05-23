"""Fetcher registry.

Each fetcher exposes `fetch(target: FetchTarget) -> FetchResult`. The CLI
driver picks the implementation by `target.fetcher` name.
"""
from __future__ import annotations

from typing import Callable, Dict

from . import bcb_sgs, govbr, http_fetcher, transparencia, webdav
from .base import FetchResult

REGISTRY: Dict[str, Callable] = {
    "http":          http_fetcher.fetch,
    "bcb_sgs":       bcb_sgs.fetch,
    "transparencia": transparencia.fetch,
    "govbr":         govbr.fetch,
    "webdav":        webdav.fetch,
}

__all__ = ["REGISTRY", "FetchResult"]
