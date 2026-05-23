"""Filename parsers — extrai metadados de nomes padronizados.

Patterns gov.br comuns:
  - YYYYMMDD_X.zip          (CEIS, CNEP)
  - YYYYMM_X.zip            (BPC, Bolsa Família, orçamento)
  - X_YYYY.zip              (TSE, comex)
  - K3241.K..D{YYMMDD}.X    (Receita Federal)
"""
from __future__ import annotations

import re

YYYYMMDD_RE = re.compile(r"^(\d{8})_")
YYYYMM_RE = re.compile(r"^(\d{6})_")
RFB_DATE_RE = re.compile(r"\.D(\d{6})\.")


def parse_yyyymmdd(name: str) -> str | None:
    """`20260430_CEIS.zip` → `2026-04-30`. None se não bater."""
    m = YYYYMMDD_RE.match(name)
    if not m:
        return None
    d = m.group(1)
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


def parse_yyyymm(name: str) -> str | None:
    """`202503_BPC.zip` → `2025-03`. None se não bater."""
    m = YYYYMM_RE.match(name)
    if not m:
        return None
    d = m.group(1)
    return f"{d[:4]}-{d[4:6]}"


def parse_rfb_date(name: str) -> str | None:
    """`K3241.K03200Y5.D60411.ESTABELE` → `2026-04-11`."""
    m = RFB_DATE_RE.search(name)
    if not m:
        return None
    d = m.group(1)
    return f"20{d[:2]}-{d[2:4]}-{d[4:6]}"
