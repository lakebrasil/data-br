"""Enrichment helpers — municipios dim + (uf, nome) → ibge_code resolver.

Lê a dim `data_br.municipios` uma única vez por processo (cache via
`@lru_cache`) e expõe um lookup `resolve_ibge(uf, municipio_name) → int|None`.

Normalização: lowercase + sem acento + sem pontuação. Igual ao `slug`
que a tabela municipios já tem (`name` em São Paulo → slug `sao-paulo`).
A maioria das fontes de dados usa exatamente esse formato; quando não,
queda silenciosa para None — pipeline não falha, ibge_code fica null
e o registro continua válido com (uf, municipio) string.
"""
from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
from typing import Optional

from lakebrasil.loaders.iceberg import catalog


def _slug(text: str) -> str:
    """Replicar a normalização do `slug` em data_br.municipios.

    Exemplo: 'São Paulo' → 'sao-paulo'. Mantém só [a-z0-9-]."""
    if not text:
        return ""
    s = unicodedata.normalize("NFKD", text)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


@lru_cache(maxsize=1)
def _municipios_index() -> dict[tuple[str, str], int]:
    """{(uf, slug): ibge_code} — carregado uma vez do Iceberg."""
    table = catalog().load_table("data_br.municipios")
    arrow = table.scan(selected_fields=("ibge_code", "uf", "slug")).to_arrow()
    out: dict[tuple[str, str], int] = {}
    for ibge, uf, slug in zip(
        arrow.column("ibge_code").to_pylist(),
        arrow.column("uf").to_pylist(),
        arrow.column("slug").to_pylist(),
    ):
        if uf and slug:
            out[(uf.upper(), slug)] = int(ibge)
    return out


def resolve_ibge(uf: str | None, municipio: str | None) -> Optional[int]:
    """Best-effort lookup. Devolve None quando não bate (não falha)."""
    if not uf or not municipio:
        return None
    return _municipios_index().get((uf.upper(), _slug(municipio)))


def municipios_count() -> int:
    """Sanity check pra pipelines que querem reportar quantos resolveram."""
    return len(_municipios_index())


@lru_cache(maxsize=1)
def siafi_to_ibge_map() -> dict[str, int]:
    """SIAFI 4-dígitos → IBGE 7-dígitos. Lê o dim `municipios` que tem
    `codigo_siafi` carregado via seed do municipios-br.

    Usado por loaders que recebem o código SIAFI da Receita Federal /
    Portal da Transparência (BPC, Bolsa Família, CNPJ, etc) e precisam
    converter pra ibge_code (chave universal do datalake)."""
    table = catalog().load_table("data_br.municipios")
    arrow = table.scan(selected_fields=("ibge_code", "codigo_siafi")).to_arrow()
    out: dict[str, int] = {}
    for ibge, siafi in zip(
        arrow.column("ibge_code").to_pylist(),
        arrow.column("codigo_siafi").to_pylist(),
    ):
        if siafi:
            out[str(siafi).zfill(4)] = int(ibge)
    return out
