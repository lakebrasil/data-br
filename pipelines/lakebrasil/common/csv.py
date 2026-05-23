"""CSV helpers — Portal da Transparência + Receita Federal patterns.

A maioria dos CSVs gov.br vem em latin-1, separador `;`, quoted, com
header acentuado. Padroniza:
  - normalize_header(s) → ASCII lowercase strip pontuação
  - read_csv_records(stream, header_to_field) → iterator de dicts

`header_to_field` mapeia o header normalizado pra coluna canônica.
None vira coluna ignorada.
"""
from __future__ import annotations

import csv
import io
import re
import unicodedata
from typing import Iterator


def normalize_header(s: str) -> str:
    """ASCII-fy + lowercase + colapsa whitespace+slash em espaço único.

    Idempotente. Use em headers do Portal/RFB que vêm com acentos
    cabeçudos ('CÓDIGO DA SANÇÃO' → 'codigo da sancao')."""
    n = unicodedata.normalize("NFKD", s)
    n = "".join(c for c in n if not unicodedata.combining(c))
    n = n.lower().strip().replace("�", "")  # replacement char
    return re.sub(r"[\s/]+", " ", n)


def read_csv_records(
    stream,
    header_to_field: dict[str, str],
    *,
    encoding: str = "latin-1",
    delimiter: str = ";",
    quote: str = '"',
    extra_fields: dict | None = None,
) -> Iterator[dict]:
    """Lê CSV byte-stream `stream` → dicts mapeados.

    Cada linha: começa de `extra_fields` (dict opcional, ex:
    `{cadastro: 'CEIS', snapshot: '2026-04-30'}`) e adiciona cada
    coluna conforme `header_to_field`.

    Colunas com mapping=None ou ausentes do dict ficam de fora."""
    text = io.TextIOWrapper(stream, encoding=encoding, newline="")
    reader = csv.reader(text, delimiter=delimiter, quotechar=quote)
    raw_headers = next(reader)
    mapping = [header_to_field.get(normalize_header(h)) for h in raw_headers]

    for row in reader:
        rec: dict = dict(extra_fields or {})
        for col_idx, field in enumerate(mapping):
            if field is None or col_idx >= len(row):
                continue
            val = row[col_idx].strip()
            rec[field] = val if val else None
        yield rec


def parse_money_br(s: str | None) -> float | None:
    """Money BR: '36680,49' / '36.680,49' → 36680.49. Empty/'-' → None."""
    if not s or s.strip() in ("-", "", "null", "S/I", "Sem informação"):
        return None
    s = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def parse_int_or_none(s: str | None) -> int | None:
    """Int safe — Empty/non-int → None."""
    if not s or s.strip() in ("-", "", "null", "S/I"):
        return None
    try:
        return int(s.strip())
    except ValueError:
        return None
