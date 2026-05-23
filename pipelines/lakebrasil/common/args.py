"""argparse helpers — adiciona flags comuns aos pipelines.

Flags padronizadas:
  --no-fetch           pula download (raw em S3 já está)
  --refresh            força re-fetch mesmo com manifest sha256 batendo
  --dry-run            stage local em DuckDB, não escreve no Iceberg
  --table NAME         override da tabela alvo (POC, dlt_test)
  --full-refresh       reprocessa tudo (junto com DROP TABLE manual)
"""
from __future__ import annotations

import argparse


def add_common_args(parser: argparse.ArgumentParser, *,
                    table_default: str | None = None,
                    include_table: bool = True) -> argparse.ArgumentParser:
    """Adiciona o set padrão de flags. `parser` mutado in-place."""
    parser.add_argument("--no-fetch", action="store_true",
                        help="Pula download (usa raw em S3 como está).")
    parser.add_argument("--refresh", action="store_true",
                        help="Força re-download mesmo com manifest sha256 batendo.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Stage local em DuckDB, sem tocar no Iceberg.")
    if include_table and table_default:
        parser.add_argument("--table", default=table_default,
                            help="Tabela alvo (use *_dlt_test pra POC).")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Reprocessa TODO o histórico (drope a tabela antes).")
    return parser
