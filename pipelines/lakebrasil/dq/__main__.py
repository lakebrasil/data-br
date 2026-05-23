"""CLI: roda DQ contra uma ou todas as tabelas em data_br.

Usage:
    AWS_PROFILE=<seu-perfil> python -m _dq                 # todas
    AWS_PROFILE=<seu-perfil> python -m _dq macro_serie     # uma
    AWS_PROFILE=<seu-perfil> python -m _dq --no-persist    # dry-run
"""
from __future__ import annotations

import argparse
import sys

from lakebrasil.dq.runner import run

# Ordem alinhada com prioridade: dim primeiro, fact depois.
DEFAULT_TABLES = [
    "municipios",
    "ceps",
    "macro_serie",
    "comex_municipio",
    "frota_municipio",
    "eleicoes_municipais",
    "indicadores_serie",
    "sancoes",
    "anp_postos",
    "emendas_parlamentares",
    "beneficios_municipais",
    "orcamento_execucao_municipal",
    "empresas_municipio_cnae",
]


def _color(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m"


def _print_result(r: dict) -> None:
    icon, color = ("✓", "32") if r["status"] == "pass" \
        else ("⚠", "33") if r["severity"] != "error" \
        else ("✗", "31")
    col = f".{r['column_name']}" if r.get("column_name") else ""
    print(f"    {_color(icon, color)} {r['check_name']:18s}{col:25s} "
          f"{r.get('message', ''):60s} [{r['severity']}]")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("tables", nargs="*",
                   help="tabelas a checar (default: todas as conhecidas)")
    p.add_argument("--no-persist", action="store_true",
                   help="dry-run — não escreve em data_br_meta.dq_results")
    args = p.parse_args()

    tables = args.tables or DEFAULT_TABLES
    summaries = []
    for table in tables:
        print(f"\n=== {table} ===")
        s = run(table, persist=not args.no_persist)
        if s.get("message"):
            print(f"    (skipped: {s['message']})")
        for r in s["results"]:
            _print_result(r)
        summaries.append(s)

    print("\n" + "=" * 70)
    total_fail_err = sum(s["n_fail_error"] for s in summaries)
    total_warn = sum(s["n_warn"] for s in summaries)
    total_pass = sum(s["n_pass"] for s in summaries)
    print(f"  TOTAL: {total_pass} pass / {total_warn} warn / "
          f"{total_fail_err} ERROR-fail across {len(summaries)} tables")
    if total_fail_err:
        print(_color(f"  ✗ {total_fail_err} blocking failures — fix before push", "31"))
    return 1 if total_fail_err else 0


if __name__ == "__main__":
    sys.exit(main())
