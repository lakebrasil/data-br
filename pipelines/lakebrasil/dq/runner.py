"""DQ runner — executa checks contra Iceberg, persiste em data_br_meta.dq_results.

API simples: `run(table_name)` carrega `_dq/{table_name}.py`, importa
sua lista CHECKS, roda cada uma, agrega resultados, escreve no Iceberg
e devolve summary.

Failure semantics:
  - Severity 'error' + status 'fail' → o `gate` retorna False (caller
    pode usar pra bloquear push).
  - 'warn' / 'info' não bloqueiam.
"""
from __future__ import annotations

import datetime as dt
import importlib
from typing import Any

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError

from lakebrasil.dq.checks import CheckFn
from lakebrasil.loaders.iceberg import META_NAMESPACE, NAMESPACE, catalog


def _load_checks(table_name: str) -> list[CheckFn]:
    """Importa `lakebrasil.dq.{table_name}.CHECKS`. Tabelas sem módulo têm CHECKS=[]."""
    try:
        mod = importlib.import_module(f"lakebrasil.dq.{table_name}")
    except ModuleNotFoundError:
        return []
    return list(getattr(mod, "CHECKS", []))


def _fetch_arrow(table_name: str):
    """(arrow_table, iceberg_table). Arrow é vazia se snapshot=None."""
    cat = catalog()
    iceberg = cat.load_table(f"{NAMESPACE}.{table_name}")
    snap = iceberg.current_snapshot()
    if snap is None:
        return pa.table({}), iceberg
    return iceberg.scan().to_arrow(), iceberg


def _persist(results: list[dict[str, Any]]) -> None:
    """Append `results` em `data_br_meta.dq_results`."""
    if not results:
        return
    target = catalog().load_table(f"{META_NAMESPACE}.dq_results")
    target_schema = target.schema().as_arrow()
    rows = [
        {
            "run_id": r["run_id"],
            "table_name": r["table_name"],
            "check_name": r["check_name"],
            "column_name": r.get("column_name"),
            "severity": r["severity"],
            "status": r["status"],
            "actual": r.get("actual"),
            "expected": r.get("expected"),
            "message": r.get("message"),
            "checked_at": r["checked_at"],
        }
        for r in results
    ]
    arrow = pa.Table.from_pylist(rows).cast(target_schema)
    target.append(arrow)


def run(table_name: str, *, persist: bool = True) -> dict[str, Any]:
    """Executa checks da tabela e devolve summary.

    Returns:
      {
        "table": str, "run_id": str, "results": [...],
        "n_pass": int, "n_warn": int, "n_fail_error": int, "gate_ok": bool,
      }
    """
    run_id = dt.datetime.now(dt.timezone.utc).isoformat()
    checks = _load_checks(table_name)
    if not checks:
        return {"table": table_name, "run_id": run_id, "results": [],
                "n_pass": 0, "n_warn": 0, "n_fail_error": 0,
                "gate_ok": True, "message": "no checks defined"}

    try:
        arrow, iceberg = _fetch_arrow(table_name)
    except NoSuchTableError:
        return {"table": table_name, "run_id": run_id, "results": [],
                "n_pass": 0, "n_warn": 0, "n_fail_error": 0,
                "gate_ok": False, "message": "table does not exist"}

    results: list[dict[str, Any]] = []
    now = dt.datetime.now(dt.timezone.utc)
    for check_fn in checks:
        try:
            res = check_fn(arrow, iceberg)
        except Exception as e:
            res = {
                "check_name": getattr(check_fn, "__name__", "unknown"),
                "column_name": None,
                "severity": "error", "status": "fail",
                "actual": None, "expected": "",
                "message": f"check raised: {type(e).__name__}: {e}",
            }
        res.update({
            "run_id": run_id,
            "table_name": table_name,
            "checked_at": now,
        })
        results.append(res)

    if persist:
        try:
            _persist(results)
        except Exception as e:
            # Não bloqueia o gate se a persistência falhou (raro);
            # ainda devolve resultados pra caller decidir.
            print(f"  ⚠ persist failed: {e}")

    n_pass = sum(1 for r in results if r["status"] == "pass")
    n_warn = sum(1 for r in results
                 if r["status"] == "fail" and r["severity"] == "warn")
    n_fail_error = sum(1 for r in results
                       if r["status"] == "fail" and r["severity"] == "error")
    return {
        "table": table_name, "run_id": run_id, "results": results,
        "n_pass": n_pass, "n_warn": n_warn, "n_fail_error": n_fail_error,
        "gate_ok": n_fail_error == 0,
    }


def gate(table_name: str) -> bool:
    """Convenience: True se a última run da tabela passou todos os 'error'.

    Pode ser chamada DENTRO da custom destination antes do append."""
    summary = run(table_name)
    return bool(summary["gate_ok"])
