"""DQ check primitives — declarative, composable, no side effects.

Each primitive returns a `CheckFn = Callable[(arrow_table, iceberg_table), dict]`.
The dict is the row written to `data_br_meta.dq_results`. Result schema:

    {check_name, column_name, severity, status, actual, expected, message}

`run_id`, `table_name`, `checked_at` são preenchidos pelo runner.

Convention: `severity='error'` falha duro (gate bloqueia push em modo
gated); `'warn'` loga mas passa; `'info'` puro registro.
"""
from __future__ import annotations

from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc

CheckResult = dict[str, Any]
CheckFn = Callable[[pa.Table, Any], CheckResult]  # 2nd arg: pyiceberg.Table


def _result(check_name: str, *, column_name: str | None = None,
            severity: str = "error", status: str,
            actual: float | None = None, expected: str = "",
            message: str = "") -> CheckResult:
    return {
        "check_name": check_name,
        "column_name": column_name,
        "severity": severity,
        "status": status,
        "actual": actual,
        "expected": expected,
        "message": message,
    }


# ── volumetria ─────────────────────────────────────────────────────────────

def row_count(min_rows: int = 1, max_rows: int | None = None,
              severity: str = "error") -> CheckFn:
    """Row count em [min, max]."""
    def check(arrow: pa.Table, _iceberg) -> CheckResult:
        n = arrow.num_rows
        ok = n >= min_rows and (max_rows is None or n <= max_rows)
        return _result(
            "row_count", severity=severity,
            status="pass" if ok else "fail",
            actual=float(n),
            expected=f">={min_rows:,}" + (f", <={max_rows:,}" if max_rows else ""),
            message=f"{n:,} rows",
        )
    return check


def freshness(max_age_days: int = 7, severity: str = "warn") -> CheckFn:
    """Último snapshot Iceberg < N dias."""
    def check(_arrow, iceberg) -> CheckResult:
        import datetime as dt
        snap = iceberg.current_snapshot()
        if snap is None:
            return _result("freshness", severity=severity, status="fail",
                           expected=f"<= {max_age_days}d old",
                           message="no snapshots")
        # snap.timestamp_ms is millis since epoch
        age_days = (dt.datetime.now(dt.timezone.utc).timestamp() * 1000
                    - snap.timestamp_ms) / 86_400_000
        ok = age_days <= max_age_days
        return _result("freshness", severity=severity,
                       status="pass" if ok else "fail",
                       actual=age_days,
                       expected=f"<= {max_age_days}d old",
                       message=f"last snapshot {age_days:.1f}d ago")
    return check


# ── nullability ───────────────────────────────────────────────────────────

def null_rate(column: str, max_pct: float = 0.0,
              severity: str = "error") -> CheckFn:
    """% de nulls em `column` <= max_pct."""
    def check(arrow: pa.Table, _iceberg) -> CheckResult:
        if column not in arrow.column_names:
            return _result("null_rate", column_name=column, severity=severity,
                           status="fail", expected=f"<= {max_pct}%",
                           message=f"column {column!r} not in table")
        n = arrow.num_rows
        nulls = arrow.column(column).null_count
        pct = (nulls / n * 100) if n else 0.0
        ok = pct <= max_pct
        return _result("null_rate", column_name=column, severity=severity,
                       status="pass" if ok else "fail",
                       actual=pct, expected=f"<= {max_pct}%",
                       message=f"{nulls:,}/{n:,} nulls ({pct:.2f}%)")
    return check


# ── range ──────────────────────────────────────────────────────────────────

def value_range(column: str, min_val=None, max_val=None,
                severity: str = "error") -> CheckFn:
    """min(col) >= min_val AND max(col) <= max_val (None = sem limite)."""
    def check(arrow: pa.Table, _iceberg) -> CheckResult:
        if column not in arrow.column_names:
            return _result("value_range", column_name=column, severity=severity,
                           status="fail", message=f"column {column!r} missing")
        col = arrow.column(column).drop_null()
        if col.length() == 0:
            return _result("value_range", column_name=column, severity=severity,
                           status="pass", message="no non-null values")
        actual_min = pc.min(col).as_py()
        actual_max = pc.max(col).as_py()
        bad: list[str] = []
        if min_val is not None and actual_min < min_val:
            bad.append(f"min {actual_min} < {min_val}")
        if max_val is not None and actual_max > max_val:
            bad.append(f"max {actual_max} > {max_val}")
        return _result("value_range", column_name=column, severity=severity,
                       status="pass" if not bad else "fail",
                       actual=float(actual_min) if isinstance(actual_min, (int, float)) else None,
                       expected=f"[{min_val}, {max_val}]",
                       message=f"min={actual_min}, max={actual_max}"
                               + ((" — " + "; ".join(bad)) if bad else ""))
    return check


# ── relacionais ────────────────────────────────────────────────────────────

def ibge_coverage(min_pct: float = 80.0, severity: str = "warn",
                  ibge_column: str = "ibge_code") -> CheckFn:
    """% dos 5571 municípios brasileiros que aparecem com ibge_code não-null."""
    TOTAL_MUNICIPIOS = 5571
    def check(arrow: pa.Table, _iceberg) -> CheckResult:
        if ibge_column not in arrow.column_names:
            return _result("ibge_coverage", column_name=ibge_column,
                           severity=severity, status="fail",
                           expected=f">= {min_pct}%",
                           message=f"column {ibge_column!r} missing")
        codes = arrow.column(ibge_column).drop_null()
        if codes.length() == 0:
            return _result("ibge_coverage", column_name=ibge_column,
                           severity=severity, status="fail",
                           actual=0.0, expected=f">= {min_pct}%",
                           message="all ibge_code values are null")
        unique = pc.count_distinct(codes).as_py()
        pct = unique / TOTAL_MUNICIPIOS * 100
        ok = pct >= min_pct
        return _result("ibge_coverage", column_name=ibge_column,
                       severity=severity,
                       status="pass" if ok else "fail",
                       actual=pct, expected=f">= {min_pct}%",
                       message=f"{unique:,}/{TOTAL_MUNICIPIOS} munis ({pct:.1f}%)")
    return check


def fk_to_municipios(column: str = "ibge_code",
                     severity: str = "warn") -> CheckFn:
    """Cada valor de `column` deve existir em `data_br.municipios.ibge_code`."""
    def check(arrow: pa.Table, _iceberg) -> CheckResult:
        from lakebrasil.loaders.iceberg import catalog
        if column not in arrow.column_names:
            return _result("fk_to_municipios", column_name=column,
                           severity=severity, status="fail",
                           message=f"column {column!r} missing")
        muni = catalog().load_table("data_br.municipios").scan(
            selected_fields=("ibge_code",),
        ).to_arrow()
        valid = set(muni.column("ibge_code").to_pylist())
        present = set(arrow.column(column).drop_null().to_pylist())
        orphans = present - valid
        n_orphan = len(orphans)
        ok = n_orphan == 0
        sample = list(orphans)[:3]
        return _result("fk_to_municipios", column_name=column,
                       severity=severity,
                       status="pass" if ok else "fail",
                       actual=float(n_orphan), expected="0 orphans",
                       message=f"{n_orphan:,} orphan ibge_codes"
                               + (f" (e.g. {sample})" if sample else ""))
    return check


def dedup(keys: list[str], severity: str = "error") -> CheckFn:
    """Combo `keys` deve ser único na tabela."""
    def check(arrow: pa.Table, _iceberg) -> CheckResult:
        import duckdb
        missing = [k for k in keys if k not in arrow.column_names]
        if missing:
            return _result("dedup", column_name=",".join(keys),
                           severity=severity, status="fail",
                           message=f"missing columns {missing}")
        con = duckdb.connect()
        con.register("t", arrow)
        ks = ", ".join(f'"{k}"' for k in keys)
        n_dup = con.execute(
            f"SELECT count(*) FROM (SELECT {ks}, count(*) c FROM t "
            f"GROUP BY {ks} HAVING c > 1)"
        ).fetchone()[0]
        ok = n_dup == 0
        return _result("dedup", column_name=",".join(keys), severity=severity,
                       status="pass" if ok else "fail",
                       actual=float(n_dup), expected="0 duplicate key combos",
                       message=f"{n_dup:,} duplicate combinations")
    return check


def distinct_count(column: str, min_distinct: int, severity: str = "warn") -> CheckFn:
    """Pelo menos N valores distintos em `column` (útil pra UF=27 etc)."""
    def check(arrow: pa.Table, _iceberg) -> CheckResult:
        if column not in arrow.column_names:
            return _result("distinct_count", column_name=column,
                           severity=severity, status="fail",
                           message=f"column {column!r} missing")
        n = pc.count_distinct(arrow.column(column).drop_null()).as_py()
        ok = n >= min_distinct
        return _result("distinct_count", column_name=column, severity=severity,
                       status="pass" if ok else "fail",
                       actual=float(n), expected=f">= {min_distinct}",
                       message=f"{n:,} distinct values")
    return check
