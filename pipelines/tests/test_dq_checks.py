"""Tests for `lakebrasil.dq.checks` — the declarative DQ primitives that
gate every pipeline's Iceberg push. A wrong pass/fail here either lets
bad data through (severity='error' should have blocked) or blocks good
data (false positive) — both are high-cost failures, so every primitive
gets a pass and a fail case at minimum.
"""
from __future__ import annotations

from types import SimpleNamespace

import pyarrow as pa

from lakebrasil.dq import checks


def _iceberg_with_snapshot(timestamp_ms):
    return SimpleNamespace(
        current_snapshot=lambda: SimpleNamespace(timestamp_ms=timestamp_ms)
    )


def _iceberg_no_snapshot():
    return SimpleNamespace(current_snapshot=lambda: None)


class TestRowCount:
    def test_within_bounds_passes(self):
        arrow = pa.table({"x": [1, 2, 3]})
        result = checks.row_count(min_rows=1, max_rows=10)(arrow, None)
        assert result["status"] == "pass"

    def test_below_min_fails(self):
        arrow = pa.table({"x": []})
        result = checks.row_count(min_rows=1)(arrow, None)
        assert result["status"] == "fail"
        assert result["actual"] == 0.0

    def test_above_max_fails(self):
        arrow = pa.table({"x": [1, 2, 3]})
        result = checks.row_count(min_rows=1, max_rows=2)(arrow, None)
        assert result["status"] == "fail"

    def test_no_max_means_unbounded_above(self):
        arrow = pa.table({"x": list(range(1_000_000))})
        result = checks.row_count(min_rows=1)(arrow, None)
        assert result["status"] == "pass"

    def test_default_severity_is_error(self):
        result = checks.row_count()(pa.table({"x": [1]}), None)
        assert result["severity"] == "error"


class TestFreshness:
    def test_recent_snapshot_passes(self):
        import datetime as dt
        now_ms = dt.datetime.now(dt.UTC).timestamp() * 1000
        iceberg = _iceberg_with_snapshot(now_ms)
        result = checks.freshness(max_age_days=7)(None, iceberg)
        assert result["status"] == "pass"

    def test_stale_snapshot_fails(self):
        import datetime as dt
        old_ms = (dt.datetime.now(dt.UTC) - dt.timedelta(days=30)).timestamp() * 1000
        iceberg = _iceberg_with_snapshot(old_ms)
        result = checks.freshness(max_age_days=7)(None, iceberg)
        assert result["status"] == "fail"

    def test_no_snapshot_fails(self):
        result = checks.freshness()(None, _iceberg_no_snapshot())
        assert result["status"] == "fail"
        assert "no snapshots" in result["message"]

    def test_default_severity_is_warn(self):
        result = checks.freshness()(None, _iceberg_no_snapshot())
        assert result["severity"] == "warn"


class TestNullRate:
    def test_no_nulls_passes_strict_check(self):
        arrow = pa.table({"ibge_code": [1, 2, 3]})
        result = checks.null_rate("ibge_code", max_pct=0.0)(arrow, None)
        assert result["status"] == "pass"

    def test_nulls_over_threshold_fails(self):
        arrow = pa.table({"ibge_code": [1, None, None, None]})
        result = checks.null_rate("ibge_code", max_pct=10.0)(arrow, None)
        assert result["status"] == "fail"
        assert result["actual"] == 75.0

    def test_nulls_under_threshold_passes(self):
        arrow = pa.table({"x": [1, None] + [1] * 98})
        result = checks.null_rate("x", max_pct=5.0)(arrow, None)
        assert result["status"] == "pass"

    def test_missing_column_fails(self):
        arrow = pa.table({"other": [1]})
        result = checks.null_rate("ibge_code")(arrow, None)
        assert result["status"] == "fail"
        assert "not in table" in result["message"]

    def test_empty_table_does_not_divide_by_zero(self):
        arrow = pa.table({"x": pa.array([], type=pa.int64())})
        result = checks.null_rate("x", max_pct=0.0)(arrow, None)
        assert result["status"] == "pass"
        assert result["actual"] == 0.0


class TestValueRange:
    def test_within_range_passes(self):
        arrow = pa.table({"pct": [0.0, 50.0, 100.0]})
        result = checks.value_range("pct", min_val=0, max_val=100)(arrow, None)
        assert result["status"] == "pass"

    def test_below_min_fails(self):
        arrow = pa.table({"pct": [-5.0, 50.0]})
        result = checks.value_range("pct", min_val=0, max_val=100)(arrow, None)
        assert result["status"] == "fail"
        assert "min" in result["message"]

    def test_above_max_fails(self):
        arrow = pa.table({"pct": [50.0, 150.0]})
        result = checks.value_range("pct", min_val=0, max_val=100)(arrow, None)
        assert result["status"] == "fail"
        assert "max" in result["message"]

    def test_no_bounds_always_passes(self):
        arrow = pa.table({"x": [-999, 999]})
        result = checks.value_range("x")(arrow, None)
        assert result["status"] == "pass"

    def test_all_null_column_passes(self):
        arrow = pa.table({"x": pa.array([None, None], type=pa.float64())})
        result = checks.value_range("x", min_val=0, max_val=100)(arrow, None)
        assert result["status"] == "pass"
        assert "no non-null values" in result["message"]

    def test_missing_column_fails(self):
        result = checks.value_range("nope")(pa.table({"x": [1]}), None)
        assert result["status"] == "fail"


class TestIbgeCoverage:
    def test_full_coverage_passes(self):
        arrow = pa.table({"ibge_code": list(range(1000, 1000 + 5571))})
        result = checks.ibge_coverage(min_pct=99.0)(arrow, None)
        assert result["status"] == "pass"

    def test_low_coverage_fails(self):
        arrow = pa.table({"ibge_code": [1, 2, 3]})
        result = checks.ibge_coverage(min_pct=80.0)(arrow, None)
        assert result["status"] == "fail"

    def test_all_null_fails(self):
        arrow = pa.table({"ibge_code": pa.array([None, None], type=pa.int64())})
        result = checks.ibge_coverage()(arrow, None)
        assert result["status"] == "fail"
        assert result["actual"] == 0.0

    def test_missing_column_fails(self):
        result = checks.ibge_coverage()(pa.table({"x": [1]}), None)
        assert result["status"] == "fail"

    def test_default_severity_is_warn(self):
        result = checks.ibge_coverage()(pa.table({"ibge_code": [1]}), None)
        assert result["severity"] == "warn"


class TestDedup:
    def test_unique_keys_pass(self):
        arrow = pa.table({"ibge_code": [1, 2, 3], "periodo": ["2024", "2024", "2024"]})
        result = checks.dedup(["ibge_code", "periodo"])(arrow, None)
        assert result["status"] == "pass"
        assert result["actual"] == 0.0

    def test_duplicate_combo_fails(self):
        arrow = pa.table({
            "ibge_code": [1, 1, 2],
            "periodo": ["2024", "2024", "2024"],
        })
        result = checks.dedup(["ibge_code", "periodo"])(arrow, None)
        assert result["status"] == "fail"
        assert result["actual"] == 1.0

    def test_single_key_column(self):
        arrow = pa.table({"snapshot": ["2024-01", "2024-01"]})
        result = checks.dedup(["snapshot"])(arrow, None)
        assert result["status"] == "fail"

    def test_missing_key_column_fails(self):
        result = checks.dedup(["nope"])(pa.table({"x": [1]}), None)
        assert result["status"] == "fail"
        assert "missing columns" in result["message"]

    def test_default_severity_is_error(self):
        arrow = pa.table({"x": [1, 2]})
        result = checks.dedup(["x"])(arrow, None)
        assert result["severity"] == "error"


class TestDistinctCount:
    def test_meets_minimum_passes(self):
        arrow = pa.table({"uf": ["SP", "RJ", "MG"]})
        result = checks.distinct_count("uf", min_distinct=3)(arrow, None)
        assert result["status"] == "pass"

    def test_below_minimum_fails(self):
        arrow = pa.table({"uf": ["SP", "SP", "SP"]})
        result = checks.distinct_count("uf", min_distinct=27)(arrow, None)
        assert result["status"] == "fail"
        assert result["actual"] == 1.0

    def test_nulls_excluded_from_distinct_count(self):
        arrow = pa.table({"uf": ["SP", None, None]})
        result = checks.distinct_count("uf", min_distinct=1)(arrow, None)
        assert result["actual"] == 1.0

    def test_missing_column_fails(self):
        result = checks.distinct_count("nope", min_distinct=1)(pa.table({"x": [1]}), None)
        assert result["status"] == "fail"
