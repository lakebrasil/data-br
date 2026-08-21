"""Tests for `lakebrasil.common.incremental` — the 4 dedup/incremental
patterns every pipeline uses to avoid reprocessing already-loaded data.
A bug here means either silent data loss (over-matching -> skips rows
that were never actually loaded) or wasted reprocessing (under-matching).

Only the catalog/network boundary (`_scan_arrow`) is faked, with real
pyarrow tables — everything downstream (zip/set/duckdb aggregation) runs
for real, per this org's preference for real integration over mocks.
"""
from __future__ import annotations

import pyarrow as pa

from lakebrasil.common import incremental as inc


class TestScanArrowMissing:
    def test_table_not_found_returns_none(self, monkeypatch):
        def fake_load_table(name):
            from pyiceberg.exceptions import NoSuchTableError
            raise NoSuchTableError(name)

        monkeypatch.setattr(inc, "catalog", lambda: type(
            "C", (), {"load_table": staticmethod(fake_load_table)}
        )())
        assert inc._scan_arrow("nope", ("a",)) is None

    def test_table_with_no_snapshots_returns_none(self, monkeypatch):
        fake_table = type("T", (), {"metadata": type("M", (), {"snapshots": []})()})()
        monkeypatch.setattr(inc, "catalog", lambda: type(
            "C", (), {"load_table": staticmethod(lambda name: fake_table)}
        )())
        assert inc._scan_arrow("empty", ("a",)) is None


class TestLoadedSnapshots:
    def test_empty_table_yields_empty_set(self, monkeypatch):
        monkeypatch.setattr(inc, "_scan_arrow", lambda *a, **k: None)
        assert inc.loaded_snapshots("anp_postos") == set()

    def test_distinct_values(self, monkeypatch):
        arrow = pa.table({"snapshot": ["2024-01", "2024-01", "2024-02"]})
        monkeypatch.setattr(inc, "_scan_arrow", lambda *a, **k: arrow)
        assert inc.loaded_snapshots("anp_postos") == {"2024-01", "2024-02"}


class TestLoadedPairs:
    def test_empty_table_yields_empty_set(self, monkeypatch):
        monkeypatch.setattr(inc, "_scan_arrow", lambda *a, **k: None)
        assert inc.loaded_pairs("sancoes", "cadastro", "snapshot") == set()

    def test_zips_columns_into_tuples(self, monkeypatch):
        arrow = pa.table({
            "cadastro": ["CEIS", "CEIS", "CNEP"],
            "snapshot": ["2024-01", "2024-02", "2024-01"],
        })
        monkeypatch.setattr(inc, "_scan_arrow", lambda *a, **k: arrow)
        result = inc.loaded_pairs("sancoes", "cadastro", "snapshot")
        assert result == {
            ("CEIS", "2024-01"), ("CEIS", "2024-02"), ("CNEP", "2024-01"),
        }


class TestLoadedTriples:
    def test_empty_table_yields_empty_set(self, monkeypatch):
        monkeypatch.setattr(inc, "_scan_arrow", lambda *a, **k: None)
        assert inc.loaded_triples("indicadores_serie", "fonte", "indicador_id", "periodo") == set()

    def test_zips_three_columns(self, monkeypatch):
        arrow = pa.table({
            "fonte": ["pam", "pam", "ppm"],
            "indicador_id": ["1612", "1613", "3939"],
            "periodo": ["2023", "2023", "2023"],
        })
        monkeypatch.setattr(inc, "_scan_arrow", lambda *a, **k: arrow)
        result = inc.loaded_triples("indicadores_serie", "fonte", "indicador_id", "periodo")
        assert result == {
            ("pam", "1612", "2023"), ("pam", "1613", "2023"), ("ppm", "3939", "2023"),
        }

    def test_fonte_filter_is_pushed_down_as_row_filter(self, monkeypatch):
        captured = {}

        def fake_scan_arrow(table_name, columns, row_filter=None):
            captured["row_filter"] = row_filter
            return pa.table({"fonte": [], "indicador_id": [], "periodo": []})

        monkeypatch.setattr(inc, "_scan_arrow", fake_scan_arrow)
        inc.loaded_triples(
            "indicadores_serie", "fonte", "indicador_id", "periodo", fonte="pam",
        )
        assert captured["row_filter"] is not None

    def test_no_fonte_means_no_row_filter(self, monkeypatch):
        captured = {}

        def fake_scan_arrow(table_name, columns, row_filter=None):
            captured["row_filter"] = row_filter
            return pa.table({"fonte": [], "indicador_id": [], "periodo": []})

        monkeypatch.setattr(inc, "_scan_arrow", fake_scan_arrow)
        inc.loaded_triples("indicadores_serie", "fonte", "indicador_id", "periodo")
        assert captured["row_filter"] is None


class TestMaxValuePer:
    def test_empty_table_yields_empty_dict(self, monkeypatch):
        monkeypatch.setattr(inc, "_scan_arrow", lambda *a, **k: None)
        assert inc.max_value_per("macro_serie", "serie_id", "data") == {}

    def test_groups_and_takes_max_per_group(self, monkeypatch):
        arrow = pa.table({
            "serie_id": ["433", "433", "433", "11"],
            "data": ["2024-01-01", "2024-03-01", "2024-02-01", "2024-05-01"],
        })
        monkeypatch.setattr(inc, "_scan_arrow", lambda *a, **k: arrow)
        result = inc.max_value_per("macro_serie", "serie_id", "data")
        assert result == {"433": "2024-03-01", "11": "2024-05-01"}
