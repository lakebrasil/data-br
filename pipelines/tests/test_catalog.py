"""Tests for `lakebrasil.scripts.catalog` — the generator/expansion logic
every one of the 35 pipelines depends on to know what URLs to fetch. A bug
here silently corrupts data collection across the whole lake, so this is
the highest-leverage module to keep covered.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pytest

from lakebrasil.scripts import catalog as cat


def _freeze_today(monkeypatch: pytest.MonkeyPatch, frozen: _dt.date) -> None:
    """`_parse_ym("today")` calls `_dt.date.today()` directly (not via the
    `_today_year`/`_today_ym` helpers, which are separate call sites) — so
    tests exercising the "today" monthly_range sentinel need to patch the
    `date` class itself, not just those two helpers.
    """

    class _FrozenDate(_dt.date):
        @classmethod
        def today(cls):
            return frozen

    monkeypatch.setattr(cat._dt, "date", _FrozenDate)


# ---------------------------------------------------------------------------
# _expand_param
# ---------------------------------------------------------------------------


class TestExpandParamYearlyRange:
    def test_explicit_start_and_end(self):
        assert cat._expand_param("ano", {"yearly_range": [2020, 2023]}) == [
            "2020", "2021", "2022", "2023",
        ]

    def test_single_year_range(self):
        assert cat._expand_param("ano", {"yearly_range": [2020, 2020]}) == ["2020"]

    def test_today_year_sentinel(self, monkeypatch):
        monkeypatch.setattr(cat, "_today_year", lambda: 2026)
        assert cat._expand_param("ano", {"yearly_range": [2024, "today_year"]}) == [
            "2024", "2025", "2026",
        ]


class TestExpandParamMonthlyRange:
    def test_explicit_start_and_end_same_year(self):
        assert cat._expand_param("ym", {"monthly_range": ["2026-01", "2026-04"]}) == [
            "2026-01", "2026-02", "2026-03", "2026-04",
        ]

    def test_crosses_year_boundary(self):
        assert cat._expand_param("ym", {"monthly_range": ["2025-11", "2026-02"]}) == [
            "2025-11", "2025-12", "2026-01", "2026-02",
        ]

    def test_single_month(self):
        assert cat._expand_param("ym", {"monthly_range": ["2026-06", "2026-06"]}) == ["2026-06"]

    def test_today_sentinel(self, monkeypatch):
        _freeze_today(monkeypatch, _dt.date(2026, 3, 15))
        result = cat._expand_param("ym", {"monthly_range": ["2026-01", "today"]})
        assert result == ["2026-01", "2026-02", "2026-03"]


class TestExpandParamList:
    def test_literal_list_stringified(self):
        assert cat._expand_param("file", {"list": ["Cnaes", "Motivos", 7]}) == [
            "Cnaes", "Motivos", "7",
        ]

    def test_empty_list(self):
        assert cat._expand_param("x", {"list": []}) == []


class TestExpandParamUfs:
    def test_returns_all_27(self):
        result = cat._expand_param("uf", {"ufs": True})
        assert len(result) == 27
        assert "SP" in result and "AC" in result and "DF" in result

    def test_ufs_false_is_not_expanded(self):
        # `ufs: false` (or absent) should not match the ufs branch — falls
        # through to the ValueError, since it isn't any recognised spec.
        with pytest.raises(ValueError):
            cat._expand_param("uf", {"ufs": False})


class TestExpandParamErrors:
    def test_unrecognised_spec_raises(self):
        with pytest.raises(ValueError, match="unrecognised param spec"):
            cat._expand_param("x", {"bogus_generator": [1, 2]})

    def test_non_dict_spec_raises(self):
        with pytest.raises(ValueError):
            cat._expand_param("x", "not-a-dict")


# ---------------------------------------------------------------------------
# _filename_from_url
# ---------------------------------------------------------------------------


class TestFilenameFromUrl:
    def test_explicit_out_filename_wins(self):
        assert cat._filename_from_url("https://x.gov.br/a/b/c.zip", "custom.zip") == "custom.zip"

    def test_derives_from_last_path_segment(self):
        assert cat._filename_from_url("https://x.gov.br/a/b/data.csv", None) == "data.csv"

    def test_strips_querystring(self):
        assert cat._filename_from_url("https://x.gov.br/download?id=123&fmt=csv", None) == "download"

    def test_falls_back_to_index_html_when_empty(self):
        assert cat._filename_from_url("https://x.gov.br/", None) == "index.html"


# ---------------------------------------------------------------------------
# expand_targets
# ---------------------------------------------------------------------------


def _spec(**overrides) -> cat.SourceSpec:
    base = dict(
        name="test_source",
        fetcher="http",
        schedule="monthly",
        license="CC0",
        out="test/raw/",
        tier=1,
    )
    base.update(overrides)
    return cat.SourceSpec(**base)


class TestExpandTargetsBcbSgs:
    def test_emits_one_synthetic_target(self):
        spec = _spec(fetcher="bcb_sgs", series=433, name="bacen_ipca_mensal")
        targets = list(cat.expand_targets(spec, Path("/raw")))
        assert len(targets) == 1
        t = targets[0]
        assert t.url == "sgs://433"
        assert t.extra == {"series": 433}
        assert t.out_path == Path("/raw/test/raw/433_ipca_mensal.json")

    def test_missing_series_raises(self):
        spec = _spec(fetcher="bcb_sgs", series=None)
        with pytest.raises(ValueError, match="missing `series`"):
            list(cat.expand_targets(spec, Path("/raw")))


class TestExpandTargetsStaticUrl:
    def test_single_target_no_params(self):
        spec = _spec(url="https://x.gov.br/fixed.csv")
        targets = list(cat.expand_targets(spec, Path("/raw")))
        assert len(targets) == 1
        assert targets[0].url == "https://x.gov.br/fixed.csv"
        assert targets[0].out_path == Path("/raw/test/raw/fixed.csv")


class TestExpandTargetsPattern:
    def test_cartesian_product_of_params(self):
        spec = _spec(
            pattern="https://x.gov.br/{ano}/{uf}.csv",
            params={"ano": {"list": [2024, 2025]}, "uf": {"list": ["SP", "RJ"]}},
        )
        targets = list(cat.expand_targets(spec, Path("/raw")))
        urls = sorted(t.url for t in targets)
        assert urls == [
            "https://x.gov.br/2024/RJ.csv",
            "https://x.gov.br/2024/SP.csv",
            "https://x.gov.br/2025/RJ.csv",
            "https://x.gov.br/2025/SP.csv",
        ]

    def test_extra_carries_the_resolved_params(self):
        spec = _spec(pattern="https://x.gov.br/{ano}.csv", params={"ano": {"list": [2024]}})
        [target] = list(cat.expand_targets(spec, Path("/raw")))
        assert target.extra == {"ano": "2024"}

    def test_fallback_pattern_is_resolved_with_same_params(self):
        spec = _spec(
            pattern="https://primary.gov.br/{ym}.csv",
            fallback_pattern="https://fallback.gov.br/{ym}.csv",
            params={"ym": {"list": ["2026-01"]}},
        )
        [target] = list(cat.expand_targets(spec, Path("/raw")))
        assert target.fallback_url == "https://fallback.gov.br/2026-01.csv"

    def test_no_fallback_pattern_means_none(self):
        spec = _spec(pattern="https://x.gov.br/{ano}.csv", params={"ano": {"list": [2024]}})
        [target] = list(cat.expand_targets(spec, Path("/raw")))
        assert target.fallback_url is None


class TestExpandTargetsErrors:
    def test_neither_url_nor_pattern_raises(self):
        spec = _spec(url=None, pattern=None)
        with pytest.raises(ValueError, match="has neither url nor pattern"):
            list(cat.expand_targets(spec, Path("/raw")))


# ---------------------------------------------------------------------------
# load_catalog — real catalog.yaml regression test
# ---------------------------------------------------------------------------


class TestLoadRealCatalog:
    def test_real_catalog_loads_without_error(self):
        sources = cat.load_catalog()
        assert len(sources) > 0

    def test_every_source_expands_without_raising(self, monkeypatch):
        # Freeze "today" so yearly_range/monthly_range sentinels are
        # deterministic and the test doesn't depend on the run date.
        monkeypatch.setattr(cat, "_today_year", lambda: 2026)
        monkeypatch.setattr(cat, "_today_ym", lambda: "2026-08")
        sources = cat.load_catalog()
        for name, spec in sources.items():
            targets = list(cat.expand_targets(spec, Path("/raw")))
            assert len(targets) > 0, f"source {name!r} expanded to zero targets"

    def test_every_source_has_a_known_fetcher(self):
        known = {"http", "govbr", "transparencia", "bcb_sgs", "webdav"}
        sources = cat.load_catalog()
        unknown = {s.fetcher for s in sources.values()} - known
        assert not unknown, f"catalog.yaml references undeclared fetcher(s): {unknown}"
