"""Tests for `lakebrasil.common.enrich._slug` — the normalization every
`resolve_ibge()` lookup depends on to match a source's free-text (uf,
município) pair against the `data_br.municipios` dimension's `slug`
column. A drift here means silent `ibge_code = None` across a pipeline.
"""
from __future__ import annotations

from lakebrasil.common import enrich


class TestSlug:
    def test_accented_name(self):
        assert enrich._slug("São Paulo") == "sao-paulo"

    def test_already_lowercase_no_accents(self):
        assert enrich._slug("santos") == "santos"

    def test_multiple_words_collapse_to_single_hyphens(self):
        assert enrich._slug("Rio de Janeiro") == "rio-de-janeiro"

    def test_apostrophe_and_punctuation_become_hyphen(self):
        assert enrich._slug("Sant'Ana do Livramento") == "sant-ana-do-livramento"

    def test_cedilla(self):
        assert enrich._slug("Conceição") == "conceicao"

    def test_leading_and_trailing_whitespace_stripped(self):
        assert enrich._slug("  Recife  ") == "recife"

    def test_repeated_separators_collapse(self):
        assert enrich._slug("Foz  do -- Iguaçu") == "foz-do-iguacu"

    def test_empty_string(self):
        assert enrich._slug("") == ""

    def test_none_like_falsy_input_returns_empty(self):
        # _slug guards on `if not text` before any processing.
        assert enrich._slug("") == ""

    def test_all_caps(self):
        assert enrich._slug("BRASÍLIA") == "brasilia"

    def test_leading_trailing_hyphens_from_punctuation_are_trimmed(self):
        assert enrich._slug("-São Paulo-") == "sao-paulo"


class TestResolveIbgeGuards:
    def test_missing_uf_returns_none_without_lookup(self):
        assert enrich.resolve_ibge(None, "São Paulo") is None

    def test_missing_municipio_returns_none_without_lookup(self):
        assert enrich.resolve_ibge("SP", None) is None

    def test_both_missing_returns_none(self):
        assert enrich.resolve_ibge(None, None) is None
