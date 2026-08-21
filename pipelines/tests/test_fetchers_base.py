"""Tests for the pure S3-key-mangling helpers in `lakebrasil.fetchers.base`.
Every fetch/manifest write in the raw layer depends on these two functions
agreeing on layout — a drift here means raw files and their manifests land
in different, unmatched S3 keys.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from lakebrasil.fetchers import base


class TestS3KeyForTarget:
    def test_standard_layout(self):
        target = SimpleNamespace(
            out_path=Path("data-br-sources/bacen/raw/433_ipca_mensal.json")
        )
        assert base.s3_key_for_target(target) == "bacen/raw/433_ipca_mensal.json"

    def test_nested_source_dir_still_anchors_on_last_three_parts(self):
        target = SimpleNamespace(
            out_path=Path("/abs/path/data-br-sources/rais_estab/raw/2023.zip")
        )
        assert base.s3_key_for_target(target) == "rais_estab/raw/2023.zip"

    def test_missing_raw_segment_falls_back_to_filename_only(self):
        target = SimpleNamespace(out_path=Path("just/a/file.csv"))
        # parts[-2] != "raw" here ('a' != 'raw') -> fallback branch.
        assert base.s3_key_for_target(target) == "file.csv"

    def test_too_few_parts_falls_back_to_filename_only(self):
        target = SimpleNamespace(out_path=Path("file.csv"))
        assert base.s3_key_for_target(target) == "file.csv"


class TestManifestKeyFor:
    def test_standard_layout(self):
        key = base.manifest_key_for("bacen", "bacen/raw/433_ipca_mensal.json")
        assert key == "_manifest/bacen/433_ipca_mensal.manifest.json"

    def test_filename_without_extension(self):
        key = base.manifest_key_for("snis", "snis/raw/2023")
        assert key == "_manifest/snis/2023.manifest.json"

    def test_filename_with_multiple_dots_keeps_only_first_stem_segment(self):
        # rsplit(".", 1) on 'archive.tar.gz' -> stem 'archive.tar'.
        key = base.manifest_key_for("rais_estab", "rais_estab/raw/archive.tar.gz")
        assert key == "_manifest/rais_estab/archive.tar.manifest.json"
