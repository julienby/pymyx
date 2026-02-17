from pathlib import Path

import pytest

from pymyx.core.filename import (
    FileParts,
    build_parquet_path,
    list_parquet_files,
    parse_parquet_path,
    parse_raw_stem,
)


# --- parse_raw_stem ---

class TestParseRawStem:
    def test_basic(self):
        exp, dev, day = parse_raw_stem("PREMANIP-GRACE_pil-90_2026-01-25")
        assert exp == "PREMANIP-GRACE"
        assert dev == "pil-90"
        assert day == "2026-01-25"

    def test_with_substitutions(self):
        subs = [{"src": "PREMANIP_GRACE", "target": "PREMANIP-GRACE"}]
        exp, dev, day = parse_raw_stem("PREMANIP_GRACE_pil-90_2026-01-25", subs)
        assert exp == "PREMANIP-GRACE"
        assert dev == "pil-90"
        assert day == "2026-01-25"

    def test_case_insensitive_sub(self):
        subs = [{"src": "premanip_grace", "target": "PREMANIP-GRACE"}]
        exp, dev, day = parse_raw_stem("premanip_grace_pil-98_2026-01-20", subs)
        assert exp == "PREMANIP-GRACE"
        assert dev == "pil-98"
        assert day == "2026-01-20"

    def test_no_date_raises(self):
        with pytest.raises(ValueError, match="No date found"):
            parse_raw_stem("no_date_here")

    def test_no_experience(self):
        exp, dev, day = parse_raw_stem("pil-90_2026-01-25")
        assert exp == ""
        assert dev == "pil-90"
        assert day == "2026-01-25"


# --- parse_parquet_path ---

class TestParseParquetPath:
    def test_standard(self):
        p = Path("output/domain=bio_signal/PREMANIP-GRACE__pil-90__clean__2026-01-25.parquet")
        parts = parse_parquet_path(p)
        assert parts.experience == "PREMANIP-GRACE"
        assert parts.device_id == "pil-90"
        assert parts.step == "clean"
        assert parts.day == "2026-01-25"
        assert parts.domain == "bio_signal"
        assert parts.aggregation is None

    def test_aggregated(self):
        p = Path("out/domain=bio_signal/PREMANIP-GRACE__pil-90__aggregated__60s__2026-01-25.parquet")
        parts = parse_parquet_path(p)
        assert parts.step == "aggregated"
        assert parts.aggregation == "60s"
        assert parts.day == "2026-01-25"

    def test_no_domain_prefix(self):
        p = Path("flat_dir/PREMANIP-GRACE__pil-90__parsed__2026-01-25.parquet")
        parts = parse_parquet_path(p)
        assert parts.domain == ""

    def test_bad_parts_count_raises(self):
        p = Path("domain=x/bad__name.parquet")
        with pytest.raises(ValueError, match="Cannot parse"):
            parse_parquet_path(p)


# --- build_parquet_path ---

class TestBuildParquetPath:
    def test_standard(self, tmp_path):
        parts = FileParts("PREMANIP-GRACE", "pil-90", "clean", "2026-01-25", "bio_signal")
        result = build_parquet_path(parts, tmp_path)
        assert result == tmp_path / "domain=bio_signal" / "PREMANIP-GRACE__pil-90__clean__2026-01-25.parquet"
        assert result.parent.is_dir()

    def test_aggregated(self, tmp_path):
        parts = FileParts("PREMANIP-GRACE", "pil-90", "aggregated", "2026-01-25", "bio_signal", "60s")
        result = build_parquet_path(parts, tmp_path)
        assert result.name == "PREMANIP-GRACE__pil-90__aggregated__60s__2026-01-25.parquet"

    def test_roundtrip(self, tmp_path):
        original = FileParts("PREMANIP-GRACE", "pil-90", "resampled", "2026-01-25", "environment")
        path = build_parquet_path(original, tmp_path)
        parsed = parse_parquet_path(path)
        assert parsed.experience == original.experience
        assert parsed.device_id == original.device_id
        assert parsed.step == original.step
        assert parsed.day == original.day
        assert parsed.domain == original.domain

    def test_roundtrip_aggregated(self, tmp_path):
        original = FileParts("PREMANIP-GRACE", "pil-90", "aggregated", "2026-01-25", "bio_signal", "5min")
        path = build_parquet_path(original, tmp_path)
        parsed = parse_parquet_path(path)
        assert parsed.aggregation == "5min"
        assert parsed.day == "2026-01-25"


# --- list_parquet_files ---

class TestListParquetFiles:
    def test_finds_in_domain_dirs(self, tmp_path):
        d1 = tmp_path / "domain=bio_signal"
        d2 = tmp_path / "domain=environment"
        d1.mkdir()
        d2.mkdir()
        (d1 / "a__b__c__2026-01-25.parquet").touch()
        (d2 / "a__b__c__2026-01-25.parquet").touch()
        (tmp_path / "stray.parquet").touch()  # not in domain= dir

        result = list_parquet_files(tmp_path)
        assert len(result) == 2
        assert all("domain=" in str(r.parent.name) for r in result)

    def test_empty_dir(self, tmp_path):
        assert list_parquet_files(tmp_path) == []


# --- FileParts helpers ---

class TestFileParts:
    def test_with_step(self):
        parts = FileParts("EXP", "dev", "parsed", "2026-01-25", "bio", aggregation="10s")
        new = parts.with_step("clean")
        assert new.step == "clean"
        assert new.aggregation is None  # cleared
        assert new.experience == "EXP"  # preserved

    def test_with_aggregation(self):
        parts = FileParts("EXP", "dev", "parsed", "2026-01-25", "bio")
        new = parts.with_aggregation("aggregated", "60s")
        assert new.step == "aggregated"
        assert new.aggregation == "60s"
