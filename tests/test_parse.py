"""Tests for the parse treatment — dtype coercion behavior."""

import pandas as pd
import pytest

from pymyx.treatments.parse.run import parse_file, resolve_columns, run


@pytest.fixture
def make_csv(tmp_path):
    """Helper to create a raw CSV file with the expected naming convention."""

    def _make(filename, lines):
        csv_file = tmp_path / "input" / filename
        csv_file.parent.mkdir(parents=True, exist_ok=True)
        csv_file.write_text("\n".join(lines))
        return csv_file

    return _make


@pytest.fixture
def default_params():
    return {
        "delimiter": ";",
        "tz": "UTC",
        "timestamp_column": "ts",
        "domains": {
            "bio_signal": {"prefix": "m", "dtype": "int"},
            "environment": {"columns": ["outdoor_temp"], "dtype": "float"},
        },
        "file_name_substitute": [],
    }


class TestDtypeCoercion:
    """Verify that int columns reject floats and strings, keeping only valid ints."""

    def test_int_column_valid_values(self, make_csv, tmp_path, default_params):
        """Valid integer values are preserved as Int64."""
        make_csv(
            "EXP__dev01__2026-01-20.csv",
            [
                "2026-01-20T10:00:00Z;m0:10;m1:20",
                "2026-01-20T10:00:01Z;m0:15;m1:25",
            ],
        )
        output_dir = tmp_path / "output"
        run(str(tmp_path / "input"), str(output_dir), default_params)

        pf = list(output_dir.rglob("*.parquet"))
        bio = [f for f in pf if "bio_signal" in str(f)]
        assert len(bio) == 1
        df = pd.read_parquet(bio[0])
        assert df["m0"].tolist() == [10, 15]
        assert df["m1"].tolist() == [20, 25]
        assert df["m0"].dtype.name == "Int64"

    def test_int_column_rejects_float(self, make_csv, tmp_path, default_params):
        """Float values in an int column become pd.NA."""
        make_csv(
            "EXP__dev01__2026-01-20.csv",
            [
                "2026-01-20T10:00:00Z;m0:10;m1:3.7",
                "2026-01-20T10:00:01Z;m0:2.5;m1:20",
            ],
        )
        output_dir = tmp_path / "output"
        run(str(tmp_path / "input"), str(output_dir), default_params)

        bio = [f for f in output_dir.rglob("*.parquet") if "bio_signal" in str(f)]
        df = pd.read_parquet(bio[0])
        assert df["m0"].tolist() == [10, pd.NA]
        assert df["m1"].tolist() == [pd.NA, 20]

    def test_int_column_rejects_string(self, make_csv, tmp_path, default_params):
        """Non-numeric strings in an int column become pd.NA."""
        make_csv(
            "EXP__dev01__2026-01-20.csv",
            [
                "2026-01-20T10:00:00Z;m0:hello;m1:10",
                "2026-01-20T10:00:01Z;m0:15;m1:N/A",
            ],
        )
        output_dir = tmp_path / "output"
        run(str(tmp_path / "input"), str(output_dir), default_params)

        bio = [f for f in output_dir.rglob("*.parquet") if "bio_signal" in str(f)]
        df = pd.read_parquet(bio[0])
        assert df["m0"].tolist() == [pd.NA, 15]
        assert df["m1"].tolist() == [10, pd.NA]

    def test_float_column_accepts_float(self, make_csv, tmp_path, default_params):
        """Float values in a float column are preserved."""
        make_csv(
            "EXP__dev01__2026-01-20.csv",
            [
                "2026-01-20T10:00:00Z;outdoor_temp:18.94",
                "2026-01-20T10:00:01Z;outdoor_temp:19.5",
            ],
        )
        output_dir = tmp_path / "output"
        run(str(tmp_path / "input"), str(output_dir), default_params)

        env = [f for f in output_dir.rglob("*.parquet") if "environment" in str(f)]
        df = pd.read_parquet(env[0])
        assert df["outdoor_temp"].tolist() == [18.94, 19.5]
        assert df["outdoor_temp"].dtype.name == "Float64"

    def test_float_column_rejects_string(self, make_csv, tmp_path, default_params):
        """Non-numeric strings in a float column become pd.NA."""
        make_csv(
            "EXP__dev01__2026-01-20.csv",
            [
                "2026-01-20T10:00:00Z;outdoor_temp:18.94",
                "2026-01-20T10:00:01Z;outdoor_temp:broken",
            ],
        )
        output_dir = tmp_path / "output"
        run(str(tmp_path / "input"), str(output_dir), default_params)

        env = [f for f in output_dir.rglob("*.parquet") if "environment" in str(f)]
        df = pd.read_parquet(env[0])
        assert df["outdoor_temp"].tolist() == [18.94, pd.NA]


class TestResolveColumns:
    """Verify column resolution by prefix and explicit list."""

    def test_prefix_mode(self):
        cols = resolve_columns(["ts", "m0", "m1", "m10", "outdoor_temp"], {"prefix": "m"})
        assert cols == ["m0", "m1", "m10"]

    def test_explicit_columns_mode(self):
        cols = resolve_columns(["ts", "m0", "outdoor_temp"], {"columns": ["outdoor_temp"]})
        assert cols == ["outdoor_temp"]

    def test_explicit_columns_missing(self):
        cols = resolve_columns(["ts", "m0"], {"columns": ["outdoor_temp"]})
        assert cols == []

    def test_no_mode_returns_empty(self):
        cols = resolve_columns(["ts", "m0"], {"dtype": "int"})
        assert cols == []
