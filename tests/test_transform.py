"""Tests for the transform treatment."""

import numpy as np
import pandas as pd
import pytest

from pymyx.treatments.transform.run import TRANSFORMS, _resolve_target, run


@pytest.fixture
def make_parquet(tmp_path):
    """Create a parquet file with the expected naming convention."""

    def _make(domain, data, source="EXP__dev01", day="2026-01-20", step="clean"):
        domain_dir = tmp_path / "input" / f"domain={domain}"
        domain_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(data)
        path = domain_dir / f"{source}__{step}__{day}.parquet"
        df.to_parquet(path, index=False)
        return path

    return _make


@pytest.fixture
def output_dir(tmp_path):
    return tmp_path / "output"


class TestSqrtInv:
    def test_positive_values(self):
        s = pd.array([4, 9, 16], dtype="Int64")
        result = TRANSFORMS["sqrt_inv"](pd.Series(s))
        expected = [1.0 / 2.0, 1.0 / 3.0, 1.0 / 4.0]
        np.testing.assert_allclose(result.to_numpy(dtype=float), expected)

    def test_zero_gives_nan(self):
        s = pd.array([0, 4], dtype="Int64")
        result = TRANSFORMS["sqrt_inv"](pd.Series(s))
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == pytest.approx(0.5)

    def test_na_gives_nan(self):
        s = pd.array([pd.NA, 4], dtype="Int64")
        result = TRANSFORMS["sqrt_inv"](pd.Series(s))
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == pytest.approx(0.5)


class TestLog:
    def test_positive_values(self):
        s = pd.array([1, np.e, np.e**2], dtype="Float64")
        result = TRANSFORMS["log"](pd.Series(s))
        np.testing.assert_allclose(result.to_numpy(dtype=float), [0.0, 1.0, 2.0])

    def test_zero_gives_nan(self):
        s = pd.array([0.0, 1.0], dtype="Float64")
        result = TRANSFORMS["log"](pd.Series(s))
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == pytest.approx(0.0)

    def test_na_gives_nan(self):
        s = pd.array([pd.NA, 1.0], dtype="Float64")
        result = TRANSFORMS["log"](pd.Series(s))
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == pytest.approx(0.0)


class TestModeAdd:
    def test_suffixed_columns_created(self, make_parquet, output_dir):
        make_parquet("bio_signal", {
            "ts": pd.to_datetime(["2026-01-20T10:00:00Z", "2026-01-20T10:00:01Z"]),
            "m0": pd.array([4, 9], dtype="Int64"),
            "m1": pd.array([16, 25], dtype="Int64"),
        })
        params = {"transforms": [
            {"function": "sqrt_inv", "target": {"domain": "bio_signal"}, "mode": "add"},
        ]}
        run(str(make_parquet.__wrapped__  if hasattr(make_parquet, '__wrapped__') else (output_dir.parent / "input")), str(output_dir), params)

        pf = list(output_dir.rglob("*.parquet"))
        assert len(pf) == 1
        df = pd.read_parquet(pf[0])
        assert "m0__sqrt_inv" in df.columns
        assert "m1__sqrt_inv" in df.columns
        assert df["m0__sqrt_inv"].iloc[0] == pytest.approx(0.5)

    def test_interleaved_order(self, make_parquet, output_dir):
        make_parquet("bio_signal", {
            "ts": pd.to_datetime(["2026-01-20T10:00:00Z"]),
            "m0": pd.array([4], dtype="Int64"),
            "m1": pd.array([9], dtype="Int64"),
        })
        params = {"transforms": [
            {"function": "sqrt_inv", "target": {"domain": "bio_signal"}, "mode": "add"},
        ]}
        run(str(output_dir.parent / "input"), str(output_dir), params)

        df = pd.read_parquet(list(output_dir.rglob("*.parquet"))[0])
        assert list(df.columns) == ["ts", "m0", "m0__sqrt_inv", "m1", "m1__sqrt_inv"]


class TestModeReplace:
    def test_columns_replaced_inplace(self, make_parquet, output_dir):
        make_parquet("bio_signal", {
            "ts": pd.to_datetime(["2026-01-20T10:00:00Z"]),
            "m0": pd.array([4], dtype="Int64"),
            "m1": pd.array([9], dtype="Int64"),
        })
        params = {"transforms": [
            {"function": "sqrt_inv", "target": {"domain": "bio_signal"}, "mode": "replace"},
        ]}
        run(str(output_dir.parent / "input"), str(output_dir), params)

        df = pd.read_parquet(list(output_dir.rglob("*.parquet"))[0])
        assert "m0__sqrt_inv" not in df.columns
        assert list(df.columns) == ["ts", "m0", "m1"]
        assert df["m0"].iloc[0] == pytest.approx(0.5)
        assert df["m1"].iloc[0] == pytest.approx(1.0 / 3.0)


class TestTargetDomain:
    def test_matching_domain(self):
        cols = pd.Index(["ts", "m0", "m1"])
        result = _resolve_target({"domain": "bio_signal"}, cols, "bio_signal")
        assert result == ["m0", "m1"]

    def test_non_matching_domain_passthrough(self, make_parquet, output_dir):
        make_parquet("environment", {
            "ts": pd.to_datetime(["2026-01-20T10:00:00Z"]),
            "outdoor_temp": pd.array([18.5], dtype="Float64"),
        })
        params = {"transforms": [
            {"function": "sqrt_inv", "target": {"domain": "bio_signal"}, "mode": "add"},
        ]}
        run(str(output_dir.parent / "input"), str(output_dir), params)

        df = pd.read_parquet(list(output_dir.rglob("*.parquet"))[0])
        assert list(df.columns) == ["ts", "outdoor_temp"]
        assert df["outdoor_temp"].iloc[0] == pytest.approx(18.5)


class TestTargetColumns:
    def test_explicit_columns(self):
        cols = pd.Index(["ts", "m0", "m1", "m2"])
        result = _resolve_target({"columns": ["m0", "m2"]}, cols, "bio_signal")
        assert result == ["m0", "m2"]

    def test_missing_columns_ignored(self):
        cols = pd.Index(["ts", "m0"])
        result = _resolve_target({"columns": ["m0", "m99"]}, cols, "bio_signal")
        assert result == ["m0"]


class TestUnknownFunction:
    def test_raises_value_error(self, make_parquet, output_dir):
        make_parquet("bio_signal", {
            "ts": pd.to_datetime(["2026-01-20T10:00:00Z"]),
            "m0": pd.array([4], dtype="Int64"),
        })
        params = {"transforms": [
            {"function": "nonexistent", "target": {"domain": "bio_signal"}, "mode": "add"},
        ]}
        with pytest.raises(ValueError, match="Unknown transform function 'nonexistent'"):
            run(str(output_dir.parent / "input"), str(output_dir), params)


class TestEmptyTransforms:
    def test_passthrough(self, make_parquet, output_dir):
        make_parquet("bio_signal", {
            "ts": pd.to_datetime(["2026-01-20T10:00:00Z"]),
            "m0": pd.array([4], dtype="Int64"),
        })
        params = {"transforms": []}
        run(str(output_dir.parent / "input"), str(output_dir), params)

        df = pd.read_parquet(list(output_dir.rglob("*.parquet"))[0])
        assert list(df.columns) == ["ts", "m0"]
        assert df["m0"].iloc[0] == 4


class TestMultipleTransforms:
    def test_two_transforms_on_same_column(self, make_parquet, output_dir):
        make_parquet("bio_signal", {
            "ts": pd.to_datetime(["2026-01-20T10:00:00Z"]),
            "m0": pd.array([4], dtype="Int64"),
        })
        params = {"transforms": [
            {"function": "sqrt_inv", "target": {"domain": "bio_signal"}, "mode": "add"},
            {"function": "log", "target": {"domain": "bio_signal"}, "mode": "add"},
        ]}
        run(str(output_dir.parent / "input"), str(output_dir), params)

        df = pd.read_parquet(list(output_dir.rglob("*.parquet"))[0])
        assert "m0__sqrt_inv" in df.columns
        assert "m0__log" in df.columns
        assert df["m0__sqrt_inv"].iloc[0] == pytest.approx(0.5)
        assert df["m0__log"].iloc[0] == pytest.approx(np.log(4))
        assert list(df.columns) == ["ts", "m0", "m0__sqrt_inv", "m0__log"]
