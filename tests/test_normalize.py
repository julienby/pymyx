import json

import numpy as np
import pandas as pd
import pytest

from pymyx.treatments.normalize.run import PARAMS_FILE, run


def _make_parquet(base_dir, source, domain, day, data):
    domain_dir = base_dir / f"domain={domain}"
    domain_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{source}__{domain}__{day}.parquet"
    pd.DataFrame(data).to_parquet(domain_dir / filename, index=False)


@pytest.fixture
def sample_data(tmp_path):
    """Two devices, two days, known values for deterministic percentile checks."""
    inp = tmp_path / "input"
    for source in ["EXP_pil-90", "EXP_pil-98"]:
        _make_parquet(inp, source, "bio_signal", "2026-01-20", {
            "m0": list(range(0, 100)),    # 0..99
            "m1": list(range(100, 200)),  # 100..199
        })
        _make_parquet(inp, source, "bio_signal", "2026-01-21", {
            "m0": list(range(0, 100)),
            "m1": list(range(100, 200)),
        })
    return tmp_path


DEFAULT_PARAMS = {"domain": "bio_signal", "fit": False, "clip": True,
                  "method": "percentile", "percentile_min": 0.0, "percentile_max": 100.0,
                  "columns": []}


def _params(**overrides):
    return {**DEFAULT_PARAMS, **overrides}


class TestFit:
    def test_creates_params_file(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params(fit=True))
        assert (out / PARAMS_FILE).exists()

    def test_params_structure(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params(fit=True))
        data = json.loads((out / PARAMS_FILE).read_text())
        assert "_meta" in data
        assert "EXP_pil-90" in data
        assert "m0" in data["EXP_pil-90"]
        assert "p2" in data["EXP_pil-90"]["m0"]
        assert "p98" in data["EXP_pil-90"]["m0"]

    def test_meta_contains_method_and_counts(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params(fit=True))
        meta = json.loads((out / PARAMS_FILE).read_text())["_meta"]
        assert meta["method"] == "percentile"
        assert meta["n_devices"] == 2
        assert meta["n_files"] == 4

    def test_percentile_values_correct(self, sample_data):
        out = sample_data / "output"
        # P0/P100 = true min/max of range(0, 100) across 2 days = 0 and 99
        run(str(sample_data / "input"), str(out), _params(fit=True,
                                                           percentile_min=0.0,
                                                           percentile_max=100.0))
        params = json.loads((out / PARAMS_FILE).read_text())
        m0 = params["EXP_pil-90"]["m0"]
        assert m0["p2"] == pytest.approx(0.0, abs=1.0)
        assert m0["p98"] == pytest.approx(99.0, abs=1.0)


class TestApply:
    def _fit_and_apply(self, sample_data, apply_params=None):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params(fit=True))
        p = _params(**(apply_params or {}))
        run(str(sample_data / "input"), str(out), p)
        return out

    def test_produces_parquet_files(self, sample_data):
        out = self._fit_and_apply(sample_data)
        files = list((out / "domain=bio_signal").glob("*.parquet"))
        assert len(files) == 4

    def test_values_in_0_1_range(self, sample_data):
        out = self._fit_and_apply(sample_data)
        for f in (out / "domain=bio_signal").glob("*.parquet"):
            df = pd.read_parquet(f)
            assert df["m0"].between(0.0, 1.0).all(), f"m0 out of range in {f.name}"
            assert df["m1"].between(0.0, 1.0).all(), f"m1 out of range in {f.name}"

    def test_per_device_normalization(self, sample_data):
        """Both devices have same values -> same normalized output."""
        out = self._fit_and_apply(sample_data)
        files_90 = sorted((out / "domain=bio_signal").glob("*pil-90*"))
        files_98 = sorted((out / "domain=bio_signal").glob("*pil-98*"))
        for f90, f98 in zip(files_90, files_98):
            df90 = pd.read_parquet(f90)
            df98 = pd.read_parquet(f98)
            np.testing.assert_allclose(df90["m0"].values, df98["m0"].values)


class TestClip:
    def test_clip_true_keeps_values_in_range(self, sample_data):
        """With P2/P98 narrower than data range, clip=True keeps [0,1]."""
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out),
            _params(fit=True, percentile_min=10.0, percentile_max=90.0))
        run(str(sample_data / "input"), str(out), _params(clip=True))
        for f in (out / "domain=bio_signal").glob("*.parquet"):
            df = pd.read_parquet(f)
            assert df["m0"].between(0.0, 1.0).all()

    def test_clip_false_allows_values_outside_range(self, sample_data):
        """With P2/P98 narrower than data range, clip=False allows >1 or <0."""
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out),
            _params(fit=True, percentile_min=10.0, percentile_max=90.0))
        run(str(sample_data / "input"), str(out), _params(clip=False))
        all_vals = []
        for f in (out / "domain=bio_signal").glob("*.parquet"):
            df = pd.read_parquet(f)
            all_vals.extend(df["m0"].tolist())
        assert any(v > 1.0 or v < 0.0 for v in all_vals)


class TestFitWindowDays:
    def _make_multiday(self, tmp_path):
        """4 days: day1-2 with small range (closed), day3-4 with large range (open+close)."""
        inp = tmp_path / "input"
        # Day 1-2: mussel mostly closed, small range (0..10)
        for day in ["2026-01-20", "2026-01-21"]:
            _make_parquet(inp, "EXP_pil-90", "bio_signal", day, {
                "m0": list(range(0, 10))
            })
        # Day 3-4: full behavioral range (0..99)
        for day in ["2026-01-22", "2026-01-23"]:
            _make_parquet(inp, "EXP_pil-90", "bio_signal", day, {
                "m0": list(range(0, 100))
            })
        return tmp_path

    def test_window_restricts_fit_files(self, tmp_path):
        data = self._make_multiday(tmp_path)
        out = data / "output"
        # Window of 2 days -> only day3 and day4 used for fit
        run(str(data / "input"), str(out),
            {**DEFAULT_PARAMS, "fit": True, "fit_window_days": 2})
        meta = json.loads((out / PARAMS_FILE).read_text())["_meta"]
        assert meta["n_files"] == 2

    def test_window_zero_uses_all_files(self, tmp_path):
        data = self._make_multiday(tmp_path)
        out = data / "output"
        run(str(data / "input"), str(out),
            {**DEFAULT_PARAMS, "fit": True, "fit_window_days": 0})
        meta = json.loads((out / PARAMS_FILE).read_text())["_meta"]
        assert meta["n_files"] == 4

    def test_large_window_on_closed_days_biases_p98(self, tmp_path):
        """Fitting on closed days only yields a small P98 — the risk case."""
        data = self._make_multiday(tmp_path)
        out = data / "output"
        # Window of 4 days but first 2 are "closed" -> P98 biased by small-range days
        # Window of 2 days on last 2 days -> correct P98
        out_full = data / "out_full"
        out_window = data / "out_window"
        run(str(data / "input"), str(out_full),
            {**DEFAULT_PARAMS, "fit": True, "fit_window_days": 0})
        run(str(data / "input"), str(out_window),
            {**DEFAULT_PARAMS, "fit": True, "fit_window_days": 2})
        p98_full = json.loads((out_full / PARAMS_FILE).read_text())["EXP_pil-90"]["m0"]["p98"]
        p98_window = json.loads((out_window / PARAMS_FILE).read_text())["EXP_pil-90"]["m0"]["p98"]
        # Window (days 3-4 only) should have higher or equal P98 than full (diluted by closed days)
        assert p98_window >= p98_full

    def test_window_one_day_picks_last_day_only(self, tmp_path):
        data = self._make_multiday(tmp_path)
        out = data / "output"
        # Window of 1 day -> only 2026-01-23 (last day, large range)
        run(str(data / "input"), str(out),
            {**DEFAULT_PARAMS, "fit": True, "fit_window_days": 1})
        meta = json.loads((out / PARAMS_FILE).read_text())["_meta"]
        assert meta["n_files"] == 1


class TestMinRangeWarn:
    def test_warns_when_range_too_small(self, sample_data, capsys):
        out = sample_data / "output"
        # P10/P90 on range(0,100) -> range ~80, warn threshold 200 -> triggers
        run(str(sample_data / "input"), str(out),
            {**DEFAULT_PARAMS, "fit": True,
             "percentile_min": 10.0, "percentile_max": 90.0,
             "min_range_warn": 200})
        captured = capsys.readouterr()
        assert "WARNING" in captured.out
        assert "range" in captured.out

    def test_no_warn_when_range_sufficient(self, sample_data, capsys):
        out = sample_data / "output"
        # P0/P100 on range(0,100) -> range=99, warn threshold=50 -> no warning
        run(str(sample_data / "input"), str(out),
            {**DEFAULT_PARAMS, "fit": True,
             "percentile_min": 0.0, "percentile_max": 100.0,
             "min_range_warn": 50})
        captured = capsys.readouterr()
        assert "WARNING" not in captured.out

    def test_warn_disabled_by_default(self, sample_data, capsys):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out),
            {**DEFAULT_PARAMS, "fit": True,
             "percentile_min": 49.0, "percentile_max": 51.0})  # tiny range
        captured = capsys.readouterr()
        assert "WARNING" not in captured.out


class TestErrors:
    def test_missing_params_file_raises(self, sample_data):
        out = sample_data / "output"
        out.mkdir()
        with pytest.raises(FileNotFoundError, match="fit=true"):
            run(str(sample_data / "input"), str(out), _params(fit=False))

    def test_empty_input_raises(self, tmp_path):
        inp = tmp_path / "input"
        inp.mkdir()
        out = tmp_path / "output"
        with pytest.raises(ValueError, match="No parquet files"):
            run(str(inp), str(out), _params(fit=True))

    def test_unknown_device_raises(self, sample_data):
        """Params fitted without a device -> error when applying to that device."""
        out = sample_data / "output"
        # Fit only on data containing pil-90
        inp_partial = sample_data / "input_partial"
        (inp_partial / "domain=bio_signal").mkdir(parents=True)
        src = sample_data / "input" / "domain=bio_signal"
        for f in src.glob("*pil-90*"):
            (inp_partial / "domain=bio_signal" / f.name).symlink_to(f)
        run(str(inp_partial), str(out), _params(fit=True))

        # Apply to full input (contains pil-98 which has no params)
        with pytest.raises(KeyError, match="pil-98"):
            run(str(sample_data / "input"), str(out), _params(fit=False))
