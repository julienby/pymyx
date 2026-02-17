import pandas as pd
import pytest

from pymyx.treatments.exportnour.run import run


def _make_aggregated_parquet(base_dir, device, day, data):
    """Create a fake aggregated 10s parquet file in the expected directory layout."""
    domain_dir = base_dir / "domain=bio_signal"
    domain_dir.mkdir(parents=True, exist_ok=True)
    filename = f"TEST-EXP__{device}__aggregated__10s__{day}.parquet"
    df = pd.DataFrame(data)
    df.to_parquet(domain_dir / filename, index=False)


@pytest.fixture
def sample_data(tmp_path):
    """Create 2 days of fake aggregated data for 2 devices."""
    ts_day1 = pd.date_range("2026-01-20 09:00:00", periods=6, freq="10s", tz="UTC")
    ts_day2 = pd.date_range("2026-01-21 08:00:00", periods=4, freq="10s", tz="UTC")

    for device in ["pil-90", "pil-98"]:
        _make_aggregated_parquet(tmp_path / "input", device, "2026-01-20", {
            "ts": ts_day1,
            "m0__raw__mean": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
            "m1__raw__mean": [20.0, 21.0, 22.0, 23.0, 24.0, 25.0],
            "m2__raw__mean": [30.0, 31.0, 32.0, 33.0, 34.0, 35.0],
        })
        _make_aggregated_parquet(tmp_path / "input", device, "2026-01-21", {
            "ts": ts_day2,
            "m0__raw__mean": [100.0, 101.0, 102.0, 103.0],
            "m1__raw__mean": [200.0, 201.0, 202.0, 203.0],
            "m2__raw__mean": [300.0, 301.0, 302.0, 303.0],
        })
    return tmp_path


DEFAULT_PARAMS = {
    "aggregation": "10s",
    "domain": "bio_signal",
    "tz": "Europe/Paris",
    "from": "",
    "to": "",
    "columns": {"m0__raw__mean": "c0", "m1__raw__mean": "c1"},
}


def _params(**overrides):
    return {**DEFAULT_PARAMS, **overrides}


class TestOneFilePerDevice:
    def test_generates_one_csv_per_device(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params())

        csvs = sorted(out.glob("*.csv"))
        assert len(csvs) == 2
        assert "pil-90" in csvs[0].name
        assert "pil-98" in csvs[1].name

    def test_each_device_has_correct_row_count(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params())

        for csv in out.glob("*.csv"):
            df = pd.read_csv(csv, sep=";")
            assert len(df) == 10  # 6 + 4 rows per device


class TestBasicExport:
    def test_column_names(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params())

        df = pd.read_csv(sorted(out.glob("*.csv"))[0], sep=";")
        assert list(df.columns) == ["Time", "c0", "c1"]

    def test_column_order_matches_declaration(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params(
            columns={"m2__raw__mean": "z", "m0__raw__mean": "a"},
        ))

        df = pd.read_csv(sorted(out.glob("*.csv"))[0], sep=";")
        assert list(df.columns) == ["Time", "z", "a"]


class TestTimeFormat:
    def test_time_format_paris(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params())

        df = pd.read_csv(sorted(out.glob("*.csv"))[0], sep=";")
        # 2026-01-20 09:00:00 UTC = 2026-01-20 10:00:00 Paris (CET = UTC+1)
        assert df["Time"].iloc[0] == "2026-01-20 10:00:00"

    def test_time_format_pattern(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params())

        df = pd.read_csv(sorted(out.glob("*.csv"))[0], sep=";")
        for t in df["Time"]:
            pd.Timestamp(t)
            assert len(t) == 19  # "YYYY-MM-DD HH:MM:SS"


class TestFromTo:
    def test_from_filters_start(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params(**{"from": "2026-01-21"}))

        df = pd.read_csv(sorted(out.glob("*.csv"))[0], sep=";")
        assert len(df) == 4
        assert all(t.startswith("2026-01-21") for t in df["Time"])

    def test_to_filters_end(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params(to="2026-01-20"))

        df = pd.read_csv(sorted(out.glob("*.csv"))[0], sep=";")
        assert len(df) == 6

    def test_from_and_to(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params(**{"from": "2026-01-20"}, to="2026-01-20"))

        df = pd.read_csv(sorted(out.glob("*.csv"))[0], sep=";")
        assert len(df) == 6


class TestFilename:
    def test_filename_with_from_to(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params(**{"from": "2026-01-20"}, to="2026-01-21"))

        csvs = sorted(out.glob("*.csv"))
        # Dates come from actual data (Paris tz: UTC+1)
        assert csvs[0].name == "TEST-EXP_pil-90_aggregated_10s_2026-01-20_2026-01-21.csv"
        assert csvs[1].name == "TEST-EXP_pil-98_aggregated_10s_2026-01-20_2026-01-21.csv"

    def test_filename_without_from_to(self, sample_data):
        out = sample_data / "output"
        run(str(sample_data / "input"), str(out), _params())

        csvs = sorted(out.glob("*.csv"))
        # Dates come from actual data range (Paris tz: UTC+1)
        assert csvs[0].name == "TEST-EXP_pil-90_aggregated_10s_2026-01-20_2026-01-21.csv"


class TestErrors:
    def test_missing_column_raises(self, sample_data):
        with pytest.raises(ValueError, match="Columns not found"):
            run(str(sample_data / "input"), str(sample_data / "output"),
                _params(columns={"nonexistent__col": "x"}))

    def test_no_matching_files_raises(self, sample_data):
        with pytest.raises(FileNotFoundError):
            run(str(sample_data / "input"), str(sample_data / "output"),
                _params(aggregation="1h"))
