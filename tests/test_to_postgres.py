from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pymyx.treatments.to_postgres.run import (
    _copy_to_postgres,
    _ensure_columns,
    _ensure_table,
    _get_max_ts,
    _matches_structured_filter,
    _pg_type,
    _pivot_wide,
    _render_table_name,
    _resolve_allowed_columns,
    run,
)


# ---------------------------------------------------------------------------
# Helpers to create test parquet files
# ---------------------------------------------------------------------------

def _make_parquet(
    tmp_path: Path,
    domain: str,
    experience: str,
    device_id: str,
    step: str,
    day: str,
    data: dict,
    aggregation: str | None = None,
) -> Path:
    """Create a parquet file following the naming convention."""
    domain_dir = tmp_path / f"domain={domain}"
    domain_dir.mkdir(parents=True, exist_ok=True)

    if aggregation:
        name = f"{experience}__{device_id}__{step}__{aggregation}__{day}.parquet"
    else:
        name = f"{experience}__{device_id}__{step}__{day}.parquet"

    df = pd.DataFrame(data)
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True)

    path = domain_dir / name
    df.to_parquet(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Unit tests: _render_table_name
# ---------------------------------------------------------------------------

class TestRenderTableName:
    def test_default_template(self):
        result = _render_table_name(
            "{experience}_{step}_{aggregation}",
            {"experience": "PREMANIP-GRACE", "step": "aggregated", "aggregation": "60s"},
        )
        assert result == "PREMANIP_GRACE_AGGREGATED_60S"

    def test_custom_template(self):
        result = _render_table_name(
            "LIVE_{experience}_{step}_{aggregation}",
            {"experience": "PREMANIP-GRACE", "step": "aggregated", "aggregation": "10s"},
        )
        assert result == "LIVE_PREMANIP_GRACE_AGGREGATED_10S"

    def test_no_aggregation(self):
        result = _render_table_name(
            "{experience}_{step}_{aggregation}",
            {"experience": "PREMANIP-GRACE", "step": "resampled", "aggregation": None},
        )
        assert result == "PREMANIP_GRACE_RESAMPLED"

    def test_sanitization(self):
        result = _render_table_name(
            "{experience}_{step}",
            {"experience": "MY-EXP.v2", "step": "clean"},
        )
        assert result == "MY_EXP_V2_CLEAN"


# ---------------------------------------------------------------------------
# Unit tests: _pivot_wide
# ---------------------------------------------------------------------------

class TestPivotWide:
    def test_merge_two_devices_two_domains(self, tmp_path):
        """Merge 2 devices x 2 domains into a single wide DataFrame."""
        ts = ["2026-01-25T00:00:00Z", "2026-01-25T00:01:00Z"]

        f1 = _make_parquet(
            tmp_path, "bio_signal", "EXP", "pil-90", "aggregated", "2026-01-25",
            {"ts": ts, "m0__raw__mean": [1.0, 2.0]}, aggregation="60s",
        )
        f2 = _make_parquet(
            tmp_path, "bio_signal", "EXP", "pil-98", "aggregated", "2026-01-25",
            {"ts": ts, "m0__raw__mean": [3.0, 4.0]}, aggregation="60s",
        )
        f3 = _make_parquet(
            tmp_path, "environment", "EXP", "pil-90", "aggregated", "2026-01-25",
            {"ts": ts, "outdoor_temp__raw__mean": [18.0, 19.0]}, aggregation="60s",
        )
        f4 = _make_parquet(
            tmp_path, "environment", "EXP", "pil-98", "aggregated", "2026-01-25",
            {"ts": ts, "outdoor_temp__raw__mean": [20.0, 21.0]}, aggregation="60s",
        )

        sources = [{"domain": "bio_signal"}, {"domain": "environment"}]
        df = _pivot_wide([f1, f2, f3, f4], sources)

        assert len(df) == 2
        assert "pil_90__m0__raw__mean" in df.columns
        assert "pil_98__m0__raw__mean" in df.columns
        assert "pil_90__outdoor_temp__raw__mean" in df.columns
        assert "pil_98__outdoor_temp__raw__mean" in df.columns
        assert "ts" in df.columns

    def test_column_filter(self, tmp_path):
        """Only selected columns are included when 'columns' is specified."""
        ts = ["2026-01-25T00:00:00Z"]

        f1 = _make_parquet(
            tmp_path, "bio_signal", "EXP", "pil-90", "aggregated", "2026-01-25",
            {"ts": ts, "m0__raw__mean": [1.0], "m0__raw__std": [0.5], "m1__raw__mean": [2.0]},
            aggregation="60s",
        )

        sources = [{"domain": "bio_signal", "columns": ["m0__raw__mean"]}]
        df = _pivot_wide([f1], sources)

        assert "pil_90__m0__raw__mean" in df.columns
        assert "pil_90__m0__raw__std" not in df.columns
        assert "pil_90__m1__raw__mean" not in df.columns

    def test_unmatched_domain_ignored(self, tmp_path):
        """Files from domains not in sources are ignored."""
        ts = ["2026-01-25T00:00:00Z"]

        f1 = _make_parquet(
            tmp_path, "bio_signal", "EXP", "pil-90", "aggregated", "2026-01-25",
            {"ts": ts, "m0__raw__mean": [1.0]}, aggregation="60s",
        )

        sources = [{"domain": "environment"}]
        df = _pivot_wide([f1], sources)

        assert df.empty

    def test_empty_files(self, tmp_path):
        """Empty parquet files produce an empty result."""
        f1 = _make_parquet(
            tmp_path, "bio_signal", "EXP", "pil-90", "aggregated", "2026-01-25",
            {"ts": pd.Series([], dtype="datetime64[ns, UTC]"), "m0__raw__mean": pd.Series([], dtype="Float64")},
            aggregation="60s",
        )
        sources = [{"domain": "bio_signal"}]
        df = _pivot_wide([f1], sources)
        assert df.empty


# ---------------------------------------------------------------------------
# Unit tests: dtype mapping
# ---------------------------------------------------------------------------

class TestDtypeMapping:
    def test_timestamptz(self):
        assert _pg_type(pd.DatetimeTZDtype(tz="UTC")) == "TIMESTAMPTZ"

    def test_int64(self):
        assert _pg_type(pd.Int64Dtype()) == "BIGINT"

    def test_float64(self):
        assert _pg_type(pd.Float64Dtype()) == "DOUBLE PRECISION"

    def test_numpy_float(self):
        import numpy as np
        assert _pg_type(np.dtype("float64")) == "DOUBLE PRECISION"


# ---------------------------------------------------------------------------
# Unit tests: SQL generation (_ensure_table, _ensure_columns)
# ---------------------------------------------------------------------------

class TestSQLGeneration:
    def test_ensure_table_sql(self):
        """Verify the CREATE TABLE statement is correct."""
        df = pd.DataFrame({
            "ts": pd.to_datetime(["2026-01-25T00:00:00Z"], utc=True),
            "pil_90__m0__raw__mean": pd.array([1.0], dtype="Float64"),
            "pil_90__m0__raw__min": pd.array([1], dtype="Int64"),
        })

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        _ensure_table(mock_conn, "TEST_TABLE", df)

        sql = mock_cur.execute.call_args[0][0]
        assert 'CREATE TABLE IF NOT EXISTS "TEST_TABLE"' in sql
        assert "ts TIMESTAMPTZ PRIMARY KEY" in sql
        assert "pil_90__m0__raw__mean DOUBLE PRECISION" in sql
        assert "pil_90__m0__raw__min BIGINT" in sql

    def test_ensure_columns_adds_missing(self):
        """Verify ALTER TABLE ADD COLUMN for new columns."""
        df = pd.DataFrame({
            "ts": pd.to_datetime(["2026-01-25T00:00:00Z"], utc=True),
            "pil_90__m0__raw__mean": pd.array([1.0], dtype="Float64"),
            "pil_99__m0__raw__mean": pd.array([2.0], dtype="Float64"),
        })

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate existing columns: ts and pil_90__m0__raw__mean
        mock_cur.fetchall.return_value = [("ts",), ("pil_90__m0__raw__mean",)]

        added = _ensure_columns(mock_conn, "TEST_TABLE", df)

        assert "pil_99__m0__raw__mean" in added
        # Check ALTER TABLE was called
        alter_calls = [
            call for call in mock_cur.execute.call_args_list
            if "ALTER TABLE" in str(call)
        ]
        assert len(alter_calls) == 1
        assert "pil_99__m0__raw__mean" in str(alter_calls[0])


# ---------------------------------------------------------------------------
# Unit tests: _get_max_ts with mock
# ---------------------------------------------------------------------------

class TestGetMaxTs:
    def test_returns_ts(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cur.fetchone.return_value = (pd.Timestamp("2026-01-25T12:00:00Z"),)

        result = _get_max_ts(mock_conn, "TEST")
        assert result == pd.Timestamp("2026-01-25T12:00:00Z")

    def test_returns_none_for_empty(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cur.fetchone.return_value = (None,)

        result = _get_max_ts(mock_conn, "TEST")
        assert result is None


# ---------------------------------------------------------------------------
# Unit tests: _copy_to_postgres with mock
# ---------------------------------------------------------------------------

class TestCopyToPostgres:
    def test_copy_calls_copy_expert(self):
        df = pd.DataFrame({
            "ts": pd.to_datetime(["2026-01-25T00:00:00Z"], utc=True),
            "val": [1.0],
        })

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        rows = _copy_to_postgres(mock_conn, "TEST", df)
        assert rows == 1
        mock_cur.copy_expert.assert_called_once()
        sql_arg = mock_cur.copy_expert.call_args[0][0]
        assert 'COPY "TEST"' in sql_arg
        assert "FORMAT csv" in sql_arg

    def test_copy_empty_returns_zero(self):
        df = pd.DataFrame()
        mock_conn = MagicMock()
        assert _copy_to_postgres(mock_conn, "TEST", df) == 0


# ---------------------------------------------------------------------------
# Unit tests: aggregation filter
# ---------------------------------------------------------------------------

class TestAggregationFilter:
    @patch("pymyx.treatments.to_postgres.run.psycopg2")
    def test_aggregation_filter(self, mock_psycopg2, tmp_path):
        """Only the specified aggregation window is processed."""
        ts = ["2026-01-25T00:00:00Z"]
        _make_parquet(
            tmp_path / "input", "bio_signal", "EXP", "pil-90", "aggregated", "2026-01-25",
            {"ts": ts, "m0__raw__mean": [1.0]}, aggregation="60s",
        )
        _make_parquet(
            tmp_path / "input", "bio_signal", "EXP", "pil-90", "aggregated", "2026-01-25",
            {"ts": ts, "m0__raw__mean": [2.0]}, aggregation="5min",
        )

        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cur.fetchone.return_value = (None,)
        mock_cur.fetchall.return_value = []

        run(
            str(tmp_path / "input"),
            str(tmp_path / "output"),
            {
                "host": "localhost", "port": 5432, "dbname": "test",
                "user": "test", "password": "",
                "table_template": "{experience}_{step}_{aggregation}",
                "mode": "replace",
                "sources": [{"domain": "bio_signal"}],
                "aggregations": ["5min"],
            },
        )

        # Check that only the 5min table was created, not 60s
        create_calls = [
            str(call) for call in mock_cur.execute.call_args_list
            if "CREATE TABLE" in str(call)
        ]
        assert any("5MIN" in c for c in create_calls)
        assert not any("60S" in c for c in create_calls)


# ---------------------------------------------------------------------------
# Unit tests: device filter
# ---------------------------------------------------------------------------

class TestDeviceFilter:
    def test_device_filter(self, tmp_path):
        """Only the specified device is included in the pivot."""
        ts = ["2026-01-25T00:00:00Z"]
        f1 = _make_parquet(
            tmp_path, "bio_signal", "EXP", "pil-90", "aggregated", "2026-01-25",
            {"ts": ts, "m0__raw__mean": [1.0]}, aggregation="60s",
        )
        f2 = _make_parquet(
            tmp_path, "bio_signal", "EXP", "pil-98", "aggregated", "2026-01-25",
            {"ts": ts, "m0__raw__mean": [2.0]}, aggregation="60s",
        )

        sources = [{"domain": "bio_signal", "devices": ["pil-90"]}]
        df = _pivot_wide([f1, f2], sources)

        assert "pil_90__m0__raw__mean" in df.columns
        assert "pil_98__m0__raw__mean" not in df.columns


# ---------------------------------------------------------------------------
# Unit tests: structured column filter
# ---------------------------------------------------------------------------

class TestStructuredColumnFilter:
    def test_resolve_with_columns_takes_priority(self):
        """'columns' key takes priority over sensors/transforms/metrics."""
        source = {"domain": "bio_signal", "columns": ["m0__raw__mean"], "sensors": ["m0"], "metrics": ["mean"]}
        result = _resolve_allowed_columns(source)
        assert result == ["m0__raw__mean"]

    def test_resolve_no_filter(self):
        """No columns/sensors/transforms/metrics returns None."""
        source = {"domain": "bio_signal"}
        assert _resolve_allowed_columns(source) is None

    def test_structured_column_filter(self, tmp_path):
        """sensors/transforms/metrics generate the correct column filter."""
        ts = ["2026-01-25T00:00:00Z"]
        f1 = _make_parquet(
            tmp_path, "bio_signal", "EXP", "pil-90", "aggregated", "2026-01-25",
            {
                "ts": ts,
                "m0__sqrt_inv__mean": [1.0],
                "m0__sqrt_inv__std": [0.5],
                "m1__sqrt_inv__mean": [2.0],
                "m0__raw__mean": [3.0],
            },
            aggregation="60s",
        )

        sources = [{"domain": "bio_signal", "sensors": ["m0", "m1"], "transforms": ["sqrt_inv"], "metrics": ["mean"]}]
        df = _pivot_wide([f1], sources)

        assert "pil_90__m0__sqrt_inv__mean" in df.columns
        assert "pil_90__m1__sqrt_inv__mean" in df.columns
        assert "pil_90__m0__sqrt_inv__std" not in df.columns
        assert "pil_90__m0__raw__mean" not in df.columns

    def test_structured_filter_partial(self, tmp_path):
        """A single axis specified (metrics only) filters correctly."""
        ts = ["2026-01-25T00:00:00Z"]
        f1 = _make_parquet(
            tmp_path, "bio_signal", "EXP", "pil-90", "aggregated", "2026-01-25",
            {
                "ts": ts,
                "m0__raw__mean": [1.0],
                "m0__raw__std": [0.5],
                "m1__sqrt_inv__mean": [2.0],
            },
            aggregation="60s",
        )

        sources = [{"domain": "bio_signal", "metrics": ["mean"]}]
        df = _pivot_wide([f1], sources)

        assert "pil_90__m0__raw__mean" in df.columns
        assert "pil_90__m1__sqrt_inv__mean" in df.columns
        assert "pil_90__m0__raw__std" not in df.columns

    def test_matches_structured_filter_basic(self):
        """_matches_structured_filter works with full and partial patterns."""
        allowed = {("m0", "sqrt_inv", "mean")}
        assert _matches_structured_filter("m0__sqrt_inv__mean", allowed)
        assert not _matches_structured_filter("m0__sqrt_inv__std", allowed)

        # Wildcard sensor
        allowed_wild = {(None, "sqrt_inv", "mean")}
        assert _matches_structured_filter("m0__sqrt_inv__mean", allowed_wild)
        assert _matches_structured_filter("m5__sqrt_inv__mean", allowed_wild)
        assert not _matches_structured_filter("m0__raw__mean", allowed_wild)

    def test_matches_non_triple_columns_excluded(self):
        """Columns that don't have 3 parts are excluded by structured filter."""
        allowed = {("m0", None, "mean")}
        assert not _matches_structured_filter("m0__mean", allowed)
        assert not _matches_structured_filter("single_col", allowed)


# ---------------------------------------------------------------------------
# Unit tests: full run() with mocked psycopg2
# ---------------------------------------------------------------------------

class TestRunMocked:
    @patch("pymyx.treatments.to_postgres.run.psycopg2")
    def test_run_calls_connect(self, mock_psycopg2, tmp_path):
        """run() connects to postgres and processes files."""
        ts = ["2026-01-25T00:00:00Z", "2026-01-25T00:01:00Z"]
        _make_parquet(
            tmp_path / "input", "bio_signal", "EXP", "pil-90", "aggregated", "2026-01-25",
            {"ts": ts, "m0__raw__mean": [1.0, 2.0]}, aggregation="60s",
        )

        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        # _get_max_ts returns None (fresh table)
        mock_cur.fetchone.return_value = (None,)
        # _ensure_columns: no existing columns yet
        mock_cur.fetchall.return_value = []

        output_dir = tmp_path / "output"
        run(
            str(tmp_path / "input"),
            str(output_dir),
            {
                "host": "localhost",
                "port": 5432,
                "dbname": "test",
                "user": "test",
                "password": "",
                "table_template": "{experience}_{step}_{aggregation}",
                "mode": "append",
                "sources": [{"domain": "bio_signal"}],
            },
        )

        mock_psycopg2.connect.assert_called_once()
        mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# Integration tests (require a running PostgreSQL, skip otherwise)
# ---------------------------------------------------------------------------

def _pg_available():
    """Check if a local PostgreSQL is available for testing."""
    try:
        import psycopg2 as pg2
        conn = pg2.connect(host="localhost", port=5432, dbname="pymyx", user="pymyx", password="pymyx")
        conn.close()
        return True
    except Exception:
        return False


skip_no_pg = pytest.mark.skipif(not _pg_available(), reason="PostgreSQL not available")


@skip_no_pg
class TestIntegration:
    TABLE_NAME = "TEST_INTEGRATION_TO_POSTGRES"

    @pytest.fixture(autouse=True)
    def cleanup_table(self):
        """Drop the test table before and after each test."""
        import psycopg2 as pg2
        conn = pg2.connect(host="localhost", port=5432, dbname="pymyx", user="pymyx", password="pymyx")
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{self.TABLE_NAME}"')
        conn.commit()
        conn.close()
        yield
        conn = pg2.connect(host="localhost", port=5432, dbname="pymyx", user="pymyx", password="pymyx")
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{self.TABLE_NAME}"')
        conn.commit()
        conn.close()

    def _run_export(self, tmp_path, mode, ts_values, m0_values):
        _make_parquet(
            tmp_path / "input", "bio_signal", "TEST-INT", "dev-1", "aggregated", "2026-01-25",
            {"ts": ts_values, "m0__raw__mean": m0_values}, aggregation="60s",
        )
        run(
            str(tmp_path / "input"),
            str(tmp_path / "output"),
            {
                "host": "localhost", "port": 5432, "dbname": "pymyx",
                "user": "pymyx", "password": "pymyx",
                "table_template": self.TABLE_NAME,
                "mode": mode,
                "sources": [{"domain": "bio_signal"}],
            },
        )

    def _count_rows(self):
        import psycopg2 as pg2
        conn = pg2.connect(host="localhost", port=5432, dbname="pymyx", user="pymyx", password="pymyx")
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{self.TABLE_NAME}"')
            count = cur.fetchone()[0]
        conn.close()
        return count

    def _get_columns(self):
        import psycopg2 as pg2
        conn = pg2.connect(host="localhost", port=5432, dbname="pymyx", user="pymyx", password="pymyx")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                (self.TABLE_NAME.lower(),),
            )
            cols = {row[0] for row in cur.fetchall()}
        conn.close()
        return cols

    def test_replace_mode(self, tmp_path):
        """Replace mode: truncate and re-insert."""
        ts = ["2026-01-25T00:00:00Z", "2026-01-25T00:01:00Z"]
        self._run_export(tmp_path, "replace", ts, [1.0, 2.0])
        assert self._count_rows() == 2

        # Run again with replace → should still be 2 rows
        import shutil
        shutil.rmtree(tmp_path / "input")
        self._run_export(tmp_path, "replace", ts, [3.0, 4.0])
        assert self._count_rows() == 2

    def test_append_mode(self, tmp_path):
        """Append mode: only new rows are inserted."""
        ts1 = ["2026-01-25T00:00:00Z", "2026-01-25T00:01:00Z"]
        self._run_export(tmp_path, "append", ts1, [1.0, 2.0])
        assert self._count_rows() == 2

        # Run again with same data → 0 new rows
        import shutil
        shutil.rmtree(tmp_path / "input")
        self._run_export(tmp_path, "append", ts1, [1.0, 2.0])
        assert self._count_rows() == 2  # no duplicates

        # Run with newer data
        shutil.rmtree(tmp_path / "input")
        ts2 = ["2026-01-25T00:02:00Z", "2026-01-25T00:03:00Z"]
        self._run_export(tmp_path, "append", ts2, [5.0, 6.0])
        assert self._count_rows() == 4

    def test_auto_add_column(self, tmp_path):
        """A new device appearing triggers ALTER TABLE ADD COLUMN."""
        ts = ["2026-01-25T00:00:00Z"]
        self._run_export(tmp_path, "replace", ts, [1.0])
        cols_before = self._get_columns()
        assert "dev_1__m0__raw__mean" in cols_before

        # Add a second device
        import shutil
        shutil.rmtree(tmp_path / "input")
        _make_parquet(
            tmp_path / "input", "bio_signal", "TEST-INT", "dev-1", "aggregated", "2026-01-25",
            {"ts": ts, "m0__raw__mean": [1.0]}, aggregation="60s",
        )
        _make_parquet(
            tmp_path / "input", "bio_signal", "TEST-INT", "dev-2", "aggregated", "2026-01-25",
            {"ts": ts, "m0__raw__mean": [2.0]}, aggregation="60s",
        )
        run(
            str(tmp_path / "input"),
            str(tmp_path / "output"),
            {
                "host": "localhost", "port": 5432, "dbname": "pymyx",
                "user": "pymyx", "password": "pymyx",
                "table_template": self.TABLE_NAME,
                "mode": "replace",
                "sources": [{"domain": "bio_signal"}],
            },
        )
        cols_after = self._get_columns()
        assert "dev_2__m0__raw__mean" in cols_after
