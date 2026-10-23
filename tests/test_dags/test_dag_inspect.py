"""
Tests for pipeline.pipeline_utils.dag_inspect: get_coverage, get_stats_for_month
and get_runs_metrics.

get_coverage/get_runs_metrics only mock the database (psycopg connection/cursor);
pandas runs for real so the gap-detection and aggregation logic is genuinely
exercised. get_stats_for_month takes a DataFrame directly, so no DB mocking is
needed there.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pipeline.pipeline_utils.dag_inspect import get_coverage, get_runs_metrics, get_stats_for_month

# Mirrors the pipeline_control table (see alembic/versions/370bdc98024e_*.py).
PIPELINE_CONTROL_COLUMNS = [
    "pipeline_name", "market", "month", "layer", "completed_at", "tries", "status", "days", "failed_days",
]


def make_row(month, layer, status="success", tries=1, days=None, failed_days=None, market="jc"):
    return ("trips", market, month, layer, "2024-01-01 00:00:00", tries, status, days, failed_days)


@pytest.fixture
def fake_conn():
    """Mocks:  with conn.cursor() as cursor: ...  (`select *` from pipeline_control)."""
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.description = [SimpleNamespace(name=c) for c in PIPELINE_CONTROL_COLUMNS]
    cursor.fetchall.return_value = []
    return conn, cursor


def monthly_df(rows):
    columns = ["month", "layer", "status", "tries", "days", "failed_days"]
    return pd.DataFrame(rows, columns=columns)


# ---------------------------------------------------------------------------
# get_coverage
# ---------------------------------------------------------------------------
class TestGetCoverage:
    def test_contiguous_months_returns_full_coverage(self, fake_conn):
        conn, cursor = fake_conn
        cursor.fetchall.return_value = [
            make_row(m, "transform-to-gold") for m in ["2024-01", "2024-02", "2024-03"]
        ]

        result = get_coverage(conn, "jc")

        assert result == {
            "earliest": "2024-01",
            "watermark": "2024-03",
            "complete": 3,
            "gaps": [],
            "next": "2024-04",
        }

    def test_gap_is_reported_and_watermark_stops_before_it(self, fake_conn):
        conn, cursor = fake_conn
        cursor.fetchall.return_value = [
            make_row(m, "transform-to-gold") for m in ["2024-01", "2024-02", "2024-04"]
        ]

        result = get_coverage(conn, "jc")

        assert result["earliest"] == "2024-01"
        assert result["watermark"] == "2024-02"
        assert result["next"] == "2024-03"
        assert result["gaps"] == ["2024-03"]
        assert result["complete"] == 3

    def test_no_rows_returns_none(self, fake_conn, capsys):
        conn, _cursor = fake_conn

        result = get_coverage(conn, "jc")

        assert result is None
        assert "No data found for market: jc" in capsys.readouterr().out

    def test_query_failure_returns_none(self, fake_conn, capsys):
        conn, cursor = fake_conn
        cursor.execute.side_effect = Exception('relation "pipeline_control" does not exist')

        result = get_coverage(conn, "jc")

        assert result is None
        assert "First attempt to get watermark failed" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# get_stats_for_month
# ---------------------------------------------------------------------------
class TestGetStatsForMonth:
    def test_all_layers_success(self):
        rows = [
            ("2024-01", "ingest-to-bronze", "success", 1, None, None),
            ("2024-01", "transform-to-silver", "success", 1, None, None),
            ("2024-01", "transform-to-gold", "success", 1, 30, []),
        ]

        stats = get_stats_for_month(monthly_df(rows), "2024-01", "jc")

        assert stats["month"] == "2024-01"
        assert stats["state"] == "success"
        assert stats["tasks"]["ingest-to-bronze trips:jc 2024-01"] == {"state": "success", "tries": 1}
        assert stats["tasks"]["transform-to-silver:jc 2024-01"] == {"state": "success", "tries": 1}
        assert stats["tasks"]["transform-to-gold:jc 2024-01"] == {
            "state": "success",
            "tries": 1,
            "days": 30,
            "failed_days": [],
        }

    def test_failed_layer_is_reported_as_failed(self):
        rows = [
            ("2024-01", "ingest-to-bronze", "success", 1, None, None),
            ("2024-01", "transform-to-silver", "failed", 2, None, None),
            ("2024-01", "transform-to-gold", "success", 1, 30, []),
        ]

        stats = get_stats_for_month(monthly_df(rows), "2024-01", "jc")

        assert stats["state"] == "failed"
        assert stats["tasks"]["transform-to-silver:jc 2024-01"] == {"state": "failed", "tries": 2}

    # A layer can be absent for a given month (e.g. container startup failure or an
    # interrupted DAG) instead of simply recording a non-success status. This used to
    # raise IndexError on `.values[0]`, which get_runs_metrics then swallowed, silently
    # dropping the whole market's run metrics.
    @pytest.mark.parametrize("missing_layer", ["ingest-to-bronze", "transform-to-silver", "transform-to-gold"])
    def test_missing_layer_does_not_raise_and_is_reported_as_failed(self, missing_layer):
        present_rows = {
            "ingest-to-bronze": ("2024-01", "ingest-to-bronze", "success", 1, None, None),
            "transform-to-silver": ("2024-01", "transform-to-silver", "success", 1, None, None),
            "transform-to-gold": ("2024-01", "transform-to-gold", "success", 1, 30, []),
        }
        rows = [row for layer, row in present_rows.items() if layer != missing_layer]

        stats = get_stats_for_month(monthly_df(rows), "2024-01", "jc")  # must not raise

        task_key = {
            "ingest-to-bronze": "ingest-to-bronze trips:jc 2024-01",
            "transform-to-silver": "transform-to-silver:jc 2024-01",
            "transform-to-gold": "transform-to-gold:jc 2024-01",
        }[missing_layer]
        task = stats["tasks"][task_key]
        assert task["state"] == "failed"
        assert task["tries"] == 0
        if missing_layer == "transform-to-gold":
            assert task["days"] == 0
            assert task["failed_days"] == []


# ---------------------------------------------------------------------------
# get_runs_metrics
# ---------------------------------------------------------------------------
class TestGetRunsMetrics:
    def test_builds_one_entry_per_distinct_month(self, fake_conn):
        conn, cursor = fake_conn
        cursor.fetchall.return_value = [
            make_row("2024-02", "ingest-to-bronze"),
            make_row("2024-02", "transform-to-silver"),
            make_row("2024-02", "transform-to-gold", days=28, failed_days=[]),
            make_row("2024-01", "ingest-to-bronze"),
            make_row("2024-01", "transform-to-silver"),
            make_row("2024-01", "transform-to-gold", days=31, failed_days=[]),
        ]

        result = get_runs_metrics(conn, "jc")

        assert result["market"] == "jc"
        months = [run["month"] for run in result["runs"]]
        assert sorted(months) == ["2024-01", "2024-02"]
        assert all(run["state"] == "success" for run in result["runs"])

    def test_partial_month_does_not_crash_and_is_reported_as_failed(self, fake_conn):
        conn, cursor = fake_conn
        # transform-to-silver never ran for 2024-01 (e.g. interrupted DAG).
        cursor.fetchall.return_value = [
            make_row("2024-01", "ingest-to-bronze"),
            make_row("2024-01", "transform-to-gold", days=31, failed_days=[]),
        ]

        result = get_runs_metrics(conn, "jc")

        assert result is not None
        run = result["runs"][0]
        assert run["tasks"]["transform-to-silver:jc 2024-01"] == {"state": "failed", "tries": 0}

    def test_no_rows_returns_none(self, fake_conn):
        conn, _cursor = fake_conn

        result = get_runs_metrics(conn, "jc")

        assert result is None

    def test_query_failure_returns_none(self, fake_conn, capsys):
        conn, cursor = fake_conn
        cursor.execute.side_effect = Exception('relation "pipeline_control" does not exist')

        result = get_runs_metrics(conn, "jc")

        assert result is None
        assert "First attempt to get watermark failed" in capsys.readouterr().out
