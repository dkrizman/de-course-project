"""
Tests for the `get_watermark` task.
Only the database is mocked (psycopg.connect); pandas runs for real so the
gap-detection logic is genuinely exercised.
"""
import random
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
import pytest

# Assumed shape of pipeline_control; only `month` matters to the function.
COLUMNS = ["market", "layer", "status", "month", "updated_at"]


@pytest.fixture
def get_watermark(controller):
    return controller.tasks["get_watermark"]


def make_rows(months, market="jc"):
    return [(market, "transform-to-gold", "success", m, "2024-01-01 00:00:00") for m in months]


class FakeDb:
    def __init__(self, connect, cursor):
        self.connect = connect
        self.cursor = cursor

    def returns(self, months, market="jc", ordered_desc=True):
        """Rows the 'database' returns (SQL does `order by month desc`)."""
        months = sorted(months, reverse=True) if ordered_desc else list(months)
        self.cursor.fetchall.return_value = make_rows(months, market)


@pytest.fixture
def db():
    """Mocks:  with psycopg.connect(...) as conn: with conn.cursor() as cur: ..."""
    with patch("psycopg.connect") as mock_connect:
        conn = mock_connect.return_value.__enter__.return_value
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.description = [SimpleNamespace(name=c) for c in COLUMNS]
        cursor.fetchall.return_value = []
        yield FakeDb(mock_connect, cursor)


# ---- happy paths -------------------------------------------------------------
@pytest.mark.parametrize(
    "months, expected",
    [
        pytest.param(["2024-01"], "2024-02", id="single-month"),
        pytest.param(["2024-01", "2024-02", "2024-03"], "2024-04", id="continuous"),
        pytest.param(["2024-12"], "2025-01", id="single-december-rolls-year"),
        pytest.param(["2024-11", "2024-12"], "2025-01", id="contiguous-across-year"),
        pytest.param(
            [f"2023-{m:02d}" for m in range(1, 13)] + ["2024-01", "2024-02"],
            "2024-03",
            id="long-contiguous-multi-year",
        ),
    ],
)
def test_contiguous_history_returns_month_after_latest(db, get_watermark, make_dag_run, months, expected):
    db.returns(months)
    assert get_watermark(dag_run=make_dag_run(market="jc")) == expected


# ---- gaps: the watermark is the first missing month, not latest + 1 -----------
@pytest.mark.parametrize(
    "months, expected",
    [
        pytest.param(["2024-01", "2024-02", "2024-04"], "2024-03", id="gap-at-end"),
        pytest.param(["2024-01", "2024-03", "2024-04"], "2024-02", id="gap-after-first"),
        pytest.param(["2024-01", "2024-02", "2024-04", "2024-05"], "2024-03", id="gap-in-middle"),
        pytest.param(["2024-01", "2024-03", "2024-06"], "2024-02", id="multiple-gaps-first-wins"),
        pytest.param(["2023-11", "2024-01"], "2023-12", id="gap-across-year-boundary"),
        pytest.param(["2023-12", "2024-02"], "2024-01", id="gap-is-january"),
        pytest.param(["2024-01", "2025-01"], "2024-02", id="year-long-gap"),
    ],
)
def test_gap_returns_first_missing_month(db, get_watermark, make_dag_run, months, expected):
    db.returns(months)
    assert get_watermark(dag_run=make_dag_run(market="jc")) == expected


def test_result_is_independent_of_row_order(db, get_watermark, make_dag_run):
    shuffled = ["2024-01", "2024-02", "2024-04", "2024-05"]
    random.Random(42).shuffle(shuffled)
    db.returns(shuffled, ordered_desc=False)
    assert get_watermark(dag_run=make_dag_run(market="jc")) == "2024-03"


# ---- no data / error handling --------------------------------------------------
def test_no_successful_runs_returns_none(db, get_watermark, make_dag_run):
    db.returns([])
    assert get_watermark(dag_run=make_dag_run(market="jc")) is None


def test_table_missing_returns_none_and_logs(db, get_watermark, make_dag_run, capsys):
    db.cursor.execute.side_effect = psycopg.errors.UndefinedTable(
        'relation "pipeline_control" does not exist'
    )
    assert get_watermark(dag_run=make_dag_run(market="jc")) is None
    assert "First attempt to get watermark failed" in capsys.readouterr().out


def test_connection_failure_returns_none(db, get_watermark, make_dag_run):
    db.connect.side_effect = psycopg.OperationalError("could not connect to server")
    assert get_watermark(dag_run=make_dag_run(market="jc")) is None


def test_malformed_month_value_returns_none(db, get_watermark, make_dag_run):
    db.returns(["2024-01", "not-a-month"])
    assert get_watermark(dag_run=make_dag_run(market="jc")) is None


# ---- interaction with the DB / dag_run ------------------------------------------
@pytest.mark.parametrize("market", ["mkt_de", "mkt_fr", "mkt_us"])
def test_query_is_parameterised_with_market_layer_and_status(db, get_watermark, make_dag_run, market):
    db.returns(["2024-01"], market=market)
    get_watermark(dag_run=make_dag_run(market=market))

    db.cursor.execute.assert_called_once()
    sql, params = db.cursor.execute.call_args.args
    assert params == (market, "transform-to-gold", "success")
    assert market not in sql  # values are bound, not interpolated
    assert "%s" in sql


def test_connection_opened_once(db, get_watermark, make_dag_run):
    db.returns(["2024-01"])
    get_watermark(dag_run=make_dag_run(market="jc"))
    db.connect.assert_called_once()


def test_missing_market_in_conf_raises_keyerror(db, get_watermark, make_dag_run):
    # the market lookup is outside the try/except, so it is NOT swallowed
    with pytest.raises(KeyError):
        get_watermark(dag_run=make_dag_run())
    db.connect.assert_not_called()


def test_dag_run_none_raises(db, get_watermark):
    with pytest.raises(AttributeError):
        get_watermark(dag_run=None)

