"""
Tests for the `get_months` task.
`month_range` is mocked: these tests check WHICH range get_months asks for,
not how month_range builds it (test that separately in test_utils.py).
"""
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def get_months(controller):
    return controller.tasks["get_months"]


@pytest.fixture
def month_range(monkeypatch, controller):
    mock = MagicMock(name="month_range", return_value=["sentinel-range"])
    monkeypatch.setattr(controller.module, "month_range", mock)
    return mock


@pytest.mark.parametrize(
    "conf, watermark, expected_args",
    [
        # start_month == "" -> "continue from where we left off" (single month)
        pytest.param({"start_month": ""}, None, ("2021-01", "2021-01"), id="auto-no-watermark-uses-fallback"),
        pytest.param({"start_month": ""}, "2024-03", ("2024-03", "2024-03"), id="auto-uses-watermark"),
        pytest.param(
            {"start_month": "", "end_month": "2024-09"},
            "2024-03",
            ("2024-03", "2024-03"),
            id="auto-ignores-end_month",
        ),
        # explicit single month -> reprocess exactly that month, watermark ignored
        pytest.param({"start_month": "2024-05"}, None, ("2024-05", "2024-05"), id="single-month-no-end"),
        pytest.param({"start_month": "2024-05"}, "2024-02", ("2024-05", "2024-05"), id="single-month-ignores-watermark"),
        pytest.param(
            {"start_month": "2024-05", "end_month": "2024-05"},
            "2024-02",
            ("2024-05", "2024-05"),
            id="start-equals-end",
        ),
        # explicit range -> watermark (if any) replaces the start
        pytest.param(
            {"start_month": "2024-01", "end_month": "2024-04"}, None, ("2024-01", "2024-04"), id="range-no-watermark"
        ),
        pytest.param(
            {"start_month": "2024-01", "end_month": "2024-04"},
            "2024-03",
            ("2024-03", "2024-04"),
            id="range-resumes-from-watermark",
        ),
    ],
)
def test_month_range_called_with_expected_bounds(
    get_months, month_range, dag_mapping, make_dag_run, conf, watermark, expected_args
):
    result = get_months(dag_run=make_dag_run(market="jc", **conf), watermark=watermark)

    month_range.assert_called_once_with(*expected_args)
    assert result == ["sentinel-range"]  # returned as-is from month_range


def test_fallback_date_is_per_market(get_months, month_range, dag_mapping, make_dag_run):
    get_months(dag_run=make_dag_run(market="nyc", start_month=""), watermark=None)
    month_range.assert_called_once_with("2026-01", "2026-01")


def test_unknown_market_raises_keyerror_even_with_explicit_months(get_months, month_range, dag_mapping, make_dag_run):
    # fallback_date is looked up eagerly, before the branches
    with pytest.raises(KeyError):
        get_months(dag_run=make_dag_run(market="xx", start_month="2024-01"), watermark=None)
    month_range.assert_not_called()


def test_missing_start_month_raises_keyerror(get_months, month_range, dag_mapping, make_dag_run):
    with pytest.raises(KeyError):
        get_months(dag_run=make_dag_run(market="jc"), watermark=None)


def test_missing_market_raises_keyerror(get_months, month_range, dag_mapping, make_dag_run):
    with pytest.raises(KeyError):
        get_months(dag_run=make_dag_run(start_month=""), watermark=None)


# ---- characterisation of questionable behaviour (passes today; revisit if intended otherwise) ----
def test_watermark_overrides_start_even_when_earlier_than_requested(get_months, month_range, dag_mapping, make_dag_run):
    # user asked for 2024-03..2024-05 but the watermark says 2024-01 -> months before start get reprocessed
    get_months(
        dag_run=make_dag_run(market="jc", start_month="2024-03", end_month="2024-05"),
        watermark="2024-01",
    )
    month_range.assert_called_once_with("2024-01", "2024-05")


def test_watermark_past_end_month_is_passed_through(get_months, month_range, dag_mapping, make_dag_run):
    # nothing guards wm > end_month; the result then depends entirely on month_range
    get_months(
        dag_run=make_dag_run(market="jc", start_month="2024-01", end_month="2024-03"),
        watermark="2024-08",
    )
    month_range.assert_called_once_with("2024-08", "2024-03")