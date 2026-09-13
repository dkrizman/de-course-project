"""Task 3 — the exception hierarchy. This file IS the specification."""
from __future__ import annotations

import pytest

from pipeline.silver_layer_processing import errors


def test_every_named_failure_exists():
    for name in ("IngestError", "RowError", "MissingField", "BadTimestamp",
                 "JobError", "SlotUnavailable"):
        assert hasattr(errors, name), f"errors.{name} is missing"


def test_everything_is_an_ingest_error():
    for name in ("RowError", "JobError"):
        assert issubclass(getattr(errors, name), errors.IngestError)


@pytest.mark.parametrize("name", ["MissingField", "BadTimestamp"])
def test_the_three_bad_rows_are_row_errors(name: str):
    assert issubclass(getattr(errors, name), errors.RowError)


def test_a_bad_row_is_not_a_bad_job():
    """The load loop catches `RowError` and keeps going. If `SlotUnavailable` were caught by that
    same `except`, a job that could not start would look like a job with one bad line in it."""
    assert not issubclass(errors.SlotUnavailable, errors.RowError)
    assert issubclass(errors.SlotUnavailable, errors.JobError)
    assert not issubclass(errors.RowError, errors.JobError)
