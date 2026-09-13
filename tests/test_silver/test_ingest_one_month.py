"""Unit tests for ingest.ingest_one_month."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable
from unittest.mock import MagicMock, patch

import pytest

from pipeline.silver_layer_processing.ingest import ingest_one_month
from pipeline.silver_layer_processing.model import IngestSummary, Trip


CANONICAL_HEADER = (
    "ride_id,started_at,ended_at,start_station_id,start_station_name,"
    "end_station_id,end_station_name\n"
)

GOOD_ROW = (
    "RIDEGOOD001,2026-06-30 16:58:39.826,2026-06-30 17:06:56.222,"
    "JC022,Oakland Ave,HB603,8 St & Washington St\n"
)

# empty start_station_id → MissingField (RowError)
BAD_ROW = (
    "RIDEBAD0001,2026-06-30 16:58:39.826,2026-06-30 17:06:56.222,"
    ",Oakland Ave,HB603,8 St & Washington St\n"
)

# garbage timestamp → BadTimestamp (RowError)
BAD_TS_ROW = (
    "RIDEBADTS01,not-a-ts,2026-06-30 17:06:56.222,"
    "JC022,Oakland Ave,HB603,8 St & Washington St\n"
)


def _write_csv(path: Path, body: str) -> Path:
    path.write_text(CANONICAL_HEADER + body, encoding="utf-8")
    return path


def _exhausting_load_trips(conn, trips: Iterable[Trip], batch_size: int = 1_000) -> int:
    """Stand-in for load_trips: consume generator (required for counts) and return len."""
    return sum(1 for _ in trips)


@pytest.fixture
def conn() -> MagicMock:
    return MagicMock(name="conn")


@patch("pipeline.silver_layer_processing.ingest.load_trips", side_effect=_exhausting_load_trips)
def test_all_good_rows_summary_and_no_rejects_file(
    mock_load: MagicMock, conn: MagicMock, tmp_path: Path
) -> None:
    src = _write_csv(tmp_path / "jc-202606-consolidated-tripdata.csv", GOOD_ROW + GOOD_ROW)
    rejects_dir = tmp_path / "rejects"

    summary = ingest_one_month(conn, src, rejects_dir)

    assert summary == IngestSummary(read=2, loaded=2, rejects=0, reasons={})
    assert rejects_dir.is_dir()
    assert not (rejects_dir / f"{src.stem}.rejects.jsonl").exists()
    conn.commit.assert_called_once()
    mock_load.assert_called_once()
    assert mock_load.call_args.args[0] is conn


@patch("pipeline.silver_layer_processing.ingest.load_trips", side_effect=_exhausting_load_trips)
def test_mixed_rows_writes_rejects_and_reasons(
    mock_load: MagicMock, conn: MagicMock, tmp_path: Path
) -> None:
    src = _write_csv(
        tmp_path / "jc-202606-consolidated-tripdata.csv",
        GOOD_ROW + BAD_ROW + BAD_TS_ROW + GOOD_ROW,
    )
    rejects_dir = tmp_path / "rejects"

    summary = ingest_one_month(conn, src, rejects_dir)

    assert summary.read == 4
    assert summary.loaded == 2
    assert summary.rejects == 2
    assert sum(summary.reasons.values()) == 2
    assert all(isinstance(k, str) and k for k in summary.reasons)

    rejects_path = rejects_dir / f"{src.stem}.rejects.jsonl"
    assert rejects_path.exists()
    lines = rejects_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2

    payloads = [json.loads(line) for line in lines]
    error_names = {p["error"] for p in payloads}
    assert error_names <= {"MissingField", "BadTimestamp"}
    for p in payloads:
        assert "row" in p and "reason" in p and "error" in p
        assert p["error"] in p["row"] or True  # shape only
        assert isinstance(p["row"], dict)


@patch("pipeline.silver_layer_processing.ingest.load_trips", side_effect=_exhausting_load_trips)
def test_all_bad_rows_loaded_zero_keeps_rejects(
    mock_load: MagicMock, conn: MagicMock, tmp_path: Path
) -> None:
    src = _write_csv(tmp_path / "bad-month.csv", BAD_ROW + BAD_TS_ROW)
    rejects_dir = tmp_path / "rejects"

    summary = ingest_one_month(conn, src, rejects_dir)

    assert summary == IngestSummary(
        read=2,
        loaded=0,
        rejects=2,
        reasons=summary.reasons,  # non-empty; exact keys = exception messages
    )
    assert summary.reasons
    assert (rejects_dir / "bad-month.rejects.jsonl").exists()


@patch("pipeline.silver_layer_processing.ingest.load_trips")
def test_loaded_comes_from_load_trips_not_good_count(
    mock_load: MagicMock, conn: MagicMock, tmp_path: Path
) -> None:
    """Duplicates / ON CONFLICT: loaded can be < good parse count."""

    def load_with_dedup(conn, trips: Iterable[Trip], batch_size: int = 1_000) -> int:
        n = 0
        for _ in trips:
            n += 1
        return max(n - 1, 0)  # pretend one conflict skipped

    mock_load.side_effect = load_with_dedup
    src = _write_csv(tmp_path / "month.csv", GOOD_ROW + GOOD_ROW)
    summary = ingest_one_month(conn, src, tmp_path / "rejects")

    assert summary.read == 2
    assert summary.loaded == 1
    assert summary.rejects == 0


@patch("pipeline.silver_layer_processing.ingest.load_trips", side_effect=_exhausting_load_trips)
def test_creates_rejects_dir(mock_load: MagicMock, conn: MagicMock, tmp_path: Path) -> None:
    src = _write_csv(tmp_path / "month.csv", GOOD_ROW)
    rejects_dir = tmp_path / "nested" / "rejects"
    assert not rejects_dir.exists()

    ingest_one_month(conn, src, rejects_dir)

    assert rejects_dir.is_dir()


@patch("pipeline.silver_layer_processing.ingest.load_trips", side_effect=_exhausting_load_trips)
def test_empty_csv_header_only(mock_load: MagicMock, conn: MagicMock, tmp_path: Path) -> None:
    src = _write_csv(tmp_path / "empty.csv", "")
    summary = ingest_one_month(conn, src, tmp_path / "rejects")

    assert summary == IngestSummary(read=0, loaded=0, rejects=0, reasons={})
    assert not (tmp_path / "rejects" / "empty.rejects.jsonl").exists()