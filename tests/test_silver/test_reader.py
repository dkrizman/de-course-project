"""Unit tests for reader.parse_row."""
from __future__ import annotations

import hashlib
from datetime import datetime

import pytest

from pipeline.silver_layer_processing import errors
from pipeline.silver_layer_processing.model import Trip
from pipeline.silver_layer_processing.reader import parse_row


def _sha1(*parts: str) -> str:
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


CANONICAL = {
    "ride_id": "07A20ED5330BC0E3",
    "started_at": "2026-06-30 16:58:39.826",
    "ended_at": "2026-06-30 17:06:56.222",
    "start_station_id": "JC022",
    "start_station_name": "Oakland Ave",
    "end_station_id": "HB603",
    "end_station_name": "8 St & Washington St",
}

JC_MID = {
    "bikeid": "24510",
    "starttime": "2016-10-01 00:00:38",
    "stoptime": "2016-10-01 00:03:41",
    "start station id": "3272",
    "start station name": "Jersey & 3rd",
    "end station id": "3203",
    "end station name": "Hamilton Park",
}

LEGACY = {
    "Bike ID": "24510",
    "Start Time": "2016-10-01 00:00:38",
    "Stop Time": "2016-10-01 00:03:41",
    "Start Station ID": "3272",
    "Start Station Name": "Jersey & 3rd",
    "End Station ID": "3203",
    "End Station Name": "Hamilton Park",
}


def test_parse_canonical_row() -> None:
    trip = parse_row(CANONICAL)
    assert isinstance(trip, Trip)
    assert trip == Trip(
        ride_id="07A20ED5330BC0E3",
        start_station_id="JC022",
        start_station_name="Oakland Ave",
        end_station_id="HB603",
        end_station_name="8 St & Washington St",
        started_at=datetime(2026, 6, 30, 16, 58, 39, 826000),
        ended_at=datetime(2026, 6, 30, 17, 6, 56, 222000),
    )


def test_parse_jc_mid_schema_synthesizes_ride_id() -> None:
    trip = parse_row(JC_MID)
    assert trip.ride_id == _sha1("24510", "2016-10-01 00:00:38", "2016-10-01 00:03:41")
    assert trip.start_station_id == "3272"
    assert trip.started_at == datetime(2016, 10, 1, 0, 0, 38)
    assert trip.ended_at == datetime(2016, 10, 1, 0, 3, 41)


def test_parse_legacy_schema_synthesizes_ride_id() -> None:
    trip = parse_row(LEGACY)
    assert trip.ride_id == _sha1("24510", "2016-10-01 00:00:38", "2016-10-01 00:03:41")
    assert trip.end_station_name == "Hamilton Park"


@pytest.mark.parametrize(
    "field",
    [
        "ride_id",
        "start_station_id",
        "start_station_name",
        "end_station_id",
        "end_station_name",
        "started_at",
        "ended_at",
    ],
)
def test_missing_required_field(field: str) -> None:
    raw = {k: v for k, v in CANONICAL.items() if k != field}
    with pytest.raises(errors.MissingField):
        parse_row(raw)


@pytest.mark.parametrize("field", ["start_station_id", "end_station_id"])
@pytest.mark.parametrize("bad", ["", None])
def test_empty_or_none_station_id(field: str, bad: str | None) -> None:
    raw = {**CANONICAL, field: bad}
    with pytest.raises(errors.MissingField):
        parse_row(raw)


@pytest.mark.parametrize(
    "field, value",
    [
        ("started_at", "not-a-timestamp"),
        ("started_at", ""),
        ("ended_at", "32/13/2020 99:99:99"),
        ("ended_at", ""),
    ],
)
def test_bad_timestamp(field: str, value: str) -> None:
    raw = {**CANONICAL, field: value}
    with pytest.raises(errors.BadTimestamp):
        parse_row(raw)


def test_cannot_synthesize_ride_id() -> None:
    """Legacy-ish keys incomplete → normalize still tries synthesize."""
    raw = {
        "Start Time": "2016-10-01 00:00:38",
        "Stop Time": "2016-10-01 00:03:41",
        # no Bike ID / bikeid
        "Start Station ID": "3272",
        "Start Station Name": "Jersey & 3rd",
        "End Station ID": "3203",
        "End Station Name": "Hamilton Park",
    }
    with pytest.raises(errors.MissingField):
        parse_row(raw)


def test_row_errors_are_skippable() -> None:
    """Ingest catches RowError; parse failures must stay under that."""
    with pytest.raises(errors.RowError):
        parse_row({"ride_id": "x"})  # missing everything else