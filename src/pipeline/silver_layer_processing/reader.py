"""Tasks 2 and 3 — get rows out of a file, and turn them into trips."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator
import hashlib

from .model import RawRow, Trip
import json
from . import errors
from datetime import datetime


def read_monthly_trip(path: Path) -> Iterator[RawRow]:
    """Yield one raw CSV row per line of a monthly data."""
    with path.open(encoding="utf-8", newline="") as f:
        yield from csv.DictReader(f)


_LEGACY_FIELD_MAP = {
    "Start Time": "started_at",
    "Stop Time": "ended_at",
    "Start Station ID": "start_station_id",
    "Start Station Name": "start_station_name",
    "End Station ID": "end_station_id",
    "End Station Name": "end_station_name",
}

def _synthesize_ride_id(raw: RawRow) -> str:
    basis = f"{raw['Bike ID']}|{raw['Start Time']}|{raw['Stop Time']}"
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()

def normalize_row(raw: RawRow) -> RawRow:
    """Adapt either the current or legacy JC schema onto the canonical field names."""
    if "ride_id" in raw:
        return raw  # already canonical
    row = {new: raw[old] for old, new in _LEGACY_FIELD_MAP.items() if old in raw}
    row["ride_id"] = _synthesize_ride_id(raw)
    return row

def parse_row(raw: RawRow) -> Trip:
    """
    Normalize a raw row and turn it into a Trip, or raise an error if it is malformed.
    """
    row = normalize_row(raw)
    required_fields = ["ride_id", "start_station_id", "start_station_name", "end_station_id", "end_station_name", "started_at", "ended_at"]
    for field in required_fields:
        if field not in row:
            raise errors.MissingField(f"Required field missing: {field}")

    try:
        started_at = datetime.fromisoformat(row["started_at"])
    except Exception:
        raise errors.BadTimestamp("started_at is malformed")

    try:
        ended_at = datetime.fromisoformat(row["ended_at"])
    except Exception:
        raise errors.BadTimestamp("ended_at is malformed")

    if row['start_station_id'] == '' or row['start_station_id'] is None:
        raise errors.MissingField("start_station_id is missing or empty")

    if row['end_station_id'] == '' or row['end_station_id'] is None:
            raise errors.MissingField("end_station_id is missing or empty")

    try:
        return Trip(
            ride_id=row["ride_id"],
            start_station_id=row["start_station_id"],
            start_station_name=row["start_station_name"],
            end_station_id=row["end_station_id"],
            end_station_name=row["end_station_name"],
            started_at=started_at,
            ended_at=ended_at,
        )
    except Exception as e:
        raise errors.JobError(f"Failed to construct Trip: {e}") from e
