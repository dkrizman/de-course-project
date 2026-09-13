"""The one row this pipeline moves. Given — do not edit."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeAlias

# One line of a drop, straight from `json.loads`. Nothing has been checked yet — that is `parse_row`'s
# job, and the name is here to say so out loud.
RawRow: TypeAlias = dict[str, Any]


@dataclass(frozen=True, slots=True)
class Trip:
    ride_id: str
    start_station_id: str
    start_station_name: str
    end_station_id: str
    end_station_name: str
    started_at: datetime
    ended_at: datetime


@dataclass(frozen=True, slots=True)
class Report:
    """What one drop did. `read` counts lines; `loaded` counts rows the database did not already
    have; `rejected` counts rows that never reached it.
    {"layer":"silver","job":"trips:jc","window":"2026-06","rows":109510,
       "rejects":387,"reasons":{"never docked":387}}
    """
    layer: str
    job: str
    window: str
    rows: int
    rejects: int
    reasons: dict[str, int]

@dataclass(frozen=True, slots=True)
class IngestSummary:
    """What one drop did. `read` counts lines; `loaded` counts rows the database did not already
    have; `rejected` counts rows that never reached it.
    {"layer":"silver","job":"trips:jc","window":"2026-06","rows":109510,
       "rejects":387,"reasons":{"never docked":387}}
    """
    read: int
    loaded: int
    rejects: int
    reasons: dict[str, int]
