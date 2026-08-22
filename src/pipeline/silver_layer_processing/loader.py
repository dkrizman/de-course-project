"""Task 5 — put the trips in the database."""
from __future__ import annotations

from typing import Iterable

import psycopg

from .model import Trip

def _flush(conn: psycopg.Connection, batch: list[Trip]) -> int:
    total_inserted_rows = 0
    with conn.cursor() as cur:
        for trip in batch:
            cur.execute(
                "INSERT INTO trips_silver (ride_id, start_station_id, start_station_name, end_station_id, end_station_name, started_at, ended_at) " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s) " \
                "ON CONFLICT (ride_id) DO NOTHING",
                (trip.ride_id, trip.start_station_id, trip.start_station_name, trip.end_station_id, trip.end_station_name, trip.started_at, trip.ended_at)
            )
            total_inserted_rows += cur.rowcount
        conn.commit()
        return total_inserted_rows


def load_trips(conn: psycopg.Connection, trips: Iterable[Trip], batch_size: int = 1_000) -> int:
    """
    Load trips into the database in batches.
    Returns the total number of inserted rows.
    """
    inserted, batch = 0, []
    for trip in trips:
        batch.append(trip)
        if len(batch)>=batch_size:
            inserted += _flush(conn, batch)
            batch.clear()
    if batch:
        inserted += _flush(conn, batch)
    return inserted
