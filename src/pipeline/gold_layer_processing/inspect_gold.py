
from dataclasses import asdict, dataclass
import json

import psycopg


@dataclass
class DailyStationReport:
    market: str
    station: str
    day: str
    departures: int
    arrivals: int

_SELECT_DAILY_STATION_AGG = """
SELECT *
FROM trips_gold_daily_station
WHERE ride_day = %(window)s AND station = %(station)s
"""

def query_gold(conn: psycopg.Connection, window: str, station: str, region: str) -> DailyStationReport:
    """Read the aggregated per-station stats for a given day."""
    agg_per_station = conn.execute(_SELECT_DAILY_STATION_AGG, {"window": window, "station": station}).fetchone()
    if agg_per_station is None:
        return None
    day, station, arrivals, departures = agg_per_station
    return DailyStationReport(day=str(day), station=station, arrivals=arrivals, departures=departures, market=region)


def inspect_gold(conn: psycopg.Connection, window: str, station: str, region: str) -> None:
    stats = query_gold(conn, window, station, region)
    if stats is None:
        print("No data found")
    else:
        print(json.dumps(asdict(stats), indent=4))