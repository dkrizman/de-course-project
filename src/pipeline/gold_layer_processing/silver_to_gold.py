import psycopg

_UPSERT = """
WITH departures AS (
    SELECT
        start_station_id AS station,
        DATE(started_at) AS ride_day,
        COUNT(*) AS departures
    FROM trips_silver
    WHERE DATE(started_at) = %(window)s
    GROUP BY 1, 2
),

arrivals AS (
    SELECT
        end_station_id AS station,
        DATE(ended_at) AS ride_day,
        COUNT(*) AS arrivals
    FROM trips_silver
    WHERE DATE(ended_at) = %(window)s
    GROUP BY 1, 2
)

INSERT INTO trips_gold_daily_station (
    ride_day,
    station,
    arrivals,
    departures
)
SELECT
    COALESCE(a.ride_day, d.ride_day) AS ride_day,
    COALESCE(a.station, d.station) AS station,
    COALESCE(a.arrivals, 0) AS arrivals,
    COALESCE(d.departures, 0) AS departures
FROM arrivals a
FULL JOIN departures d
    ON a.station = d.station

ON CONFLICT (ride_day, station)
DO UPDATE SET
    arrivals = EXCLUDED.arrivals,
    departures = EXCLUDED.departures
"""


def transform_to_gold(conn: psycopg.Connection, window: str) -> None:
    """Transform silver trip data into daily station-level gold data."""
    conn.execute(_UPSERT, {"window": window})
    conn.commit()