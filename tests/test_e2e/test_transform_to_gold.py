import os
import json
import datetime
import pytest
import psycopg
from testcontainers.core.container import DockerContainer
from pipeline.silver_layer_processing.migrate import run_migrations

INSERT_SILVER = """
INSERT INTO trips_silver
    (ride_id, start_station_id, end_station_id, started_at, ended_at,
     start_station_name, end_station_name)
VALUES (%(ride_id)s, %(start_station_id)s, %(end_station_id)s,
        %(started_at)s, %(ended_at)s, %(start_station_name)s, %(end_station_name)s)
"""

TRIPS = [
    {
        "ride_id": "RIDEGOOD001",
        "start_station_id": "JC022",
        "end_station_id": "HB603",
        "started_at": "2026-06-30 16:58:39.826",
        "ended_at": "2026-06-30 17:06:56.222",
        "start_station_name": "Oakland Ave",
        "end_station_name": "8 St & Washington St",
    },
    {
        "ride_id": "RIDEGOOD002",
        "start_station_id": "HB603",
        "end_station_id": "JC022",
        "started_at": "2026-06-30 17:10:00.000",
        "ended_at": "2026-06-30 17:20:00.000",
        "start_station_name": "8 St & Washington St",
        "end_station_name": "Oakland Ave",
    },
]


@pytest.mark.e2e
def test_transform_to_gold(ingest_image, network, silver_db):
    db_url = f"postgresql://{silver_db.username}:{silver_db.password}@db:5432/{silver_db.dbname}"

    # run migrations to have the db ready with the required schema
    host_url = silver_db.get_connection_url()
    os.environ["DATABASE_URL"] = host_url
    run_migrations()

    # push few silver level messages into the database
    with psycopg.connect(host_url) as conn, conn.cursor() as cur:
        for trip in TRIPS:
            cur.execute(INSERT_SILVER, trip)
        conn.commit()

    container = (
        DockerContainer(ingest_image)
        .with_network(network)
        .with_env("LAYER", "transform-to-gold")
        .with_env("JOB", "trips:jc")
        .with_env("WINDOW", "2026-06-30")
        .with_env("DATABASE_URL", db_url)
    )

    with container:
        exit_code = container.wait()
        if exit_code != 0:
            stdout, stderr = container.get_logs()
            print(stdout.decode(errors="replace"))
            print(stderr.decode(errors="replace"))

    assert exit_code == 0

    with psycopg.connect(host_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT ride_day, station, arrivals, departures "
            "FROM trips_gold_daily_station ORDER BY station"
        )
        rows = cur.fetchall()

    assert rows == [
        (datetime.date(2026, 6, 30), "HB603", 1, 1),
        (datetime.date(2026, 6, 30), "JC022", 1, 1),
    ]

    # inspect the daily-station-trips for a specific station

    inspect_container = (
        DockerContainer(ingest_image)
        .with_network(network)
        .with_env("LAYER", "daily-station-trips")
        .with_env("JOB", "trips:jc")
        .with_env("WINDOW", "2026-06-30")
        .with_env("STATION", "HB603")
        .with_env("DATABASE_URL", db_url)
    )

    with inspect_container:
        exit_code = inspect_container.wait()
        stdout, stderr = inspect_container.get_logs()
        if exit_code != 0:
            print(stdout.decode(errors="replace"))
            print(stderr.decode(errors="replace"))

    assert exit_code == 0

    output = stdout.decode()
    json_str = output[output.index("{") : output.rindex("}") + 1]
    report = json.loads(json_str)

    assert report == {
        "market": "jc",
        "station": "HB603",
        "day": "2026-06-30",
        "departures": 1,
        "arrivals": 1,
    }