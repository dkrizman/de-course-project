import json

import psycopg
import pytest
from testcontainers.core.container import DockerContainer


CANONICAL_HEADER = (
    "ride_id,started_at,ended_at,start_station_id,start_station_name,"
    "end_station_id,end_station_name\n"
)
GOOD_ROW1 = (
    "RIDEGOOD001,2026-06-30 16:58:39.826,2026-06-30 17:06:56.222,"
    "JC022,Oakland Ave,HB603,8 St & Washington St\n"
)

GOOD_ROW2 = (
    "RIDEGOOD002,2026-06-30 16:58:39.826,2026-06-30 17:06:56.222,"
    "JC022,Oakland Ave,HB603,8 St & Washington St\n"
)

BAD_ROW = (  # missing start_station_id -> MissingField
    "RIDEBAD0001,2026-06-30 16:58:39.826,2026-06-30 17:06:56.222,"
    ",Oakland Ave,HB603,8 St & Washington St\n"
)


@pytest.mark.e2e
def test_ingest_to_silver(tmp_path, ingest_image, network, silver_db):
    raw_bronze = tmp_path / "raw_bronze"
    rejects = tmp_path / "rejects"
    reports = tmp_path / "reports"
    for d in (raw_bronze, rejects, reports):
        d.mkdir()

    (raw_bronze / "jc-202606-consolidated-tripdata.csv").write_text(
        CANONICAL_HEADER + GOOD_ROW1 + GOOD_ROW2 + BAD_ROW, encoding="utf-8"
    )

    db_url = f"postgresql://{silver_db.username}:{silver_db.password}@db:5432/{silver_db.dbname}"

    container = (
        DockerContainer(ingest_image)
        .with_network(network)
        .with_env("LAYER", "transform-to-silver")
        .with_env("JOB", "trips:jc")
        .with_env("WINDOW", "2026-06")
        .with_env("DATABASE_URL", db_url)
        .with_volume_mapping(str(raw_bronze), "/app/data/raw_bronze", mode="rw")
        .with_volume_mapping(str(rejects), "/app/data/rejects", mode="rw")
        .with_volume_mapping(str(reports), "/app/data/reports", mode="rw")
    )

    with container:
        exit_code = container.wait()
        if exit_code != 0:
            stdout, stderr = container.get_logs()
            print(stdout.decode(errors="replace"))
            print(stderr.decode(errors="replace"))

    assert exit_code == 0

    report_path = reports / "silver-jc-202606-report.json"
    assert report_path.exists()
    report_data = json.loads(report_path.read_text(encoding="utf-8"))
    assert report_data == {
        "layer": "silver",
        "job": "trips:jc",
        "window": "2026-06",
        "rows": 3,
        "rejects": 1,
        "reasons": {"start_station_id is missing or empty": 1},  # match exact message from errors.py
    }

    rejects_path = rejects / "jc-202606-consolidated-tripdata.rejects.jsonl"
    assert rejects_path.exists()
    lines = rejects_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1

    host_url = silver_db.get_connection_url()  # localhost:<exposed_port>, from the test's perspective
    with psycopg.connect(host_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM trips_silver")
        assert cur.fetchone()[0] == 2