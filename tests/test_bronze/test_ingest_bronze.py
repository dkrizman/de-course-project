import json

import pytest
from testcontainers.core.container import DockerContainer
from pipeline.data_ingest.data_detection import find_s3_key_by_date


@pytest.mark.parametrize(
    "date, city, expected_key",
    [
        ("2024-01", "nyc", "202401-citibike-tripdata.zip"),
        ("2014", "nyc", "2014-citibike-tripdata.zip"),
        ("2016-01", "jc", "JC-201601-citibike-tripdata.csv.zip"),
    ],
)
def test_find_s3_key_by_date(date, city, expected_key):
    assert find_s3_key_by_date(date, city) == expected_key

@pytest.mark.e2e
def test_ingest_to_bronze(tmp_path, ingest_image):
    raw_bronze = tmp_path / "raw_bronze"
    reports = tmp_path / "reports"
    raw_bronze.mkdir()
    reports.mkdir()

    container = (
        DockerContainer(ingest_image)
        .with_env("LAYER", "ingest-to-bronze")
        .with_env("JOB", "trips:jc")
        .with_env("WINDOW", "2026-06")
        .with_volume_mapping(str(raw_bronze), "/app/data/raw_bronze", mode="rw")
        .with_volume_mapping(str(reports), "/app/data/reports", mode="rw")
    )

    with container:
        exit_code = container.wait()
        if exit_code != 0:
            stdout, stderr = container.get_logs()
            print(stdout.decode(errors="replace"))
            print(stderr.decode(errors="replace"))

    assert exit_code == 0
    csv_path = raw_bronze / "jc-202606-consolidated-tripdata.csv"
    assert csv_path.exists()

    report_path = reports / "bronze-jc-202606-report.json"
    assert report_path.exists()
    report_data = json.loads(report_path.read_text(encoding="utf-8"))
    assert report_data == {
        "layer": "bronze",
        "job": "trips:jc",
        "window": "2026-06",
        "objects": 1,
        "rows": 109897,
    }

    with csv_path.open(encoding="utf-8") as f:
        row_count = sum(1 for _ in f) - 1  # exclude header
    assert row_count == report_data["rows"]
