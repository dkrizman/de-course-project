import pytest
from testcontainers.core.container import DockerContainer
from data_ingest.data_detection import find_s3_key_by_date


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


def test_ingest_to_bronze(tmp_path):
    container = (
        DockerContainer("meridian-ingest-test")
        .with_env("LAYER", "ingest-to-bronze")
        .with_env("JOB", "trips:jc")
        .with_env("WINDOW", "2026-06")
        .with_volume_mapping(
            str(tmp_path),
            "/data/raw_bronze",
            mode="rw",
        )
    )

    with container:
        exit_code = container.wait()
        # stdout, stderr = container.get_logs()

        # print(stdout.decode())
        # print(stderr.decode())

    assert exit_code == 0
    assert (tmp_path / "jc-202606-consolidated-tripdata.csv").exists()