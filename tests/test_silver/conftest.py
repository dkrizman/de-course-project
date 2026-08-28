from __future__ import annotations

from typing import Iterator

import psycopg
import pytest

from pipeline.silver_layer_processing.settings import database_url

UNREACHABLE = """
Cannot reach Postgres at {dsn}

  {err}

That DSN is not negotiable — it is the exercise. Task 1: write a `docker-compose.yml` with a `db`
service that publishes exactly this port, with exactly this user, password and database, then:

    docker compose up -d db
"""


@pytest.fixture(scope="session")
def dsn() -> str:
    url = database_url()
    failure = None
    try:
        with psycopg.connect(url, connect_timeout=3):
            pass
    except psycopg.OperationalError as exc:
        failure = str(exc).strip().splitlines()[0]
    if failure:                                  # outside the `except`, so pytest prints no traceback
        pytest.fail(UNREACHABLE.format(dsn=url, err=failure), pytrace=False)
    return url


@pytest.fixture(scope="session")
def migrated(dsn: str) -> str:
    """The schema, applied the way production applies it: by running your revisions."""
    from pipeline.silver_layer_processing.migrate import upgrade_to_head
    upgrade_to_head()
    return dsn


@pytest.fixture
def conn(migrated: str) -> Iterator[psycopg.Connection]:
    """A connection whose work is thrown away when the test ends."""
    with psycopg.connect(migrated) as connection:
        yield connection
        connection.rollback()


@pytest.fixture
def clean_trips(conn: psycopg.Connection) -> psycopg.Connection:
    conn.execute("TRUNCATE trips")
    return conn
