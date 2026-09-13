import subprocess
import time
import socket
import pytest


@pytest.fixture(scope="module", autouse=True)
def run_just_up():
    """Bring the stack up once for all tests in this file, tear down after."""
    result = subprocess.run(["just", "up"], capture_output=True, text=True)
    assert result.returncode == 0, f"just up failed: {result.stderr}"
    yield
    subprocess.run(["just", "down"], capture_output=True)


def test_db_container_is_running():
    """The db container should exist and be in 'running' state."""
    result = subprocess.run(
        ["docker", "compose", "ps", "db", "--format", "{{.State}}"],
        capture_output=True, text=True
    )
    assert result.stdout.strip() == "running", f"db state: {result.stdout.strip()}"


def test_db_healthcheck_passes():
    """Wait for Docker's own healthcheck (defined in compose) to report healthy."""
    for _ in range(10):  # up to ~30s, matches your healthcheck retries
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}",
             "-f", "{{.State.Health.Status}}"],
            capture_output=True, text=True
        )
        # simpler: get container id via compose first
        cid = subprocess.run(
            ["docker", "compose", "ps", "-q", "db"],
            capture_output=True, text=True
        ).stdout.strip()
        status = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Health.Status}}", cid],
            capture_output=True, text=True
        ).stdout.strip()
        if status == "healthy":
            break
        time.sleep(1)
    assert status == "healthy", f"db healthcheck status: {status}"


def test_db_port_is_reachable():
    """Port 55432 should be open on localhost (compose port mapping worked)."""
    with socket.create_connection(("localhost", 55432), timeout=5):
        pass  # no exception = connection succeeded


def test_db_accepts_real_connection_and_query():
    """Actually connect with psycopg2 and run a real query — the strongest check."""
    import psycopg
    conn = psycopg.connect(
        host="localhost", port=55432,
        dbname="meridian_trips", user="meridian", password="meridian"
    )
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    assert cur.fetchone() == (1,)
    conn.close()

