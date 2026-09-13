import subprocess
import pytest


@pytest.fixture(scope="module", autouse=True)
def bring_up_then_down():
    """Start the stack first so there's something real to tear down."""
    subprocess.run(["just", "up"], capture_output=True, text=True)
    yield
    result = subprocess.run(["just", "down"], capture_output=True, text=True)
    assert result.returncode == 0, f"just down failed: {result.stderr}"


def test_just_down_exits_successfully():
    """'just down' should run without errors."""
    result = subprocess.run(["just", "down"], capture_output=True, text=True)
    assert result.returncode == 0, f"just down failed: {result.stderr}"


def test_db_container_is_removed():
    """No 'db' container should remain after down."""
    result = subprocess.run(
        ["docker", "compose", "ps", "-a", "-q", "db"],
        capture_output=True, text=True
    )
    assert result.stdout.strip() == "", "db container still exists after down"


def test_pgadmin_container_is_removed():
    """No 'pgadmin' container should remain after down."""
    result = subprocess.run(
        ["docker", "compose", "ps", "-a", "-q", "pgadmin"],
        capture_output=True, text=True
    )
    assert result.stdout.strip() == "", "pgadmin container still exists after down"


def test_port_is_released():
    """Port 55432 should no longer be reachable after down."""
    import socket
    with pytest.raises(OSError):
        with socket.create_connection(("localhost", 55432), timeout=2):
            pass