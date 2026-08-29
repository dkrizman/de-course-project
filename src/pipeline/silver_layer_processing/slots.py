"""Task 7 — at most two ingests at a time."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator
import time
import psycopg

from .errors import SlotUnavailable
from .settings import database_url

_ACQUIRE = """
    UPDATE job_slots SET in_use = in_use + 1
     WHERE job_name = %s AND in_use < capacity
 RETURNING in_use
"""
_RELEASE = "UPDATE job_slots SET in_use = in_use - 1 WHERE job_name = %s AND in_use > 0"

def _try_take(conn: psycopg.Connection, job_name: str) -> bool:
    """Try to take one of `job_name`'s slots. Return True if successful, False if none free."""
    return conn.execute(_ACQUIRE, (job_name,)).fetchone() is not None

def _take_or_wait(conn: psycopg.Connection, job_name: str, timeout: float, poll: float) -> None:
    deadline = time.monotonic() + timeout
    while not _try_take(conn, job_name):
        if time.monotonic() > deadline:
            raise SlotUnavailable(f"no {job_name} slots free after {timeout:.1f}s")
        time.sleep(min(poll, max(0, deadline - time.monotonic())))
    

@contextmanager
def job_slot(job_name: str, timeout: float = 3.0, poll: float = 0.5, max_slots: int = 2) -> Iterator[None]:
    """Hold one of `job_name`'s permits for the duration of the block.

    Take a permit if one is free. If none is, wait for one — up to `timeout` seconds, then raise.
    Give the permit back when the block ends, however it ends.

    Read the README before you write this. Two details decide whether it works.
    """
    with psycopg.connect(database_url()) as conn:
        _take_or_wait(conn, job_name, timeout, poll)
        conn.commit()
        try:
            yield
        finally:
            conn.execute(_RELEASE, (job_name,))
            conn.commit()
