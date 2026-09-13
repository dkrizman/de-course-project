from __future__ import annotations


class IngestError(Exception):
    """Anything this ingest raises on purpose."""


class RowError(IngestError):
    """A single row is bad; skip it and keep going."""


class BadTimestamp(RowError):
    """A timestamp field is malformed or missing."""


class MissingField(RowError):
    """A required field is absent (vendor schema changed)."""


class JobError(IngestError):
    """The job cannot run at all; stop immediately."""

class SlotUnavailable(JobError):
    """SlotUnavailable"""

