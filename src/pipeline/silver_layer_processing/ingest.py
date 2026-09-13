"""Task 6 — wire it together. Task 7 — and then make sure only two of these run at once."""
from __future__ import annotations

from dataclasses import asdict
import json
import logging
import os
from pathlib import Path
from typing import Iterator

import psycopg

from .model import Report, IngestSummary
from .errors import RowError
from .loader import load_trips
from .model import Report, Trip
from .reader import parse_row, read_monthly_trip

log = logging.getLogger("trip_ingest")
log.setLevel(logging.INFO)


def _parsed(path: Path, rejects: Path, counts: dict[str, int]) -> Iterator[Trip]:
    """Yield the parsed rows from `path`, writing any rejects to `rejects`"""
    with rejects.open("w", encoding="utf-8") as reject_file:
        for row in read_monthly_trip(path):
            try:
                trip = parse_row(row)
                counts["good"] += 1
                yield trip
            except RowError as e:
                counts["bad"] += 1
                # counts["reasons"].setdefault(str(e), 0)
                counts["reasons"].setdefault(str(e), 0)
                counts["reasons"][str(e)] += 1
                reject_file.write(json.dumps({"row": row, "reason": str(e), "error": type(e).__name__}) + "\n")

def ingest_one_month(conn: psycopg.Connection, path: Path, rejects_dir: Path) -> IngestSummary:
    """
    Ingest a single month's worth of trip data from `path` into the database
    """
    rejects_dir.mkdir(parents=True, exist_ok=True)
    counts = {"good": 0, "bad": 0, "reasons": {}}
    rejects = rejects_dir / f"{path.stem}.rejects.jsonl"
    loaded = load_trips(conn, _parsed(path, rejects, counts))
    conn.commit()
    if counts["bad"] == 0:
        rejects.unlink(missing_ok=True)
    return IngestSummary(read=counts["good"]+counts["bad"], loaded=loaded, rejects=counts["bad"], reasons=counts["reasons"])

def run_bronze_to_silver(conn: psycopg.Connection, bronze_data_path: Path, rejects_dir: Path = Path("rejects")) -> IngestSummary:
    """
    Run the ingestion process for a given bronze data path, transforming it to silver and handling rejects.
    """
    if not bronze_data_path.exists():
        raise FileNotFoundError(f"Bronze data path {bronze_data_path} does not exist")
    report = ingest_one_month(conn, bronze_data_path, rejects_dir)
    log.info(f"Total: {report.read} read, {report.loaded} loaded, {report.rejects} rejected")
    print(report)
    return report

def transform_to_silver(conn: psycopg.Connection, region: str, window: str) -> Report:
    """Transform the data from bronze to silver for a given job and window."""
    normalized_month = window.replace('-', '')
    bronze_dir = Path("data/raw_bronze")

    def matches(path: Path) -> bool:
        name = path.name.lower()
        if normalized_month not in name:
            return False
        return "jc" in name if region.lower() == "jc" else True

    candidates = [p for p in bronze_dir.iterdir() if p.is_file() and matches(p)]
    if not candidates:
        raise FileNotFoundError(
            f"No bronze file found for region={region!r}, window={window!r} in {bronze_dir}"
        )

    bronze_data_path = candidates[0]
    rejects_dir = Path("data/rejects")
    reports_dir = Path("data/reports")
    ingest_summary = run_bronze_to_silver(conn, bronze_data_path, rejects_dir)
    ingest_report = Report(
        layer="silver",
        job=f"trips:{region.lower()}",
        window=window,
        rows=ingest_summary.read,
        rejects=ingest_summary.rejects,
        reasons=ingest_summary.reasons,
    )
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_file = reports_dir / f"silver-{region.lower()}-{normalized_month}-report.json"
    with report_file.open("w", encoding="utf-8") as f:
        json.dump(asdict(ingest_report), f, indent=4)
    return ingest_report

def inspect_silver(region, window):
    """Inspect the silver data for a given region and window"""
    report_file = f"data/reports/silver-{region.lower()}-{window.replace('-', '')}-report.json"
    if not os.path.exists(report_file):
        raise FileNotFoundError(f"No report found for region={region}, window={window}")
    
    with open(report_file, 'r', encoding='utf-8') as f:
        report_data = json.load(f)
    
    log.info(json.dumps(report_data, indent=4))