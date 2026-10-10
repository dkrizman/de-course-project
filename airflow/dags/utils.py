from datetime import date, datetime, timezone

import psycopg

DAG_MAPPING = {
    "jc": {"dag": "ingest_jc", "pipeline_name": "trips_jc", "fallback_date": "2021-01"},
    "nyc": {"dag": "ingest_nyc", "pipeline_name": "trips_nyc", "fallback_date": "2026-01"},
}

def month_to_date(month: str) -> date:
    year, month_num = map(int, month.split("-"))
    return date(year, month_num, 1)


def month_range(start_month: str, end_month: str) -> list[str]:
    start = month_to_date(start_month)
    end = month_to_date(end_month)

    months = []
    current = start

    while current <= end:
        months.append(current.strftime("%Y-%m"))

        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    return months


def record_success(market, month, layer, tries, status, days=None, failed_days=None):
    pipeline_name = DAG_MAPPING[market]["pipeline_name"]

    with psycopg.connect(
        "postgresql://meridian:meridian@db:5432/meridian_trips"
    ) as connection:

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_control (
                    pipeline_name,
                    market,
                    month,
                    layer,
                    completed_at,
                    tries,
                    status,
                    days,
                    failed_days
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pipeline_name, market, month, layer)
                DO NOTHING
                """,
                (
                    pipeline_name,
                    market,
                    month,
                    layer,
                    datetime.now(timezone.utc).replace(microsecond=0),
                    tries,
                    status,
                    days,
                    failed_days,
                ),
            )