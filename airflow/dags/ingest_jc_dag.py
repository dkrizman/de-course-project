from datetime import datetime, timedelta
import os
import subprocess
import uuid

from airflow.sdk import dag, task, TriggerRule
from utils import record_success

import calendar

MARKET = "jc"
EARLIEST_DATE = "2021-01"

def days_in_month(month: str) -> int:
    year, month = map(int, month.split("-"))
    return calendar.monthrange(year, month)[1]

@dag(
    dag_id="ingest_jc",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze"],
)
def ingest_jc_dag():
    @task
    def start_container(dag_run=None):
        container_name = f"pipeline-{uuid.uuid4().hex}"

        subprocess.run(
            [
                "docker", "compose",
                "--profile", "ingest",
                "run",
                "-d",
                "--build",
                "--name", container_name,
                "ingest",
                "sleep", "infinity",
            ],
            cwd=os.environ["PROJECT_DIR"],
            check=True,
            stderr=subprocess.STDOUT,
        )

        return container_name

    @task(
    retries=3,
    retry_delay=timedelta(seconds=5),
    )
    def run_ingest(container_name, dag_run=None, ti=None):
        market = MARKET
        month = dag_run.conf["month"]
        try:
            subprocess.run(
                [
                    "docker", "exec",
                    "-e", "LAYER=ingest-to-bronze",
                    "-e", f"JOB={market}",
                    "-e", f"WINDOW={month}",
                    container_name,
                    "python", "-m", "pipeline",
                ],
                cwd=os.environ["PROJECT_DIR"],
                check=True,
                stderr=subprocess.STDOUT,
            )

            retries_used = ti.try_number - 1
            print(f"Ingest to bronze succeeded after {retries_used} retries")

            record_success(MARKET, month, "ingest-to-bronze", retries_used, "succeeded")
        except subprocess.CalledProcessError as e:
            print(f"Ingest to bronze failed: {e}")
            record_success(MARKET, month, "ingest-to-bronze", retries_used, "failed")

    @task(
    retries=3,
    retry_delay=timedelta(seconds=5),
    )
    def run_transform_silver(container_name, dag_run=None, ti=None):
        market = MARKET
        month = dag_run.conf["month"]
        try:
            subprocess.run(
                [
                    "docker", "exec",
                    "-e", "LAYER=transform-to-silver",
                    "-e", f"JOB={market}",
                    "-e", f"WINDOW={month}",
                    container_name,
                    "python", "-m", "pipeline",
                ],
                cwd=os.environ["PROJECT_DIR"],
                check=True,
                stderr=subprocess.STDOUT,
            )
            retries_used = ti.try_number - 1
            print(f"Transform to silver succeeded after {retries_used} retries")
            record_success(MARKET, month, "transform-to-silver", retries_used, "succeeded")
        except subprocess.CalledProcessError as e:
            print(f"Transform to silver failed: {e}")
            record_success(MARKET, month, "transform-to-silver", retries_used, "failed")

    @task(
    retries=3,
    retry_delay=timedelta(seconds=5),
    )
    def run_transform_gold(container_name, dag_run=None, ti=None):
        market = MARKET
        month = dag_run.conf["month"]
        days = days_in_month(month)
        failed_days = []
        for day in range(1, days + 1):
            try:
                day_str = f"{month}-{day:02d}"
                print(f"Processing day: {day_str}")
                subprocess.run(
                    [
                        "docker", "exec",
                        "-e", "LAYER=transform-to-gold",
                        "-e", f"JOB={market}",
                        "-e", f"WINDOW={day_str}",
                        container_name,
                        "python", "-m", "pipeline",
                    ],
                    cwd=os.environ["PROJECT_DIR"],
                    check=True,
                    stderr=subprocess.STDOUT,
                )
            except subprocess.CalledProcessError as e:
                print(f"Transform to gold failed for day {day_str}: {e}")
                failed_days.append(day_str)
        retries_used = ti.try_number
        if failed_days:
            print(f"Transform to gold failed for days: {', '.join(failed_days)}")
            record_success(MARKET, month, "transform-to-gold", retries_used, "failed")
        else:
            print(f"Transform to gold succeeded after {retries_used} retries")
            record_success(MARKET, month, "transform-to-gold", retries_used, "succeeded")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_container(container_name):
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            check=False,
            stderr=subprocess.STDOUT,
        )

    container_name = start_container()
    ingest_bronze = run_ingest(container_name)
    transform_silver = run_transform_silver(container_name)
    transform_gold = run_transform_gold(container_name)

    ingest_bronze >> transform_silver >> transform_gold

    cleanup = cleanup_container(container_name)

    [ingest_bronze, transform_silver, transform_gold] >> cleanup

ingest_jc_dag()