from datetime import datetime
import os
import subprocess
import uuid

from airflow.sdk import dag, task, TriggerRule

import calendar

MARKET = "jc"

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

    @task
    def run_ingest(container_name, dag_run=None):
        market = MARKET
        month = dag_run.conf["month"]
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

    @task
    def run_transform_silver(container_name, dag_run=None):
        market = MARKET
        month = dag_run.conf["month"]
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

    @task
    def run_transform_gold(container_name, dag_run=None):
        market = MARKET
        month = dag_run.conf["month"]
        days = days_in_month(month)
        for day in range(1, days + 1):
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