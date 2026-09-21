from datetime import datetime
import os
import subprocess

from airflow.sdk import dag, task

@dag(
    dag_id="ingest_trips",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze"],
)
def ingest_trips_dag():
    @task
    def run_ingest(dag_run=None):
        market = dag_run.conf["market"]
        month = dag_run.conf["month"]
        subprocess.run(
            [
                "docker", "compose", "--profile", "ingest", "run", "--rm", "--build",
                "-e", "LAYER=ingest-to-bronze",
                "-e", f"JOB={market}",
                "-e", f"WINDOW={month}",
                "ingest",
            ],
            cwd=os.environ["PROJECT_DIR"],
            check=True,
            stderr=subprocess.STDOUT,
        )

    @task
    def run_transform(dag_run=None):
        market = dag_run.conf["market"]
        month = dag_run.conf["month"]
        subprocess.run(
            [
                "docker", "compose", "--profile", "ingest", "run", "--rm", "--build",
                "-e", "LAYER=transform-to-silver",
                "-e", f"JOB={market}",
                "-e", f"WINDOW={month}",
                "ingest",
            ],
            cwd=os.environ["PROJECT_DIR"],
            check=True,
            stderr=subprocess.STDOUT,
        )

    ingest_bronze = run_ingest()
    transform_silver = run_transform()

    ingest_bronze >> transform_silver

ingest_trips_dag()