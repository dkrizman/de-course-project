from datetime import datetime
import os
import subprocess

from airflow.sdk import dag, task

@dag(
    dag_id="ingest_to_bronze",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze"],
)
def ingest_to_bronze_dag():
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

    run_ingest()

ingest_to_bronze_dag()