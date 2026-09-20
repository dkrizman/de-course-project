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
    def run_ingest():
        subprocess.run(
            [
                "docker", "compose", "--profile", "ingest", "run", "--rm", "--build",
                "-e", "LAYER=ingest-to-bronze",
                "-e", "JOB=JC",
                "-e", "WINDOW=2016-09",
                "ingest",
            ],
            cwd=os.environ["PROJECT_DIR"],
            check=True,
        )

    run_ingest()

ingest_to_bronze_dag()