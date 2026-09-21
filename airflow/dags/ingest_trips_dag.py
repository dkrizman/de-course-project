from datetime import datetime
import os
import subprocess
import uuid

from airflow.sdk import dag, task, TriggerRule

@dag(
    dag_id="ingest_trips",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze"],
)
def ingest_trips_dag():
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
        market = dag_run.conf["market"]
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
        market = dag_run.conf["market"]
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

    # @task
    # def run_transform_gold(dag_run=None):
    #     market = dag_run.conf["market"]
    #     month = dag_run.conf["month"]
    #     subprocess.run(
    #         [
    #             "docker", "compose", "--profile", "ingest", "run", "--rm", "--build",
    #             "-e", "LAYER=transform-to-gold",
    #             "-e", f"JOB={market}",
    #             "-e", f"WINDOW={month}",
    #             "ingest",
    #         ],
    #         cwd=os.environ["PROJECT_DIR"],
    #         check=True,
    #         stderr=subprocess.STDOUT,
    #     )
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
    # transform_gold = run_transform_gold(container_name)

    ingest_bronze >> transform_silver# >> transform_gold

    cleanup = cleanup_container(container_name)

    [ingest_bronze,transform_silver] >> cleanup

ingest_trips_dag()