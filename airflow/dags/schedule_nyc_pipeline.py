
from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

@dag(
    dag_id="schedule_nyc_pipeline",
    schedule="0 6 1 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["scheduler", "nyc"],
)
def schedule_nyc_pipeline():

    TriggerDagRunOperator(
        task_id="trigger_controller",
        trigger_dag_id="pipeline_controller",
        conf={
            "market": "nyc",
            "start_month": "",
            "end_month": "",
        },
        wait_for_completion=True,
        deferrable=True,
    )

schedule_nyc_pipeline()