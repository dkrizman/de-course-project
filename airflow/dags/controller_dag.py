from datetime import date, datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

DAG_MAPPING = {
    "jc": "ingest_jc",
    "nyc": "ingest_nyc",
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

@dag(
    dag_id="pipeline_controller",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def pipeline_controller():

    @task
    def get_months(dag_run=None):
        start_month = dag_run.conf["start_month"]
        end_month = dag_run.conf.get("end_month", start_month)

        return month_range(start_month, end_month)

    @task
    def choose_dag(dag_run=None):
        market = dag_run.conf["market"]

        if market not in DAG_MAPPING:
            raise ValueError(f"Unknown market: {market}")

        return DAG_MAPPING[market]

    @task
    def build_trigger_configs(months, target_dag, dag_run=None):
        controller_run_id = dag_run.run_id

        return [
            {
                "trigger_dag_id": target_dag,
                "trigger_run_id": f"{controller_run_id}__{target_dag}__{month}",
                "conf": {
                    "month": month,
                },
            }
            for month in months
        ]

    months = get_months()
    target_dag = choose_dag()

    trigger_configs = build_trigger_configs(
        months,
        target_dag,
    )

    TriggerDagRunOperator.partial(
        task_id="trigger_month",
        wait_for_completion=True,
        deferrable=True,
        max_active_tis_per_dag=1,
    ).expand_kwargs(trigger_configs)


pipeline_controller()