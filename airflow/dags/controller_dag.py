from datetime import date, datetime, timezone
import pandas as pd

from airflow.sdk import dag, task
import psycopg
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from utils import DAG_MAPPING, month_range

@dag(
    dag_id="pipeline_controller",
    schedule=None,
    catchup=False,
)
def pipeline_controller():

    @task
    def get_watermark(dag_run=None):
        # our progress function to determine the watermark (latest successfully processed month) for the given market
        # returns the first month after the latest successfully processed month
        market = dag_run.conf["market"]
        try:
            with psycopg.connect(
                "postgresql://meridian:meridian@db:5432/meridian_trips"
            ) as connection:

                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        select *
                        from pipeline_control
                        where market = %s and layer = %s
                        order by month desc
                        """,
                        (market, "Full_month_pipeline"),
                    )
                    columns = [desc.name for desc in cursor.description]
                    result = cursor.fetchall()

            df = pd.DataFrame(result, columns=columns)
            if df.empty:
                return None
            
            df["month_date"] = pd.to_datetime(df["month"], format="%Y-%m")
            df.sort_values(by="month_date", ascending=True, inplace=True)

            expected = df["month_date"] + pd.DateOffset(months=1)
            gap_mask = expected.shift(1) != df["month_date"]
            
            if gap_mask.any():
                gap_mask.iloc[0] = False
                first_gap_pos = gap_mask.values.argmax()
                wm = df['month'].iloc[first_gap_pos - 1]
            else:
                wm = df['month'].sort_values(ascending=False).iloc[0]
            first_gap_pos = (pd.to_datetime(wm, format="%Y-%m") + pd.DateOffset(months=1)).strftime("%Y-%m")
            return first_gap_pos
        except Exception as e:
            print(f"First attempt to get watermark failed as table wasn't created yet")
            return None

    @task
    def get_months(dag_run=None, watermark=None):
        start_month = dag_run.conf["start_month"]
        end_month = dag_run.conf.get("end_month", start_month)
        market = dag_run.conf["market"]
        fallback_date = DAG_MAPPING[market]["fallback_date"]

        if start_month == "":
            start_month = end_month = watermark or fallback_date
            return month_range(start_month, end_month)
        elif start_month == end_month:
            return month_range(start_month, end_month)
        else:
            return month_range(watermark or start_month, end_month)

    @task
    def choose_dag(dag_run=None):
        market = dag_run.conf["market"]

        if market not in DAG_MAPPING:
            raise ValueError(f"Unknown market: {market}")

        return DAG_MAPPING[market]["dag"]

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

    watermark = get_watermark()
    months = get_months(watermark=watermark)
    target_dag = choose_dag()

    trigger_configs = build_trigger_configs(
        months,
        target_dag,
    )

    trigger_months = TriggerDagRunOperator.partial(
        task_id="trigger_month",
        wait_for_completion=True,
        deferrable=True,
        max_active_tis_per_dag=1,
    ).expand_kwargs(trigger_configs)

    @task
    def record_success(month, layer="Full_month_pipeline", dag_run=None):
        market = dag_run.conf["market"]
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
                        status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (pipeline_name, market, month, layer)
                    DO UPDATE SET
                        tries = EXCLUDED.tries,
                        completed_at = EXCLUDED.completed_at
                    """,
                    (
                        pipeline_name,
                        market,
                        month,
                        layer,
                        datetime.now(timezone.utc).replace(microsecond=0),
                        0,
                        "succeeded",
                    ),
                )

    record = record_success.expand(month=months)

    trigger_months >> record


pipeline_controller()