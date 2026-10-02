from datetime import date, datetime, timezone
import pandas as pd

from airflow.sdk import dag, task
import psycopg
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

DAG_MAPPING = {
    "jc": {"dag": "ingest_jc", "pipeline_name": "trips_jc", "fallback_date": "2021-01"},
    "nyc": {"dag": "ingest_nyc", "pipeline_name": "trips_nyc", "fallback_date": "2026-01"},
}

PIPELINE_NAME = "trips_jc"

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
                        where market = %s
                        order by month desc
                        """,
                        (market,),
                    )
                    columns = [desc.name for desc in cursor.description]
                    result = cursor.fetchall()

            df = pd.DataFrame(result, columns=columns)
            df["month_date"] = pd.to_datetime(df["month"], format="%Y-%m")
            df.sort_values(by="month_date", ascending=True, inplace=True)

            expected = df["month_date"] + pd.DateOffset(months=1)
            gap_mask = expected.shift(1) != df["month_date"]
            if df.empty:
                wm = None
            elif gap_mask.any():
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

        # if start_month and start_month == end_month:
        #     # if only start month provided
        #     # run only it!
        #     watermark = start_month
        # elif not start_month and not watermark:
        #     # if no start month is provided and no watermark is available, 
        #     # fall back to the default date for the market
        #     market = dag_run.conf["market"]
        #     start_month = DAG_MAPPING[market]["fallback_date"]
        #     end_month = start_month
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
    def record_success(month, dag_run=None):
        market = dag_run.conf["market"]

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
                        completed_at
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (pipeline_name, market, month)
                    DO NOTHING
                    """,
                    (
                        PIPELINE_NAME,
                        market,
                        month,
                        datetime.now(timezone.utc).replace(microsecond=0),
                    ),
                )

    record = record_success.expand(month=months)

    trigger_months >> record


pipeline_controller()