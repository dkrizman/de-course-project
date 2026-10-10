import pandas as pd
import psycopg

def get_coverage(conn: psycopg.Connection, market: str):
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                        """
                        select *
                        from pipeline_control
                        where market = %s and layer = %s and status = %s
                        order by month desc
                        """,
                        (market, "transform-to-gold", "success"),
                    )
                columns = [desc.name for desc in cursor.description]
                data = cursor.fetchall()

            df = pd.DataFrame(data, columns=columns)
            if df.empty:
                print(f"No data found for market: {market}")
                return None
            
            df["month_date"] = pd.to_datetime(df["month"], format="%Y-%m")
            df.sort_values(by="month_date", ascending=True, inplace=True)

            expected = df["month_date"] + pd.DateOffset(months=1)
            gap_mask = expected.shift(1) != df["month_date"]
            
            if gap_mask.any():
                gap_mask.iloc[0] = False
                next = gap_mask.values.argmax()
                wm = df['month'].iloc[next - 1]
            else:
                wm = df['month'].sort_values(ascending=False).iloc[0]
            next = (pd.to_datetime(wm, format="%Y-%m") + pd.DateOffset(months=1)).strftime("%Y-%m")
            earliest = df['month'].iloc[0]
            watermark = wm
            complete = len(df)
            gaps = expected.shift(1)[gap_mask].dt.strftime("%Y-%m").to_list()
            return {"earliest": earliest, "watermark": watermark, "complete": complete, "gaps": gaps, "next": next}
        except Exception as e:
            print(f"First attempt to get watermark failed as table wasn't created yet")
            return None


def get_stats_for_month(monthly_data, month, market):
    status = monthly_data[monthly_data['status'] != 'success']
    stats = {
        "month": month,
        "type" : "manual", #placeholder
        "state": "success" if status.empty else "failed",
        "tasks": {
            f"ingest-to-bronze trips:{market} {month}": {
                "state": "success" if monthly_data[(monthly_data['month'] == month)\
                                    & (monthly_data['layer'] == 'ingest-to-bronze')]['status'].values[0] == 'success' else "failed",
                "tries": int(monthly_data[(monthly_data['month'] == month)
                                    & (monthly_data['layer'] == 'ingest-to-bronze')]['tries'].values[0])
            },
            f"transform-to-silver:{market} {month}": {
                    "state": "success" if monthly_data[(monthly_data['month'] == month)\
                                        & (monthly_data['layer'] == 'transform-to-silver')]['status'].values[0] == 'success' else "failed",
                    "tries": int(monthly_data[(monthly_data['month'] == month)
                                        & (monthly_data['layer'] == 'transform-to-silver')]['tries'].values[0])
                },
            f"transform-to-gold:{market} {month}": {
                    "state": "success" if monthly_data[(monthly_data['month'] == month)\
                                        & (monthly_data['layer'] == 'transform-to-gold')]['status'].values[0] == 'success' else "failed",
                    "tries": int(monthly_data[(monthly_data['month'] == month)
                                        & (monthly_data['layer'] == 'transform-to-gold')]['tries'].values[0]),
                    "days": int(monthly_data[(monthly_data['month'] == month)
                                        & (monthly_data['layer'] == 'transform-to-gold')]['days'].values[0]),
                    "failed_days": monthly_data[(monthly_data['month'] == month)
                                        & (monthly_data['layer'] == 'transform-to-gold')]['failed_days'].values[0],
                },
        }
    }
    return stats

def get_runs_metrics(conn: psycopg.Connection, market):
        runs_stats = {}
        runs_stats["market"] = market
        runs_stats["runs"] = []
        try:
            with conn.cursor() as cursor:
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
            if df.empty:
                return None
            else:
                distinct_months = df['month'].unique().tolist()
                for month in distinct_months:
                    monthly_stats = get_stats_for_month(df[df['month'] == month], month, market)
                    runs_stats["runs"].append(monthly_stats)
            return runs_stats
        except Exception as e:
                print(f"First attempt to get watermark failed as table wasn't created yet\n",e)
                return None