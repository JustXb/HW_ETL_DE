from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2

CSV_PATH = "/opt/airflow/data/IOT-temp.csv"

POSTGRES_CONN = {
    "host": "postgres",
    "dbname": "temperature_db",
    "user": "airflow",
    "password": "airflow"
}

def transform_and_load():
    df = pd.read_csv(CSV_PATH)

    df = df[df["out/in"] == "In"]
    df["noted_date"] = pd.to_datetime(
        df["noted_date"],
        format="%d-%m-%Y %H:%M"
    ).dt.date

    p05 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)

    df = df[(df["temp"] >= p05) & (df["temp"] <= p95)]

    daily = (
        df.groupby("noted_date")
        .agg(
            avg_temp=("temp", "mean"),
            min_temp=("temp", "min"),
            max_temp=("temp", "max")
        )
        .reset_index()
        .rename(columns={"noted_date": "day"})
    )

    hottest = daily.sort_values("avg_temp", ascending=False).head(5)
    coldest = daily.sort_values("avg_temp").head(5)

    final_df = pd.concat([hottest, coldest]).drop_duplicates("day")

    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    for _, row in final_df.iterrows():
        cur.execute("""
            INSERT INTO temperature_daily_stats (day, avg_temp, min_temp, max_temp)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (day) DO UPDATE
            SET avg_temp = EXCLUDED.avg_temp,
                min_temp = EXCLUDED.min_temp,
                max_temp = EXCLUDED.max_temp
        """, (
            row["day"],
            row["avg_temp"],
            row["min_temp"],
            row["max_temp"]
        ))

    conn.commit()
    cur.close()
    conn.close()


default_args = {
    "start_date": datetime(2026, 2, 3)
}

with DAG(
    dag_id="temperature_etl",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "temperature"]
) as dag:

    etl_task = PythonOperator(
        task_id="transform_and_load_temperature",
        python_callable=transform_and_load
    )
