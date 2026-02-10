from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
import uuid
import psycopg2

POSTGRES_CONN = {
    "host": "postgres",
    "dbname": "temperature_db",
    "user": "airflow",
    "password": "airflow"
}

DATA_PATH = "/tmp/temperature_clean.csv"

CSV_PATH = "/opt/airflow/data/IOT-temp.csv"


def extract_dataset():
    df = pd.read_csv(CSV_PATH)

    df.columns = [c.lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    df["noted_date"] = pd.to_datetime(df["noted_date"], errors="coerce")
    df = df.dropna(subset=["noted_date"])

    df["location"] = df["out_in"].astype(str)

    df["device_id"] = df["id"].astype(str).str[-10:]

    df = df[["noted_date", "temp", "location", "device_id"]]
    df = df.rename(columns={"temp": "temperature"})

    df.to_csv(DATA_PATH, index=False)


def load_to_staging():
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    cur.execute("TRUNCATE staging_temperature")

    with open(DATA_PATH, "r") as f:
        next(f)
        cur.copy_from(
            f,
            "staging_temperature",
            sep=",",
            columns=("noted_date", "temperature", "location", "device_id")
        )

    conn.commit()
    cur.close()
    conn.close()


def load_to_final():
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    batch_id = str(uuid.uuid4())

    cur.execute("TRUNCATE temperature_full")

    cur.execute("""
        INSERT INTO temperature_full
        (noted_date, temperature, location, device_id)
        SELECT noted_date, temperature, location, device_id
        FROM staging_temperature
    """)

    cur.execute("""
        INSERT INTO load_history(batch_id, records_loaded)
        SELECT %s, COUNT(*) FROM staging_temperature
    """, (batch_id,))

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="temperature_full_load_v4",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_dataset",
        python_callable=extract_dataset
    )

    staging = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging
    )

    final = PythonOperator(
        task_id="load_to_final",
        python_callable=load_to_final
    )

    end = EmptyOperator(task_id="end")

    start >> extract >> staging >> final >> end
