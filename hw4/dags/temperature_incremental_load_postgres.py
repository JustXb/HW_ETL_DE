from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import psycopg2

# -------------------------
# Подключение к Postgres
# -------------------------
POSTGRES_CONN = {
    "host": "postgres",
    "dbname": "temperature_db",
    "user": "airflow",
    "password": "airflow"
}

CSV_PATH = "/opt/airflow/data/IOT-temp.csv"

# -------------------------
# Подключение к БД
# -------------------------
def get_connection():
    return psycopg2.connect(**POSTGRES_CONN)

# -------------------------
# Загрузка и подготовка датасета
# -------------------------
def download_and_prepare_dataset(ti):

    df = pd.read_csv(CSV_PATH)

    df.columns = [c.strip().lower().replace(' ', '_').replace('/', '_') for c in df.columns]

    df['noted_date'] = pd.to_datetime(df['noted_date'], errors='coerce')
    df = df.dropna(subset=['noted_date'])

    max_date = df['noted_date'].max()
    cutoff = max_date - timedelta(days=7)

    df = df[df['noted_date'] >= cutoff]

    ti.xcom_push(
        key='incremental_data',
        value=df.to_json(date_format='iso', orient='split')
    )

# -------------------------
# Инкрементальная загрузка
# -------------------------
def incremental_load_to_postgres(ti, **kwargs):

    df_json = ti.xcom_pull(
        task_ids='download_and_prepare_dataset',
        key='incremental_data'
    )

    df_dict = json.loads(df_json)
    df = pd.DataFrame(df_dict['data'], columns=df_dict['columns'])
    df['noted_date'] = pd.to_datetime(df['noted_date'])

    conn = get_connection()
    cur = conn.cursor()

    batch_id = f"inc_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    cur.execute("""
        INSERT INTO load_history_incremental(load_type,batch_id,start_date,load_status)
        VALUES(%s,%s,%s,%s)
    """, ('incremental', batch_id, datetime.now(), 'started'))

    conn.commit()

    records = []

    for idx, row in df.iterrows():

        device_id = f"row_{idx}"
        if 'id' in df.columns and pd.notna(row['id']):
            device_id = str(row['id'])[-15:]

        temp = None
        if pd.notna(row['temp']):
            temp = float(row['temp'])

        location = str(row.get('out_in', 'unknown'))[:10]

        records.append((
            row['noted_date'],
            temp,
            location,
            device_id,
            batch_id
        ))

    unique_records = {
        (r[0], r[3]): r for r in records
    }.values()

    total_loaded = 0

    for record in unique_records:

        cur.execute("""
            INSERT INTO temperature_incremental
            (noted_date,temperature,location,device_id,batch_id)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (noted_date,device_id)
            DO UPDATE SET
                temperature=EXCLUDED.temperature,
                location=EXCLUDED.location,
                load_timestamp=CURRENT_TIMESTAMP,
                batch_id=EXCLUDED.batch_id
        """, record)

        total_loaded += 1

    conn.commit()

    cur.execute("""
        UPDATE load_history_incremental
        SET end_date=%s,records_loaded=%s,load_status=%s
        WHERE batch_id=%s
    """, (datetime.now(), total_loaded, 'success', batch_id))

    conn.commit()

    cur.close()
    conn.close()

    return total_loaded

# -------------------------
# Итоговая статистика
# -------------------------
def print_incremental_summary():

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM temperature_incremental")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT MIN(noted_date),MAX(noted_date)
        FROM temperature_incremental
    """)
    dates = cur.fetchone()

    cur.execute("""
        SELECT AVG(temperature),MIN(temperature),MAX(temperature)
        FROM temperature_incremental
    """)
    stats = cur.fetchone()

    print(f"Всего записей: {total}")
    print(f"Период: {dates[0]} - {dates[1]}")
    print(f"Средняя температура: {stats[0]:.2f}")
    print(f"Мин: {stats[1]:.2f}")
    print(f"Макс: {stats[2]:.2f}")

    cur.close()
    conn.close()

# -------------------------
# DAG
# -------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="temperature_incremental_load_postgres_v6",
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["temperature", "incremental", "postgres"],
) as dag:

    start = EmptyOperator(task_id="start")

    download = PythonOperator(
        task_id="download_and_prepare_dataset",
        python_callable=download_and_prepare_dataset
    )

    load = PythonOperator(
        task_id="incremental_load_to_postgres",
        python_callable=incremental_load_to_postgres
    )

    summary = PythonOperator(
        task_id="print_incremental_summary",
        python_callable=print_incremental_summary
    )

    end = EmptyOperator(task_id="end")

    start >> download >> load >> summary >> end
