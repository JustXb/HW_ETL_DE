from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import json
import xml.etree.ElementTree as ET

PG_CONN = {
    "host": "postgres",
    "dbname": "pets_db",
    "user": "airflow",
    "password": "airflow"
}

def json_to_flat_pets():
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            post.value->>'name' AS name,
            post.value->>'species' AS species,
            post.value->>'birthYear' AS birth_year,
            post.value->>'photo' AS photo,
            post.value->'favFoods' AS fav_foods
        FROM raw_json,
             jsonb_array_elements(json_data->'pets') AS post(value)
    """)

    rows = cur.fetchall()

    for name, species, birth_year, photo, fav_foods in rows:
        cur.execute("""
            INSERT INTO flat_pets (name, species, birth_year, photo, fav_foods)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            name,
            species,
            int(birth_year) if birth_year else None,
            photo,
            json.dumps(fav_foods) if fav_foods else None
        ))

    conn.commit()
    cur.close()
    conn.close()


def xml_to_flat_foods():
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO flat_foods (
            food_name,
            manufacturer,
            serving_size,
            calories_total,
            calories_fat,
            total_fat,
            saturated_fat,
            cholesterol,
            sodium,
            carb,
            fiber,
            protein,
            vitamin_a,
            vitamin_c,
            calcium,
            iron
        )
        SELECT 
            (xpath('//name/text()', food_node))[1]::text AS food_name,
            (xpath('//mfr/text()', food_node))[1]::text AS manufacturer,
            (xpath('//serving/text()', food_node))[1]::text AS serving_size,
            NULLIF((xpath('//calories/@total', food_node))[1]::text, '')::int AS calories_total,
            NULLIF((xpath('//calories/@fat', food_node))[1]::text, '')::int AS calories_fat,
            (xpath('//total-fat/text()', food_node))[1]::text AS total_fat,
            (xpath('//saturated-fat/text()', food_node))[1]::text AS saturated_fat,
            (xpath('//cholesterol/text()', food_node))[1]::text AS cholesterol,
            (xpath('//sodium/text()', food_node))[1]::text AS sodium,
            (xpath('//carb/text()', food_node))[1]::text AS carb,
            (xpath('//fiber/text()', food_node))[1]::text AS fiber,
            (xpath('//protein/text()', food_node))[1]::text AS protein,
            COALESCE((xpath('//vitamins/a/text()', food_node))[1]::text, '0') AS vitamin_a,
            COALESCE((xpath('//vitamins/c/text()', food_node))[1]::text, '0') AS vitamin_c,
            COALESCE((xpath('//minerals/ca/text()', food_node))[1]::text, '0') AS calcium,
            COALESCE((xpath('//minerals/fe/text()', food_node))[1]::text, '0') AS iron
        FROM raw_xml,
             unnest(xpath('//food', xml_data::xml)) AS food_node;
    """)

    conn.commit()
    cur.close()
    conn.close()



default_args = {
    "start_date": datetime(2026, 1, 23)
}

with DAG(
    dag_id="etl_raw_to_flat",
    schedule_interval=None,
    catchup=False,
    default_args=default_args
) as dag:

    json_task = PythonOperator(
        task_id="json_to_flat_pets",
        python_callable=json_to_flat_pets
    )

    xml_task = PythonOperator(
        task_id="xml_to_flat_foods",
        python_callable=xml_to_flat_foods
    )

    json_task >> xml_task
