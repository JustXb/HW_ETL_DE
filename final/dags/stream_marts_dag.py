from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

STREAM_RAW_SCHEMA = "stream_raw"
STREAM_MARTS_SCHEMA = "stream_marts"


with DAG(
    dag_id="stream_marts",
    start_date=datetime(2026, 2, 26),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "xxx", "depends_on_past": False, "retries": 1},
    max_active_runs=1,
    tags=["streaming", "marts", "analytics"],
) as dag:
    mk_watch_activity = PostgresOperator(
        task_id="mk_watch_activity",
        postgres_conn_id="postgres_analytics",
        sql="""
        CREATE SCHEMA IF NOT EXISTS stream_marts;
        CREATE TABLE IF NOT EXISTS stream_marts.watch_activity_daily (
            activity_date DATE NOT NULL, user_id TEXT NOT NULL, sessions_count INTEGER NOT NULL,
            total_watch_time_minutes NUMERIC(12, 2), videos_count INTEGER, premium_ratio NUMERIC(5, 2),
            PRIMARY KEY (activity_date, user_id));
        """,
    )
    fill_watch_activity = PostgresOperator(
        task_id="fill_watch_activity",
        postgres_conn_id="postgres_analytics",
        sql="""
        DELETE FROM stream_marts.watch_activity_daily WHERE activity_date = '{{ ds }}'::date;
        INSERT INTO stream_marts.watch_activity_daily (
            activity_date, user_id, sessions_count, total_watch_time_minutes, videos_count, premium_ratio)
        SELECT s.start_time::date, s.user_id, COUNT(*),
            SUM(s.session_duration_seconds)::NUMERIC(12, 2) / 60.0,
            COUNT(DISTINCT p.video_id),
            AVG(CASE WHEN s.subscription_type = 'premium' THEN 1.0 ELSE 0.0 END)::NUMERIC(5, 2)
        FROM stream_raw.watch_sessions_raw s
        LEFT JOIN LATERAL jsonb_array_elements_text(s.pages_visited) AS p(video_id) ON TRUE
        WHERE s.start_time::date = '{{ ds }}'::date
        GROUP BY s.start_time::date, s.user_id;
        """,
    )
    mk_support_abuse = PostgresOperator(
        task_id="mk_support_abuse",
        postgres_conn_id="postgres_analytics",
        sql="""
        CREATE SCHEMA IF NOT EXISTS stream_marts;
        CREATE TABLE IF NOT EXISTS stream_marts.support_and_abuse_daily (
            snapshot_date DATE NOT NULL, user_id TEXT NOT NULL, support_cases_count INTEGER NOT NULL,
            abuse_reports_count INTEGER NOT NULL, last_support_update TIMESTAMPTZ, last_abuse_report TIMESTAMPTZ,
            PRIMARY KEY (snapshot_date, user_id));
        """,
    )
    fill_support_abuse = PostgresOperator(
        task_id="fill_support_abuse",
        postgres_conn_id="postgres_analytics",
        sql="""
        DELETE FROM stream_marts.support_and_abuse_daily WHERE snapshot_date = '{{ ds }}'::date;
        INSERT INTO stream_marts.support_and_abuse_daily (
            snapshot_date, user_id, support_cases_count, abuse_reports_count, last_support_update, last_abuse_report)
        SELECT '{{ ds }}'::date, u.user_id, COALESCE(sc.cnt, 0), COALESCE(ar.cnt, 0), sc.last_update, ar.last_report
        FROM (SELECT DISTINCT user_id FROM stream_raw.support_cases_raw UNION SELECT DISTINCT user_id FROM stream_raw.abuse_reports_raw) AS u
        LEFT JOIN (SELECT user_id, COUNT(*) AS cnt, MAX(updated_at) AS last_update FROM stream_raw.support_cases_raw WHERE created_at::date <= '{{ ds }}'::date GROUP BY user_id) AS sc ON sc.user_id = u.user_id
        LEFT JOIN (SELECT user_id, COUNT(*) AS cnt, MAX(submitted_at) AS last_report FROM stream_raw.abuse_reports_raw WHERE submitted_at::date <= '{{ ds }}'::date GROUP BY user_id) AS ar ON ar.user_id = u.user_id;
        """,
    )
    mk_watch_activity >> fill_watch_activity >> mk_support_abuse >> fill_support_abuse
