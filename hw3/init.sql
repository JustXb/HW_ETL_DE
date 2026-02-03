CREATE DATABASE temperature_db;
\c temperature_db

CREATE TABLE IF NOT EXISTS temperature_daily_stats (
            day DATE PRIMARY KEY,
            avg_temp NUMERIC,
            min_temp NUMERIC,
            max_temp NUMERIC
        )