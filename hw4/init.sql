CREATE DATABASE temperature_db;
\c temperature_db

CREATE TABLE IF NOT EXISTS staging_temperature (
    noted_date TIMESTAMP,
    temperature FLOAT,
    location TEXT,
    device_id TEXT
);

CREATE TABLE IF NOT EXISTS temperature_full (
    id SERIAL PRIMARY KEY,
    noted_date TIMESTAMP,
    temperature FLOAT,
    location TEXT,
    device_id TEXT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS load_history (
    id SERIAL PRIMARY KEY,
    batch_id TEXT,
    records_loaded INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS temperature_incremental (
    id SERIAL PRIMARY KEY,
    noted_date TIMESTAMP,
    temperature FLOAT,
    location VARCHAR(10),
    device_id VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(50),
    UNIQUE(noted_date, device_id)
);

CREATE TABLE IF NOT EXISTS load_history_incremental (
    id SERIAL PRIMARY KEY,
    load_type VARCHAR(20),
    batch_id VARCHAR(50),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    records_loaded INTEGER,
    load_status VARCHAR(20),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

