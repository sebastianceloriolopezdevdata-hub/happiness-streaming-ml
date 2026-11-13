-- ==============================================================================
-- MySQL Schema for Streaming ETL Pipeline (Happiness ML)
-- ==============================================================================

-- Drop tables in reverse dependency order to avoid foreign key errors
DROP TABLE IF EXISTS fact_predictions;
DROP TABLE IF EXISTS raw_happiness_events;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_country;

-- ------------------------------------------------------------------------------
-- 1. RAW / STAGING TABLE
-- ------------------------------------------------------------------------------
CREATE TABLE raw_happiness_events (
    raw_event_id INT AUTO_INCREMENT PRIMARY KEY,
    topic VARCHAR(100) NOT NULL,
    partition_id INT NOT NULL,
    offset_id BIGINT NOT NULL,
    raw_key VARCHAR(255),
    raw_value JSON NOT NULL,
    processing_status VARCHAR(20) DEFAULT 'RECEIVED',
    error_message TEXT,
    event_id VARCHAR(50),
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL,
    UNIQUE KEY uq_kafka_position (topic, partition_id, offset_id),
    INDEX idx_raw_event_id (event_id),
    INDEX idx_processing_status (processing_status)
);

-- ------------------------------------------------------------------------------
-- 2. DIMENSION TABLE: COUNTRY
-- ------------------------------------------------------------------------------
CREATE TABLE dim_country (
    country_id INT AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL
);

-- ------------------------------------------------------------------------------
-- 3. DIMENSION TABLE: DATE
-- ------------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    calendar_date DATE,
    INDEX idx_calendar_date (calendar_date)
);

-- ------------------------------------------------------------------------------
-- 4. FACT TABLE: PREDICTIONS
-- ------------------------------------------------------------------------------
CREATE TABLE fact_predictions (
    prediction_id INT AUTO_INCREMENT PRIMARY KEY,
    raw_event_id INT NOT NULL,
    country_id INT NOT NULL,
    date_id INT NOT NULL,
    observation_year INT NOT NULL,
    actual_score DECIMAL(5,3),
    predicted_score DECIMAL(5,3),
    prediction_error DECIMAL(5,3) GENERATED ALWAYS AS (actual_score - predicted_score) STORED,
    event_time TIMESTAMP,
    prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_name VARCHAR(50),
    model_sha256 VARCHAR(64),
    FOREIGN KEY (raw_event_id) REFERENCES raw_happiness_events(raw_event_id),
    FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    INDEX idx_country_id (country_id),
    INDEX idx_date_id (date_id),
    INDEX idx_prediction_timestamp (prediction_timestamp),
    INDEX idx_observation_year (observation_year)
);