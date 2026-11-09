-- Canonical Database Schema for Natural Hazard System (Team 1: Earthquakes)

-- 1. Source Catalog
CREATE TABLE IF NOT EXISTS source (
    source_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    url VARCHAR(255) NOT NULL,
    format VARCHAR(20) NOT NULL,
    license VARCHAR(100)
);

-- 2. Ingestion Run Audit Table
CREATE TABLE IF NOT EXISTS ingestion_run (
    run_id SERIAL PRIMARY KEY,
    source_id VARCHAR(50) REFERENCES source(source_id),
    mode VARCHAR(20) CHECK (mode IN ('historical', 'incremental')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    status VARCHAR(20) CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    rows_in INTEGER DEFAULT 0,
    rows_new INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0
);

-- 3. Incremental Ingestion State (High-Water Mark)
CREATE TABLE IF NOT EXISTS ingestion_state (
    source_id VARCHAR(50) PRIMARY KEY REFERENCES source(source_id),
    watermark VARCHAR(100) NOT NULL,
    last_success TIMESTAMPTZ NOT NULL
);

-- 4. Canonical Event Model
CREATE TABLE IF NOT EXISTS event (
    event_id VARCHAR(100) PRIMARY KEY,
    source_id VARCHAR(50) NOT NULL REFERENCES source(source_id),
    event_type VARCHAR(50) NOT NULL DEFAULT 'earthquake',
    timestamp TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90.0 AND 90.0),
    longitude DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180.0 AND 180.0),
    severity DOUBLE PRECISION NOT NULL,
    severity_unit VARCHAR(20) NOT NULL,
    raw_reference TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 5. Cross-Source Event Grouping (Matching real-world events)
CREATE TABLE IF NOT EXISTS event_group (
    group_id VARCHAR(100) NOT NULL,
    event_id VARCHAR(100) NOT NULL REFERENCES event(event_id),
    PRIMARY KEY (group_id, event_id)
);

-- 6. Quarantine for Validation Failures
CREATE TABLE IF NOT EXISTS rejected_record (
    rejected_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES ingestion_run(run_id),
    source_id VARCHAR(50) REFERENCES source(source_id),
    reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    rejected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 7. Domain Specific Table: Earthquake Details
CREATE TABLE IF NOT EXISTS earthquake (
    event_id VARCHAR(100) PRIMARY KEY REFERENCES event(event_id) ON DELETE CASCADE,
    depth_km DOUBLE PRECISION,
    magnitude_type VARCHAR(20),
    place TEXT,
    tsunami_flag BOOLEAN DEFAULT FALSE
);

-- Indexes for high-performance temporal and geospatial querying
CREATE INDEX IF NOT EXISTS idx_event_timestamp ON event(timestamp);
CREATE INDEX IF NOT EXISTS idx_event_coords ON event(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_earthquake_depth ON earthquake(depth_km);
