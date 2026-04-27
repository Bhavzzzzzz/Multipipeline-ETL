-- 1. The Central Ledger (Tracks every execution across all pipelines)
CREATE TABLE run_metadata (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(50) NOT NULL, -- Will store 'MapReduce', 'Pig', 'Hive', or 'MongoDB'
    batch_id INT NOT NULL,
    batch_size INT,
    average_batch_size NUMERIC(10, 2),
    runtime_seconds NUMERIC(10, 2),
    execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Query 1: Daily Traffic Summary
CREATE TABLE daily_traffic (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES run_metadata(run_id) ON DELETE CASCADE,
    log_date DATE NOT NULL,
    status_code INT NOT NULL,
    request_count INT NOT NULL,
    total_bytes BIGINT NOT NULL
);

-- 3. Query 2: Top Requested Resources
CREATE TABLE top_resources (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES run_metadata(run_id) ON DELETE CASCADE,
    resource_path TEXT NOT NULL,
    request_count INT NOT NULL,
    total_bytes BIGINT NOT NULL,
    distinct_host_count INT NOT NULL
);

-- 4. Query 3: Hourly Error Analysis
CREATE TABLE hourly_errors (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES run_metadata(run_id) ON DELETE CASCADE,
    log_date DATE NOT NULL,
    log_hour INT NOT NULL CHECK (log_hour >= 0 AND log_hour <= 23),
    error_request_count INT NOT NULL,
    total_request_count INT NOT NULL,
    error_rate NUMERIC(5, 2) NOT NULL,
    distinct_error_hosts INT NOT NULL
);