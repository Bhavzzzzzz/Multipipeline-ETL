-- src/pipelines/hive/queries.hql
-- Execution: main.py renders __INPUT__ and __OUTPUT_DIR__, then runs:
-- beeline -u jdbc:hive2:// -f <rendered_queries.hql>
-- Runtime target: Apache Hive 4.x

-- Force local MapReduce execution so the project does not need YARN/Tez services.
set mapreduce.framework.name=local;
set hive.execution.engine=mr;
set hive.strict.managed.tables=false;

-- 1. Create a temporary table and load the raw text file
DROP TABLE IF EXISTS raw_logs;
CREATE TABLE raw_logs (line STRING);
LOAD DATA LOCAL INPATH '__INPUT__' INTO TABLE raw_logs;

-- 2. Create a view that parses the raw strings using RegEx
-- We extract the exact same fields as the Pig script
DROP VIEW IF EXISTS parsed_logs;
CREATE VIEW IF NOT EXISTS parsed_logs AS
SELECT
    regexp_extract(line, '^(\\S+)', 1) as host,
    regexp_extract(line, '\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):', 1) as log_date,
    regexp_extract(line, '\\[\\d{2}/\\w{3}/\\d{4}:(\\d{2}):', 1) as log_hour,
    regexp_extract(line, '"(?:(\\S+)\\s+(\\S+)\\s+([^"]+)|.*)"', 1) as http_method,
    regexp_extract(line, '"(?:(\\S+)\\s+(\\S+)\\s+([^"]+)|.*)"', 2) as resource_path,
    regexp_extract(line, '"(?:(\\S+)\\s+(\\S+)\\s+([^"]+)|.*)"', 3) as protocol_version,
    cast(regexp_extract(line, '"\\s+(\\d{3})\\s+', 1) as INT) as status_code,
    regexp_extract(line, '\\s+(\\d+|-)$', 1) as bytes_str
FROM raw_logs;

-- 3. Clean the data (Handle the '-' in bytes, filter out nulls)
DROP VIEW IF EXISTS clean_logs;
CREATE VIEW IF NOT EXISTS clean_logs AS
SELECT
    host, log_date, log_hour, http_method, resource_path, protocol_version, status_code,
    CASE WHEN bytes_str = '-' THEN 0 ELSE cast(bytes_str as BIGINT) END as bytes_transferred
FROM parsed_logs
WHERE host IS NOT NULL AND host != '';

-- ==============================================================================
-- Query 1: Daily Traffic Summary
-- ==============================================================================
INSERT OVERWRITE LOCAL DIRECTORY '__OUTPUT_DIR__/query1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT log_date, status_code, count(*) as request_count, sum(bytes_transferred) as total_bytes
FROM clean_logs
GROUP BY log_date, status_code;

-- ==============================================================================
-- Query 2: Top Requested Resources
-- ==============================================================================
INSERT OVERWRITE LOCAL DIRECTORY '__OUTPUT_DIR__/query2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT resource_path, count(*) as request_count, sum(bytes_transferred) as total_bytes, count(DISTINCT host) as distinct_host_count
FROM clean_logs
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 20;

-- ==============================================================================
-- Query 3: Hourly Error Analysis
-- ==============================================================================
INSERT OVERWRITE LOCAL DIRECTORY '__OUTPUT_DIR__/query3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    log_date,
    log_hour,
    sum(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) as error_request_count,
    count(*) as total_request_count,
    cast(sum(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) as DOUBLE) / count(*) as error_rate,
    count(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host ELSE NULL END) as distinct_error_hosts
FROM clean_logs
GROUP BY log_date, log_hour;
