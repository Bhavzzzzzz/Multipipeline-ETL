-- src/pipelines/hive/queries.hql
-- Execution: main.py renders __INPUT__ and __OUTPUT_DIR__, then runs:
-- beeline -u jdbc:hive2:// -f <rendered_queries.hql>
-- Runtime target: Apache Hive 4.x

-- Force local MapReduce execution so the project does not need YARN/Tez services.
set mapreduce.framework.name=local;
set hive.execution.engine=mr;
set hive.strict.managed.tables=false;
set mapreduce.task.io.sort.mb=16;
set mapred.child.java.opts=-Xmx1024m;
set mapreduce.map.java.opts=-Xmx1024m;
set mapreduce.reduce.java.opts=-Xmx1024m;
set mapreduce.map.memory.mb=1536;
set mapreduce.reduce.memory.mb=1536;
set hive.metastore.warehouse.dir=__HIVE_WAREHOUSE_DIR__;
set hive.metastore.warehouse.external.dir=__HIVE_WAREHOUSE_DIR__;

-- 1. Create a temporary table and load the raw text file
DROP VIEW IF EXISTS clean_logs;
DROP VIEW IF EXISTS parsed_logs;
DROP TABLE IF EXISTS raw_logs;
CREATE EXTERNAL TABLE raw_logs (line STRING)
STORED AS TEXTFILE
LOCATION '__RAW_LOGS_TABLE_DIR__';
LOAD DATA LOCAL INPATH '__INPUT__' OVERWRITE INTO TABLE raw_logs;

-- 2. Create a view that parses the raw strings using RegEx
-- We extract the exact same fields as the Pig script
CREATE VIEW IF NOT EXISTS parsed_logs AS
SELECT
    regexp_extract(line, '^(\\S+)', 1) as host,
    regexp_extract(line, '\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):', 1) as log_date,
    regexp_extract(line, '\\[\\d{2}/\\w{3}/\\d{4}:(\\d{2}):', 1) as log_hour,
    regexp_extract(line, '"(?:(\\S+)\\s+(\\S+)\\s+(\\S+)|.*)"', 1) as http_method,
    regexp_extract(line, '"(?:(\\S+)\\s+(\\S+)\\s+(\\S+)|.*)"', 2) as resource_path,
    regexp_extract(line, '"(?:(\\S+)\\s+(\\S+)\\s+(\\S+)|.*)"', 3) as protocol_version,
    cast(regexp_extract(line, '"\\s+(\\d{3})\\s+', 1) as INT) as status_code,
    regexp_extract(line, '\\s+(\\d+|-)$', 1) as bytes_str
FROM raw_logs;

-- 3. Clean the data (Handle the '-' in bytes, filter out nulls)
CREATE VIEW IF NOT EXISTS clean_logs AS
SELECT
    host, log_date, log_hour, http_method, resource_path, protocol_version, status_code,
    CASE WHEN bytes_str = '-' THEN 0 ELSE cast(bytes_str as BIGINT) END as bytes_transferred
FROM parsed_logs
WHERE host IS NOT NULL AND host != ''
    AND log_date IS NOT NULL AND log_date != ''
    AND log_hour IS NOT NULL AND log_hour != ''
    AND http_method IS NOT NULL AND http_method != ''
    AND resource_path IS NOT NULL AND resource_path != ''
    AND protocol_version IS NOT NULL AND protocol_version != ''
    AND status_code IS NOT NULL
    AND bytes_str IS NOT NULL AND bytes_str != '';

-- ==============================================================================
-- Query 1: Daily Traffic Summary
-- ==============================================================================
INSERT OVERWRITE LOCAL DIRECTORY '__OUTPUT_DIR__/query1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT log_date, status_code, count(*) as request_count, sum(bytes_transferred) as total_bytes
FROM clean_logs
GROUP BY log_date, status_code
ORDER BY log_date ASC, status_code ASC;

-- ==============================================================================
-- Query 2: Top Requested Resources
-- ==============================================================================
INSERT OVERWRITE LOCAL DIRECTORY '__OUTPUT_DIR__/query2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT resource_path, count(*) as request_count, sum(bytes_transferred) as total_bytes, count(DISTINCT host) as distinct_host_count
FROM clean_logs
GROUP BY resource_path
ORDER BY request_count DESC, resource_path ASC
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
GROUP BY log_date, log_hour
ORDER BY log_date ASC, log_hour ASC;
