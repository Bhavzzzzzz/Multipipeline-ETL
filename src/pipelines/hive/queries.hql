-- Shared Hive setup for the NASA log analytics queries.
-- Per-query scripts source this file before executing their individual INSERT statements.

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

DROP VIEW IF EXISTS clean_logs;
DROP VIEW IF EXISTS parsed_logs;
DROP TABLE IF EXISTS raw_logs;

CREATE EXTERNAL TABLE raw_logs (line STRING)
STORED AS TEXTFILE
LOCATION '__RAW_LOGS_TABLE_DIR__';
LOAD DATA LOCAL INPATH '__INPUT__' OVERWRITE INTO TABLE raw_logs;

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
