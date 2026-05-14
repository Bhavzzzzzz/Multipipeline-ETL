/* * src/pipelines/pig/queries.pig
 * Execution: pig -x local -param INPUT=<batch_file> -param OUTPUT_DIR=<output_path> queries.pig
 */

-- 1. Load Raw Logs
raw_logs = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

-- 2. Parse using Regex
-- This captures: host, log_date, log_hour, method, resource, protocol, status, bytes
parsed_logs = FOREACH raw_logs GENERATE 
    FLATTEN(REGEX_EXTRACT_ALL(line, '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+[^\\]]+\\]\\s+"(?:(\\S+)\\s+(\\S+)\\s+(\\S+)|.*)"\\s+(\\d{3})\\s+(\\d+|-)$')) 
    AS (
        host:chararray, 
        log_date:chararray, 
        log_hour:chararray, 
        http_method:chararray, 
        resource_path:chararray, 
        protocol_version:chararray, 
        status_code:int, 
        bytes_str:chararray
    );

-- 3. Clean and Transform
-- Drop malformed rows and handle the '-' in bytes as 0.
clean_logs = FOREACH parsed_logs GENERATE 
    host, log_date, log_hour, http_method, resource_path, protocol_version, status_code,
    (bytes_str == '-' ? 0 : (int)bytes_str) AS bytes_transferred;

valid_logs = FILTER clean_logs BY
    host IS NOT NULL
    AND log_date IS NOT NULL
    AND log_hour IS NOT NULL
    AND http_method IS NOT NULL
    AND resource_path IS NOT NULL
    AND protocol_version IS NOT NULL
    AND status_code IS NOT NULL
    AND bytes_str IS NOT NULL;

-- ==============================================================================
-- Query 1: Daily Traffic Summary
-- ==============================================================================
q1_group = GROUP valid_logs BY (log_date, status_code);
q1_unsorted = FOREACH q1_group GENERATE 
    FLATTEN(group) AS (log_date, status_code), 
    COUNT(valid_logs) AS request_count, 
    SUM(valid_logs.bytes_transferred) AS total_bytes;
q1_result = ORDER q1_unsorted BY log_date ASC, status_code ASC;

STORE q1_result INTO '$OUTPUT_DIR/query1' USING PigStorage(',');

-- ==============================================================================
-- Query 2: Top Requested Resources
-- ==============================================================================
q2_group = GROUP valid_logs BY resource_path;
q2_agg = FOREACH q2_group {
    unique_hosts = DISTINCT valid_logs.host;
    GENERATE 
        group AS resource_path, 
        COUNT(valid_logs) AS request_count, 
        SUM(valid_logs.bytes_transferred) AS total_bytes, 
        COUNT(unique_hosts) AS distinct_host_count;
}
q2_ordered = ORDER q2_agg BY request_count DESC, resource_path ASC;
q2_top20 = LIMIT q2_ordered 20;

STORE q2_top20 INTO '$OUTPUT_DIR/query2' USING PigStorage(',');

-- ==============================================================================
-- Query 3: Hourly Error Analysis
-- ==============================================================================
-- Flag errors to make the conditional aggregations cleaner
flagged_logs = FOREACH valid_logs GENERATE 
    host, log_date, log_hour,
    (status_code >= 400 AND status_code <= 599 ? 1 : 0) AS is_error;

q3_group = GROUP flagged_logs BY (log_date, log_hour);
q3_unsorted = FOREACH q3_group {
    error_logs = FILTER flagged_logs BY is_error == 1;
    unique_error_hosts = DISTINCT error_logs.host;
    GENERATE 
        FLATTEN(group) AS (log_date, log_hour), 
        SUM(flagged_logs.is_error) AS error_request_count, 
        COUNT(flagged_logs) AS total_request_count, 
        (double)SUM(flagged_logs.is_error) / COUNT(flagged_logs) AS error_rate, 
        COUNT(unique_error_hosts) AS distinct_error_hosts;
}
q3_result = ORDER q3_unsorted BY log_date ASC, log_hour ASC;

STORE q3_result INTO '$OUTPUT_DIR/query3' USING PigStorage(',');
