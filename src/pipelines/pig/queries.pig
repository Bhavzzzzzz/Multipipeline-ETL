/* * src/pipelines/pig/queries.pig
 * Execution: pig -x local -param INPUT=<batch_file> -param OUTPUT_DIR=<output_path> queries.pig
 */

-- 1. Load Raw Logs
raw_logs = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

-- 2. Parse using Regex
-- This captures: host, log_date, log_hour, method, resource, protocol, status, bytes
parsed_logs = FOREACH raw_logs GENERATE 
    FLATTEN(REGEX_EXTRACT_ALL(line, '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+[^\\]]+\\]\\s+"(?:(\\S+)\\s+(\\S+)\\s+([^"]+)|.*)"\\s+(\\d{3})\\s+(\\d+|-)$')) 
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
-- Drop malformed rows and handle the '-' in bytes as 0[cite: 27, 31].
clean_logs = FOREACH parsed_logs GENERATE 
    host, log_date, log_hour, http_method, resource_path, protocol_version, status_code,
    (bytes_str == '-' ? 0 : (int)bytes_str) AS bytes_transferred;

valid_logs = FILTER clean_logs BY host IS NOT NULL;

-- ==============================================================================
-- Query 1: Daily Traffic Summary [cite: 44, 45, 46, 47]
-- ==============================================================================
q1_group = GROUP valid_logs BY (log_date, status_code);
q1_result = FOREACH q1_group GENERATE 
    FLATTEN(group) AS (log_date, status_code), 
    COUNT(valid_logs) AS request_count, 
    SUM(valid_logs.bytes_transferred) AS total_bytes;

STORE q1_result INTO '$OUTPUT_DIR/query1' USING PigStorage(',');

-- ==============================================================================
-- Query 2: Top Requested Resources [cite: 48, 49, 50, 51, 52]
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
q2_ordered = ORDER q2_agg BY request_count DESC;
q2_top20 = LIMIT q2_ordered 20;

STORE q2_top20 INTO '$OUTPUT_DIR/query2' USING PigStorage(',');

-- ==============================================================================
-- Query 3: Hourly Error Analysis [cite: 53, 54, 55, 56]
-- ==============================================================================
-- Flag errors to make the conditional aggregations cleaner
flagged_logs = FOREACH valid_logs GENERATE 
    host, log_date, log_hour,
    (status_code >= 400 AND status_code <= 599 ? 1 : 0) AS is_error;

q3_group = GROUP flagged_logs BY (log_date, log_hour);
q3_result = FOREACH q3_group {
    error_logs = FILTER flagged_logs BY is_error == 1;
    unique_error_hosts = DISTINCT error_logs.host;
    GENERATE 
        FLATTEN(group) AS (log_date, log_hour), 
        SUM(flagged_logs.is_error) AS error_request_count, 
        COUNT(flagged_logs) AS total_request_count, 
        (double)SUM(flagged_logs.is_error) / COUNT(flagged_logs) AS error_rate, 
        COUNT(unique_error_hosts) AS distinct_error_hosts;
}

STORE q3_result INTO '$OUTPUT_DIR/query3' USING PigStorage(',');