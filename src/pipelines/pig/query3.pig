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

STORE q3_result INTO '__OUTPUT_DIR__/query3' USING PigStorage(',');