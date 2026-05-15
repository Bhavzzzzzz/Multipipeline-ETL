q1_group = GROUP valid_logs BY (log_date, status_code);
q1_unsorted = FOREACH q1_group GENERATE
    FLATTEN(group) AS (log_date, status_code),
    COUNT(valid_logs) AS request_count,
    SUM(valid_logs.bytes_transferred) AS total_bytes;
q1_result = ORDER q1_unsorted BY log_date ASC, status_code ASC;

STORE q1_result INTO '__OUTPUT_DIR__/query1' USING PigStorage(',');