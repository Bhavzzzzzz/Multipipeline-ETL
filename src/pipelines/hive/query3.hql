source __COMMON_HQL__;

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