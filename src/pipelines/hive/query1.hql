source __COMMON_HQL__;

INSERT OVERWRITE LOCAL DIRECTORY '__OUTPUT_DIR__/query1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT log_date, status_code, count(*) as request_count, sum(bytes_transferred) as total_bytes
FROM clean_logs
GROUP BY log_date, status_code
ORDER BY log_date ASC, status_code ASC;