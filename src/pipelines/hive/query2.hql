source __COMMON_HQL__;

INSERT OVERWRITE LOCAL DIRECTORY '__OUTPUT_DIR__/query2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT resource_path, count(*) as request_count, sum(bytes_transferred) as total_bytes, count(DISTINCT host) as distinct_host_count
FROM clean_logs
GROUP BY resource_path
ORDER BY request_count DESC, resource_path ASC
LIMIT 20;