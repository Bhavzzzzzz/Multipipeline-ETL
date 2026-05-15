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

STORE q2_top20 INTO '__OUTPUT_DIR__/query2' USING PigStorage(',');