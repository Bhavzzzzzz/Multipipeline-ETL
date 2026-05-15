/* Shared Pig setup for the NASA log analytics queries. */

raw_logs = LOAD '__INPUT__' USING TextLoader() AS (line:chararray);

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
    AND bytes_transferred IS NOT NULL;
