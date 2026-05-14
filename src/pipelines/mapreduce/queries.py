"""Local MapReduce-style analytics for NASA web server logs.

The script mirrors src/pipelines/pig/queries.pig and writes the same three
query outputs as CSV part files under the requested output directory.
"""

import argparse
import csv
import os
import re
import shutil
from collections import defaultdict


LOG_REGEX = re.compile(
    r'^(\S+)\s+\S+\s+\S+\s+\[(\d{2}/\w{3}/\d{4}):(\d{2}):\d{2}:\d{2}\s+[^\]]+\]\s+"(?:(\S+)\s+(\S+)\s+([^"]+)|.*)"\s+(\d{3})\s+(\d+|-)$'
)


def parse_log_line(line):
    match = LOG_REGEX.match(line)
    if not match:
        return None

    host, log_date, log_hour, method, resource, protocol, status, bytes_str = match.groups()
    if None in (host, log_date, log_hour, method, resource, protocol, status, bytes_str):
        return None

    return {
        "host": host,
        "log_date": log_date,
        "log_hour": log_hour,
        "http_method": method,
        "resource_path": resource,
        "protocol_version": protocol,
        "status_code": int(status),
        "bytes_transferred": 0 if bytes_str == "-" else int(bytes_str),
    }


def map_records(input_path):
    mapped = {
        "daily_traffic": [],
        "top_resources": [],
        "hourly_errors": [],
    }

    with open(input_path, "r", encoding="latin-1", errors="ignore") as handle:
        for line in handle:
            record = parse_log_line(line.rstrip("\n"))
            if record is None:
                continue

            mapped["daily_traffic"].append(
                (
                    (record["log_date"], record["status_code"]),
                    (1, record["bytes_transferred"]),
                )
            )
            mapped["top_resources"].append(
                (
                    record["resource_path"],
                    (1, record["bytes_transferred"], record["host"]),
                )
            )
            is_error = 400 <= record["status_code"] <= 599
            mapped["hourly_errors"].append(
                (
                    (record["log_date"], record["log_hour"]),
                    (1 if is_error else 0, 1, record["host"] if is_error else None),
                )
            )

    return mapped


def reduce_daily_traffic(mapped_rows):
    grouped = defaultdict(lambda: [0, 0])
    for key, (request_count, bytes_transferred) in mapped_rows:
        grouped[key][0] += request_count
        grouped[key][1] += bytes_transferred

    return [
        (log_date, status_code, request_count, total_bytes)
        for (log_date, status_code), (request_count, total_bytes) in sorted(grouped.items())
    ]


def reduce_top_resources(mapped_rows):
    grouped = defaultdict(lambda: [0, 0, set()])
    for resource_path, (request_count, bytes_transferred, host) in mapped_rows:
        grouped[resource_path][0] += request_count
        grouped[resource_path][1] += bytes_transferred
        grouped[resource_path][2].add(host)

    rows = [
        (resource_path, request_count, total_bytes, len(hosts))
        for resource_path, (request_count, total_bytes, hosts) in grouped.items()
    ]
    rows.sort(key=lambda row: (-row[1], row[0]))
    return rows[:20]


def reduce_hourly_errors(mapped_rows):
    grouped = defaultdict(lambda: [0, 0, set()])
    for key, (error_count, total_count, error_host) in mapped_rows:
        grouped[key][0] += error_count
        grouped[key][1] += total_count
        if error_host is not None:
            grouped[key][2].add(error_host)

    return [
        (
            log_date,
            log_hour,
            error_count,
            total_count,
            error_count / total_count if total_count else 0.0,
            len(error_hosts),
        )
        for (log_date, log_hour), (error_count, total_count, error_hosts) in sorted(grouped.items())
    ]


def write_part_file(output_dir, query_name, rows):
    query_dir = os.path.join(output_dir, query_name)
    os.makedirs(query_dir, exist_ok=True)
    part_path = os.path.join(query_dir, "part-00000")

    with open(part_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)


def run(input_path, output_dir):
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    mapped = map_records(input_path)
    write_part_file(output_dir, "query1", reduce_daily_traffic(mapped["daily_traffic"]))
    write_part_file(output_dir, "query2", reduce_top_resources(mapped["top_resources"]))
    write_part_file(output_dir, "query3", reduce_hourly_errors(mapped["hourly_errors"]))


def main():
    parser = argparse.ArgumentParser(description="Run local MapReduce-style NASA log analytics")
    parser.add_argument("--input", required=True, help="Path to a raw log batch file")
    parser.add_argument("--output-dir", required=True, help="Directory for query output folders")
    args = parser.parse_args()

    run(args.input, args.output_dir)


if __name__ == "__main__":
    main()
