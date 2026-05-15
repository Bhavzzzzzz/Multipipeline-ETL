import csv
import os
import re
import shutil
from collections import defaultdict


LOG_REGEX = re.compile(
    r'^(?P<host>\S+)\s+\S+\s+\S+\s+\[(?P<log_date>\d{2}/\w{3}/\d{4}):(?P<log_hour>\d{2}):\d{2}:\d{2}\s+[^\]]+\]\s+"(?:(?P<http_method>\S+)\s+(?P<resource_path>\S+)\s+(?P<protocol_version>\S+)|.*)"\s+(?P<status>\d{3})\s+(?P<bytes>\d+|-)$'
)

QUERY_NAMES = ("query1", "query2", "query3")


def parse_log_line(line):
    match = LOG_REGEX.match(line.strip())
    if not match:
        return None

    data = match.groupdict()
    required_fields = [
        "host",
        "log_date",
        "log_hour",
        "http_method",
        "resource_path",
        "protocol_version",
        "status",
        "bytes",
    ]
    if any(data[field] is None or data[field] == "" for field in required_fields):
        return None

    return {
        "host": data["host"],
        "log_date": data["log_date"],
        "log_hour": data["log_hour"],
        "http_method": data["http_method"],
        "resource_path": data["resource_path"],
        "protocol_version": data["protocol_version"],
        "status_code": int(data["status"]),
        "bytes_transferred": 0 if data["bytes"] == "-" else int(data["bytes"]),
    }


def load_records(input_path):
    records = []
    with open(input_path, "r", encoding="latin-1", errors="ignore") as handle:
        for line in handle:
            record = parse_log_line(line)
            if record is not None:
                records.append(record)
    return records


def reduce_daily_traffic(records):
    grouped = defaultdict(lambda: [0, 0])
    for record in records:
        key = (record["log_date"], record["status_code"])
        grouped[key][0] += 1
        grouped[key][1] += record["bytes_transferred"]

    return [
        (log_date, status_code, request_count, total_bytes)
        for (log_date, status_code), (request_count, total_bytes) in sorted(grouped.items())
    ]


def reduce_top_resources(records):
    grouped = defaultdict(lambda: [0, 0, set()])
    for record in records:
        key = record["resource_path"]
        grouped[key][0] += 1
        grouped[key][1] += record["bytes_transferred"]
        grouped[key][2].add(record["host"])

    rows = [
        (resource_path, request_count, total_bytes, len(hosts))
        for resource_path, (request_count, total_bytes, hosts) in grouped.items()
    ]
    rows.sort(key=lambda row: (-row[1], row[0]))
    return rows[:20]


def reduce_hourly_errors(records):
    grouped = defaultdict(lambda: [0, 0, set()])
    for record in records:
        key = (record["log_date"], record["log_hour"])
        is_error = 1 if 400 <= record["status_code"] <= 599 else 0
        grouped[key][0] += is_error
        grouped[key][1] += 1
        if is_error:
            grouped[key][2].add(record["host"])

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


def rows_for_query(query_name, records):
    if query_name == "query1":
        return reduce_daily_traffic(records)
    if query_name == "query2":
        return reduce_top_resources(records)
    if query_name == "query3":
        return reduce_hourly_errors(records)
    raise ValueError(f"Unsupported query: {query_name}")


def prepare_output_dir(output_dir):
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)


def write_part_file(output_dir, rows):
    os.makedirs(output_dir, exist_ok=True)
    part_path = os.path.join(output_dir, "part-00000")
    with open(part_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)
