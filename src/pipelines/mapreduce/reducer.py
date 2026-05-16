#!/usr/bin/env python3
"""Hadoop Streaming reducer for NASA log queries."""

import os
import sys

sys.path.insert(0, os.getcwd())
import nasa_log_common as common  # type: ignore


def emit_row(cols):
    print(",".join(str(c) for c in cols))


def reduce_query1(key, values):
    total_count = 0
    total_bytes = 0
    for v in values:
        parts = v.split("\t")
        total_count += int(parts[0])
        total_bytes += int(parts[1])
    log_date, status_code = key.split("|")
    emit_row([log_date, int(status_code), total_count, total_bytes])


def reduce_query2(key, values):
    total_count = 0
    total_bytes = 0
    hosts = set()
    for v in values:
        parts = v.split("\t")
        total_count += int(parts[0])
        total_bytes += int(parts[1])
        if len(parts) > 2:
            hosts.add(parts[2])
    return (key, total_count, total_bytes, len(hosts))


def reduce_query3(key, values):
    total_error = 0
    total_count = 0
    hosts = set()
    for v in values:
        parts = v.split("\t")
        total_error += int(parts[0])
        total_count += int(parts[1])
        if len(parts) > 2 and parts[2]:
            hosts.add(parts[2])
    log_date, log_hour = key.split("|")
    rate = total_error / total_count if total_count else 0.0
    emit_row([log_date, log_hour, total_error, total_count, rate, len(hosts)])


def main():
    query = os.environ.get("MAPREDUCE_QUERY")
    if query not in {"query1", "query2", "query3"}:
        raise RuntimeError("MAPREDUCE_QUERY must be set to query1, query2, or query3")

    current_key = None
    acc = []
    query2_rows = []
    for raw in sys.stdin.buffer:
        line = raw.decode("latin-1", errors="ignore").rstrip("\n")
        if not line:
            continue
        try:
            key, value = line.split("\t", 1)
        except ValueError:
            continue
        if current_key is None:
            current_key = key
            acc = [value]
        elif key == current_key:
            acc.append(value)
        else:
            if query == "query1":
                reduce_query1(current_key, acc)
            elif query == "query2":
                query2_rows.append(reduce_query2(current_key, acc))
            elif query == "query3":
                reduce_query3(current_key, acc)
            current_key = key
            acc = [value]

    if current_key is not None:
        if query == "query1":
            reduce_query1(current_key, acc)
        elif query == "query2":
            query2_rows.append(reduce_query2(current_key, acc))
        elif query == "query3":
            reduce_query3(current_key, acc)

    if query == "query2":
        query2_rows.sort(key=lambda row: (-row[1], row[0]))
        for row in query2_rows[:20]:
            emit_row(row)


if __name__ == "__main__":
    main()
