#!/usr/bin/env python3
"""Hadoop Streaming mapper for NASA log queries."""

import os
import sys

# At runtime the Hadoop Streaming job ships `nasa_log_common.py` into the
# working directory. Import it directly from there. There are no local fallbacks.
sys.path.insert(0, os.getcwd())
import nasa_log_common as common  # type: ignore


def map_line(line, query):
    rec = common.parse_log_line(line)
    if not rec:
        return
    if query == "query1":
        key = f"{rec['log_date']}|{rec['status_code']}"
        value = f"1\t{rec['bytes_transferred']}"
    elif query == "query2":
        key = rec['resource_path']
        value = f"1\t{rec['bytes_transferred']}\t{rec['host']}"
    elif query == "query3":
        key = f"{rec['log_date']}|{rec['log_hour']}"
        is_error = 1 if 400 <= rec['status_code'] <= 599 else 0
        host = rec['host'] if is_error else ""
        value = f"{is_error}\t1\t{host}"
    else:
        return
    print(f"{key}\t{value}")


def main():
    query = os.environ.get("MAPREDUCE_QUERY")
    if query not in {"query1", "query2", "query3"}:
        raise RuntimeError("MAPREDUCE_QUERY must be set to query1, query2, or query3")

    for raw in sys.stdin.buffer:
        line = raw.decode("latin-1", errors="ignore")
        map_line(line, query)


if __name__ == "__main__":
    main()
