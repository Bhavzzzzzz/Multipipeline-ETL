"""Local MapReduce-style analytics for NASA web server logs."""

import argparse
import os
import sys


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.pipelines.common.nasa_log_common import QUERY_NAMES, load_records, prepare_output_dir, rows_for_query, write_part_file


def run(input_path, output_dir, query_name="all"):
    records = load_records(input_path)

    if query_name == "all":
        prepare_output_dir(output_dir)
        for current_query in QUERY_NAMES:
            query_output_dir = os.path.join(output_dir, current_query)
            write_part_file(query_output_dir, rows_for_query(current_query, records))
        return

    query_output_dir = os.path.join(output_dir, query_name)
    prepare_output_dir(query_output_dir)
    write_part_file(query_output_dir, rows_for_query(query_name, records))


def main():
    parser = argparse.ArgumentParser(description="Run local MapReduce-style NASA log analytics")
    parser.add_argument("--input", required=True, help="Path to a raw log batch file")
    parser.add_argument("--output-dir", required=True, help="Directory for query output folders")
    parser.add_argument("--query", choices=["query1", "query2", "query3", "all"], default="all", help="Which query to execute")
    args = parser.parse_args()

    run(args.input, args.output_dir, query_name=args.query)


if __name__ == "__main__":
    main()
