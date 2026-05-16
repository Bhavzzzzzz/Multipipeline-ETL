import os
import sys
import time
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.pipelines.common.nasa_log_common import QUERY_NAMES, load_records, prepare_output_dir, write_part_file


INSERT_CHUNK_SIZE = 10000


def _collect_query_rows(collection, query_name):
    if query_name == "query1":
        results = collection.aggregate([
            {
                "$group": {
                    "_id": {"log_date": "$log_date", "status_code": "$status_code"},
                    "request_count": {"$sum": 1},
                    "total_bytes": {"$sum": "$bytes_transferred"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "log_date": "$_id.log_date",
                    "status_code": "$_id.status_code",
                    "request_count": 1,
                    "total_bytes": 1,
                }
            },
            {"$sort": {"log_date": 1, "status_code": 1}},
        ], allowDiskUse=True)
        return [[row["log_date"], row["status_code"], row["request_count"], row["total_bytes"]] for row in results]

    if query_name == "query2":
        results = collection.aggregate([
            {
                "$group": {
                    "_id": "$resource_path",
                    "request_count": {"$sum": 1},
                    "total_bytes": {"$sum": "$bytes_transferred"},
                    "distinct_hosts": {"$addToSet": "$host"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "resource_path": "$_id",
                    "request_count": 1,
                    "total_bytes": 1,
                    "distinct_host_count": {"$size": "$distinct_hosts"},
                }
            },
            {"$sort": {"request_count": -1, "resource_path": 1}},
            {"$limit": 20},
        ], allowDiskUse=True)
        return [
            [row["resource_path"], row["request_count"], row["total_bytes"], row["distinct_host_count"]]
            for row in results
        ]

    if query_name == "query3":
        results = collection.aggregate([
            {
                "$group": {
                    "_id": {"log_date": "$log_date", "log_hour": "$log_hour"},
                    "error_request_count": {
                        "$sum": {
                            "$cond": [
                                {"$and": [{"$gte": ["$status_code", 400]}, {"$lte": ["$status_code", 599]}]},
                                1,
                                0,
                            ]
                        }
                    },
                    "total_request_count": {"$sum": 1},
                    "error_hosts": {
                        "$addToSet": {
                            "$cond": [
                                {"$and": [{"$gte": ["$status_code", 400]}, {"$lte": ["$status_code", 599]}]},
                                "$host",
                                "$$REMOVE",
                            ]
                        }
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "log_date": "$_id.log_date",
                    "log_hour": "$_id.log_hour",
                    "error_request_count": 1,
                    "total_request_count": 1,
                    "error_rate": {"$divide": ["$error_request_count", "$total_request_count"]},
                    "distinct_error_hosts": {"$size": "$error_hosts"},
                }
            },
            {"$sort": {"log_date": 1, "log_hour": 1}},
        ], allowDiskUse=True)
        return [
            [
                row["log_date"],
                row["log_hour"],
                row["error_request_count"],
                row["total_request_count"],
                row["error_rate"],
                row["distinct_error_hosts"],
            ]
            for row in results
        ]

    raise ValueError(f"Unsupported query: {query_name}")


def _connect_to_mongodb():
    """Create a MongoDB client and verify the server is reachable before ETL work starts."""
    client = MongoClient(
        os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
        serverSelectionTimeoutMS=5000,
    )
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError as exc:
        raise RuntimeError(
            "Unable to reach MongoDB. Start mongod or set MONGO_URI to a reachable server before running this pipeline."
        ) from exc

    return client


def _insert_records_in_chunks(collection, records, chunk_size=INSERT_CHUNK_SIZE):
    if not records:
        return

    for start in range(0, len(records), chunk_size):
        chunk = records[start:start + chunk_size]
        collection.insert_many(chunk, ordered=False)


def _stream_insert_records(collection, input_path, chunk_size=INSERT_CHUNK_SIZE):
    """Parse and insert records incrementally to keep memory usage bounded."""
    from src.pipelines.common.nasa_log_common import parse_log_line

    buffer = []
    with open(input_path, "r", encoding="latin-1", errors="ignore") as handle:
        for line in handle:
            record = parse_log_line(line)
            if record is None:
                continue
            buffer.append(record)
            if len(buffer) >= chunk_size:
                collection.insert_many(buffer, ordered=False)
                buffer.clear()

    if buffer:
        collection.insert_many(buffer, ordered=False)

def run_pipeline(input_path, output_dir, query_name="all"):
    """Executes the MongoDB ETL pipeline."""
    client = _connect_to_mongodb()
    collection = None
    try:
        db = client[os.getenv("MONGO_DB", "nosql_project")]
        # Use an isolated, per-run collection to avoid mutating shared collections
        # and reduce the chance of service disruption during concurrent/manual use.
        collection_name = f"logs_tmp_{os.getpid()}_{int(time.time() * 1000)}"
        collection = db[collection_name]
        collection.delete_many({})

        # 2. Parse and load incrementally to avoid large in-memory buffers.
        _stream_insert_records(collection, input_path)

        if query_name == "all":
            prepare_output_dir(output_dir)
            for current_query in QUERY_NAMES:
                query_output_dir = os.path.join(output_dir, current_query)
                write_part_file(query_output_dir, _collect_query_rows(collection, current_query))
        else:
            query_output_dir = os.path.join(output_dir, query_name)
            prepare_output_dir(query_output_dir)
            write_part_file(query_output_dir, _collect_query_rows(collection, query_name))

        print(f"[SUCCESS] MongoDB pipeline completed. Results in {output_dir}")
    except PyMongoError as exc:
        raise RuntimeError(
            "MongoDB operation failed. Ensure mongod is running and MONGO_URI points to the correct server before running this pipeline."
        ) from exc
    finally:
        if collection is not None:
            try:
                collection.drop()
            except Exception:
                pass
        client.close()

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run the MongoDB ETL pipeline")
    parser.add_argument("--input", required=True, help="Path to a raw log batch file")
    parser.add_argument("--output-dir", required=True, help="Directory for query output folders")
    parser.add_argument("--query", choices=["query1", "query2", "query3", "all"], default="all", help="Which query to execute")
    args = parser.parse_args()

    try:
        run_pipeline(args.input, args.output_dir, query_name=args.query)
    except RuntimeError as exc:
        print(f"[-] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
