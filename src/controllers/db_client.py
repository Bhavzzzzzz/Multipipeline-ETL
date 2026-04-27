import csv
import glob
import os
from datetime import datetime

import psycopg2  # type: ignore[import-not-found]


# ==========================================
# DATABASE CONFIGURATION
# ==========================================
DB_CONFIG = {
    "dbname": os.getenv("PGDATABASE", "nosql_project"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
}

def get_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        print(f"[ERROR] Could not connect to PostgreSQL: {e}")
        raise

def ingest_query_results(batch_output_dir, metadata):
    """Load run metadata and Pig query outputs for a single batch directory."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        conn.autocommit = False
        cursor = conn.cursor()

        run_id = _insert_run_metadata(cursor, metadata)
        daily_rows = _read_daily_traffic_rows(_resolve_query_output(batch_output_dir, "query1"), run_id)
        resource_rows = _read_top_resource_rows(_resolve_query_output(batch_output_dir, "query2"), run_id)
        hourly_rows = _read_hourly_error_rows(_resolve_query_output(batch_output_dir, "query3"), run_id)

        _bulk_insert(
            cursor,
            "daily_traffic",
            "(run_id, log_date, status_code, request_count, total_bytes)",
            daily_rows,
        )
        _bulk_insert(
            cursor,
            "top_resources",
            "(run_id, resource_path, request_count, total_bytes, distinct_host_count)",
            resource_rows,
        )
        _bulk_insert(
            cursor,
            "hourly_errors",
            "(run_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts)",
            hourly_rows,
        )

        conn.commit()
        print(f"[SUCCESS] Loaded PostgreSQL results for run_id={run_id} from {batch_output_dir}.")
        return run_id
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        print(f"[ERROR] Failed to ingest query results from {batch_output_dir}: {exc}")
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def update_run_runtime(run_id, runtime_seconds):
    """Update the stored runtime after the batch finishes writing to the database."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE run_metadata SET runtime_seconds = %s WHERE run_id = %s;",
            (runtime_seconds, run_id),
        )
        conn.commit()
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        print(f"[ERROR] Failed to update runtime for run_id={run_id}: {exc}")
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def _insert_run_metadata(cursor, metadata):
    query = """
        INSERT INTO run_metadata (
            pipeline_name,
            run_identifier,
            batch_id,
            batch_size,
            average_batch_size,
            runtime_seconds,
            malformed_record_count
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING run_id;
    """
    cursor.execute(
        query,
        (
            metadata.get("pipeline_name", "Pig"),
            metadata.get("run_identifier", "run_unknown"),
            metadata.get("batch_id"),
            metadata.get("batch_size"),
            metadata.get("average_batch_size"),
            metadata.get("runtime_seconds"),
            metadata.get("malformed_record_count", 0),
        ),
    )
    return cursor.fetchone()[0]


def _resolve_query_output(batch_output_dir, query_name):
    query_dir = os.path.join(batch_output_dir, query_name)
    if os.path.isdir(query_dir):
        part_files = sorted(glob.glob(os.path.join(query_dir, "part*")))
        if not part_files:
            raise FileNotFoundError(f"No Pig part files found in {query_dir}")
        return part_files

    if os.path.isfile(query_dir):
        return [query_dir]

    raise FileNotFoundError(f"Expected Pig output directory or file at {query_dir}")


def _read_csv_rows(paths):
    for path in paths:
        with open(path, "r", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            for row in reader:
                if row:
                    yield row


def _read_daily_traffic_rows(paths, run_id):
    rows = []
    for row in _read_csv_rows(paths):
        if len(row) != 4:
            continue
        rows.append((run_id, _parse_pig_date(row[0]), int(row[1]), int(row[2]), int(row[3])))
    return rows


def _read_top_resource_rows(paths, run_id):
    rows = []
    for row in _read_csv_rows(paths):
        if len(row) != 4:
            continue
        rows.append((run_id, row[0], int(row[1]), int(row[2]), int(row[3])))
    return rows


def _read_hourly_error_rows(paths, run_id):
    rows = []
    for row in _read_csv_rows(paths):
        if len(row) != 6:
            continue
        rows.append(
            (
                run_id,
                _parse_pig_date(row[0]),
                int(row[1]),
                int(row[2]),
                int(row[3]),
                float(row[4]),
                int(row[5]),
            )
        )
    return rows


def _parse_pig_date(value):
    return datetime.strptime(value, "%d/%b/%Y").date()


def _bulk_insert(cursor, table_name, column_sql, rows):
    if not rows:
        print(f"[INFO] No rows found for {table_name}.")
        return

    placeholders = ", ".join(["%s"] * len(rows[0]))
    query = f"INSERT INTO {table_name} {column_sql} VALUES ({placeholders});"
    cursor.executemany(query, rows)
    print(f"[SUCCESS] Staged {len(rows)} rows for {table_name}.")


if __name__ == "__main__":
    print("Database client is ready to be imported by the orchestrator.")