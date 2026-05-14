# src/controllers/reporting.py
import argparse
import os
import subprocess
import sys
import time

try:
    from . import db_client
except Exception:
    import db_client


PIPELINE_OPTIONS = {
    "1": "pig",
    "2": "mapreduce",
    "3": "hive",
    "4": "mongodb",
}

QUERY_OPTIONS = {
    "1": "query1",
    "2": "query2",
    "3": "query3",
    "4": "all",
}


def _script_path(filename):
    return os.path.join(os.path.dirname(__file__), filename)


def _missing_db_env_vars():
    required = ["PGDATABASE", "PGUSER", "PGPASSWORD", "PGHOST", "PGPORT"]
    return [name for name in required if not os.getenv(name)]


def _print_db_env_warning():
    missing = _missing_db_env_vars()
    if not missing:
        return False

    print("[-] PostgreSQL environment is not configured.")
    print(f"    Missing: {', '.join(missing)}")
    print("    Source setup.sh or export the PostgreSQL variables before using report or cleanup actions.")
    return True

def print_header(title):
    print("\n" + "="*80)
    print(f" {title.center(78)} ")
    print("="*80)


def _controller_command(*args):
    return [sys.executable, _script_path("main.py"), *args]


def _first_existing_path(candidates):
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


def _prompt_choice(prompt, valid_choices):
    while True:
        choice = input(prompt).strip().lower()
        if choice in valid_choices:
            return choice
        print("[!] Invalid choice. Please try again.")


def _prompt_batch_size(default_value="100000"):
    value = input(f"Enter Batch Size [default {default_value}]: ").strip()
    return value or default_value


def _prompt_pipeline():
    print_header("SELECT PIPELINE")
    print("  1. Pig")
    print("  2. MapReduce")
    print("  3. Hive")
    print("  4. MongoDB")
    print("  b. Back to Main Menu")
    choice = _prompt_choice("\nEnter choice [1-4 or b]: ", {"1", "2", "3", "4", "b"})
    if choice == "b":
        return None
    return PIPELINE_OPTIONS[choice]


def _prompt_query():
    print_header("SELECT QUERY")
    print("  1. Query 1")
    print("  2. Query 2")
    print("  3. Query 3")
    print("  4. All Queries")
    print("  b. Back to Pipeline Selection")
    choice = _prompt_choice("\nEnter choice [1-4 or b]: ", {"1", "2", "3", "4", "b"})
    if choice == "b":
        return None
    return QUERY_OPTIONS[choice]


def _run_selected_pipeline(pipeline_name, query_name, batch_size):
    print_header("EXECUTING ETL RUN")
    print(f"Pipeline : {pipeline_name.upper()}")
    print(f"Query    : {query_name.upper()}")
    print(f"Batch Size: {batch_size}")
    cmd = _controller_command(
        "--pipeline", pipeline_name,
        "--query", query_name,
        "--batch-size", str(batch_size),
    )
    jul_path = "data/raw/NASA_access_log_Jul95.gz"
    aug_path = "data/raw/NASA_access_log_Aug95.gz"

    missing = []
    if not os.path.exists(jul_path):
        missing.append(jul_path)
    if not os.path.exists(aug_path):
        missing.append(aug_path)

    if missing:
        print("[-] Required input file(s) not present:")
        for path in missing:
            print(f"    - {path}")
        print("[*] Place both files in data/raw and try again.")
        return

    cmd += ["--inputs", jul_path, aug_path]
    print(f"[*] Triggering: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def _handle_begin_flow():
    if _print_db_env_warning():
        return

    while True:
        pipeline_name = _prompt_pipeline()
        if pipeline_name is None:
            return

        while True:
            query_name = _prompt_query()
            if query_name is None:
                break

            batch_size = _prompt_batch_size()
            try:
                _run_selected_pipeline(pipeline_name, query_name, batch_size)
            except subprocess.CalledProcessError:
                print("[-] Pipeline execution failed.")
                time.sleep(1)
                return

            print("\n[*] Execution finished. The controller printed runtime and batch metadata above.")
            try:
                show_latest_report = input("Display the latest stored database report now? [y/N]: ").strip().lower()
            except EOFError:
                show_latest_report = "n"
            if show_latest_report == "y":
                generate_report()

            return


def _clean_database_flow():
    print_header("CLEAN DATABASE")
    if _print_db_env_warning():
        return

    confirm = input("This will drop and recreate the reporting tables. Continue? [y/N]: ").strip().lower()
    if confirm != "y":
        print("[*] Database cleanup cancelled.")
        return

    try:
        db_client.reset_database()
        print("[SUCCESS] Reporting database reset complete.")
    except Exception as exc:
        print(f"[-] Database reset failed: {exc}")

def format_table(title, columns, rows):
    if not rows:
        print(f"\n[!] No data found for {title}.")
        return

    print(f"\n--- {title} ---")
    
    # Calculate column widths
    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            val = str(row.get(col, ""))
            widths[col] = max(widths[col], len(val))
    
    # Print Header
    header_row = " | ".join(col.ljust(widths[col]) for col in columns)
    print(header_row)
    print("-" * len(header_row))
    
    # Print Rows
    for row in rows:
        print(" | ".join(str(row.get(col, "")).ljust(widths[col]) for col in columns))

def generate_report(run_id=None):
    if _print_db_env_warning():
        return

    if not run_id:
        try:
            run_id = db_client.get_latest_run_id()
        except Exception as exc:
            print(f"[-] Could not retrieve the latest run: {exc}")
            return
    
    if not run_id:
        print("\n[-] No execution history found in the database.")
        time.sleep(2)
        return

    try:
        meta = db_client.get_run_metadata(run_id)
    except Exception as exc:
        print(f"[-] Could not retrieve metadata for Run ID {run_id}: {exc}")
        return
    if not meta:
        print(f"\n[-] Could not retrieve metadata for Run ID {run_id}.")
        return

    print_header(f"FINAL EXECUTION REPORT (Run ID: {run_id})")
    print(f"{'Pipeline':<20}: {meta['pipeline_name']}")
    print(f"{'Query':<20}: {meta.get('query_name', 'all')}")
    print(f"{'Run Identifier':<20}: {meta['run_identifier']}")
    print(f"{'Timestamp':<20}: {meta['execution_timestamp']}")
    print(f"{'Total Runtime':<20}: {meta['runtime_seconds']} seconds")
    print(f"{'Batch Size':<20}: {meta['batch_size']}")
    print(f"{'Records Processed':<20}: {meta.get('records_processed', meta['batch_size'])}")
    print(f"{'Avg Batch Size':<20}: {meta['average_batch_size']}")
    print(f"{'Malformed Records':<20}: {meta['malformed_record_count']}")

    # Query 1
    q1_rows = db_client.get_query_results("daily_traffic", run_id)
    format_table("Query 1: Daily Traffic Summary", 
                 ["log_date", "status_code", "request_count", "total_bytes"], q1_rows)

    # Query 2
    q2_rows = db_client.get_query_results("top_resources", run_id, limit=20)
    format_table("Query 2: Top Requested Resources (Top 20)", 
                 ["resource_path", "request_count", "total_bytes", "distinct_host_count"], q2_rows)

    # Query 3
    q3_rows = db_client.get_query_results("hourly_errors", run_id)
    format_table("Query 3: Hourly Error Analysis", 
                 ["log_date", "log_hour", "error_request_count", "total_request_count", "error_rate", "distinct_error_hosts"], q3_rows)

    input("\nPress Enter to exit...")


def show_main_menu():
    print_header("NASA LOG ANALYTICS - MULTI-PIPELINE ETL")
    print("  1. Begin")
    print("  2. Clean Database")
    print("  3. View Latest Report")
    print("  q. Exit")
    return _prompt_choice("\nEnter choice [1-3 or q]: ", {"1", "2", "3", "q"})

def main():
    parser = argparse.ArgumentParser(description="Display the latest stored ETL report")
    parser.add_argument("--run-id", type=int, default=None, help="Specific run_id to display")
    args = parser.parse_args()

    if args.run_id is not None:
        generate_report(args.run_id)
        return

    while True:
        choice = show_main_menu()
        if choice == "1":
            _handle_begin_flow()
        elif choice == "2":
            _clean_database_flow()
        elif choice == "3":
            generate_report()
        elif choice == "q":
            print("\nExiting. Goodbye!")
            break

if __name__ == "__main__":
    main()
