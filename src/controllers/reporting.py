# src/controllers/reporting.py
import os
import sys
import subprocess
import time
import db_client

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header(title):
    print("\n" + "="*80)
    print(f" {title.center(78)} ")
    print("="*80)

def check_env():
    required_vars = [
        "PGDATABASE", "PGUSER", "PGPASSWORD", "PGHOST", "PGPORT",
        "JAVA_HOME", "PIG_HOME"
    ]
    missing = [v for v in required_vars if not os.getenv(v)]
    
    if missing:
        print_header("ENVIRONMENT CHECK")
        print("[-] WARNING: The following environment variables are missing:")
        for v in missing:
            print(f"    - {v}")
        print("\n[!] Please ensure your PostgreSQL and Big Data tools are correctly configured.")
        print("[!] Refer to README.md for setup instructions.")
        input("\nPress Enter to continue anyway (or Ctrl+C to exit)...")

def show_menu():
    print_header("NASA LOG ANALYTICS - MULTI-PIPELINE ETL")
    print(" Select an execution pipeline:")
    print("  1. Apache Pig (Ready)")
    print("  2. MapReduce  (Phase 2 Placeholder)")
    print("  3. Apache Hive (Phase 2 Placeholder)")
    print("  4. MongoDB     (Phase 2 Placeholder)")
    print("  5. View Latest Report Only")
    print("  q. Quit")
    choice = input("\nEnter choice [1-5 or q]: ").strip().lower()
    return choice

def run_pipeline(pipeline_name):
    print_header(f"LAUNCHING {pipeline_name.upper()} PIPELINE")
    
    # Validation for placeholders
    if pipeline_name in ["mapreduce", "hive", "mongodb"]:
        print(f"[-] {pipeline_name.capitalize()} pipeline is currently a placeholder for Phase 2.")
        time.sleep(2)
        return False

    batch_size = input("Enter Batch Size [default 100000]: ").strip()
    if not batch_size:
        batch_size = "100000"
    
    input_file = input("Enter Path to Raw Log File [default data/raw/access_log_Jul95]: ").strip()
    if not input_file:
        input_file = "data/raw/access_log_Jul95"

    if not os.path.exists(input_file):
        print(f"[-] ERROR: Input file not found at {input_file}")
        time.sleep(2)
        return False

    # Execute main.py via subprocess
    cmd = [
        sys.executable, "src/controllers/main.py",
        "--pipeline", pipeline_name,
        "--batch-size", batch_size,
        "--input", input_file
    ]
    
    print(f"[*] Triggering: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError:
        print("[-] Pipeline execution failed.")
        time.sleep(2)
        return False

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
    if not run_id:
        run_id = db_client.get_latest_run_id()
    
    if not run_id:
        print("\n[-] No execution history found in the database.")
        time.sleep(2)
        return

    meta = db_client.get_run_metadata(run_id)
    if not meta:
        print(f"\n[-] Could not retrieve metadata for Run ID {run_id}.")
        return

    print_header(f"FINAL EXECUTION REPORT (Run ID: {run_id})")
    print(f"{'Pipeline':<20}: {meta['pipeline_name']}")
    print(f"{'Run Identifier':<20}: {meta['run_identifier']}")
    print(f"{'Timestamp':<20}: {meta['execution_timestamp']}")
    print(f"{'Total Runtime':<20}: {meta['runtime_seconds']} seconds")
    print(f"{'Batch Size':<20}: {meta['batch_size']}")
    print(f"{'Avg Batch Size':<20}: {meta['average_batch_size']}")
    print(f"{'Malformed Records':<20}: {meta['malformed_record_count']}")

    # Query 1
    q1_rows = db_client.get_query_results("daily_traffic", run_id)
    format_table("Query 1: Daily Traffic Summary", 
                 ["log_date", "status_code", "request_count", "total_bytes"], q1_rows)

    # Query 2
    q2_rows = db_client.get_query_results("top_resources", run_id, limit=10)
    format_table("Query 2: Top Requested Resources (Top 10)", 
                 ["resource_path", "request_count", "total_bytes", "distinct_host_count"], q2_rows)

    # Query 3
    q3_rows = db_client.get_query_results("hourly_errors", run_id)
    format_table("Query 3: Hourly Error Analysis", 
                 ["log_date", "log_hour", "error_request_count", "total_request_count", "error_rate"], q3_rows)

    input("\nPress Enter to return to menu...")

def main():
    check_env()
    while True:
        clear_screen()
        choice = show_menu()
        
        if choice == '1':
            if run_pipeline("pig"):
                generate_report()
        elif choice == '2':
            run_pipeline("mapreduce")
        elif choice == '3':
            run_pipeline("hive")
        elif choice == '4':
            run_pipeline("mongodb")
        elif choice == '5':
            generate_report()
        elif choice == 'q':
            print("\nExiting. Goodbye!")
            break
        else:
            print("\n[!] Invalid choice. Please try again.")
            time.sleep(1)

if __name__ == "__main__":
    main()
