# src/controller/main.py
import os
import shutil
import subprocess
import time
import argparse

import db_client
from utils import process_and_batch_logs

def run_pig_pipeline(batch_path: str, output_dir: str):
    """Executes the Pig script via local system call."""
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    cmd = [
        "pig", "-x", "local",
        "-param", f"INPUT={batch_path}",
        "-param", f"OUTPUT_DIR={output_dir}",
        "src/pipelines/pig/queries.pig"
    ]
    
    print(f"[*] Executing Pig pipeline for {os.path.basename(batch_path)}...")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    if result.returncode != 0:
        print("[-] Pig Job Failed!")
        print(result.stderr)
        raise RuntimeError("Pig execution error")

def trigger_db_load(batch_id: int, output_dir: str, metadata: dict):
    """
    Handoff point for Member 3's PostgreSQL ingestion.
    """
    print(f"[*] Loading results from {output_dir} into PostgreSQL for Batch {batch_id}...")
    return db_client.ingest_query_results(output_dir, metadata)

def main():
    parser = argparse.ArgumentParser(description="Multi-Pipeline ETL Orchestrator")
    parser.add_argument("--pipeline", choices=["pig", "hive", "mongodb"], default="pig", help="Select execution backend")
    parser.add_argument("--batch-size", type=int, default=100000, help="Number of records per batch")
    parser.add_argument("--input", type=str, default="data/raw/NASA_access_log_Jul95.txt", help="Path to raw logs")
    args = parser.parse_args()

    staging_dir = "data/output/staging_batches"
    base_output_dir = "data/output/pig_results"

    if os.path.exists(staging_dir):
        shutil.rmtree(staging_dir)
    os.makedirs(staging_dir)

    # Start the official runtime timer
    start_time = time.time()

    # 1. Split logs using the shared utility helper
    batch_files, total_records, total_malformed_records = process_and_batch_logs(
        [args.input],
        staging_dir,
        batch_size=args.batch_size,
    )
    num_batches = len(batch_files)
    avg_batch_size = total_records / num_batches if num_batches > 0 else 0
    
    # 2. Process each batch sequentially
    for batch_id, batch_path, records_in_batch, malformed_in_batch in batch_files:
        batch_output_dir = os.path.join(base_output_dir, f"batch_{batch_id}")
        
        if args.pipeline == "pig":
            batch_start = time.time()
            run_pig_pipeline(batch_path, batch_output_dir)
            
            # Formulate metadata to pass down to the database script
            metadata = {
                "pipeline_name": "Pig",
                "run_identifier": f"run_{int(start_time)}",
                "batch_id": batch_id,
                "batch_size": records_in_batch,
                "average_batch_size": avg_batch_size,
                "runtime_seconds": None,
                "malformed_record_count": malformed_in_batch,
            }
            
            # 3. Load into DB
            run_id = trigger_db_load(batch_id, batch_output_dir, metadata)
            batch_runtime = time.time() - batch_start
            db_client.update_run_runtime(run_id, batch_runtime)

    # Calculate final runtime (must include write to DB)
    total_runtime = time.time() - start_time
    # 4. Final Console Report
    print("\n" + "="*50)
    print(" ETL EXECUTION REPORT")
    print("="*50)
    print(f"Pipeline Selected : {args.pipeline.upper()}")
    print(f"Total Runtime     : {total_runtime:.2f} seconds")
    print(f"Total Records     : {total_records}")
    print(f"Malformed Records  : {total_malformed_records}")
    print(f"Total Batches     : {num_batches}")
    print(f"Avg Batch Size    : {avg_batch_size:.2f} records")
    print("="*50)

if __name__ == "__main__":
    main()