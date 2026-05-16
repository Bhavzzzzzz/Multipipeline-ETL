# src/controllers/utils.py
import argparse
import gzip
import os

def process_and_batch_logs(input_files, output_dir, batch_size=100000):
    """
    Pure batcher: Reads compressed/raw logs and blindly splits them into chunks.
    Data cleaning and malformed record tracking is delegated entirely to the individual pipelines.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    batch_files = []
    batch_id = 1
    current_batch_records = 0
    total_processed = 0
    
    current_batch_file = open(os.path.join(output_dir, f"batch_{batch_id}.txt"), 'w', encoding='latin-1')

    print(f"Starting pure physical batching (No Regex). Batch size: {batch_size}")

    for file_path in input_files:
        print(f"Processing: {file_path}")
        with _open_log_file(file_path) as f:
            for line in f:
                total_processed += 1
                current_batch_file.write(line)
                current_batch_records += 1

                if current_batch_records >= batch_size:
                    current_batch_file.close()
                    # Return 0 for malformed; pipelines will calculate this natively
                    batch_files.append((batch_id, os.path.join(output_dir, f"batch_{batch_id}.txt"), current_batch_records, 0))
                    batch_id += 1
                    current_batch_records = 0
                    current_batch_file = open(os.path.join(output_dir, f"batch_{batch_id}.txt"), 'w', encoding='latin-1')

    current_batch_file.close()
    if current_batch_records > 0:
        batch_files.append((batch_id, os.path.join(output_dir, f"batch_{batch_id}.txt"), current_batch_records, 0))
    
    if current_batch_records == 0 and os.path.exists(os.path.join(output_dir, f"batch_{batch_id}.txt")):
        os.remove(os.path.join(output_dir, f"batch_{batch_id}.txt"))
        batch_id -= 1

    print("\n--- Data Prep Summary ---")
    print(f"Total Records Batched: {total_processed}")
    print(f"Total Batches Generated: {batch_id}")
    print(f"Data ready in: {output_dir}/")
    
    return batch_files, total_processed, 0

def _open_log_file(file_path):
    if file_path.endswith(".gz"):
        return gzip.open(file_path, 'rt', encoding='ascii', errors='ignore')
    return open(file_path, 'r', encoding='latin-1', errors='ignore')