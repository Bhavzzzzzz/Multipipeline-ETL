import argparse
import gzip
import os
import re

# Master Regex for NASA HTTP Logs (Common Log Format)
# Extracts: Host, Timestamp, Method, Resource, Protocol, Status Code, Bytes
LOG_REGEX = re.compile(
    r'^(?P<host>\S+)\s+\S+\s+\S+\s+\[(?P<timestamp>[^\]]+)\]\s+"(?P<request>[^"]*)"\s+(?P<status>\d{3})\s+(?P<bytes>\d+|-)$'
)

def parse_log_line(line):
    """
    Parses a single log line, extracts required fields, and handles missing data.
    Returns a formatted string or None if malformed.
    """
    match = LOG_REGEX.match(line)
    if not match:
        return None

    data = match.groupdict()
    
    # Handle timestamp -> log_date and log_hour
    # Timestamp format: 01/Jul/1995:00:00:01 -0400
    raw_timestamp = data['timestamp']
    try:
        # Extract date (DD/MMM/YYYY) and hour (HH)
        log_date = raw_timestamp.split(':')[0]
        log_hour = raw_timestamp.split(':')[1]
    except IndexError:
        return None

    # Handle request -> http_method, resource_path, protocol_version
    request_parts = data['request'].split()
    if len(request_parts) == 3:
        http_method, resource_path, protocol_version = request_parts
    elif len(request_parts) == 2:
        http_method, resource_path = request_parts
        protocol_version = "UNKNOWN"
    else:
        # Malformed request string
        return None

    # Handle missing bytes ('-') by treating as '0'
    bytes_transferred = '0' if data['bytes'] == '-' else data['bytes']

    # Assemble the final structured record (Tab-separated for easy MapReduce parsing)
    structured_record = (
        f"{data['host']}\t"
        f"{raw_timestamp}\t"
        f"{log_date}\t"
        f"{log_hour}\t"
        f"{http_method}\t"
        f"{resource_path}\t"
        f"{protocol_version}\t"
        f"{data['status']}\t"
        f"{bytes_transferred}\n"
    )
    return structured_record

def process_and_batch_logs(input_files, output_dir, batch_size=100000):
    """
    Reads raw or compressed logs and splits them into sequential batches of raw lines.
    Malformed records are counted for reporting, but the original log lines are preserved
    so the Pig pipeline can parse them directly.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    batch_files = []
    batch_id = 1
    current_batch_records = 0
    current_batch_malformed = 0
    malformed_count = 0
    total_processed = 0
    
    current_batch_file = open(os.path.join(output_dir, f"batch_{batch_id}.txt"), 'w', encoding='latin-1')

    print(f"Starting data prep & batching. Batch size: {batch_size}")

    for file_path in input_files:
        print(f"Processing: {file_path}")
        with _open_log_file(file_path) as f:
            for line in f:
                total_processed += 1
                if parse_log_line(line.strip()) is None:
                    malformed_count += 1
                    current_batch_malformed += 1

                # Write the original raw line so Pig can parse the batch file directly.
                current_batch_file.write(line)
                current_batch_records += 1

                # Roll over to next batch if limit reached
                if current_batch_records >= batch_size:
                    current_batch_file.close()
                    batch_files.append((batch_id, os.path.join(output_dir, f"batch_{batch_id}.txt"), current_batch_records, current_batch_malformed))
                    batch_id += 1
                    current_batch_records = 0
                    current_batch_malformed = 0
                    current_batch_file = open(os.path.join(output_dir, f"batch_{batch_id}.txt"), 'w', encoding='latin-1')

    # Close the last batch file
    current_batch_file.close()
    if current_batch_records > 0:
        batch_files.append((batch_id, os.path.join(output_dir, f"batch_{batch_id}.txt"), current_batch_records, current_batch_malformed))
    
    # Cleanup if the last batch file was opened but remained empty
    if current_batch_records == 0:
        os.remove(os.path.join(output_dir, f"batch_{batch_id}.txt"))
        batch_id -= 1

    print("\n--- Data Prep Summary ---")
    print(f"Total Records Processed: {total_processed}")
    print(f"Total Malformed Records (Counted): {malformed_count}")
    print(f"Total Batches Generated: {batch_id}")
    print(f"Data ready in: {output_dir}/")
    return batch_files, total_processed, malformed_count


def _open_log_file(file_path):
    if file_path.endswith(".gz"):
        return gzip.open(file_path, 'rt', encoding='ascii', errors='ignore')
    return open(file_path, 'r', encoding='latin-1', errors='ignore')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse NASA logs and batch them into TSV files")
    parser.add_argument("input_files", nargs="+", help="Input NASA log files (.gz)")
    parser.add_argument("--output-dir", default="data/output/batches", help="Directory to write batch TSV files")
    parser.add_argument("--batch-size", type=int, default=500000, help="Number of valid records per batch")
    args = parser.parse_args()

    process_and_batch_logs(args.input_files, args.output_dir, batch_size=args.batch_size)