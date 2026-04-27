import gzip
import re
import os
from datetime import datetime

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
    Reads compressed logs, parses them, and splits them into sequential batches.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    batch_id = 1
    current_batch_records = 0
    malformed_count = 0
    total_processed = 0
    
    current_batch_file = open(os.path.join(output_dir, f"batch_{batch_id}.tsv"), 'w', encoding='utf-8')

    print(f"Starting data prep & parsing. Batch size: {batch_size}")

    for file_path in input_files:
        print(f"Processing: {file_path}")
        with gzip.open(file_path, 'rt', encoding='ascii', errors='ignore') as f:
            for line in f:
                parsed_line = parse_log_line(line.strip())
                
                if parsed_line is None:
                    malformed_count += 1
                    continue
                
                # Write to current batch
                current_batch_file.write(parsed_line)
                current_batch_records += 1
                total_processed += 1

                # Roll over to next batch if limit reached
                if current_batch_records >= batch_size:
                    current_batch_file.close()
                    batch_id += 1
                    current_batch_records = 0
                    current_batch_file = open(os.path.join(output_dir, f"batch_{batch_id}.tsv"), 'w', encoding='utf-8')

    # Close the last batch file
    current_batch_file.close()
    
    # Cleanup if the last batch file was opened but remained empty
    if current_batch_records == 0:
        os.remove(os.path.join(output_dir, f"batch_{batch_id}.tsv"))
        batch_id -= 1

    print("\n--- Data Prep Summary ---")
    print(f"Total Valid Records: {total_processed}")
    print(f"Total Malformed Records (Skipped): {malformed_count}")
    print(f"Total Batches Generated: {batch_id}")
    print(f"Data ready in: {output_dir}/")

# Example execution (Member 4 will eventually call this from main.py)
if __name__ == "__main__":
    raw_files = [
        "data/raw/NASA_access_log_Jul95.gz",
        "data/raw/NASA_access_log_Aug95.gz"
    ]
    output_directory = "data/output/batches"
    
    # 500,000 records per batch is a good starting point for MapReduce testing
    process_and_batch_logs(raw_files, output_directory, batch_size=500000)