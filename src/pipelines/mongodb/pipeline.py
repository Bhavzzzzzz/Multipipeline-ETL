import csv
import os
import re
import sys
from pymongo import MongoClient

# Master Regex for NASA HTTP Logs (Common Log Format)
LOG_REGEX = re.compile(
    r'^(?P<host>\S+)\s+\S+\s+\S+\s+\[(?P<timestamp>[^\]]+)\]\s+"(?P<request>[^"]*)"\s+(?P<status>\d{3})\s+(?P<bytes>\d+|-)$'
)

def parse_log_line(line):
    """Parses a single log line into a dictionary for MongoDB insertion."""
    match = LOG_REGEX.match(line.strip())
    if not match:
        return None

    data = match.groupdict()
    
    try:
        raw_timestamp = data['timestamp']
        log_date = raw_timestamp.split(':')[0]
        log_hour = raw_timestamp.split(':')[1]
    except IndexError:
        return None

    request_parts = data['request'].split()
    if len(request_parts) == 3:
        http_method, resource_path, protocol_version = request_parts
    elif len(request_parts) == 2:
        http_method, resource_path = request_parts
        protocol_version = "UNKNOWN"
    else:
        return None

    bytes_transferred = 0 if data['bytes'] == '-' else int(data['bytes'])

    return {
        "host": data['host'],
        "log_date": log_date,
        "log_hour": log_hour,
        "http_method": http_method,
        "resource_path": resource_path,
        "protocol_version": protocol_version,
        "status_code": int(data['status']),
        "bytes_transferred": bytes_transferred
    }

def run_pipeline(input_path, output_dir):
    """Executes the MongoDB ETL pipeline."""
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
    db = client[os.getenv("MONGO_DB", "nosql_project")]
    collection = db["logs"]
    
    # 1. Clear existing data for this batch (or just drop it since we process one batch at a time)
    collection.drop()
    
    # 2. Parse and Load
    records = []
    with open(input_path, 'r', encoding='latin-1') as f:
        for line in f:
            record = parse_log_line(line)
            if record:
                records.append(record)
            
            # Batch insert for efficiency
            if len(records) >= 5000:
                collection.insert_many(records)
                records = []
    
    if records:
        collection.insert_many(records)
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 3. Query 1: Daily Traffic Summary
    q1_result = collection.aggregate([
        {
            "$group": {
                "_id": {"log_date": "$log_date", "status_code": "$status_code"},
                "request_count": {"$sum": 1},
                "total_bytes": {"$sum": "$bytes_transferred"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "log_date": "$_id.log_date",
                "status_code": "$_id.status_code",
                "request_count": 1,
                "total_bytes": 1
            }
        }
    ])
    
    with open(os.path.join(output_dir, "query1"), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for row in q1_result:
            writer.writerow([row['log_date'], row['status_code'], row['request_count'], row['total_bytes']])

    # 4. Query 2: Top Requested Resources
    q2_result = collection.aggregate([
        {
            "$group": {
                "_id": "$resource_path",
                "request_count": {"$sum": 1},
                "total_bytes": {"$sum": "$bytes_transferred"},
                "distinct_hosts": {"$addToSet": "$host"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "resource_path": "$_id",
                "request_count": 1,
                "total_bytes": 1,
                "distinct_host_count": {"$size": "$distinct_hosts"}
            }
        },
        {"$sort": {"request_count": -1}},
        {"$limit": 20}
    ])
    
    with open(os.path.join(output_dir, "query2"), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for row in q2_result:
            writer.writerow([row['resource_path'], row['request_count'], row['total_bytes'], row['distinct_host_count']])

    # 5. Query 3: Hourly Error Analysis
    q3_result = collection.aggregate([
        {
            "$group": {
                "_id": {"log_date": "$log_date", "log_hour": "$log_hour"},
                "error_request_count": {
                    "$sum": {
                        "$cond": [
                            {"$and": [{"$gte": ["$status_code", 400]}, {"$lte": ["$status_code", 599]}]},
                            1,
                            0
                        ]
                    }
                },
                "total_request_count": {"$sum": 1},
                "error_hosts": {
                    "$addToSet": {
                        "$cond": [
                            {"$and": [{"$gte": ["$status_code", 400]}, {"$lte": ["$status_code", 599]}]},
                            "$host",
                            "$$REMOVE"
                        ]
                    }
                }
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
                "distinct_error_hosts": {"$size": "$error_hosts"}
            }
        }
    ])
    
    with open(os.path.join(output_dir, "query3"), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for row in q3_result:
            writer.writerow([
                row['log_date'], 
                row['log_hour'], 
                row['error_request_count'], 
                row['total_request_count'], 
                row['error_rate'], 
                row['distinct_error_hosts']
            ])

    client.close()
    print(f"[SUCCESS] MongoDB pipeline completed. Results in {output_dir}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python pipeline.py <input_path> <output_dir>")
        sys.exit(1)
    
    run_pipeline(sys.argv[1], sys.argv[2])
