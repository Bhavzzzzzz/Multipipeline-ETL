# Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics

This repository contains the prototype for a multi-pipeline ETL (Extract, Transform, Load) and reporting tool. The objective is to process semi-structured web server logs using different data processing paradigms (Apache Pig, Hive, MongoDB) while maintaining identical logical ETL steps and query definitions for fair comparison.

This project was developed for the DAS 839-NoSQL Systems End Semester Project.

---

## 📊 DatasetThis project uses the official **NASA HTTP Web Server Logs** (July/August 1995) from the Internet Traffic Archive. 

* **Format:** ASCII text log files, one HTTP request per line.
* **Fields extracted:** `host`, `timestamp`, `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, and `bytes_transferred`.
* **Important:** Do not manually clean or preprocess the files outside of the defined ETL pipelines.

---

## 🏗️ Architecture & Core Infrastructure

The framework is orchestrated by a Python controller that physically batches the data, triggers the selected execution engine, and handles the database loading phase.

1. **Orchestration & Controller (Python):** Slices the massive log files into sequential physical batches and triggers the execution jobs.
2.  **Execution Pipelines:**
    * **Phase 1:** Apache Pig (replacing MapReduce).
    * **Phase 2:** Apache Hive & MongoDB.
3. **Reporting Database (PostgreSQL):** Stores the final aggregated query results alongside execution metadata (pipeline name, run identifier, batch ID, runtime, etc.).

---

## 🔍 Analytical Workload
All pipelines must successfully compute the following three mandatory queries using the exact same output schemas:

* **Query 1: Daily Traffic Summary** - Computes total request count and bytes transferred per `log_date` and `status_code`.
* **Query 2: Top Requested Resources** - Identifies the top 20 requested resource paths by request count, including distinct hosts.
* **Query 3: Hourly Error Analysis** - Calculates error rates (status codes 400-599) and distinct error-generating hosts per `log_date` and `log_hour`.

---

## 🚀 Setup & Execution

### Prerequisites
* Python 3.8+
* Apache Pig (Local Mode)
* PostgreSQL (Running inside WSL/Ubuntu recommended)
* `psycopg2` (Python library for PostgreSQL)

### 1. Data Preparation
Create the required local directories (these are ignored by `.gitignore`) and download the NASA logs:
```bash
mkdir -p data/raw data/output
# Download the dataset into data/raw/
```

### 2. Running the Pipeline (Phase 1)
To execute the ETL flow using the Apache Pig pipeline, use the Python orchestrator:

```bash
python src/controller/main.py --pipeline pig --batch-size 100000 --input data/raw/NASA_access_log_Jul95.txt
```

### 3. Output
The orchestrator will output batch results into `data/output/pig_results/batch_<id>/` and generate a final console execution report measuring total runtime and batch statistics.

---

## 👥 Team Roles & Handoffs

To keep development clean and prevent merge conflicts, responsibilities are divided as follows:

* **Member 1 (Data & Controller):** * Design the master regex for log parsing.
    * Build the core `main.py` Python orchestrator to handle physical file batching and sequential execution triggering. *(Completed)*
* **Member 2 (Pig Pipeline):** * Write the Apache Pig scripts (`queries.pig`) to handle the ETL aggregations for all three queries. *(Completed)*
* **Member 3 (Database & Ingestion):** * **[NEXT STEP]** Design the PostgreSQL schema for the three queries (`database/schema.sql`).
    * Implement `src/controller/db_client.py` using `psycopg2`.
    * *Integration note:* Hook your ingestion function into the `trigger_db_load()` handoff point inside `src/controller/main.py`. The controller will pass you the `batch_id`, the directory containing the Pig CSV outputs, and a dictionary of run metadata.
* **Member 4 (Reporting UI & Phase 2 Pipelines):** * Build the CLI dashboard in `src/controller/reporting.py` to query PostgreSQL and render the final formatted console output.
    * Begin scaffolding Hive and MongoDB pipelines for Phase 2.