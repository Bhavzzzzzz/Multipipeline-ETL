# Multi-Pipeline ETL & Reporting Framework
### Web Server Log Analytics · DAS 839 — NoSQL Systems End Semester Project

![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Pig](https://img.shields.io/badge/Apache%20Pig-0.18.0-F5820D?style=flat-square)
![Apache Hive](https://img.shields.io/badge/Apache%20Hive-4.1.0-FDEE21?style=flat-square&logoColor=black)
![MongoDB](https://img.shields.io/badge/MongoDB-Community-47A248?style=flat-square&logo=mongodb&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Reporting%20DB-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Java](https://img.shields.io/badge/Java-17%20(OpenJDK)-ED8B00?style=flat-square&logo=openjdk&logoColor=white)

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Dataset](#2-dataset)
3. [Architecture](#3-architecture)
   - [3.1 Orchestration & Controller](#31-orchestration--controller)
   - [3.2 Execution Pipelines](#32-execution-pipelines)
   - [3.3 Reporting Database](#33-reporting-database)
4. [Repository Structure](#4-repository-structure)
5. [Analytical Workload](#5-analytical-workload)
6. [Setup & Installation](#6-setup--installation)
7. [Environment Variables Reference](#7-environment-variables-reference)
8. [Running the Framework](#8-running-the-framework)
9. [Notes & Caveats](#9-notes--caveats)

---

## 1. Project Overview

This repository contains the prototype for a **multi-pipeline ETL and reporting framework** designed to process semi-structured web server logs. The goal is to evaluate different data processing paradigms — **Apache Pig**, **MapReduce**, **Apache Hive**, and **MongoDB** — while maintaining identical logical ETL steps and query definitions across all pipelines, enabling a fair, apples-to-apples performance comparison.

A Python controller orchestrates the entire workflow: it physically batches the raw input data, triggers the selected execution engine, collects results, and loads them into a central PostgreSQL reporting database alongside execution metadata.

---

## 2. Dataset

This project uses the official **NASA HTTP Web Server Logs** (July & August 1995) from the Internet Traffic Archive.

| Property | Detail |
|---|---|
| **Format** | ASCII text, one HTTP request per line |
| **Files in `data/raw/`** | `NASA_access_log_Jul95.gz`, `NASA_access_log_Aug95.gz`, `access_log_Jul95`, `access_log_Aug95` |
| **Fields Extracted** | `host`, `timestamp`, `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, `bytes_transferred` |
| **Valid Record** | Line must match the common log format; quoted request must contain exactly `method resource protocol` |
| **Malformed Lines** | Lines failing the above check are counted and skipped by every pipeline — not silently dropped |

The shared batching utility accepts both compressed (`.gz`) and plain-text log files, so the controller can work with either source form.

> ⚠️ **Important:** Do **not** manually clean or preprocess the raw log files outside of the defined ETL pipelines.

---

## 3. Architecture

### 3.1 Orchestration & Controller

A Python controller (`src/controllers/main.py`) drives the entire framework. It:
- Slices large log files into sequential, record-based physical batches.
- Invokes the selected execution engine for each batch.
- Collects aggregated output and loads it into PostgreSQL.

### 3.2 Execution Pipelines

Each pipeline implements the same three analytical queries (see [§5](#5-analytical-workload)) against an identical input schema.

| Pipeline | Mode | Entry Point |
|---|---|---|
| Apache Pig | Local | `src/pipelines/pig/queries.pig` |
| MapReduce | Hadoop Streaming (requires Hadoop) | `src/pipelines/mapreduce/mapper.py` + `src/pipelines/mapreduce/reducer.py` |
| Apache Hive | Local (Beeline/JDBC) | `src/pipelines/hive/queries.hql` |
| MongoDB | Aggregation Pipeline | `src/pipelines/mongodb/pipeline.py` |

### 3.3 Reporting Database

A **PostgreSQL** database stores the final aggregated query results alongside execution metadata for every run.

**Metadata captured per batch run:**

| Field | Description |
|---|---|
| `pipeline_name` | Engine used (Pig / MapReduce / Hive / MongoDB) |
| `query_name` | Query executed for the batch (`query1`, `query2`, `query3`, or `all`) |
| `run_identifier` | Unique run label for the execution |
| `batch_id` | Sequential batch index |
| `batch_size` | Number of records in the batch |
| `records_processed` | Records processed for the batch |
| `average_batch_size` | Average batch size across the run |
| `runtime_seconds` | Wall-clock execution time for the batch |
| `malformed_record_count` | Lines skipped due to parse failure |
| `execution_timestamp` | Timestamp captured when the metadata row is inserted |

---

## 4. Repository Structure

```text
Multipipeline-ETL/
├── README.md
├── Dockerfile                         # ETL runtime image (Python, Java, Pig, Hadoop, Hive)
├── docker-compose.yml                 # PostgreSQL, MongoDB, and ETL services
├── .dockerignore
├── .env.example                       # Shared Docker/native environment template
├── requirements.txt
├── .env                              # Local environment variables (gitignored)
├── .gitignore
├── data/
│   ├── hive/
│   │   └── warehouse/
│   ├── output/
│   │   ├── hive_results/
│   │   ├── mapreduce_results/
│   │   ├── mongodb_results/
│   │   ├── pig_results/
│   │   └── staging_batches/
│   └── raw/
│       ├── NASA_access_log_Aug95.gz
│       ├── NASA_access_log_Jul95.gz
│       ├── access_log_Aug95
│       └── access_log_Jul95
├── database/
│   ├── schema.sql                    # Reporting schema definition
│   └── reset_and_create.sql          # Drops and recreates the schema
├── docs/
│   ├── NoSQL26_ET_project_statement.pdf
│   ├── Phase 1 Status Report.docx
│   ├── Project_evaluation_guidelines_2026.pdf
│   ├── WhatsApp Image 2026-04-28 at 03.15.27.jpeg
│   ├── temp.md
│   ├── temp2.md
│   └── phase1_status.md
└── src/
    ├── controllers/
    │   ├── main.py                   # Orchestrates batching, execution, and DB loading
    │   ├── reporting.py              # Interactive CLI for running pipelines and viewing reports
    │   ├── db_client.py              # Loads pipeline results into PostgreSQL
    │   ├── env_utils.py              # Runtime environment checks (shared by CLI & orchestrator)
    │   ├── utils.py                  # Record-based batch creation and malformed-line counting
    └── pipelines/
        ├── common/
        │   └── nasa_log_common.py     # Shared parsing and output helpers
        ├── pig/
        │   ├── queries.pig
        │   ├── query1.pig
        │   ├── query2.pig
        │   └── query3.pig
        ├── hive/
        │   ├── queries.hql
        │   ├── query1.hql
        │   ├── query2.hql
        │   └── query3.hql
        ├── mapreduce/
        │   ├── queries.py
        │   ├── mapper.py
        │   └── reducer.py
        └── mongodb/
            ├── pipeline.py
            ├── query1.py
            ├── query2.py
            └── query3.py
```

The workspace also contains generated or local-only artifacts such as `derby.log`, `metastore_db/`, and `venv/`; these are intentionally not part of the project source tree.

---

## 5. Analytical Workload

All four pipelines compute the following three queries using **identical output schemas**.

### Query 1 — Daily Traffic Summary
Computes total request count and bytes transferred, grouped by `log_date` and `status_code`.

### Query 2 — Top Requested Resources
Identifies the **top 20** resource paths by request count, also reporting the number of distinct requesting hosts.

### Query 3 — Hourly Error Analysis
Calculates error rates for status codes **400–599** and counts distinct error-generating hosts per `log_date` and `log_hour`.

---

## 6. Setup & Installation

### Recommended Path — Docker

The Docker setup runs PostgreSQL and MongoDB as separate services and builds one ETL image containing Python, Java 17, Pig, Hadoop, and Hive.

Prerequisites:

| Dependency | Notes |
|---|---|
| Docker Engine | Required to build and run the containers |
| Docker Compose v2 | Usually available as `docker compose` |
| Raw NASA logs | Keep them in `data/raw/` on the host |

Build the ETL image and start the databases:

```bash
cp .env.example .env
# Edit .env and set PGPASSWORD plus any native tool paths you need.
docker compose build etl
docker compose up -d
```

Verify the ETL runtime:

```bash
docker compose run --rm etl python -c "import psycopg2, pymongo, tqdm; print('Python dependencies OK')"
docker compose run --rm etl pig -version
docker compose run --rm etl hadoop version
docker compose run --rm etl hive --version
```

Run the interactive CLI:

```bash
docker compose run --rm etl python src/controllers/reporting.py
```

Run a pipeline directly:

```bash
docker compose run --rm etl python src/controllers/main.py \
    --pipeline mongodb \
    --query query1 \
    --batch-size 100000 \
    --input data/raw/NASA_access_log_Jul95.gz
```

Reset the reporting schema:

```bash
docker compose run --rm etl python src/controllers/main.py \
    --pipeline mongodb \
    --query query1 \
    --batch-size 100000 \
    --input data/raw/NASA_access_log_Jul95.gz \
    --reset-db
```

Stop the services:

```bash
docker compose down
```

Remove database volumes and start from a completely clean database state:

```bash
docker compose down -v
```

Docker reads shared values such as `PGDATABASE`, `PGUSER`, `PGPASSWORD`, and `MONGO_DB` from `.env`. The native `PGHOST=localhost` and `MONGO_URI=mongodb://localhost:27017/` values are intentionally overridden inside Docker, where service hostnames are used instead: `PGHOST=postgres` and `MONGO_URI=mongodb://mongo:27017/`.

Keep secrets in a local `.env` file. This file is gitignored and should not be committed. Docker Compose reads it automatically; native shell runs can load it with `source .env`. If the Postgres volume already exists, changing `PGPASSWORD` in `.env` will not update the existing database password; use `docker compose down -v` to recreate the development database, or change the password inside Postgres manually.

### Native Setup

Use the native setup only if you want to run the project directly on your host machine without Docker.

### Setup Index

- [Step 1 — System Packages](#step-1--install-system-packages)
- [Step 2 — Verify Core Dependencies](#step-2--verify-core-dependencies)
- [Step 3 — Python Virtual Environment](#step-3--python-virtual-environment)
- [Step 4 — Apache Pig](#step-4--apache-pig)
- [Step 5 — Apache Hadoop](#step-5--apache-hadoop)
- [Step 6 — Apache Hive 4](#step-6--apache-hive-4)
- [Step 7 — MongoDB](#step-7--mongodb)
- [Step 8 — Environment Variables (`.env`)](#step-8--environment-variables-env)
- [Step 9 — PostgreSQL Schema](#step-9--postgresql-schema)

---

### Prerequisites

| Dependency | Version | Notes |
|---|---|---|
| Java (OpenJDK) | 17 | Required by Pig and Hive 4 |
| Python | 3.8+ | Controller and MapReduce pipeline |
| Apache Pig | 0.18.0 | Local mode |
| Apache Hadoop | 3.3.6 | Local mode support for Hive |
| Apache Hive | 4.1.0 | Local mode via Beeline |
| MongoDB | Community | Local or remote instance |
| PostgreSQL | Any recent | Reporting database |
| `psycopg2-binary` | pip | PostgreSQL Python client |
| `pymongo` | pip | MongoDB Python client |
| `tqdm` | pip | CLI progress bar |

---

### Step 1 — Install System Packages

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk python3 python3-pip python3-venv \
    postgresql postgresql-contrib wget curl gnupg tar
```

For MongoDB, use the [official MongoDB repository](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/) for your Ubuntu release.

---

### Step 2 — Verify Core Dependencies

```bash
java -version
python3 --version
psql --version
```

---

### Step 3 — Python Virtual Environment

```bash
cd /mnt/c/Codes/Multipipeline-ETL
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Verify
python -c "import psycopg2, pymongo, tqdm; print('Python dependencies OK')"
```

---

### Step 4 — Apache Pig

```bash
cd /tmp
wget https://downloads.apache.org/pig/pig-0.18.0/pig-0.18.0.tar.gz
sudo tar -xzf pig-0.18.0.tar.gz -C /opt

# Verify
pig -version
```

---

### Step 5 — Apache Hadoop

```bash
cd /tmp
wget -c https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
sudo tar -xzf hadoop-3.3.6.tar.gz -C /opt

# Verify
/opt/hadoop-3.3.6/bin/hadoop version
```

#### Hadoop Streaming and MapReduce pipeline

This project uses **Hadoop Streaming** for the MapReduce pipeline. The MapReduce pipeline is implemented as a streaming job that runs the project-provided Python `mapper.py` and `reducer.py` under `src/pipelines/mapreduce/` and requires the `hadoop-streaming` jar to be available on the system.

Setup notes:

- Ensure `HADOOP_HOME` points at your Hadoop distribution (for example `/opt/hadoop-3.3.6`).
- Optionally set `HADOOP_BIN` to the `hadoop` executable path if it's not on `PATH`.
- Locate the streaming jar and set `HADOOP_STREAMING_JAR` to its full path. Typical location inside Hadoop distros:

```
$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar
```

Example environment exports to add to `.env`:

```bash
export HADOOP_HOME=/opt/hadoop-3.3.6
export HADOOP_BIN=$HADOOP_HOME/bin/hadoop
export HADOOP_STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
export PATH="$HADOOP_BIN:$PATH"
```

Run the MapReduce pipeline (controller will call this runner when you select the MapReduce engine):

```bash
# Run MapReduce through the controller (example)
python3 src/controllers/main.py --pipeline mapreduce --query query1 --input data/raw/access_log_Aug95 --batch-size 100000
```

Important: this pipeline has no local fallback — Hadoop and the streaming jar **must** be available. The runner will fail fast if the streaming jar or `hadoop` binary cannot be found.

---

### Step 6 — Apache Hive 4

```bash
cd /tmp
wget -c https://archive.apache.org/dist/hive/hive-4.1.0/apache-hive-4.1.0-bin.tar.gz
sudo tar -xzf apache-hive-4.1.0-bin.tar.gz -C /opt

# Initialize the local Derby metastore
cd /mnt/c/Codes/Multipipeline-ETL
mkdir -p data/hive/warehouse
schematool -dbType derby -initSchema

# Verify
/opt/apache-hive-4.1.0-bin/bin/hive --version
/opt/apache-hive-4.1.0-bin/bin/beeline --version
```

> 💡 **Hive Batch Size Note:** Hive carries high per-batch startup and MapReduce planning overhead. Avoid very small batch sizes (e.g., `1000`) for full-dataset experiments — they can result in many hours of runtime across thousands of batches. Prefer larger values such as `100000` or `1000000` when benchmarking Hive, and use a consistent batch size across all pipelines for fair comparison.
>
> The Hive pipeline uses a project-local warehouse under `data/hive/warehouse`. `main.py` renders this path into the Hive script at runtime — you do not need to create or configure `/user/hive/warehouse`.

---

### Step 7 — MongoDB

Follow the [official MongoDB installation guide](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/) for your Ubuntu release, then verify and start the service:

```bash
# Verify binaries
mongod --version
mongosh --version

# Start and check the service
sudo service mongod start
service mongod status
mongosh --quiet --eval 'db.runCommand({ ping: 1 })'
```

> ℹ️ The official package service name is `mongod`, not `mongodb`. If the commands above are unavailable, consider using MongoDB Atlas or Docker and set `MONGO_URI` accordingly.

> ⚠️ **Warning:** The MongoDB pipeline drops and recreates the `logs` collection inside `MONGO_DB` for each batch run. Do **not** point `MONGO_DB` at a database containing a `logs` collection with unrelated data you wish to keep.

---

### Step 8 — Environment Variables (`.env`)

Create a local `.env` file (this file is gitignored), populate it with the values below, and source it before native runs:

```bash
cp .env.example .env
source .env
```

**Template — copy into your local `.env`:**

```bash
# ── PostgreSQL ────────────────────────────────────────────────────────────────
export PGDATABASE=nosql_project
export PGUSER=postgres
export PGPASSWORD='your_password'
export PGHOST=localhost
export PGPORT=5432

# ── MongoDB ───────────────────────────────────────────────────────────────────
export MONGO_URI='mongodb://localhost:27017/'
export MONGO_DB='nosql_project'

# ── Big Data Tools ────────────────────────────────────────────────────────────
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PIG_HOME=/opt/pig-0.18.0
export HADOOP_HOME=/opt/hadoop-3.3.6
export HADOOP_BIN="$HADOOP_HOME/bin/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"
export HIVE_HOME=/opt/apache-hive-4.1.0-bin
export HIVE_BIN="$HIVE_HOME/bin/hive"
export HIVE_BEELINE_BIN="$HIVE_HOME/bin/beeline"
export HIVE_JDBC_URL='jdbc:hive2://'
export PATH="$JAVA_HOME/bin:$PIG_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$PATH"
export PIG_CLASSPATH=/usr/share/java/commons-text.jar:/usr/share/java/commons-compress.jar:/usr/share/java/commons-lang3.jar:$PIG_CLASSPATH
```

**To find the correct paths on your machine:**

```bash
# Java
readlink -f "$(which java)"

# Pig
which pig && pig -version

# Hadoop
which hadoop && hadoop version

# Hive / Beeline
which hive && hive --version
which beeline

# PostgreSQL
which psql && psql --version

# MongoDB
which mongod && mongod --version
which mongosh
```

Verify all key variables are exported correctly after sourcing:

```bash
source .env
echo "$JAVA_HOME"
echo "$PIG_HOME"
echo "$HADOOP_HOME"
echo "$HIVE_HOME"
echo "$PGDATABASE"
echo "$MONGO_URI"
echo "$MONGO_DB"
```

---

### Step 9 — PostgreSQL Schema

```bash
sudo service postgresql start
sudo -u postgres createdb nosql_project 2>/dev/null || true
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'your_password';"
sudo -u postgres psql -d nosql_project -f database/reset_and_create.sql
```

---

## 7. Environment Variables Reference

| Variable | Example Value | Description |
|---|---|---|
| `PGDATABASE` | `nosql_project` | Target PostgreSQL database |
| `PGUSER` | `postgres` | PostgreSQL user |
| `PGPASSWORD` | `your_password` | PostgreSQL password |
| `PGHOST` | `localhost` native, `postgres` Docker | PostgreSQL host |
| `PGPORT` | `5432` | PostgreSQL port |
| `MONGO_URI` | `mongodb://localhost:27017/` native, `mongodb://mongo:27017/` Docker | MongoDB connection string |
| `MONGO_DB` | `nosql_project` | MongoDB database name |
| `JAVA_HOME` | `/usr/lib/jvm/java-17-openjdk-amd64` | Java 17 installation root |
| `PIG_HOME` | `/opt/pig-0.18.0` | Apache Pig root |
| `HADOOP_HOME` | `/opt/hadoop-3.3.6` | Apache Hadoop root |
| `HADOOP_CONF_DIR` | `$HADOOP_HOME/etc/hadoop` | Hadoop configuration directory |
| `HIVE_HOME` | `/opt/apache-hive-4.1.0-bin` | Apache Hive root |
| `HIVE_BIN` | `$HIVE_HOME/bin/hive` | Hive executable path |
| `HIVE_BEELINE_BIN` | `$HIVE_HOME/bin/beeline` | Beeline executable path |
| `HIVE_JDBC_URL` | `jdbc:hive2://` | JDBC URL for local Hive (embedded) |

> 🔒 Keep actual credentials in your local `.env` or shell profile — never commit them to version control.

---

## 8. Running the Framework

With Docker, ensure the database services are running before executing pipeline commands:

```bash
docker compose up -d
```

Without Docker, ensure PostgreSQL and MongoDB are running, and that your virtual environment and environment variables are active before executing any of the native commands below.

### Docker CLI

```bash
docker compose run --rm etl python src/controllers/reporting.py
```

### Docker Pipeline Execution

```bash
# MongoDB
docker compose run --rm etl python src/controllers/main.py \
    --pipeline mongodb \
    --batch-size 100000 \
    --input data/raw/NASA_access_log_Jul95.gz

# Apache Pig
docker compose run --rm etl python src/controllers/main.py \
    --pipeline pig \
    --batch-size 100000 \
    --input data/raw/NASA_access_log_Jul95.gz

# MapReduce
docker compose run --rm etl python src/controllers/main.py \
    --pipeline mapreduce \
    --batch-size 100000 \
    --input data/raw/NASA_access_log_Jul95.gz

# Apache Hive
docker compose run --rm etl python src/controllers/main.py \
    --pipeline hive \
    --batch-size 100000 \
    --input data/raw/NASA_access_log_Jul95.gz
```

### Native Interactive CLI

```bash
source venv/bin/activate
source .env
sudo service postgresql start
sudo service mongod start

python src/controllers/reporting.py
```

### Native Pipeline Execution

```bash
source venv/bin/activate
source .env

# Apache Pig
python src/controllers/main.py --pipeline pig \
    --batch-size 100000 \
    --input data/raw/NASA_access_log_Jul95.gz

# Apache Hive
python src/controllers/main.py --pipeline hive \
    --batch-size 100000 \
    --input data/raw/NASA_access_log_Jul95.gz

# MongoDB
python src/controllers/main.py --pipeline mongodb \
    --batch-size 100000 \
    --input data/raw/NASA_access_log_Jul95.gz
```

---

## 9. Notes & Caveats

- **Raw data integrity:** Never manually preprocess the raw log files. All parsing and cleaning is the responsibility of the ETL pipelines.
- **Input formats:** The controller can read either the gzipped NASA logs or the extracted plain-text copies in `data/raw/`.
- **Hive performance:** Hive is not optimized for small batches. Use `--batch-size 100000` or larger for any meaningful experiment.
- **MongoDB `logs` collection:** The MongoDB pipeline drops and recreates this collection on every batch run. Avoid reusing `MONGO_DB` for any other data.
- **`.env` is gitignored:** This is intentional. Store all secrets and local paths there, never in tracked files.
- **Malformed records:** Every pipeline reports a malformed-record count per batch. These counts are stored in PostgreSQL and can be used to verify consistency across pipelines.
- **Typical install paths** (Linux): `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`, `PIG_HOME=/opt/pig-0.18.0`, `HADOOP_HOME=/opt/hadoop-3.3.6`, `HIVE_HOME=/opt/apache-hive-4.1.0-bin`. Adjust if your system differs.

---

<div align="center">
  <sub>DAS 839 — NoSQL Systems · Multi-Pipeline ETL Framework</sub>
</div>
