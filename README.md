# Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics

This repository contains the prototype for a multi-pipeline ETL (Extract, Transform, Load) and reporting tool. The objective is to process semi-structured web server logs using different data processing paradigms (Apache Pig, MapReduce, Hive, MongoDB) while maintaining identical logical ETL steps and query definitions for fair comparison.

This project was developed for the DAS 839-NoSQL Systems End Semester Project.

---

## 📊 Dataset

This project uses the official **NASA HTTP Web Server Logs** (July/August 1995) from the Internet Traffic Archive.

* **Format:** ASCII text log files, one HTTP request per line.
* **Fields extracted:** `host`, `timestamp`, `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, and `bytes_transferred`.
* **Valid analytical record:** A line must match the common log shape and its quoted request must contain exactly `method resource protocol`. Lines that do not expose all required fields are counted as malformed and skipped by every pipeline.
* **Important:** Do not manually clean or preprocess the files outside of the defined ETL pipelines.

---

## 🏗️ Architecture & Core Infrastructure

The framework is orchestrated by a Python controller that physically batches the data, triggers the selected execution engine, and handles the database loading phase.

1. **Orchestration & Controller (Python):** Slices the massive log files into sequential physical batches and triggers the execution jobs.
2. **Execution Pipelines:** Apache Pig, local MapReduce, Apache Hive, and MongoDB.
3. **Reporting Database (PostgreSQL):** Stores the final aggregated query results alongside execution metadata (pipeline name, run identifier, batch ID, batch size, runtime, and malformed-record count).

## 📁 File Structure
```text
Multipipeline-ETL/
├── README.md
├── setup.sh                         # local/gitignored environment file
├── temp.md
├── temp2.md
├── .gitignore
├── data/
│   ├── output/
│   │   ├── pig_results/
│   │   └── staging_batches/
│   └── raw/
│       ├── NASA_access_log_Aug95.gz
│       ├── NASA_access_log_Jul95.gz
│       ├── access_log_Aug95
│       └── access_log_Jul95
├── database/
│   ├── reset_and_create.sql
│   └── schema.sql
├── docs/
│   ├── NoSQL26_ET_project_statement.pdf
│   └── phase1_status.md
└── src/
    ├── controllers/
    │   ├── main.py
    │   ├── env_utils.py
    │   ├── utils.py
    │   └── db_client.py
    └── pipelines/
      ├── pig/
      │   └── queries.pig
      ├── hive/
      │   └── queries.hql
      ├── mapreduce/
      │   └── queries.py
      └── mongodb/
        └── pipeline.py
```

The key files for the current phase are:

* `src/controllers/main.py` - orchestrates batching, backend execution, and DB loading.
* `src/controllers/env_utils.py` - centralizes runtime environment checks shared by the CLI and orchestrator.
* `src/controllers/utils.py` - creates record-based batches from the raw input files and counts malformed lines.
* `src/controllers/db_client.py` - loads pipeline results into PostgreSQL.
* `src/pipelines/pig/queries.pig` - performs the Pig ETL and aggregation work.
* `src/pipelines/hive/queries.hql` - performs the Hive ETL and aggregation work.
* `src/pipelines/mapreduce/queries.py` - performs the local MapReduce-style ETL and aggregation work.
* `src/pipelines/mongodb/pipeline.py` - performs the MongoDB ETL and aggregation work.
* `setup.sh` - local/gitignored file that exports the environment variables used by `main.py` and `reporting.py`.
* `database/schema.sql` and `database/reset_and_create.sql` - define and recreate the reporting schema.

---

## 🔍 Analytical Workload
All pipelines must successfully compute the following three mandatory queries using the exact same output schemas:

* **Query 1: Daily Traffic Summary** - Computes total request count and bytes transferred per `log_date` and `status_code`.
* **Query 2: Top Requested Resources** - Identifies the top 20 requested resource paths by request count, including distinct hosts.
* **Query 3: Hourly Error Analysis** - Calculates error rates (status codes 400-599) and distinct error-generating hosts per `log_date` and `log_hour`.

---

## 🚀 Setup & Execution

### Prerequisites
* Java 17 (OpenJDK, required for Pig and Hive 4)
* Python 3.8+
* Apache Pig (Local Mode)
* Apache Hadoop (Local Mode support for Hive)
* Apache Hive 4.x (Local Mode)
* MongoDB Community Server or a reachable MongoDB instance
* PostgreSQL (Running inside WSL/Ubuntu or systemd Linux recommended)
* `psycopg2-binary` (Python library for PostgreSQL)
* `pymongo` (Python library for MongoDB)
* `tqdm` (CLI progress bar used by the controller)

### Environment setup

Use the following order. Each step has a quick verification command so you can confirm it worked before moving on.

1) Install the system packages you need:

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk python3 python3-pip python3-venv postgresql postgresql-contrib wget curl gnupg tar
```

If you want MongoDB on the same machine, install it using MongoDB's official repository for your Ubuntu release.

2) Verify Java, Python, and PostgreSQL:

```bash
java -version
python3 --version
psql --version
```

3) Create the virtual environment and install Python packages:

```bash
cd /mnt/c/Codes/Multipipeline-ETL
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

python -c "import psycopg2, pymongo, tqdm; print('Python dependencies OK')"
```

4) Install Pig under `/opt` and verify it:

```bash
cd /tmp
wget https://downloads.apache.org/pig/pig-0.18.0/pig-0.18.0.tar.gz
sudo tar -xzf pig-0.18.0.tar.gz -C /opt

pig -version
java -version
```

5) Install Hadoop under `/opt` and verify it:

```bash
cd /tmp
wget -c https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
sudo tar -xzf hadoop-3.3.6.tar.gz -C /opt

/opt/hadoop-3.3.6/bin/hadoop version
```

6) Install Hive 4 under `/opt` and initialize the local metastore:

```bash
cd /tmp
wget -c https://archive.apache.org/dist/hive/hive-4.1.0/apache-hive-4.1.0-bin.tar.gz
sudo tar -xzf apache-hive-4.1.0-bin.tar.gz -C /opt

cd /mnt/c/Codes/Multipipeline-ETL
mkdir -p data/hive/warehouse
schematool -dbType derby -initSchema

/opt/apache-hive-4.1.0-bin/bin/hive --version
/opt/apache-hive-4.1.0-bin/bin/beeline --version
```

The Hive pipeline uses a project-local warehouse under `data/hive/warehouse`.
`main.py` renders that path into the Hive script for each run, so you do not need to create or use `/user/hive/warehouse`.
Hive also stages the current batch table under the selected batch output directory before writing `query1`, `query2`, and `query3`.
Avoid very small Hive batch sizes for full-dataset experiments. Hive has high per-batch startup and MapReduce planning overhead, so a batch size like `1000` can take many hours across thousands of batches. Use the same batch size across pipelines for a fair experiment, but prefer larger values such as `100000` or `1000000` when comparing Hive.

7) Install MongoDB Community Server if you want to run the MongoDB pipeline locally.

Use MongoDB's official Ubuntu instructions for your Ubuntu release:
https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/

After installation, verify the binaries, start the `mongod` service, and confirm that the server accepts connections:

```bash
mongod --version
mongosh --version

sudo service mongod start
service mongod status
mongosh --quiet --eval 'db.runCommand({ ping: 1 })'
```

The service name used by the official MongoDB packages is usually `mongod`, not `mongodb`.
If the commands above are missing, install MongoDB from the official MongoDB repository for your Ubuntu release, use MongoDB Atlas, or use Docker and set `MONGO_URI` accordingly.

8) Create your local `setup.sh`, source it, and verify the key variables:

```bash
cd /mnt/c/Codes/Multipipeline-ETL
source setup.sh

echo "$JAVA_HOME"
echo "$PIG_HOME"
echo "$HADOOP_HOME"
echo "$HIVE_HOME"
echo "$PGDATABASE"
echo "$MONGO_URI"
echo "$MONGO_DB"
```

9) Create the PostgreSQL schema:

```bash
sudo service postgresql start
sudo -u postgres createdb nosql_project 2>/dev/null || true
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'your_password';"
sudo -u postgres psql -d nosql_project -f database/reset_and_create.sql
```

10) Launch the CLI:

```bash
source venv/bin/activate
source setup.sh
sudo service postgresql start
sudo service mongod start
python src/controllers/reporting.py
```

11) Manual execution, if needed:

```bash
source venv/bin/activate
source setup.sh

python src/controllers/main.py --pipeline pig --batch-size 100000 --input data/raw/NASA_access_log_Jul95.gz
python src/controllers/main.py --pipeline hive --batch-size 100000 --input data/raw/NASA_access_log_Jul95.gz
python src/controllers/main.py --pipeline mongodb --batch-size 100000 --input data/raw/NASA_access_log_Jul95.gz
```

---

## 🚀 Environment Variables

Before running `reporting.py` or `main.py`, ensure the following variables are set in your session. The easiest option is to edit the password/path values in your local `setup.sh`, then source it:

```bash
source setup.sh
```

The file exports the same variables shown below. You can also add these to your `.bashrc` or a shell profile:

Copy the example below into your local `setup.sh` and keep it untracked:

```bash
#!/usr/bin/env bash

# Database (PostgreSQL)
export PGDATABASE=nosql_project
export PGUSER=postgres
export PGPASSWORD='your_password'
export PGHOST=localhost
export PGPORT=5432

# MongoDB
export MONGO_URI='mongodb://localhost:27017/'
export MONGO_DB='nosql_project'

# Big Data Tools
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PIG_HOME=/opt/pig-0.18.0
export HADOOP_HOME=/opt/hadoop-3.3.6
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HIVE_HOME=/opt/apache-hive-4.1.0-bin
export HIVE_BIN="$HIVE_HOME/bin/hive"
export HIVE_BEELINE_BIN="$HIVE_HOME/bin/beeline"
export HIVE_JDBC_URL='jdbc:hive2://'
export PATH="$JAVA_HOME/bin:$PIG_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$PATH"
export PIG_CLASSPATH=/usr/share/java/commons-text.jar:/usr/share/java/commons-compress.jar:/usr/share/java/commons-lang3.jar:$PIG_CLASSPATH
```

### How to find the install locations

Use these commands on your machine to discover the correct paths before filling the variables above:

```bash
# Java
which java
readlink -f "$(which java)"

# Pig
which pig
pig -version

# Hadoop
which hadoop
hadoop version

# Hive / Beeline
which hive
which beeline
hive --version

# PostgreSQL
which psql
psql --version

# MongoDB
which mongod
which mongosh
mongosh --quiet --eval 'db.runCommand({ ping: 1 })'
```

Typical path patterns on Linux are:

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
PIG_HOME=/opt/pig-0.18.0
HADOOP_HOME=/opt/hadoop-3.3.6
HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
HIVE_HOME=/opt/apache-hive-4.1.0-bin
HIVE_BIN=$HIVE_HOME/bin/hive
HIVE_BEELINE_BIN=$HIVE_HOME/bin/beeline
```

For MongoDB, `MONGO_URI` is usually `mongodb://localhost:27017/` when `mongod` is running locally. Use a remote MongoDB or Atlas connection string if that is where your database runs. `MONGO_DB` can stay `nosql_project` unless you intentionally want a different database name.

The MongoDB pipeline loads each physical batch into the `logs` collection inside `MONGO_DB`, drops that collection for the current batch run, computes the three aggregations, and writes local output files that are then loaded into PostgreSQL. Do not point `MONGO_DB` at a database that contains unrelated data in a `logs` collection you care about.

Keep the actual values in your own local `setup.sh` or shell profile, not in git.
