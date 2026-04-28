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
3. **Reporting Database (PostgreSQL):** Stores the final aggregated query results alongside execution metadata (pipeline name, run identifier, batch ID, batch size, runtime, and malformed-record count).

## 📁 File Structure

```text
Multipipeline-ETL/
├── README.md
├── temp.md
├── data/
│   ├── raw/
│   └── output/
├── database/
│   ├── schema.sql
│   ├── reset_and_create.sql
│   └── test_queries.sql
├── docs/
│   └── NoSQL26_ET_project_statement.pdf
└── src/
    ├── controllers/
    │   ├── main.py
    │   ├── utils.py
    │   └── db_client.py
    └── pipelines/
        ├── pig/
        ├── hive/
        ├── mongodb/
        └── mapreduce/
```

The key files for the current phase are:

* `src/controllers/main.py` - orchestrates batching, Pig execution, and DB loading.
* `src/controllers/utils.py` - parses log lines and creates batches from the raw input files.
* `src/controllers/db_client.py` - loads Pig results into PostgreSQL.
* `src/pipelines/pig/queries.pig` - performs the Pig ETL and aggregation work.
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
* Java 11 (OpenJDK)
* Python 3.8+
* Apache Pig (Local Mode)
* PostgreSQL (Running inside WSL/Ubuntu or systemd Linux recommended)
* `psycopg2` (Python library for PostgreSQL)

### Environment setup

These commands install and configure the runtime used by this project. They assume a Debian/Ubuntu-style system and that you want a system-wide Apache Pig install under `/opt` (recommended).

1) Install system packages (JDK, Python, Postgres tooling):

```bash
sudo apt update
sudo apt install -y openjdk-11-jdk python3 python3-pip python3-venv postgresql postgresql-contrib wget curl tar
```

2) Verify Java and Python:

```bash
java -version
python3 --version
psql --version
```

3) Create and activate a Python virtualenv for controller development, then install the dependencies:

```bash
cd /mnt/c/Codes/Multipipeline-ETL
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

4) Install Apache Pig system-wide under `/opt` (example uses Pig 0.18.0):

```bash
cd /tmp
wget https://downloads.apache.org/pig/pig-0.18.0/pig-0.18.0.tar.gz
sudo tar -xzf pig-0.18.0.tar.gz -C /opt

# Create a system profile so Pig is on PATH and Java is pointed to your JDK
sudo tee /etc/profile.d/pig.sh > /dev/null <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PIG_HOME=/opt/pig-0.18.0
export PATH="$PIG_HOME/bin:$PATH"
EOF

sudo chmod 644 /etc/profile.d/pig.sh
source /etc/profile.d/pig.sh

# optional convenient symlink
sudo ln -sfn /opt/pig-0.18.0/bin/pig /usr/local/bin/pig

# verify
pig -version
java -version
```

If `java -version` still reports Java 8, update your active Java binary before running Pig:

```bash
sudo update-alternatives --config java
sudo update-alternatives --config javac
```

Pig also needs the runtime jars used by this environment:

```bash
sudo apt install -y libcommons-lang3-java libcommons-compress-java libcommons-text-java
export PIG_CLASSPATH=/usr/share/java/commons-text.jar:/usr/share/java/commons-compress.jar:/usr/share/java/commons-lang3.jar:$PIG_CLASSPATH
```

Note: Do NOT install Pig inside `/usr/lib` (that's for JVM distributions). Keep Pig under `/opt` or `/usr/local` so it is easy to manage and not overwritten by package managers.

5) PostgreSQL setup (create DB and load schema)

The project schema is in `database/reset_and_create.sql`. Use it to create a fresh database state when needed:

```bash
sudo service postgresql start
sudo -u postgres psql -d nosql_project -f database/reset_and_create.sql
```

If you need to create the database first, use:

```bash
sudo service postgresql start
sudo -u postgres psql -c "CREATE DATABASE nosql_project;"
# set a password for postgres user (replace 'your_password' with your chosen password)
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'your_password';"
# load the schema into the new database
sudo -u postgres psql -d nosql_project -f database/reset_and_create.sql
# verify schema/tables (simple check)
sudo -u postgres psql -d nosql_project -c "\dt"
```

6) Launch the Orchestrator CLI (Recommended)

The reporting dashboard is the central entry point for the project. It handles environment checks, pipeline execution, and final data visualization.

```bash
source venv/bin/activate
# Ensure environment variables are exported (see below)
python src/controllers/reporting.py
```

### 7) Manual execution (Advanced)

If you prefer to run the orchestrator directly without the interactive CLI:

```bash
python src/controllers/main.py --pipeline pig --batch-size 100000 --input data/raw/access_log_Jul95
```

---

## 🚀 Environment Variables

Before running `reporting.py`, ensure the following variables are set in your session. You can add these to your `.bashrc` or a `.env` file:

```bash
# Database
export PGDATABASE=nosql_project
export PGUSER=postgres
export PGPASSWORD='your_password'
export PGHOST=localhost
export PGPORT=5432

# Big Data Tools
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PIG_HOME=/opt/pig-0.18.0
export PATH="$JAVA_HOME/bin:$PIG_HOME/bin:$PATH"
export PIG_CLASSPATH=/usr/share/java/commons-text.jar:/usr/share/java/commons-compress.jar:/usr/share/java/commons-lang3.jar:$PIG_CLASSPATH
```

---

## 👥 Team Roles & Handoffs

To keep development clean and prevent merge conflicts, responsibilities are divided as follows:

* **Member 1 (Data & Controller):** * Design the master regex for log parsing.
    * Build the core `main.py` Python orchestrator to handle physical file batching and sequential execution triggering. *(Completed)*
* **Member 2 (Pig Pipeline):** * Write the Apache Pig scripts (`queries.pig`) to handle the ETL aggregations for all three queries. *(Completed)*
* **Member 3 (Database & Ingestion):** * Design the PostgreSQL schema for the three queries (`database/schema.sql`) and reset script (`database/reset_and_create.sql`). *(Completed)*
    * Implement `src/controllers/db_client.py` using `psycopg2`. *(Completed)*
* **Member 4 (Reporting UI & Orchestration):** * **[COMPLETED]** Build the CLI dashboard in `src/controllers/reporting.py` to orchestrate pipeline execution and render the final formatted console output.
    * Extended `db_client.py` with reporting fetch functions.
    * Finalized system integration and README documentation.

### Member 4 CLI Flow

The reporting CLI (`src/controllers/reporting.py`) follows this flow:

1. **Environment Check:** Alerts the user if critical variables (`PGPASSWORD`, `PIG_HOME`, etc.) are missing.
2. **Main Menu:** Allows selection of Pig (Ready) or Phase 2 placeholders (MapReduce, Hive, MongoDB).
3. **Execution:** Prompts for batch size and input file, then triggers the orchestrator.
4. **Reporting:** Automatically fetches results from PostgreSQL and renders formatted tables for Query 1, 2, and 3.