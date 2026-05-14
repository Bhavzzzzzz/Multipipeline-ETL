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
├── setup.sh                         # local/gitignored environment file
├── temp.md
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
            └── test.py
```

The key files for the current phase are:

* `src/controllers/main.py` - orchestrates batching, Pig execution, and DB loading.
* `src/controllers/env_utils.py` - centralizes runtime environment checks shared by the CLI and orchestrator.
* `src/controllers/utils.py` - parses log lines and creates batches from the raw input files.
* `src/controllers/db_client.py` - loads Pig results into PostgreSQL.
* `src/pipelines/pig/queries.pig` - performs the Pig ETL and aggregation work.
* `src/pipelines/hive/queries.hql` - performs the Hive ETL and aggregation work.
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
* Java 17 (OpenJDK, required for the Hive 4 workflow)
* Python 3.8+
* Apache Pig (Local Mode)
* Apache Hadoop (Local Mode support for Hive)
* Apache Hive 4.x (Local Mode)
* MongoDB (Running locally or accessible via URI)
* PostgreSQL (Running inside WSL/Ubuntu or systemd Linux recommended)
* `psycopg2` (Python library for PostgreSQL)
* `pymongo` (Python library for MongoDB)

### Environment setup

These commands install and configure the runtime used by this project. They assume a Debian/Ubuntu-style system and that you want system-wide Apache Pig, Hadoop, and Hive installs under `/opt` (recommended).

The examples below use the Debian/Ubuntu Java 17 path `/usr/lib/jvm/java-17-openjdk-amd64`. On Arch-based systems this is commonly `/usr/lib/jvm/java-17-openjdk`; use the path that exists on your machine.

1) Install system packages (JDK, Python, Postgres tooling, MongoDB):

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk python3 python3-pip python3-venv postgresql postgresql-contrib wget curl tar mongodb
sudo service mongodb start
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
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
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

5) Install Apache Hadoop for Hive local execution

The Hive pipeline in `src/controllers/main.py` runs:

```bash
hive -f src/pipelines/hive/queries.hql -hiveconf INPUT=<batch_file> -hiveconf OUTPUT_DIR=<output_path>
```

The query file sets `mapreduce.framework.name=local`, so you do **not** need to start a Hadoop/YARN cluster. You still need a Hadoop installation because the Hive CLI uses Hadoop libraries and commands internally.

```bash
cd /tmp
wget -c https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
sudo tar -xzf hadoop-3.3.6.tar.gz -C /opt

sudo tee /etc/profile.d/hadoop.sh > /dev/null <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.3.6
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
EOF

sudo chmod 644 /etc/profile.d/hadoop.sh
source /etc/profile.d/hadoop.sh

hadoop version
```

6) Install Apache Hive 4 and initialize the local metastore

This project uses Hive 4 in single-machine local mode. The setup below uses Hive's embedded Derby metastore, which is enough for the local batch pipeline. Run the `schematool` command from the repository root so the `metastore_db` directory is created there.

If you previously initialized this repository with Hive 3.x, move the old local metastore out of the way before initializing Hive 4. Hive metastore schemas are versioned, and reusing the Hive 3 Derby directory can cause startup errors.

```bash
cd /tmp
wget -c https://archive.apache.org/dist/hive/hive-4.1.0/apache-hive-4.1.0-bin.tar.gz
sudo tar -xzf apache-hive-4.1.0-bin.tar.gz -C /opt

sudo tee /etc/profile.d/hive.sh > /dev/null <<'EOF'
export HIVE_HOME=/opt/apache-hive-4.1.0-bin
export HIVE_BIN="$HIVE_HOME/bin/hive"
export PATH="$HIVE_HOME/bin:$PATH"
EOF

sudo chmod 644 /etc/profile.d/hive.sh
source /etc/profile.d/hive.sh

# Run the rest of this Hive setup from the repository root.
cd /mnt/c/Codes/Multipipeline-ETL
mkdir -p data/hive/warehouse

# Optional when migrating from the previous Hive 3 local setup:
# mv metastore_db "metastore_db.hive3.$(date +%Y%m%d%H%M%S)"

cat > /tmp/hive-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=metastore_db;create=true</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>file://${user.dir}/data/hive/warehouse</value>
  </property>
  <property>
    <name>hive.exec.scratchdir</name>
    <value>file://${user.dir}/data/hive/scratch</value>
  </property>
  <property>
    <name>hive.exec.local.scratchdir</name>
    <value>${user.dir}/data/hive/local_scratch</value>
  </property>
</configuration>
EOF
sudo cp /tmp/hive-site.xml "$HIVE_HOME/conf/hive-site.xml"

schematool -dbType derby -initSchema

hive --version
```

Important Hive notes:

* The embedded Derby metastore supports one Hive process at a time. That is fine for this project's sequential batch execution.
* `main.py` now validates that the configured Hive binary is Hive 4.x before running the Hive pipeline.
* If your Hive binary is not named `hive` or is not first on `PATH`, set `HIVE_BIN=/path/to/apache-hive-4.1.0-bin/bin/hive`.
* The Hive script uses `LOAD DATA LOCAL INPATH`, so input files can stay on your normal local filesystem.
* Hive writes engine-specific output filenames; `main.py` normalizes each Hive query output to `part-00000` so `db_client.py` can ingest it.
* If `schematool -initSchema` says the schema already exists, that is okay. Do not delete `metastore_db` unless you intentionally want to reset the local Hive metastore.

7) Quick Hive pipeline check

After exporting the variables in the Environment Variables section, run a small batch to confirm Hive can execute and PostgreSQL can ingest the results:

```bash
source venv/bin/activate
python src/controllers/main.py --pipeline hive --batch-size 1000 --input data/raw/access_log_Jul95
```

Expected output directories:

```text
data/output/hive_results/batch_1/query1/part-00000
data/output/hive_results/batch_1/query2/part-00000
data/output/hive_results/batch_1/query3/part-00000
```

8) PostgreSQL setup (create DB and load schema)

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

9) Launch the Orchestrator CLI (Recommended)

The reporting dashboard is the central entry point for the project. It handles environment checks, pipeline execution, and final data visualization.

```bash
source venv/bin/activate
source setup.sh
python src/controllers/reporting.py
```

### 10) Manual execution (Advanced)

If you prefer to run the orchestrator directly without the interactive CLI:

```bash
# For Pig
python src/controllers/main.py --pipeline pig --batch-size 100000 --input data/raw/access_log_Jul95

# For Hive
python src/controllers/main.py --pipeline hive --batch-size 100000 --input data/raw/access_log_Jul95

# For MongoDB
python src/controllers/main.py --pipeline mongodb --batch-size 100000 --input data/raw/access_log_Jul95
```

---

## 🚀 Environment Variables

Before running `reporting.py` or `main.py`, ensure the following variables are set in your session. The easiest option is to edit the password/path values in your local `setup.sh`, then source it:

```bash
source setup.sh
```

The file exports the same variables shown below. You can also add these to your `.bashrc` or a `.env` file:

```bash
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
export PATH="$JAVA_HOME/bin:$PIG_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$PATH"
export PIG_CLASSPATH=/usr/share/java/commons-text.jar:/usr/share/java/commons-compress.jar:/usr/share/java/commons-lang3.jar:$PIG_CLASSPATH
```
