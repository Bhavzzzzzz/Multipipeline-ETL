# src/controllers/main.py
import os
import shutil
import subprocess
import sys
import tempfile
import time
import argparse
import re
from tqdm import tqdm

try:
    # Use package-relative imports when executed as a package
    from . import db_client
    from .env_utils import PIPELINE_DISPLAY_NAMES, validate_runtime_environment
    from .utils import process_and_batch_logs
except Exception:
    # Fallback for direct script execution / legacy PATH setups
    import db_client
    from env_utils import PIPELINE_DISPLAY_NAMES, validate_runtime_environment
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

def run_mapreduce_pipeline(batch_path: str, output_dir: str):
    """Executes the local MapReduce-style query runner."""
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    cmd = [
        sys.executable,
        "src/pipelines/mapreduce/queries.py",
        "--input",
        batch_path,
        "--output-dir",
        output_dir,
    ]

    print(f"[*] Executing MapReduce pipeline for {os.path.basename(batch_path)}...")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if result.returncode != 0:
        print("[-] MapReduce Job Failed!")
        print(result.stderr)
        raise RuntimeError("MapReduce execution error")

def _hive_command():
    """Return the configured Hive 4 CLI command."""
    return os.getenv("HIVE_BIN", "hive")


def _beeline_command():
    if os.getenv("HIVE_BEELINE_BIN"):
        return os.getenv("HIVE_BEELINE_BIN")

    hive_home = os.getenv("HIVE_HOME")
    if hive_home:
        return os.path.join(hive_home, "bin", "beeline")

    return "beeline"


def _hive_environment():
    env = os.environ.copy()
    env.pop("DEBUG", None)
    env.setdefault("HADOOP_HEAPSIZE", "2048")
    env["HADOOP_CLIENT_OPTS"] = _append_java_option(
        env.get("HADOOP_CLIENT_OPTS", ""),
        "-Xmx2048m",
        "-Xmx",
    )
    return env


def _append_java_option(existing_options: str, option: str, replace_prefix: str = None):
    options = [
        item
        for item in existing_options.split()
        if not replace_prefix or not item.startswith(replace_prefix)
    ]
    options.append(option)
    return " ".join(options)


def _hive_sql_string(value: str):
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _hive_file_uri(path: str):
    return "file://" + os.path.abspath(path).replace("\\", "/")


def _render_hive_script(batch_path: str, output_dir: str):
    template_path = os.path.join("src", "pipelines", "hive", "queries.hql")
    with open(template_path, "r", encoding="utf-8") as handle:
        script = handle.read()

    hive_workspace = os.path.abspath(os.path.join("data", "hive"))
    hive_warehouse_dir = os.path.join(hive_workspace, "warehouse")
    raw_logs_table_dir = os.path.join(output_dir, "hive_raw_logs")

    os.makedirs(hive_warehouse_dir, exist_ok=True)
    os.makedirs(os.path.dirname(raw_logs_table_dir), exist_ok=True)
    if os.path.exists(raw_logs_table_dir):
        shutil.rmtree(raw_logs_table_dir)
    os.makedirs(raw_logs_table_dir, exist_ok=True)

    script = script.replace("__INPUT__", _hive_sql_string(os.path.abspath(batch_path)))
    script = script.replace("__OUTPUT_DIR__", _hive_sql_string(os.path.abspath(output_dir)))
    script = script.replace("__HIVE_WAREHOUSE_DIR__", _hive_file_uri(hive_warehouse_dir))
    script = script.replace("__RAW_LOGS_TABLE_DIR__", _hive_file_uri(raw_logs_table_dir))

    rendered = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix="hive4_",
        suffix=".hql",
        delete=False,
    )
    try:
        rendered.write(script)
        return rendered.name
    finally:
        rendered.close()


def _validate_hive4_runtime(hive_bin: str):
    result = subprocess.run(
        [hive_bin, "--version"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=_hive_environment(),
    )
    version_output = result.stdout.strip()
    if result.returncode != 0:
        raise RuntimeError(f"Unable to check Hive version with {hive_bin}: {version_output}")

    match = re.search(r"Hive\s+([0-9]+)\.", version_output)
    if not match:
        raise RuntimeError(f"Unable to parse Hive version from: {version_output}")

    if int(match.group(1)) < 4:
        raise RuntimeError(
            "Hive 4.x is required for this pipeline. "
            f"{hive_bin} reported: {version_output}"
        )


def _schematool_command():
    hive_home = os.getenv("HIVE_HOME")
    if hive_home:
        candidate = os.path.join(hive_home, "bin", "schematool")
        if os.path.exists(candidate):
            return candidate
    return "schematool"


def _ensure_hive_metastore_initialized():
    schematool_bin = _schematool_command()
    metastore_dir = os.path.abspath("metastore_db")

    info_result = subprocess.run(
        [schematool_bin, "-dbType", "derby", "-info"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=_hive_environment(),
    )
    if info_result.returncode == 0:
        return

    info_output = info_result.stdout.strip()

    init_result = subprocess.run(
        [schematool_bin, "-dbType", "derby", "-initSchema"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=_hive_environment(),
    )
    init_output = init_result.stdout.strip()

    if init_result.returncode != 0:
        shutil.rmtree(metastore_dir, ignore_errors=True)
        retry_result = subprocess.run(
            [schematool_bin, "-dbType", "derby", "-initSchema"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=_hive_environment(),
        )
        retry_output = retry_result.stdout.strip()
        if retry_result.returncode == 0:
            print("[*] Initialized local Hive metastore schema.")
            return

        lower_output = f"{init_output}\n{retry_output}".lower()
        if "already initialized" in lower_output or "table/view 'version' already exists" in lower_output:
            return
        raise RuntimeError(f"Unable to initialize Hive metastore: {init_output}")

    print("[*] Initialized local Hive metastore schema.")


def _normalize_hive_output_files(output_dir: str):
    """Expose Hive output files with the part-* names expected by db_client.py."""
    for query_folder in ["query1", "query2", "query3"]:
        folder_path = os.path.join(output_dir, query_folder)
        if not os.path.isdir(folder_path):
            continue

        data_files = sorted(
            file_name
            for file_name in os.listdir(folder_path)
            if not file_name.startswith(".") and file_name != "_SUCCESS"
        )
        if not data_files:
            continue

        if all(file_name.startswith("part-") for file_name in data_files):
            continue

        for index, file_name in enumerate(data_files):
            if file_name.startswith("part-"):
                continue

            target_name = f"part-{index:05d}"
            target_path = os.path.join(folder_path, target_name)
            if os.path.exists(target_path):
                continue

            os.rename(os.path.join(folder_path, file_name), target_path)


def run_hive_pipeline(batch_path: str, output_dir: str):
    """Executes the Hive 4 script via local system call."""
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    hive_bin = _hive_command()
    beeline_bin = _beeline_command()
    _validate_hive4_runtime(hive_bin)
    _ensure_hive_metastore_initialized()
    rendered_script_path = _render_hive_script(batch_path, output_dir)

    cmd = [
        beeline_bin,
        "-u", os.getenv("HIVE_JDBC_URL", "jdbc:hive2://"),
        "-f", rendered_script_path,
    ]
    
    print(f"[*] Executing Hive pipeline for {os.path.basename(batch_path)}...")
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=_hive_environment(),
        )
        
        if result.returncode != 0:
            print("[-] Hive Job Failed!")
            print(result.stderr)
            raise RuntimeError("Hive execution error")
    finally:
        if os.path.exists(rendered_script_path):
            os.unlink(rendered_script_path)

    _normalize_hive_output_files(output_dir)

def run_mongodb_pipeline(batch_path: str, output_dir: str):
    """Executes the MongoDB pipeline via a Python script."""
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    
    cmd = [
        sys.executable, "src/pipelines/mongodb/pipeline.py",
        batch_path,
        output_dir
    ]
    
    print(f"[*] Executing MongoDB pipeline for {os.path.basename(batch_path)}...")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    if result.returncode != 0:
        print("[-] MongoDB Pipeline Failed!")
        print(result.stderr)
        raise RuntimeError("MongoDB execution error")

def trigger_db_load(batch_id: int, output_dir: str, metadata: dict):
    """
    PostgreSQL ingestion.
    """
    print(f"[*] Loading results from {output_dir} into PostgreSQL for Batch {batch_id}...")
    return db_client.ingest_query_results(output_dir, metadata)


def _normalize_query_selection(query_name: str):
    aliases = {
        "1": "query1",
        "q1": "query1",
        "query1": "query1",
        "2": "query2",
        "q2": "query2",
        "query2": "query2",
        "3": "query3",
        "q3": "query3",
        "query3": "query3",
        "all": "all",
        "a": "all",
    }
    return aliases.get(query_name.lower(), "all")

def main():
    parser = argparse.ArgumentParser(description="Multi-Pipeline ETL Orchestrator")
    parser.add_argument("--pipeline", choices=["pig", "mapreduce", "hive", "mongodb"], default="pig", help="Select execution backend")
    parser.add_argument("--query", choices=["query1", "query2", "query3", "all"], default="all", help="Select a query placeholder for the interface")
    parser.add_argument("--batch-size", type=int, default=100000, help="Number of records per batch")
    parser.add_argument("--input", type=str, default="data/raw/NASA_access_log_Jul95.gz", help="Path to raw logs")
    parser.add_argument("--inputs", nargs="+", help="Paths to raw log files (combine multiple inputs)")
    parser.add_argument("--reset-db", action="store_true", help="Reset the PostgreSQL reporting schema before running")
    args = parser.parse_args()
    validate_runtime_environment(args.pipeline)

    selected_query = _normalize_query_selection(args.query)

    if args.reset_db:
        print("[*] Resetting PostgreSQL reporting schema before run...")
        db_client.reset_database()

    staging_dir = "data/output/staging_batches"
    base_output_dir = f"data/output/{args.pipeline}_results"

    if os.path.exists(staging_dir):
        shutil.rmtree(staging_dir)
    os.makedirs(staging_dir)

    # Start the official runtime timer
    start_time = time.time()

    # 1. Split logs using the shared utility helper
    input_files = args.inputs if args.inputs else [args.input]

    batch_files, total_records, total_malformed_records = process_and_batch_logs(
        input_files,
        staging_dir,
        batch_size=args.batch_size,
    )
    num_batches = len(batch_files)
    avg_batch_size = total_records / num_batches if num_batches > 0 else 0

    if selected_query != "all":
        print(
            f"[*] Query selection is a placeholder for now. "
            f"The selected backend will still execute all three queries until per-query pipeline isolation is implemented."
        )
    
    # 2. Process each batch sequentially
    batch_iterator = tqdm(
        batch_files,
        total=num_batches,
        desc=f"Processing {args.pipeline.upper()} batches",
        unit="batch",
    )
    for batch_id, batch_path, records_in_batch, malformed_in_batch in batch_iterator:
        batch_iterator.set_postfix(
            batch_id=batch_id,
            records=records_in_batch,
            malformed=malformed_in_batch,
        )
        batch_output_dir = os.path.join(base_output_dir, f"batch_{batch_id}")
        
        if args.pipeline == "pig":
            batch_start = time.time()
            run_pig_pipeline(batch_path, batch_output_dir)
        elif args.pipeline == "mapreduce":
            batch_start = time.time()
            run_mapreduce_pipeline(batch_path, batch_output_dir)
        elif args.pipeline == "hive":            
            batch_start = time.time()            
            run_hive_pipeline(batch_path, batch_output_dir)
        elif args.pipeline == "mongodb":
            batch_start = time.time()
            run_mongodb_pipeline(batch_path, batch_output_dir)
        else:
            raise NotImplementedError(f"{args.pipeline} pipeline is not implemented yet")

        metadata = {
            "pipeline_name": PIPELINE_DISPLAY_NAMES[args.pipeline],
            "query_name": selected_query,
            "run_identifier": f"run_{int(start_time)}",
            "batch_id": batch_id,
            "batch_size": records_in_batch,
            "records_processed": records_in_batch,
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
    print(f"Query Selection   : {selected_query.upper()}")
    print(f"Total Runtime     : {total_runtime:.2f} seconds")
    print(f"Total Records     : {total_records}")
    print(f"Malformed Records  : {total_malformed_records}")
    print(f"Total Batches     : {num_batches}")
    print(f"Avg Batch Size    : {avg_batch_size:.2f} records")
    print("="*50)

if __name__ == "__main__":
    main()
