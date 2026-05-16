#!/usr/bin/env python3
"""Run the full ETL benchmark matrix and persist one CSV row per run."""

from __future__ import annotations

import csv
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List


REPO_ROOT = Path(__file__).resolve().parent
CONTROLLER_PATH = REPO_ROOT / "src" / "controllers" / "main.py"
OUTPUT_DIR = REPO_ROOT / "data" / "output"
OUTPUT_CSV = OUTPUT_DIR / "benchmark_results.csv"
SETUP_SH_PATH = REPO_ROOT / "setup.sh"
BENCH_MONGO_PORT = 27018
BENCH_MONGO_URI = f"mongodb://127.0.0.1:{BENCH_MONGO_PORT}/"
BENCH_MONGO_DBPATH = REPO_ROOT / "data" / "local_scratch" / "mongo_benchmark_db"
BENCH_MONGO_LOGDIR = REPO_ROOT / "data" / "local_scratch" / "mongo_benchmark_log"
BENCH_MONGO_SOCKDIR = REPO_ROOT / "data" / "local_scratch" / "mongo_benchmark_sock"

_BENCH_MONGO_STARTED = False

PIPELINES = ["mongodb", "pig", "mapreduce", "hive"]
QUERIES = ["query1", "query2", "query3"]
BATCH_SIZES = [1_000_000, 500_000, 100_000, 50_000]

SUMMARY_PATTERNS = {
    "pipeline_name": re.compile(r"^Pipeline Selected\s*:\s*(?P<value>.+)$", re.MULTILINE),
    "query_name": re.compile(r"^Query Selection\s*:\s*(?P<value>.+)$", re.MULTILINE),
    "total_runtime_seconds": re.compile(r"^Total Runtime\s*:\s*(?P<value>[0-9.]+)\s+seconds$", re.MULTILINE),
    "total_records": re.compile(r"^Total Records\s*:\s*(?P<value>[0-9]+)$", re.MULTILINE),
    "malformed_records": re.compile(r"^Malformed Records\s*:\s*(?P<value>[0-9]+)$", re.MULTILINE),
    "total_batches": re.compile(r"^Total Batches\s*:\s*(?P<value>[0-9]+)$", re.MULTILINE),
    "avg_batch_size": re.compile(r"^Avg Batch Size\s*:\s*(?P<value>[0-9.]+)\s+records$", re.MULTILINE),
}


def _resolve_input_files() -> List[str]:
    candidates = [
        REPO_ROOT / "data" / "raw" / "NASA_access_log_Jul95.gz",
        REPO_ROOT / "data" / "raw" / "NASA_access_log_Aug95.gz",
        REPO_ROOT / "data" / "raw" / "access_log_Jul95",
        REPO_ROOT / "data" / "raw" / "access_log_Aug95",
    ]

    preferred = [str(path) for path in candidates[:2] if path.exists()]
    if preferred:
        return preferred

    fallback = [str(path) for path in candidates[2:] if path.exists()]
    return fallback


def _parse_summary(output: str) -> Dict[str, object]:
    parsed: Dict[str, object] = {}
    for field_name, pattern in SUMMARY_PATTERNS.items():
        match = pattern.search(output)
        if not match:
            raise ValueError(f"Could not parse '{field_name}' from controller output")

        value = match.group("value")
        if field_name in {"total_runtime_seconds", "avg_batch_size"}:
            parsed[field_name] = float(value)
        elif field_name in {"total_records", "malformed_records", "total_batches"}:
            parsed[field_name] = int(value)
        else:
            parsed[field_name] = value.strip().lower()

    return parsed


def _ensure_output_directory() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _load_setup_environment() -> None:
    """Load exported vars from setup.sh into this process environment."""
    if not SETUP_SH_PATH.exists():
        raise FileNotFoundError(f"setup.sh not found at {SETUP_SH_PATH}")

    shell_cmd = f"set -a && source {SETUP_SH_PATH} >/dev/null 2>&1 && env -0"
    result = subprocess.run(
        ["bash", "-lc", shell_cmd],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="ignore").strip()
        raise RuntimeError(f"Failed to load setup.sh environment: {stderr}")

    loaded = 0
    for entry in result.stdout.split(b"\0"):
        if not entry or b"=" not in entry:
            continue
        key, value = entry.split(b"=", 1)
        key_text = key.decode("utf-8", errors="ignore")
        value_text = value.decode("utf-8", errors="ignore")
        os.environ[key_text] = value_text
        loaded += 1

    if loaded == 0:
        raise RuntimeError("setup.sh was sourced but no environment variables were loaded")


def _append_row(row: Dict[str, object], fieldnames: Iterable[str]) -> None:
    file_exists = OUTPUT_CSV.exists()
    with OUTPUT_CSV.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _mongo_ping(uri: str) -> bool:
    if not uri:
        return False

    result = subprocess.run(
        [
            "mongosh",
            uri,
            "--quiet",
            "--eval",
            "db.runCommand({ ping: 1 })",
        ],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.returncode == 0 and "ok" in (result.stdout or "")


def _ensure_benchmark_mongo() -> None:
    global _BENCH_MONGO_STARTED

    # Prefer and enforce an isolated benchmark-local MongoDB instance to avoid
    # interference from unstable system services.
    if _mongo_ping(BENCH_MONGO_URI):
        os.environ["MONGO_URI"] = BENCH_MONGO_URI
        _BENCH_MONGO_STARTED = True
        return

    BENCH_MONGO_DBPATH.mkdir(parents=True, exist_ok=True)
    BENCH_MONGO_LOGDIR.mkdir(parents=True, exist_ok=True)
    BENCH_MONGO_SOCKDIR.mkdir(parents=True, exist_ok=True)

    start_result = subprocess.run(
        [
            "mongod",
            "--dbpath",
            str(BENCH_MONGO_DBPATH),
            "--bind_ip",
            "127.0.0.1",
            "--port",
            str(BENCH_MONGO_PORT),
            "--unixSocketPrefix",
            str(BENCH_MONGO_SOCKDIR),
            "--logpath",
            str(BENCH_MONGO_LOGDIR / "mongod.log"),
            "--fork",
        ],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if start_result.returncode != 0 and not _mongo_ping(BENCH_MONGO_URI):
        raise RuntimeError(
            "Unable to start benchmark MongoDB instance. "
            f"stdout={start_result.stdout.strip()} stderr={start_result.stderr.strip()}"
        )

    os.environ["MONGO_URI"] = BENCH_MONGO_URI
    _BENCH_MONGO_STARTED = True

    if not _mongo_ping(BENCH_MONGO_URI):
        raise RuntimeError("Benchmark MongoDB instance did not become ready on port 27018")


def _stop_benchmark_mongo() -> None:
    global _BENCH_MONGO_STARTED
    if not _BENCH_MONGO_STARTED:
        return

    subprocess.run(
        [
            "mongosh",
            BENCH_MONGO_URI,
            "--quiet",
            "--eval",
            "db.adminCommand({ shutdown: 1 })",
        ],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    _BENCH_MONGO_STARTED = False


def _reset_database() -> None:
    controllers_path = str(REPO_ROOT / "src" / "controllers")
    if controllers_path not in sys.path:
        sys.path.insert(0, controllers_path)
    import db_client  # type: ignore

    db_client.reset_database()


def _run_controller(pipeline: str, query: str, batch_size: int, input_files: List[str]) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(CONTROLLER_PATH),
        "--pipeline",
        pipeline,
        "--query",
        query,
        "--batch-size",
        str(batch_size),
        "--inputs",
        *input_files,
        "--reset-db",
    ]

    return subprocess.run(
        command,
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )


def main() -> int:
    _load_setup_environment()

    input_files = _resolve_input_files()
    if not input_files:
        print("[-] No benchmark input files found in data/raw.")
        return 1

    _ensure_output_directory()

    fieldnames = [
        "suite_id",
        "timestamp_utc",
        "pipeline",
        "query",
        "batch_size",
        "status",
        "returncode",
        "total_runtime_seconds",
        "total_records",
        "malformed_records",
        "total_batches",
        "avg_batch_size",
        "error_message",
        "input_files",
    ]

    suite_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    input_file_text = " | ".join(input_files)

    try:
        for query in QUERIES:
            for batch_size in BATCH_SIZES:
                for pipeline in PIPELINES:
                    if pipeline == "mongodb":
                        _ensure_benchmark_mongo()

                    timestamp_utc = datetime.now(timezone.utc).isoformat()
                    print(f"[*] Benchmarking pipeline={pipeline} query={query} batch_size={batch_size}")

                    result = _run_controller(pipeline, query, batch_size, input_files)
                    if pipeline == "mongodb" and result.returncode != 0:
                        combined_output = f"{result.stdout}\n{result.stderr}".lower()
                        if "unable to reach mongodb" in combined_output or "mongodb operation failed" in combined_output:
                            # One automatic retry after ensuring benchmark-local Mongo is up.
                            _ensure_benchmark_mongo()
                            result = _run_controller(pipeline, query, batch_size, input_files)

                    stdout = result.stdout.strip()
                    stderr = result.stderr.strip()

                    status = "success" if result.returncode == 0 else "failed"
                    error_message = ""

                    try:
                        if result.returncode == 0:
                            summary = _parse_summary(stdout)
                        else:
                            summary = {
                                "pipeline_name": pipeline,
                                "query_name": query,
                                "total_runtime_seconds": "",
                                "total_records": "",
                                "malformed_records": "",
                                "total_batches": "",
                                "avg_batch_size": "",
                            }
                            error_message = stderr or stdout or "Controller returned a non-zero exit code"

                    except Exception as exc:
                        status = "failed"
                        error_message = f"{type(exc).__name__}: {exc}"
                        summary = {
                            "pipeline_name": pipeline,
                            "query_name": query,
                            "total_runtime_seconds": "",
                            "total_records": "",
                            "malformed_records": "",
                            "total_batches": "",
                            "avg_batch_size": "",
                        }

                    row = {
                        "suite_id": suite_id,
                        "timestamp_utc": timestamp_utc,
                        "pipeline": pipeline,
                        "query": query,
                        "batch_size": batch_size,
                        "status": status,
                        "returncode": result.returncode,
                        "total_runtime_seconds": summary["total_runtime_seconds"],
                        "total_records": summary["total_records"],
                        "malformed_records": summary["malformed_records"],
                        "total_batches": summary["total_batches"],
                        "avg_batch_size": summary["avg_batch_size"],
                        "error_message": error_message.replace("\n", " ").strip(),
                        "input_files": input_file_text,
                    }

                    try:
                        _append_row(row, fieldnames)
                    finally:
                        try:
                            _reset_database()
                        except Exception as exc:
                            print(f"[!] Database cleanup failed after {pipeline} {query} {batch_size}: {exc}")

                    print(f"[+] {pipeline} {query} batch_size={batch_size} -> {status}")

                    if result.returncode != 0:
                        print(stderr or stdout)

        print(f"[+] Benchmark results written to {OUTPUT_CSV}")
        return 0
    finally:
        _stop_benchmark_mongo()


if __name__ == "__main__":
    raise SystemExit(main())