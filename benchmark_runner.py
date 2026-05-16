#!/usr/bin/env python3
"""Run the full ETL benchmark matrix and persist one CSV row per run."""

from __future__ import annotations

import csv
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

PIPELINES = ["pig", "mapreduce", "hive", "mongodb"]
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


def _append_row(row: Dict[str, object], fieldnames: Iterable[str]) -> None:
    file_exists = OUTPUT_CSV.exists()
    with OUTPUT_CSV.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


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
    )


def main() -> int:
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

    for query in QUERIES:
        for batch_size in BATCH_SIZES:
            for pipeline in PIPELINES:
                timestamp_utc = datetime.now(timezone.utc).isoformat()
                print(f"[*] Benchmarking pipeline={pipeline} query={query} batch_size={batch_size}")

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


if __name__ == "__main__":
    raise SystemExit(main())