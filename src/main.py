import argparse
import os
import subprocess
import sys
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.file_router import choose_engine
from src.batch_loader import load_csv_to_raw
from src.elt_pipeline import process_run
from src.mongo_setup import setup_mongodb
from src.metrics import save_run_metrics, get_file_size_mb
from config.settings import BATCH_SIZE


def run_python_batch(file_path, run_id):
    print("\n====================================")
    print("PYTHON BATCH PIPELINE")
    print("====================================")

    load_result = load_csv_to_raw(
        file_path=file_path,
        run_id=run_id,
    )

    quality_result = process_run(
        run_id=run_id,
    )

    total_elapsed = (
        load_result["elapsed_seconds"]
        + quality_result["elapsed_seconds"]
    )
    total_throughput = (
        load_result["read_rows"] / total_elapsed
        if total_elapsed > 0
        else 0
    )
    metrics = {
        "stage": "python_batch_full_pipeline",
        "run_id": run_id,
        "file_name": Path(file_path).name,
        "file_size_mb": get_file_size_mb(file_path),
        "used_engine": "python_batch",
        "read_rows": load_result["read_rows"],
        "loaded_raw": load_result["loaded_raw"],
        "count_valid": quality_result["count_valid"],
        "count_corrected": quality_result["count_corrected"],
        "count_quarantine": quality_result["count_quarantine"],
        "count_inserted": quality_result["count_inserted"],
        "count_updated": quality_result["count_updated"],
        "count_unchanged": quality_result["count_unchanged"],
        "seconds_elapsed": total_elapsed,
        "throughput": total_throughput,
        "batch_size": BATCH_SIZE,
        "partitions": None,
        "counts_case_error": quality_result["counts_case_error"],
    }
    save_run_metrics(metrics)

    return {
        "raw_load": load_result,
        "quality": quality_result,
        "metrics": metrics,
    }


def run_pyspark(file_path, run_id):
    print("\n====================================")
    print("PYSPARK PIPELINE")
    print("====================================")

    env = os.environ.copy()

    # Force Spark workers to use the currently active Python.
    env["PYSPARK_PYTHON"] = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark_submit = "spark-submit"

    # Portable Spark temporary directory.
    # Can be overridden with the SPARK_LOCAL_DIR environment variable.
    spark_local_dir = os.getenv(
        "SPARK_LOCAL_DIR",
        str(PROJECT_ROOT / ".spark-temp"),
    )
    Path(spark_local_dir).mkdir(parents=True, exist_ok=True)

    raw_command = [
        spark_submit,
        "--driver-memory",
        "8g",
        "--conf",
        f"spark.local.dir={spark_local_dir}",
        str(PROJECT_ROOT / "src" / "spark_loader.py"),
        "--input",
        str(file_path),
        "--run-id",
        run_id,
    ]

    quality_command = [
        spark_submit,
        "--driver-memory",
        "8g",
        "--conf",
        f"spark.local.dir={spark_local_dir}",
        str(
            PROJECT_ROOT
            / "src"
            / "spark_quality_pipeline_stringdates.py"
        ),
        "--run-id",
        run_id,
    ]

    print("\nStarting Spark Raw load...")

    subprocess.run(
        raw_command,
        cwd=PROJECT_ROOT,
        env=env,
        check=True,
    )

    print("\nStarting Spark Quality pipeline...")

    subprocess.run(
        quality_command,
        cwd=PROJECT_ROOT,
        env=env,
        check=True,
    )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Hybrid Big Data order pipeline: "
            "Python Batch for small files and "
            "PySpark for large files."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="CSV input file path.",
    )
    parser.add_argument(
        "--run-id",
        required=False,
        default=None,
        help="Optional existing run ID.",
    )

    args = parser.parse_args()

    input_path = Path(args.input).resolve()

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    run_id = args.run_id or str(uuid.uuid4())

    print("\n====================================")
    print("HYBRID ORDER PIPELINE")
    print("====================================")
    print(f"Input: {input_path}")
    print(f"Run ID: {run_id}")

    # Ensure MongoDB collections and indexes exist.
    setup_mongodb()

    route = choose_engine(
        input_path
    )

    print("\n====================================")
    print("ROUTING DECISION")
    print("====================================")
    print(f"Engine: {route['engine']}")
    print(f"Reason: {route['reason']}")

    if route["engine"] == "python_batch":
        run_python_batch(
            file_path=input_path,
            run_id=run_id,
        )

    elif route["engine"] == "pyspark":
        run_pyspark(
            file_path=input_path,
            run_id=run_id,
        )
    else:
        raise ValueError(
            f"Unsupported engine: {route['engine']}"
        )

    print("\n====================================")
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("====================================")
    print(f"Run ID: {run_id}")
    print(f"Engine used: {route['engine']}")


if __name__ == "__main__":
    main()
