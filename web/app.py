from flask import Flask, render_template, request, jsonify, send_from_directory
from pathlib import Path
from threading import Thread, Lock
from uuid import uuid4

import subprocess
import json
import os
import sys
import time


# =========================================================
# Paths
# =========================================================

ROOT_DIR = Path(__file__).resolve().parent.parent
WEB_DIR = ROOT_DIR / "web"
UPLOAD_DIR = WEB_DIR / "uploads"
SCREENSHOTS_DIR = ROOT_DIR / "reports" / "screenshots"
RESULTS_FILE = ROOT_DIR / "reports" / "results.json"

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)


# =========================================================
# Flask
# =========================================================

app = Flask(
    __name__,
    template_folder=str(WEB_DIR / "templates"),
    static_folder=str(WEB_DIR / "static"),
)

# Maximum upload size = 20 GB
app.config["MAX_CONTENT_LENGTH"] = 20 * 1024 * 1024 * 1024


# =========================================================
# Jobs Memory
# =========================================================

jobs = {}
jobs_lock = Lock()


# =========================================================
# Helpers
# =========================================================

def get_file_size_mb(file_path):
    return Path(file_path).stat().st_size / (1024 * 1024)


def detect_engine(file_path):
    """
    Automatic Router:

    <= 200 MB  -> Python Batch
    > 200 MB   -> Apache PySpark
    """

    size_mb = get_file_size_mb(file_path)

    if size_mb <= 200:
        return {
            "engine": "python_batch",
            "file_size_mb": round(size_mb, 2),
            "reason": "حجم الملف أقل من أو يساوي 200 MB",
        }

    return {
        "engine": "pyspark",
        "file_size_mb": round(size_mb, 2),
        "reason": "حجم الملف أكبر من 200 MB",
    }


def read_results_file():
    """
    Read reports/results.json safely.
    """

    if not RESULTS_FILE.exists():
        return []

    try:
        with open(RESULTS_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)

        if isinstance(data, list):
            return data

        if isinstance(data, dict):

            if isinstance(data.get("results"), list):
                return data["results"]

            if isinstance(data.get("runs"), list):
                return data["runs"]

            return [data]

    except Exception as exc:
        print(f"Could not read results.json: {exc}")
        return []

    return []


def first_present(data, *keys, default=0):
    """
    Return first existing value.

    Important:
    Zero is a valid value and must not be skipped.
    """

    for key in keys:

        if key in data and data[key] is not None:
            return data[key]

    return default


# =========================================================
# Find Final Metrics
# =========================================================

def find_run_metrics(run_id):
    """
    A run can save multiple metrics records.

    This function selects the most complete/final record
    for the requested Run ID.
    """

    results = read_results_file()

    matched = []

    for item in results:

        if not isinstance(item, dict):
            continue

        item_run_id = (
            item.get("run_id")
            or item.get("runId")
            or item.get("run")
        )

        if str(item_run_id) == str(run_id):
            matched.append(item)

    if not matched:
        return None

    def score(item):

        points = 0

        stage = str(
            item.get("stage", "")
        ).lower()

        # Prefer final/full pipeline records
        if "full_pipeline" in stage:
            points += 1000

        if "quality_pipeline" in stage:
            points += 900

        if "quality" in stage:
            points += 800

        if "elt" in stage:
            points += 700

        # Classification metrics
        classification_keys = [
            "valid",
            "valid_count",
            "count_valid",

            "corrected",
            "corrected_count",
            "count_corrected",

            "quarantine",
            "quarantine_count",
            "count_quarantine",
        ]

        for key in classification_keys:

            if key in item:
                points += 50

        # Upsert metrics
        upsert_keys = [
            "inserted",
            "inserted_count",
            "count_inserted",

            "updated",
            "updated_count",
            "count_updated",

            "unchanged",
            "unchanged_count",
            "count_unchanged",
        ]

        for key in upsert_keys:

            if key in item:
                points += 30

        # Raw metrics
        raw_keys = [
            "raw",
            "raw_count",
            "loaded_raw",
            "read_rows",
            "rows_loaded_raw",
        ]

        for key in raw_keys:

            if key in item:
                points += 10

        return points

    return max(
        matched,
        key=score
    )


# =========================================================
# Normalize Metrics
# =========================================================

def normalize_metrics(metrics):
    """
    Convert metrics from different pipeline stages into
    one consistent structure for the Dashboard.
    """

    if not metrics:
        return {}

    return {

        "run_id": first_present(
            metrics,
            "run_id",
            "runId",
            "run",
            default=None,
        ),

        "stage": metrics.get(
            "stage"
        ),

        "engine": first_present(
            metrics,
            "engine",
            "used_engine",
            default=None,
        ),

        # -------------------------
        # Classification
        # -------------------------

        "raw": first_present(
            metrics,
            "raw",
            "raw_count",
            "loaded_raw",
            "read_rows",
            "rows_loaded_raw",
            default=0,
        ),

        "valid": first_present(
            metrics,
            "valid",
            "valid_count",
            "count_valid",
            default=0,
        ),

        "corrected": first_present(
            metrics,
            "corrected",
            "corrected_count",
            "count_corrected",
            default=0,
        ),

        "quarantine": first_present(
            metrics,
            "quarantine",
            "quarantine_count",
            "count_quarantine",
            default=0,
        ),

        # -------------------------
        # Idempotency / Upsert
        # -------------------------

        "inserted": first_present(
            metrics,
            "inserted",
            "inserted_count",
            "count_inserted",
            default=0,
        ),

        "updated": first_present(
            metrics,
            "updated",
            "updated_count",
            "count_updated",
            default=0,
        ),

        "unchanged": first_present(
            metrics,
            "unchanged",
            "unchanged_count",
            "count_unchanged",
            default=0,
        ),

        # -------------------------
        # Performance
        # -------------------------

        "elapsed_seconds": first_present(
            metrics,
            "elapsed_seconds",
            "elapsed_time",
            "elapsed",
            "seconds_elapsed",
            default=None,
        ),

        "throughput": first_present(
            metrics,
            "throughput",
            "throughput_records_sec",
            default=None,
        ),

        "partitions": first_present(
            metrics,
            "output_partitions",
            "partitions",
            "input_partitions",
            default=None,
        ),

        "batch_size": first_present(
            metrics,
            "batch_size",
            default=None,
        ),

        # -------------------------
        # Consistency / Errors
        # -------------------------

        "consistency": first_present(
            metrics,
            "consistency",
            "consistency_check",
            default=None,
        ),

        "errors": first_present(
            metrics,
            "errors",
            "error_counts",
            "counts_case_error",
            default={},
        ),
    }


# =========================================================
# Job Log
# =========================================================

def append_job_log(job_id, line):

    with jobs_lock:

        if job_id not in jobs:
            return

        jobs[job_id]["log"].append(line)

        # Prevent unlimited memory growth
        if len(jobs[job_id]["log"]) > 3000:

            jobs[job_id]["log"] = (
                jobs[job_id]["log"][-3000:]
            )


# =========================================================
# Background Pipeline
# =========================================================

def run_pipeline_job(
    job_id,
    input_file,
    run_id,
    router_info,
):

    start_time = time.time()

    with jobs_lock:

        jobs[job_id]["status"] = "running"
        jobs[job_id]["started_at"] = start_time

    append_job_log(
        job_id,
        "=" * 65
    )

    append_job_log(
        job_id,
        "HYBRID BIG DATA PIPELINE"
    )

    append_job_log(
        job_id,
        "=" * 65
    )

    append_job_log(
        job_id,
        f"Run ID: {run_id}"
    )

    append_job_log(
        job_id,
        f"File: {Path(input_file).name}"
    )

    append_job_log(
        job_id,
        f"File Size: {router_info['file_size_mb']:.2f} MB"
    )

    append_job_log(
        job_id,
        f"Selected Engine: {router_info['engine']}"
    )

    append_job_log(
        job_id,
        f"Reason: {router_info['reason']}"
    )

    append_job_log(
        job_id,
        "-" * 65
    )

    # =====================================================
    # Python executable
    # =====================================================

    python_exe = (
        ROOT_DIR
        / ".venv"
        / "Scripts"
        / "python.exe"
    )

    if not python_exe.exists():

        python_exe = Path(
            sys.executable
        )

    command = [
        str(python_exe),
        "-u",
        "-m",
        "src.main",
        "--input",
        str(input_file),
        "--run-id",
        str(run_id),
    ]

    # =====================================================
    # Environment
    # =====================================================

    env = os.environ.copy()

    venv_python = (
        ROOT_DIR
        / ".venv"
        / "Scripts"
        / "python.exe"
    )

    if venv_python.exists():

        env["PYSPARK_PYTHON"] = str(
            venv_python
        )

        env["PYSPARK_DRIVER_PYTHON"] = str(
            venv_python
        )

    # =====================================================
    # Run
    # =====================================================

    try:

        process = subprocess.Popen(
            command,
            cwd=str(ROOT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        )

        with jobs_lock:

            jobs[job_id]["pid"] = (
                process.pid
            )

        if process.stdout:

            for line in process.stdout:

                clean_line = (
                    line.rstrip()
                )

                if clean_line:

                    append_job_log(
                        job_id,
                        clean_line
                    )

        return_code = (
            process.wait()
        )

        elapsed = (
            time.time()
            - start_time
        )

        # Wait briefly for results.json write to complete
        time.sleep(1)

        # =================================================
        # Read final metrics
        # =================================================

        metrics = find_run_metrics(
            run_id
        )

        normalized = normalize_metrics(
            metrics
        )

        if metrics:

            append_job_log(
                job_id,
                "-" * 65
            )

            append_job_log(
                job_id,
                "FINAL DASHBOARD METRICS"
            )

            append_job_log(
                job_id,
                f"Stage: {metrics.get('stage', '-')}"
            )

            append_job_log(
                job_id,
                f"Raw: {normalized.get('raw', 0)}"
            )

            append_job_log(
                job_id,
                f"Valid: {normalized.get('valid', 0)}"
            )

            append_job_log(
                job_id,
                f"Corrected: {normalized.get('corrected', 0)}"
            )

            append_job_log(
                job_id,
                f"Quarantine: {normalized.get('quarantine', 0)}"
            )

            append_job_log(
                job_id,
                f"Inserted: {normalized.get('inserted', 0)}"
            )

            append_job_log(
                job_id,
                f"Updated: {normalized.get('updated', 0)}"
            )

            append_job_log(
                job_id,
                f"Unchanged: {normalized.get('unchanged', 0)}"
            )

        else:

            append_job_log(
                job_id,
                "WARNING: Final metrics not found in reports/results.json"
            )

        # =================================================
        # Finish
        # =================================================

        if return_code == 0:

            append_job_log(
                job_id,
                "-" * 65
            )

            append_job_log(
                job_id,
                "PIPELINE COMPLETED SUCCESSFULLY"
            )

            append_job_log(
                job_id,
                "=" * 65
            )

            with jobs_lock:

                jobs[job_id]["status"] = (
                    "completed"
                )

                jobs[job_id]["success"] = (
                    True
                )

                jobs[job_id]["elapsed"] = round(
                    elapsed,
                    2
                )

                jobs[job_id]["metrics"] = (
                    normalized
                )

        else:

            append_job_log(
                job_id,
                "-" * 65
            )

            append_job_log(
                job_id,
                f"PIPELINE FAILED - Exit Code: {return_code}"
            )

            with jobs_lock:

                jobs[job_id]["status"] = (
                    "failed"
                )

                jobs[job_id]["success"] = (
                    False
                )

                jobs[job_id]["elapsed"] = round(
                    elapsed,
                    2
                )

                jobs[job_id]["metrics"] = (
                    normalized
                )

                jobs[job_id]["error"] = (
                    f"Pipeline exited with code {return_code}"
                )

    except Exception as exc:

        append_job_log(
            job_id,
            f"ERROR: {exc}"
        )

        with jobs_lock:

            jobs[job_id]["status"] = (
                "failed"
            )

            jobs[job_id]["success"] = (
                False
            )

            jobs[job_id]["error"] = (
                str(exc)
            )

            jobs[job_id]["elapsed"] = round(
                time.time() - start_time,
                2
            )


# =========================================================
# Main Dashboard
# =========================================================

@app.route("/")
def home():

    return render_template(
        "presentation.html"
    )


# =========================================================
# Screenshots
# =========================================================

@app.route(
    "/screenshots/<path:filename>"
)
def screenshots(filename):

    return send_from_directory(
        str(SCREENSHOTS_DIR),
        filename,
    )


# =========================================================
# API - Run Professor File
# =========================================================

@app.route(
    "/api/run",
    methods=["POST"]
)
def api_run():

    if "file" not in request.files:

        return jsonify({
            "success": False,
            "error": "لم يتم اختيار ملف CSV"
        }), 400

    uploaded_file = (
        request.files["file"]
    )

    if not uploaded_file.filename:

        return jsonify({
            "success": False,
            "error": "اسم الملف غير صالح"
        }), 400

    if not uploaded_file.filename.lower().endswith(
        ".csv"
    ):

        return jsonify({
            "success": False,
            "error": "يجب اختيار ملف CSV"
        }), 400

    original_name = Path(
        uploaded_file.filename
    ).name

    unique_name = (
        f"{int(time.time())}_"
        f"{uuid4().hex[:8]}_"
        f"{original_name}"
    )

    destination = (
        UPLOAD_DIR
        / unique_name
    )

    # =====================================================
    # Save uploaded file
    # =====================================================

    try:

        uploaded_file.save(
            str(destination)
        )

    except Exception as exc:

        return jsonify({
            "success": False,
            "error": f"فشل حفظ الملف: {exc}"
        }), 500

    # =====================================================
    # Router
    # =====================================================

    try:

        router_info = detect_engine(
            destination
        )

    except Exception as exc:

        return jsonify({
            "success": False,
            "error": f"فشل قراءة حجم الملف: {exc}"
        }), 500

    run_id = str(
        uuid4()
    )

    job_id = str(
        uuid4()
    )

    job = {

        "job_id": job_id,

        "run_id": run_id,

        "filename": original_name,

        "stored_file": str(
            destination
        ),

        "file_size_mb":
            router_info["file_size_mb"],

        "engine":
            router_info["engine"],

        "reason":
            router_info["reason"],

        "status":
            "queued",

        "success":
            None,

        "log":
            [],

        "metrics":
            {},

        "error":
            None,

        "created_at":
            time.time(),
    }

    with jobs_lock:

        jobs[job_id] = job

    thread = Thread(
        target=run_pipeline_job,
        args=(
            job_id,
            destination,
            run_id,
            router_info,
        ),
        daemon=True,
    )

    thread.start()

    return jsonify({

        "success":
            True,

        "job_id":
            job_id,

        "run_id":
            run_id,

        "filename":
            original_name,

        "file_size_mb":
            router_info["file_size_mb"],

        "engine":
            router_info["engine"],

        "reason":
            router_info["reason"],

        "status":
            "queued",
    })


# =========================================================
# API - Job Status
# =========================================================

@app.route(
    "/api/status/<job_id>"
)
def api_status(job_id):

    with jobs_lock:

        job = jobs.get(
            job_id
        )

        if not job:

            return jsonify({
                "success": False,
                "error": "Job غير موجود"
            }), 404

        response = {

            "success":
                True,

            "job_id":
                job["job_id"],

            "run_id":
                job["run_id"],

            "filename":
                job["filename"],

            "file_size_mb":
                job["file_size_mb"],

            "engine":
                job["engine"],

            "reason":
                job["reason"],

            "status":
                job["status"],

            "pipeline_success":
                job["success"],

            "log":
                list(
                    job["log"]
                ),

            "metrics":
                dict(
                    job.get("metrics")
                    or {}
                ),

            "error":
                job.get("error"),
        }

        if "elapsed" in job:

            response["elapsed"] = (
                job["elapsed"]
            )

    return jsonify(
        response
    )


# =========================================================
# API - Project Summary
# =========================================================

@app.route(
    "/api/summary"
)
def api_summary():

    return jsonify({

        "success": True,

        # Latest tested Python Batch result
        "small_sample": {

            "engine":
                "python_batch",

            "file":
                "orders_small_sample.csv",

            "file_size_mb":
                41.77,

            "raw":
                100000,

            "valid":
                78437,

            "corrected":
                12639,

            "quarantine":
                8924,

            "consistency":
                True,
        },

        # Proven 30M PySpark result
        "large_dataset": {

            "engine":
                "pyspark",

            "raw":
                30000000,

            "valid":
                208594,

            "corrected":
                27580876,

            "quarantine":
                2210530,

            "elapsed_seconds":
                3364.70,

            "throughput":
                8916.09,

            "input_partitions":
                229,

            "output_partitions":
                99,

            "consistency":
                True,
        },

        "router": {

            "threshold_mb":
                200,

            "small_engine":
                "python_batch",

            "large_engine":
                "pyspark",
        },

        "upsert_proof": {

            "inserted":
                0,

            "updated":
                1,

            "unchanged":
                1,

            "passed":
                True,
        }
    })


# =========================================================
# Error Handlers
# =========================================================

@app.errorhandler(413)
def file_too_large(error):

    return jsonify({
        "success": False,
        "error": "حجم الملف أكبر من الحد المسموح"
    }), 413


@app.errorhandler(404)
def not_found(error):

    return jsonify({
        "success": False,
        "error": "الصفحة المطلوبة غير موجودة"
    }), 404


# =========================================================
# Start Flask
# =========================================================

if __name__ == "__main__":

    print("=" * 65)

    print(
        "Hybrid Big Data Pipeline"
    )

    print(
        "Arabic Presentation Dashboard"
    )

    print("=" * 65)

    print(
        "Open:"
    )

    print(
        "http://127.0.0.1:5000"
    )

    print("=" * 65)

    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False,
        threaded=True,
    )