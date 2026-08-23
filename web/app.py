import json
import os
import subprocess
import sys
import threading
import uuid
from pathlib import Path

from flask import Flask, jsonify, render_template, request
from werkzeug.utils import secure_filename


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WEB_ROOT = PROJECT_ROOT / "web"
UPLOAD_DIR = WEB_ROOT / "uploads"
RESULTS_FILE = PROJECT_ROOT / "reports" / "results.json"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.file_router import choose_engine


app = Flask(
    __name__,
    template_folder=str(WEB_ROOT / "templates"),
    static_folder=str(WEB_ROOT / "static"),
)

app.config["MAX_CONTENT_LENGTH"] = 20 * 1024 * 1024 * 1024
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

jobs = {}


def load_results():
    if not RESULTS_FILE.exists():
        return {"project": "midterm-data-pipeline", "runs": []}

    try:
        return json.loads(
            RESULTS_FILE.read_text(encoding="utf-8")
        )
    except Exception:
        return {"project": "midterm-data-pipeline", "runs": []}


def find_run_metrics(run_id):
    data = load_results()

    matching = [
        run
        for run in data.get("runs", [])
        if run.get("run_id") == run_id
    ]

    if not matching:
        return None

    quality_runs = [
        run for run in matching
        if run.get("stage") in {
            "spark_quality",
            "python_batch_full_pipeline",
        }
    ]

    if quality_runs:
        return quality_runs[-1]

    return matching[-1]


def get_project_summary():
    data = load_results()

    large_run_id = "5400b97a-5d21-48c1-a3e7-6c3285bc69d6"

    for run in reversed(data.get("runs", [])):
        if (
            run.get("run_id") == large_run_id
            and run.get("stage") == "spark_quality"
        ):
            return run

    return {}


def run_pipeline(job_id, file_path, run_id):
    job = jobs[job_id]
    job["status"] = "running"

    env = os.environ.copy()
    env["PYSPARK_PYTHON"] = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable
    env["PYTHONUNBUFFERED"] = "1"

    command = [
        sys.executable,
        "-u",
        "-m",
        "src.main",
        "--input",
        str(file_path),
        "--run-id",
        run_id,
    ]

    try:
        process = subprocess.Popen(
            command,
            cwd=PROJECT_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

        job["pid"] = process.pid

        if process.stdout is not None:
            for line in process.stdout:
                job["log"].append(line.rstrip())

                if len(job["log"]) > 1000:
                    job["log"] = job["log"][-1000:]

        return_code = process.wait()

        if return_code != 0:
            job["status"] = "failed"
            job["error"] = (
                f"Pipeline exited with code {return_code}"
            )
            return

        metrics = find_run_metrics(run_id)

        if metrics is None:
            job["status"] = "failed"
            job["error"] = (
                "Pipeline completed but metrics were not found."
            )
            return

        job["result"] = metrics
        job["status"] = "completed"

    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)


@app.get("/")
def index():
    return render_template(
        "index.html",
        summary=get_project_summary(),
    )


@app.post("/api/run")
def start_run():
    if "file" not in request.files:
        return jsonify(
            {"error": "No CSV file was uploaded."}
        ), 400

    uploaded_file = request.files["file"]

    if not uploaded_file.filename:
        return jsonify(
            {"error": "No file was selected."}
        ), 400

    filename = secure_filename(uploaded_file.filename)

    if not filename.lower().endswith(".csv"):
        return jsonify(
            {"error": "Only CSV files are supported."}
        ), 400

    unique_name = (
        f"{uuid.uuid4().hex}_{filename}"
    )

    saved_path = UPLOAD_DIR / unique_name
    uploaded_file.save(saved_path)

    route = choose_engine(saved_path)
    run_id = str(uuid.uuid4())
    job_id = uuid.uuid4().hex

    jobs[job_id] = {
        "job_id": job_id,
        "run_id": run_id,
        "file_name": filename,
        "file_size_mb": route["file_size_mb"],
        "engine": route["engine"],
        "reason": route["reason"],
        "status": "queued",
        "log": [],
        "result": None,
        "error": None,
    }

    thread = threading.Thread(
        target=run_pipeline,
        args=(job_id, saved_path, run_id),
        daemon=True,
    )
    thread.start()

    return jsonify(jobs[job_id])


@app.get("/api/status/<job_id>")
def job_status(job_id):
    job = jobs.get(job_id)

    if job is None:
        return jsonify(
            {"error": "Job not found."}
        ), 404

    return jsonify(job)


@app.get("/api/summary")
def project_summary():
    return jsonify(get_project_summary())


if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False,
        threaded=True,
    )
