import json
from datetime import datetime, timezone
from pathlib import Path


RESULTS_FILE = Path("reports/results.json")


def load_results():
    if not RESULTS_FILE.exists():
        return {
            "project": "midterm-data-pipeline",
            "runs": [],
        }

    try:
        with RESULTS_FILE.open(
            "r",
            encoding="utf-8",
        ) as file:
            return json.load(file)

    except json.JSONDecodeError:
        return {
            "project": "midterm-data-pipeline",
            "runs": [],
        }


def save_run_metrics(metrics):
    RESULTS_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    results = load_results()

    metrics = dict(metrics)

    metrics["recorded_at"] = datetime.now(
        timezone.utc
    ).isoformat()

    results["runs"].append(metrics)

    with RESULTS_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            results,
            file,
            ensure_ascii=False,
            indent=4,
        )

    print(
        f"Metrics saved to: {RESULTS_FILE}"
    )


def get_file_size_mb(file_path):
    path = Path(file_path)

    if not path.exists():
        return None

    return path.stat().st_size / (
        1024 * 1024
    )