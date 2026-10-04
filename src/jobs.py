from datetime import datetime, timezone
from time import sleep

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
)

from src.indexes import create_indexes
from src.materialized_views import (
    refresh_all_materialized_views,
)


# =========================================================
# Collections / Schedule metadata
# =========================================================

JOB_LOG_COLLECTION = "job_logs"


JOB_DEFINITIONS = {
    "refresh_materialized_views": {
        "name":
            "refresh_materialized_views",

        "description":
            "Refresh all materialized views incrementally.",

        "schedule":
            "Daily at 01:00",
    },

    "ensure_indexes": {
        "name":
            "ensure_indexes",

        "description":
            "Ensure required MongoDB indexes exist.",

        "schedule":
            "Daily at 01:30",
    },
}


# =========================================================
# Helpers
# =========================================================

def utc_now():
    return datetime.now(
        timezone.utc
    )


def get_database():
    client = MongoClient(
        MONGO_URI
    )

    database = client[
        DATABASE_NAME
    ]

    return client, database


def start_job_log(
    log_collection,
    job_name,
):
    started_at = utc_now()

    result = log_collection.insert_one(
        {
            "job_name":
                job_name,

            "started_at":
                started_at,

            "ended_at":
                None,

            "status":
                "running",

            "message":
                None,
        }
    )

    return (
        result.inserted_id,
        started_at,
    )


def finish_job_log(
    log_collection,
    log_id,
    status,
    message,
):
    ended_at = utc_now()

    log_collection.update_one(
        {
            "_id":
                log_id
        },
        {
            "$set": {
                "ended_at":
                    ended_at,

                "status":
                    status,

                "message":
                    message,
            }
        },
    )

    return ended_at


# =========================================================
# Job 1
# Refresh Materialized Views
# =========================================================

def job_refresh_materialized_views():
    return (
        refresh_all_materialized_views()
    )


# =========================================================
# Job 2
# Ensure Indexes
# =========================================================

def job_ensure_indexes():
    return (
        create_indexes()
    )


# =========================================================
# Job Registry
# =========================================================

JOB_FUNCTIONS = {
    "refresh_materialized_views":
        job_refresh_materialized_views,

    "ensure_indexes":
        job_ensure_indexes,
}


# =========================================================
# Run one job manually
# =========================================================

def run_job(job_name):
    job_function = (
        JOB_FUNCTIONS.get(
            job_name
        )
    )

    if job_function is None:
        raise ValueError(
            f"Unknown job: "
            f"{job_name}"
        )

    client, database = (
        get_database()
    )

    try:
        log_collection = database[
            JOB_LOG_COLLECTION
        ]

        log_id, started_at = (
            start_job_log(
                log_collection,
                job_name,
            )
        )

        try:
            result = job_function()

            ended_at = (
                finish_job_log(
                    log_collection,
                    log_id,
                    "success",
                    "Job completed successfully.",
                )
            )

            return {
                "job_name":
                    job_name,

                "status":
                    "success",

                "started_at":
                    started_at,

                "ended_at":
                    ended_at,

                "result":
                    result,
            }

        except Exception as exc:
            ended_at = (
                finish_job_log(
                    log_collection,
                    log_id,
                    "failed",
                    str(exc),
                )
            )

            return {
                "job_name":
                    job_name,

                "status":
                    "failed",

                "started_at":
                    started_at,

                "ended_at":
                    ended_at,

                "error":
                    str(exc),
            }

    finally:
        client.close()


# =========================================================
# List Jobs
# =========================================================

def list_jobs():
    return [
        {
            **definition,
            "manual_run":
                f"python -m src.jobs "
                f"{job_name}",
        }

        for (
            job_name,
            definition,
        ) in JOB_DEFINITIONS.items()
    ]


# =========================================================
# Read recent logs
# =========================================================

def get_recent_job_logs(
    limit=20,
):
    client, database = (
        get_database()
    )

    try:
        logs = database[
            JOB_LOG_COLLECTION
        ]

        documents = list(
            logs.find(
                {}
            )
            .sort(
                "started_at",
                -1,
            )
            .limit(
                int(limit)
            )
        )

        results = []

        for document in documents:
            results.append(
                {
                    "_id":
                        str(
                            document["_id"]
                        ),

                    "job_name":
                        document.get(
                            "job_name"
                        ),

                    "started_at":
                        document.get(
                            "started_at"
                        ),

                    "ended_at":
                        document.get(
                            "ended_at"
                        ),

                    "status":
                        document.get(
                            "status"
                        ),

                    "message":
                        document.get(
                            "message"
                        ),
                }
            )

        return results

    finally:
        client.close()


# =========================================================
# Simple scheduler loop
#
# This is intentionally simple and manually runnable.
#
# Schedules:
# - refresh_materialized_views -> daily at 01:00
# - ensure_indexes             -> daily at 01:30
# =========================================================

def scheduler_loop():
    last_run = {
        "refresh_materialized_views":
            None,

        "ensure_indexes":
            None,
    }

    print(
        "Job scheduler started."
    )

    print(
        "Schedules:"
    )

    print(
        "- refresh_materialized_views: "
        "daily at 01:00"
    )

    print(
        "- ensure_indexes: "
        "daily at 01:30"
    )

    while True:
        now = datetime.now()

        current_date = (
            now.date().isoformat()
        )

        current_time = (
            now.strftime(
                "%H:%M"
            )
        )

        if (
            current_time == "01:00"
            and last_run[
                "refresh_materialized_views"
            ] != current_date
        ):
            result = run_job(
                "refresh_materialized_views"
            )

            print(
                result
            )

            last_run[
                "refresh_materialized_views"
            ] = current_date

        if (
            current_time == "01:30"
            and last_run[
                "ensure_indexes"
            ] != current_date
        ):
            result = run_job(
                "ensure_indexes"
            )

            print(
                result
            )

            last_run[
                "ensure_indexes"
            ] = current_date

        sleep(
            30
        )


# =========================================================
# Manual execution
#
# Examples:
#
# python -m src.jobs refresh_materialized_views
# python -m src.jobs ensure_indexes
# python -m src.jobs scheduler
# =========================================================

if __name__ == "__main__":
    import sys

    if len(
        sys.argv
    ) < 2:
        print(
            "Available jobs:"
        )

        for job in list_jobs():
            print(
                f"- {job['name']}: "
                f"{job['description']} "
                f"({job['schedule']})"
            )

        print(
            "\nCommands:"
        )

        print(
            "python -m src.jobs "
            "refresh_materialized_views"
        )

        print(
            "python -m src.jobs "
            "ensure_indexes"
        )

        print(
            "python -m src.jobs "
            "scheduler"
        )

    else:
        command = sys.argv[1]

        if command == "scheduler":
            scheduler_loop()

        else:
            result = run_job(
                command
            )

            print(
                result
            )