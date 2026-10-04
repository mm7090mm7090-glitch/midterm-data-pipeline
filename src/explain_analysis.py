import json
from pathlib import Path

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    VALIDATED_COLLECTION,
)

from src.indexes import (
    create_indexes,
    drop_project_indexes,
)


REPORT_PATH = Path(
    "reports/explain_results.json"
)


# =========================================================
# Helpers
# =========================================================

def get_collection():
    client = MongoClient(
        MONGO_URI
    )

    database = client[
        DATABASE_NAME
    ]

    collection = database[
        VALIDATED_COLLECTION
    ]

    return client, database, collection


def collect_stages(plan):
    stages = []

    if isinstance(plan, dict):
        stage = plan.get("stage")

        if stage:
            stages.append(stage)

        for value in plan.values():
            stages.extend(
                collect_stages(value)
            )

    elif isinstance(plan, list):
        for item in plan:
            stages.extend(
                collect_stages(item)
            )

    return stages


def run_explain(
    database,
    collection_name,
    query_filter,
    sort=None,
    limit=100,
):
    find_command = {
        "find": collection_name,
        "filter": query_filter,
        "limit": limit,
    }

    if sort:
        find_command["sort"] = {
            field: direction
            for field, direction in sort
        }

    result = database.command(
        "explain",
        find_command,
        verbosity="executionStats",
    )

    execution_stats = result.get(
        "executionStats",
        {},
    )

    query_planner = result.get(
        "queryPlanner",
        {},
    )

    winning_plan = query_planner.get(
        "winningPlan",
        {},
    )

    stages = collect_stages(
        winning_plan
    )

    return {
        "n_returned":
            execution_stats.get(
                "nReturned",
                0,
            ),

        "execution_time_ms":
            execution_stats.get(
                "executionTimeMillis",
                0,
            ),

        "total_docs_examined":
            execution_stats.get(
                "totalDocsExamined",
                0,
            ),

        "total_keys_examined":
            execution_stats.get(
                "totalKeysExamined",
                0,
            ),

        "winning_plan_stages":
            list(
                dict.fromkeys(
                    stages
                )
            ),
    }


# =========================================================
# Pick real values dynamically from the database
# No hardcoded city/status/date values
# =========================================================

def get_sample_values(collection):
    sample = collection.find_one(
        {
            "city": {
                "$nin": [
                    None,
                    "",
                ]
            },
            "status": {
                "$nin": [
                    None,
                    "",
                ]
            },
            "order_date": {
                "$nin": [
                    None,
                    "",
                ]
            },
        },
        {
            "_id": 0,
            "city": 1,
            "status": 1,
            "order_date": 1,
        },
    )

    if sample is None:
        raise RuntimeError(
            "No suitable validated order "
            "was found for explain testing."
        )

    order_date = str(
        sample["order_date"]
    )

    date_part = order_date[:10]

    start_date = (
        f"{date_part}T00:00:00"
    )

    end_date = (
        f"{date_part}T23:59:59"
    )

    return {
        "city": sample["city"],
        "status": sample["status"],
        "start_date": start_date,
        "end_date": end_date,
    }


# =========================================================
# Three practical queries required for explain comparison
# =========================================================

def build_explain_queries(values):
    return {
        "orders_by_city": {
            "filter": {
                "city":
                    values["city"]
            },
            "sort": [
                ("order_date", -1)
            ],
        },

        "orders_by_date_range": {
            "filter": {
                "order_date": {
                    "$gte":
                        values[
                            "start_date"
                        ],

                    "$lte":
                        values[
                            "end_date"
                        ],
                }
            },
            "sort": [
                ("order_date", 1)
            ],
        },

        "orders_by_status_and_date": {
            "filter": {
                "status":
                    values["status"],

                "order_date": {
                    "$gte":
                        values[
                            "start_date"
                        ],

                    "$lte":
                        values[
                            "end_date"
                        ],
                },
            },
            "sort": [
                ("order_date", 1)
            ],
        },
    }


# =========================================================
# Compare before and after indexes
# =========================================================

def compare_explain():
    client, database, collection = (
        get_collection()
    )

    try:
        if collection.count_documents(
            {}
        ) == 0:
            raise RuntimeError(
                "orders_validated is empty."
            )

        sample_values = (
            get_sample_values(
                collection
            )
        )

        queries = (
            build_explain_queries(
                sample_values
            )
        )

        # ---------------------------------------------
        # BEFORE indexes
        # ---------------------------------------------

        drop_project_indexes()

        before_results = {}

        for name, query in queries.items():
            before_results[name] = (
                run_explain(
                    database,
                    VALIDATED_COLLECTION,
                    query["filter"],
                    query["sort"],
                )
            )

        # ---------------------------------------------
        # AFTER indexes
        # ---------------------------------------------

        create_indexes()

        after_results = {}

        for name, query in queries.items():
            after_results[name] = (
                run_explain(
                    database,
                    VALIDATED_COLLECTION,
                    query["filter"],
                    query["sort"],
                )
            )

        # ---------------------------------------------
        # Build report
        # ---------------------------------------------

        comparisons = {}

        for name in queries:
            before = before_results[
                name
            ]

            after = after_results[
                name
            ]

            comparisons[name] = {
                "before": before,
                "after": after,

                "docs_examined_reduction":
                    before[
                        "total_docs_examined"
                    ]
                    - after[
                        "total_docs_examined"
                    ],

                "keys_examined_change":
                    after[
                        "total_keys_examined"
                    ]
                    - before[
                        "total_keys_examined"
                    ],
            }

        report = {
            "database":
                DATABASE_NAME,

            "collection":
                VALIDATED_COLLECTION,

            "sample_values":
                sample_values,

            "comparisons":
                comparisons,
        }

        REPORT_PATH.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with REPORT_PATH.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                report,
                file,
                ensure_ascii=False,
                indent=2,
            )

        return report

    finally:
        client.close()


# =========================================================
# Manual execution
# =========================================================

if __name__ == "__main__":
    report = compare_explain()

    print(
        "\nExplain comparison completed.\n"
    )

    for (
        query_name,
        comparison,
    ) in report[
        "comparisons"
    ].items():

        before = comparison[
            "before"
        ]

        after = comparison[
            "after"
        ]

        print(
            f"{query_name}"
        )

        print(
            "  BEFORE:"
        )

        print(
            f"    Docs examined: "
            f"{before['total_docs_examined']}"
        )

        print(
            f"    Keys examined: "
            f"{before['total_keys_examined']}"
        )

        print(
            f"    Plan: "
            f"{before['winning_plan_stages']}"
        )

        print(
            "  AFTER:"
        )

        print(
            f"    Docs examined: "
            f"{after['total_docs_examined']}"
        )

        print(
            f"    Keys examined: "
            f"{after['total_keys_examined']}"
        )

        print(
            f"    Plan: "
            f"{after['winning_plan_stages']}"
        )

        print()

    print(
        f"Report saved to: "
        f"{REPORT_PATH}"
    )