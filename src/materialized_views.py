from datetime import datetime, timezone

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    VALIDATED_COLLECTION,
)


# =========================================================
# Materialized View Collections
# =========================================================

DAILY_SALES_MV = (
    "daily_sales_summary"
)

CITY_SALES_MV = (
    "city_sales_summary"
)

MV_STATE_COLLECTION = (
    "mv_refresh_state"
)


# =========================================================
# Numeric conversion helper
# =========================================================

def numeric_total_amount():
    return {
        "$convert": {
            "input": "$total_amount",
            "to": "double",
            "onError": 0,
            "onNull": 0,
        }
    }


# =========================================================
# Build source match from watermark
# =========================================================

def build_incremental_match(
    last_processed_id,
    upper_bound_id,
):
    object_id_filter = {
        "$lte": upper_bound_id,
    }

    if last_processed_id is not None:
        object_id_filter[
            "$gt"
        ] = last_processed_id

    return {
        "_id": object_id_filter
    }


# =========================================================
# Read refresh state
# =========================================================

def get_refresh_state(
    state_collection,
    view_name,
):
    state = state_collection.find_one(
        {
            "_id": view_name
        }
    )

    if state is None:
        return None

    return state.get(
        "last_processed_id"
    )


# =========================================================
# Save refresh state
# =========================================================

def save_refresh_state(
    state_collection,
    view_name,
    upper_bound_id,
    processed_documents,
):
    state_collection.update_one(
        {
            "_id": view_name
        },
        {
            "$set": {
                "last_processed_id":
                    upper_bound_id,

                "last_refresh_at":
                    datetime.now(
                        timezone.utc
                    ),

                "processed_documents":
                    processed_documents,
            }
        },
        upsert=True,
    )


# =========================================================
# Ensure MV indexes
# =========================================================

def ensure_mv_indexes(database):
    database[
        DAILY_SALES_MV
    ].create_index(
        "date",
        unique=True,
        name="uq_daily_sales_date",
    )

    database[
        CITY_SALES_MV
    ].create_index(
        "city",
        unique=True,
        name="uq_city_sales_city",
    )


# =========================================================
# Refresh daily_sales_summary
# Incremental:
# only dates affected by NEW source documents are rebuilt.
# =========================================================

def refresh_daily_sales(
    database,
):
    source = database[
        VALIDATED_COLLECTION
    ]

    target = database[
        DAILY_SALES_MV
    ]

    state_collection = database[
        MV_STATE_COLLECTION
    ]

    latest_document = source.find_one(
        {},
        {
            "_id": 1
        },
        sort=[
            ("_id", -1)
        ],
    )

    if latest_document is None:
        return {
            "view":
                DAILY_SALES_MV,

            "status":
                "no_data",

            "processed_documents":
                0,

            "affected_keys":
                0,
        }

    upper_bound_id = (
        latest_document["_id"]
    )

    last_processed_id = (
        get_refresh_state(
            state_collection,
            DAILY_SALES_MV,
        )
    )

    # Nothing new since the last refresh.
    if (
        last_processed_id
        == upper_bound_id
    ):
        return {
            "view":
                DAILY_SALES_MV,

            "status":
                "up_to_date",

            "processed_documents":
                0,

            "affected_keys":
                0,
        }

    incremental_match = (
        build_incremental_match(
            last_processed_id,
            upper_bound_id,
        )
    )

    processed_documents = (
        source.count_documents(
            incremental_match
        )
    )

    # -----------------------------------------------------
    # Find only dates touched by new documents
    # -----------------------------------------------------

    affected_dates = list(
        source.aggregate(
            [
                {
                    "$match":
                        incremental_match
                },

                {
                    "$project": {
                        "date": {
                            "$substrBytes": [
                                "$order_date",
                                0,
                                10,
                            ]
                        }
                    }
                },

                {
                    "$match": {
                        "date": {
                            "$nin": [
                                None,
                                "",
                            ]
                        }
                    }
                },

                {
                    "$group": {
                        "_id": "$date"
                    }
                },

                {
                    "$sort": {
                        "_id": 1
                    }
                },
            ]
        )
    )

    refreshed_at = datetime.now(
        timezone.utc
    )

    # -----------------------------------------------------
    # Recalculate only the affected days.
    #
    # We use $set instead of $inc so retrying the refresh
    # cannot double-count a partially completed refresh.
    # -----------------------------------------------------

    for affected_date in affected_dates:
        date_value = (
            affected_date["_id"]
        )

        result = list(
            source.aggregate(
                [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": [
                                    {
                                        "$substrBytes": [
                                            "$order_date",
                                            0,
                                            10,
                                        ]
                                    },
                                    date_value,
                                ]
                            }
                        }
                    },

                    {
                        "$group": {
                            "_id": None,

                            "order_count": {
                                "$sum": 1
                            },

                            "total_sales": {
                                "$sum":
                                    numeric_total_amount()
                            },
                        }
                    },
                ]
            )
        )

        if not result:
            continue

        summary = result[0]

        target.update_one(
            {
                "date":
                    date_value
            },
            {
                "$set": {
                    "date":
                        date_value,

                    "order_count":
                        summary[
                            "order_count"
                        ],

                    "total_sales":
                        round(
                            summary[
                                "total_sales"
                            ],
                            2,
                        ),

                    "refreshed_at":
                        refreshed_at,
                }
            },
            upsert=True,
        )

    save_refresh_state(
        state_collection,
        DAILY_SALES_MV,
        upper_bound_id,
        processed_documents,
    )

    return {
        "view":
            DAILY_SALES_MV,

        "status":
            "refreshed",

        "processed_documents":
            processed_documents,

        "affected_keys":
            len(
                affected_dates
            ),

        "last_processed_id":
            str(
                upper_bound_id
            ),
    }


# =========================================================
# Refresh city_sales_summary
# Incremental:
# only cities affected by NEW source documents are rebuilt.
# =========================================================

def refresh_city_sales(
    database,
):
    source = database[
        VALIDATED_COLLECTION
    ]

    target = database[
        CITY_SALES_MV
    ]

    state_collection = database[
        MV_STATE_COLLECTION
    ]

    latest_document = source.find_one(
        {},
        {
            "_id": 1
        },
        sort=[
            ("_id", -1)
        ],
    )

    if latest_document is None:
        return {
            "view":
                CITY_SALES_MV,

            "status":
                "no_data",

            "processed_documents":
                0,

            "affected_keys":
                0,
        }

    upper_bound_id = (
        latest_document["_id"]
    )

    last_processed_id = (
        get_refresh_state(
            state_collection,
            CITY_SALES_MV,
        )
    )

    if (
        last_processed_id
        == upper_bound_id
    ):
        return {
            "view":
                CITY_SALES_MV,

            "status":
                "up_to_date",

            "processed_documents":
                0,

            "affected_keys":
                0,
        }

    incremental_match = (
        build_incremental_match(
            last_processed_id,
            upper_bound_id,
        )
    )

    processed_documents = (
        source.count_documents(
            incremental_match
        )
    )

    # -----------------------------------------------------
    # Find only cities touched by new documents
    # -----------------------------------------------------

    affected_cities = list(
        source.aggregate(
            [
                {
                    "$match":
                        incremental_match
                },

                {
                    "$match": {
                        "city": {
                            "$nin": [
                                None,
                                "",
                            ]
                        }
                    }
                },

                {
                    "$group": {
                        "_id": "$city"
                    }
                },

                {
                    "$sort": {
                        "_id": 1
                    }
                },
            ]
        )
    )

    refreshed_at = datetime.now(
        timezone.utc
    )

    # -----------------------------------------------------
    # Recalculate only affected cities
    # -----------------------------------------------------

    for affected_city in affected_cities:
        city_value = (
            affected_city["_id"]
        )

        result = list(
            source.aggregate(
                [
                    {
                        "$match": {
                            "city":
                                city_value
                        }
                    },

                    {
                        "$group": {
                            "_id": None,

                            "order_count": {
                                "$sum": 1
                            },

                            "total_sales": {
                                "$sum":
                                    numeric_total_amount()
                            },
                        }
                    },
                ]
            )
        )

        if not result:
            continue

        summary = result[0]

        target.update_one(
            {
                "city":
                    city_value
            },
            {
                "$set": {
                    "city":
                        city_value,

                    "order_count":
                        summary[
                            "order_count"
                        ],

                    "total_sales":
                        round(
                            summary[
                                "total_sales"
                            ],
                            2,
                        ),

                    "refreshed_at":
                        refreshed_at,
                }
            },
            upsert=True,
        )

    save_refresh_state(
        state_collection,
        CITY_SALES_MV,
        upper_bound_id,
        processed_documents,
    )

    return {
        "view":
            CITY_SALES_MV,

        "status":
            "refreshed",

        "processed_documents":
            processed_documents,

        "affected_keys":
            len(
                affected_cities
            ),

        "last_processed_id":
            str(
                upper_bound_id
            ),
    }


# =========================================================
# Refresh one materialized view
# =========================================================

def refresh_materialized_view(
    view_name,
):
    client = MongoClient(
        MONGO_URI
    )

    try:
        database = client[
            DATABASE_NAME
        ]

        ensure_mv_indexes(
            database
        )

        if view_name == DAILY_SALES_MV:
            return refresh_daily_sales(
                database
            )

        if view_name == CITY_SALES_MV:
            return refresh_city_sales(
                database
            )

        raise ValueError(
            f"Unknown materialized view: "
            f"{view_name}"
        )

    finally:
        client.close()


# =========================================================
# Refresh all Materialized Views
#
# Later used by:
# POST /refresh-mv
# and scheduled jobs
# =========================================================

def refresh_all_materialized_views():
    client = MongoClient(
        MONGO_URI
    )

    try:
        database = client[
            DATABASE_NAME
        ]

        ensure_mv_indexes(
            database
        )

        return {
            "status":
                "success",

            "views": [
                refresh_daily_sales(
                    database
                ),

                refresh_city_sales(
                    database
                ),
            ],
        }

    finally:
        client.close()


# =========================================================
# List Materialized Views
# =========================================================

def list_materialized_views():
    return [
        {
            "name":
                DAILY_SALES_MV,

            "source":
                VALIDATED_COLLECTION,

            "refresh_type":
                "incremental",

            "key":
                "date",
        },

        {
            "name":
                CITY_SALES_MV,

            "source":
                VALIDATED_COLLECTION,

            "refresh_type":
                "incremental",

            "key":
                "city",
        },
    ]


# =========================================================
# Manual execution
# =========================================================

if __name__ == "__main__":
    result = (
        refresh_all_materialized_views()
    )

    print(
        "\nMaterialized Views refresh completed.\n"
    )

    for view in result["views"]:
        print(
            f"{view['view']}"
        )

        print(
            f"  Status: "
            f"{view['status']}"
        )

        print(
            f"  Processed documents: "
            f"{view['processed_documents']}"
        )

        print(
            f"  Affected keys: "
            f"{view['affected_keys']}"
        )

        print()