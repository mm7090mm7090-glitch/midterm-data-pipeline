from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    VALIDATED_COLLECTION,
)


# =========================================================
# Helper
# =========================================================

def serialize_document(document):
    document = dict(document)

    if "_id" in document:
        document["_id"] = str(
            document["_id"]
        )

    return document


def execute_find(
    query_filter,
    limit=50,
    sort=None,
):
    client = MongoClient(
        MONGO_URI
    )

    try:
        database = client[
            DATABASE_NAME
        ]

        collection = database[
            VALIDATED_COLLECTION
        ]

        cursor = collection.find(
            query_filter
        )

        if sort is not None:
            cursor = cursor.sort(
                sort
            )

        cursor = cursor.limit(
            int(limit)
        )

        return [
            serialize_document(
                document
            )
            for document in cursor
        ]

    finally:
        client.close()


# =========================================================
# Query 1
# Orders by city
# Uses: idx_city
# =========================================================

def orders_by_city(
    city,
    limit=50,
):
    return execute_find(
        {
            "city": city
        },
        limit=limit,
        sort=[
            ("order_date", -1)
        ],
    )


# =========================================================
# Query 2
# Orders by customer
# Uses: idx_customer_id
# =========================================================

def orders_by_customer(
    customer_id,
    limit=50,
):
    return execute_find(
        {
            "customer_id":
                customer_id
        },
        limit=limit,
        sort=[
            ("order_date", -1)
        ],
    )


# =========================================================
# Query 3
# Orders within a date range
# Uses: idx_order_date
# =========================================================

def orders_by_date_range(
    start_date,
    end_date,
    limit=50,
):
    return execute_find(
        {
            "order_date": {
                "$gte": start_date,
                "$lte": end_date,
            }
        },
        limit=limit,
        sort=[
            ("order_date", 1)
        ],
    )


# =========================================================
# Query 4
# Orders by status and date range
# Uses compound index:
# idx_status_order_date
# =========================================================

def orders_by_status_and_date(
    status,
    start_date,
    end_date,
    limit=50,
):
    return execute_find(
        {
            "status": status,

            "order_date": {
                "$gte": start_date,
                "$lte": end_date,
            },
        },
        limit=limit,
        sort=[
            ("order_date", 1)
        ],
    )


# =========================================================
# Query 5
# Orders by payment status
# Practical business query
# =========================================================

def orders_by_payment_status(
    payment_status,
    limit=50,
):
    return execute_find(
        {
            "payment_status":
                payment_status
        },
        limit=limit,
        sort=[
            ("order_date", -1)
        ],
    )


# =========================================================
# Query registry
#
# This will later be reused by FastAPI:
# GET /queries
# GET /queries/{name}
# =========================================================

QUERY_REGISTRY = {
    "orders_by_city":
        orders_by_city,

    "orders_by_customer":
        orders_by_customer,

    "orders_by_date_range":
        orders_by_date_range,

    "orders_by_status_and_date":
        orders_by_status_and_date,

    "orders_by_payment_status":
        orders_by_payment_status,
}


def list_queries():
    return [
        {
            "name": "orders_by_city",
            "description":
                "Find orders for a specific city.",
            "parameters": [
                "city",
                "limit",
            ],
        },

        {
            "name": "orders_by_customer",
            "description":
                "Find orders for a specific customer.",
            "parameters": [
                "customer_id",
                "limit",
            ],
        },

        {
            "name": "orders_by_date_range",
            "description":
                "Find orders within a date range.",
            "parameters": [
                "start_date",
                "end_date",
                "limit",
            ],
        },

        {
            "name":
                "orders_by_status_and_date",
            "description":
                "Find orders by status within a date range.",
            "parameters": [
                "status",
                "start_date",
                "end_date",
                "limit",
            ],
        },

        {
            "name":
                "orders_by_payment_status",
            "description":
                "Find orders by payment status.",
            "parameters": [
                "payment_status",
                "limit",
            ],
        },
    ]


def run_query(
    query_name,
    **parameters,
):
    query_function = (
        QUERY_REGISTRY.get(
            query_name
        )
    )

    if query_function is None:
        raise ValueError(
            f"Unknown query: "
            f"{query_name}"
        )

    return query_function(
        **parameters
    )


# =========================================================
# Manual execution
# =========================================================

if __name__ == "__main__":
    print(
        "Available queries:\n"
    )

    for query in list_queries():
        print(
            f"- {query['name']}: "
            f"{query['description']}"
        )