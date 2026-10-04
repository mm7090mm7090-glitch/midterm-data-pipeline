from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    VALIDATED_COLLECTION,
)


# =========================================================
# Helper
# =========================================================

def run_pipeline(pipeline):
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

        return list(
            collection.aggregate(
                pipeline
            )
        )

    finally:
        client.close()


# =========================================================
# Common numeric conversion
#
# total_amount is stored safely from the Phase 1 pipeline,
# but aggregation should not assume it is already numeric.
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
# Report 1
# Sales by City
# =========================================================

def sales_by_city(
    limit=20,
):
    pipeline = [
        {
            "$group": {
                "_id": "$city",

                "order_count": {
                    "$sum": 1
                },

                "total_sales": {
                    "$sum":
                        numeric_total_amount()
                },
            }
        },

        {
            "$sort": {
                "total_sales": -1
            }
        },

        {
            "$limit": int(limit)
        },

        {
            "$project": {
                "_id": 0,

                "city": "$_id",

                "order_count": 1,

                "total_sales": {
                    "$round": [
                        "$total_sales",
                        2,
                    ]
                },
            }
        },
    ]

    return run_pipeline(
        pipeline
    )


# =========================================================
# Report 2
# Top Customers by Total Spending
# =========================================================

def top_customers(
    limit=20,
):
    pipeline = [
        {
            "$group": {
                "_id": {
                    "customer_id":
                        "$customer_id",

                    "customer_name":
                        "$customer_name",
                },

                "order_count": {
                    "$sum": 1
                },

                "total_spending": {
                    "$sum":
                        numeric_total_amount()
                },
            }
        },

        {
            "$sort": {
                "total_spending": -1
            }
        },

        {
            "$limit": int(limit)
        },

        {
            "$project": {
                "_id": 0,

                "customer_id":
                    "$_id.customer_id",

                "customer_name":
                    "$_id.customer_name",

                "order_count": 1,

                "total_spending": {
                    "$round": [
                        "$total_spending",
                        2,
                    ]
                },
            }
        },
    ]

    return run_pipeline(
        pipeline
    )


# =========================================================
# Report 3
# Daily Sales Summary
# =========================================================

def daily_sales(
    limit=30,
):
    pipeline = [
        {
            "$addFields": {
                "sale_date": {
                    "$substrBytes": [
                        "$order_date",
                        0,
                        10,
                    ]
                }
            }
        },

        {
            "$group": {
                "_id": "$sale_date",

                "order_count": {
                    "$sum": 1
                },

                "total_sales": {
                    "$sum":
                        numeric_total_amount()
                },
            }
        },

        {
            "$sort": {
                "_id": -1
            }
        },

        {
            "$limit": int(limit)
        },

        {
            "$project": {
                "_id": 0,

                "date": "$_id",

                "order_count": 1,

                "total_sales": {
                    "$round": [
                        "$total_sales",
                        2,
                    ]
                },
            }
        },
    ]

    return run_pipeline(
        pipeline
    )


# =========================================================
# Report 4
# Orders by Status
# =========================================================

def orders_by_status():
    pipeline = [
        {
            "$group": {
                "_id": "$status",

                "order_count": {
                    "$sum": 1
                },

                "total_sales": {
                    "$sum":
                        numeric_total_amount()
                },
            }
        },

        {
            "$sort": {
                "order_count": -1
            }
        },

        {
            "$project": {
                "_id": 0,

                "status": "$_id",

                "order_count": 1,

                "total_sales": {
                    "$round": [
                        "$total_sales",
                        2,
                    ]
                },
            }
        },
    ]

    return run_pipeline(
        pipeline
    )


# =========================================================
# Report 5
# Payment Status Summary
# =========================================================

def payment_status_summary():
    pipeline = [
        {
            "$group": {
                "_id":
                    "$payment_status",

                "order_count": {
                    "$sum": 1
                },

                "total_amount": {
                    "$sum":
                        numeric_total_amount()
                },
            }
        },

        {
            "$sort": {
                "order_count": -1
            }
        },

        {
            "$project": {
                "_id": 0,

                "payment_status":
                    "$_id",

                "order_count": 1,

                "total_amount": {
                    "$round": [
                        "$total_amount",
                        2,
                    ]
                },
            }
        },
    ]

    return run_pipeline(
        pipeline
    )


# =========================================================
# Aggregation Registry
#
# Will later be reused by FastAPI:
# GET /aggregations
# GET /aggregations/{name}
# =========================================================

AGGREGATION_REGISTRY = {
    "sales_by_city":
        sales_by_city,

    "top_customers":
        top_customers,

    "daily_sales":
        daily_sales,

    "orders_by_status":
        orders_by_status,

    "payment_status_summary":
        payment_status_summary,
}


def list_aggregations():
    return [
        {
            "name":
                "sales_by_city",

            "description":
                "Total orders and sales grouped by city.",
        },

        {
            "name":
                "top_customers",

            "description":
                "Customers ranked by total spending.",
        },

        {
            "name":
                "daily_sales",

            "description":
                "Daily order count and total sales.",
        },

        {
            "name":
                "orders_by_status",

            "description":
                "Order count and sales grouped by order status.",
        },

        {
            "name":
                "payment_status_summary",

            "description":
                "Order count and amounts grouped by payment status.",
        },
    ]


def run_aggregation(
    aggregation_name,
    **parameters,
):
    aggregation_function = (
        AGGREGATION_REGISTRY.get(
            aggregation_name
        )
    )

    if aggregation_function is None:
        raise ValueError(
            f"Unknown aggregation: "
            f"{aggregation_name}"
        )

    return aggregation_function(
        **parameters
    )


# =========================================================
# Manual execution
# =========================================================

if __name__ == "__main__":
    print(
        "Available aggregation reports:\n"
    )

    for report in list_aggregations():
        print(
            f"- {report['name']}: "
            f"{report['description']}"
        )