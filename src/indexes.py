from pymongo import MongoClient, ASCENDING

from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    VALIDATED_COLLECTION,
)


# =========================================================
# Index definitions
# =========================================================

INDEX_DEFINITIONS = [
    {
        "name": "idx_city",
        "keys": [
            ("city", ASCENDING),
        ],
    },
    {
        "name": "idx_customer_id",
        "keys": [
            ("customer_id", ASCENDING),
        ],
    },
    {
        "name": "idx_order_date",
        "keys": [
            ("order_date", ASCENDING),
        ],
    },

    # Compound Index
    {
        "name": "idx_status_order_date",
        "keys": [
            ("status", ASCENDING),
            ("order_date", ASCENDING),
        ],
    },
]


# =========================================================
# Create indexes
# =========================================================

def create_indexes():
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

        created_indexes = []

        for index_definition in INDEX_DEFINITIONS:
            index_name = collection.create_index(
                index_definition["keys"],
                name=index_definition["name"],
            )

            created_indexes.append(
                index_name
            )

        return {
            "status": "success",
            "database": DATABASE_NAME,
            "collection": VALIDATED_COLLECTION,
            "indexes": created_indexes,
        }

    finally:
        client.close()


# =========================================================
# Drop only the indexes created by this module
# Useful for explain() before/after comparison
# =========================================================

def drop_project_indexes():
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

        existing_indexes = {
            index["name"]
            for index in collection.list_indexes()
        }

        dropped_indexes = []

        for index_definition in INDEX_DEFINITIONS:
            index_name = index_definition[
                "name"
            ]

            if index_name in existing_indexes:
                collection.drop_index(
                    index_name
                )

                dropped_indexes.append(
                    index_name
                )

        return {
            "status": "success",
            "dropped_indexes": dropped_indexes,
        }

    finally:
        client.close()


# =========================================================
# List indexes
# =========================================================

def list_indexes():
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

        indexes = []

        for index in collection.list_indexes():
            indexes.append(
                {
                    "name": index.get(
                        "name"
                    ),
                    "key": dict(
                        index.get(
                            "key",
                            {}
                        )
                    ),
                    "unique": index.get(
                        "unique",
                        False
                    ),
                }
            )

        return indexes

    finally:
        client.close()


# =========================================================
# Manual execution
# =========================================================

if __name__ == "__main__":
    result = create_indexes()

    print(
        "Indexes created successfully."
    )

    for index_name in result[
        "indexes"
    ]:
        print(
            f"- {index_name}"
        )