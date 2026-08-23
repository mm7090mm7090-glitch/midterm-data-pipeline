from pymongo import MongoClient, ASCENDING

from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


def get_database():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

    # Test the connection
    client.admin.command("ping")

    database = client[DATABASE_NAME]

    return client, database


def create_collections(database):
    existing_collections = database.list_collection_names()

    # Raw collection:
    # No validator and no unique index,
    # because every raw record must be accepted as received.
    if RAW_COLLECTION not in existing_collections:
        database.create_collection(RAW_COLLECTION)
        print(f"Created collection: {RAW_COLLECTION}")
    else:
        print(f"Collection already exists: {RAW_COLLECTION}")

    # Validated collection
    validated_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "order_id",
                "quality_status",
            ],
            "properties": {
                "order_id": {
                    "bsonType": "string"
                },
                "quality_status": {
                    "enum": [
                        "valid",
                        "corrected",
                    ]
                }
            }
        }
    }

    if VALIDATED_COLLECTION not in existing_collections:
        database.create_collection(
            VALIDATED_COLLECTION,
            validator=validated_validator,
            validationLevel="strict",
            validationAction="error",
        )
        print(f"Created collection: {VALIDATED_COLLECTION}")
    else:
        print(f"Collection already exists: {VALIDATED_COLLECTION}")

    # Unique business key
    database[VALIDATED_COLLECTION].create_index(
        [("order_id", ASCENDING)],
        unique=True,
        name="uq_order_id",
    )

    print(
        f"Unique index ready on "
        f"{VALIDATED_COLLECTION}.order_id"
    )

    # Quarantine collection
    if QUARANTINE_COLLECTION not in existing_collections:
        database.create_collection(QUARANTINE_COLLECTION)
        print(f"Created collection: {QUARANTINE_COLLECTION}")
    else:
        print(f"Collection already exists: {QUARANTINE_COLLECTION}")


    # Index required for fast and idempotent quarantine upserts
    database[QUARANTINE_COLLECTION].create_index(
        [("raw_document_id", ASCENDING)],
        name="idx_raw_document_id",
    )

    print(
        f"Index ready on "
        f"{QUARANTINE_COLLECTION}.raw_document_id"
    )


def setup_mongodb():
    client = None

    try:
        client, database = get_database()

        print("MongoDB connection successful.")
        print(f"Database: {DATABASE_NAME}")

        create_collections(database)

        print("MongoDB setup completed successfully.")

    except Exception as error:
        print(f"MongoDB setup failed: {error}")
        raise

    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    setup_mongodb()