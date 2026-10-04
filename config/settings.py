import os


SMALL_FILE_THRESHOLD_MB = 200

BATCH_SIZE = 1000


MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017"
)


DATABASE_NAME = os.getenv(
    "MONGO_DB_NAME",
    "midterm_data_pipeline"
)


RAW_COLLECTION = "orders_raw"

VALIDATED_COLLECTION = "orders_validated"

QUARANTINE_COLLECTION = "quarantine_orders"