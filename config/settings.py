# File routing settings
SMALL_FILE_THRESHOLD_MB = 200

# Python Batch settings
BATCH_SIZE = 1000

# MongoDB settings
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "midterm_data_pipeline"

# MongoDB collections
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "quarantine_orders"