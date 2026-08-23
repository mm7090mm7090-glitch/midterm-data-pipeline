import argparse
import sys
import time
import uuid
from pathlib import Path

# ---------------------------------------------------------
# Make project root available for imports when using
# spark-submit src\spark_loader.py
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from pymongo import MongoClient

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    RAW_COLLECTION,
)

from src.metrics import (
    save_run_metrics,
    get_file_size_mb,
)


# =========================================================
# CSV fields supplied by the project dataset
# =========================================================

RAW_FIELDS = [
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items_json",
]


# =========================================================
# Fixed Schema
# All raw fields are strings intentionally.
# We do NOT infer types before Raw loading.
# =========================================================

CSV_SCHEMA = StructType(
    [
        StructField(
            field_name,
            StringType(),
            True,
        )
        for field_name in RAW_FIELDS
    ]
    + [
        StructField(
            "_corrupt_record",
            StringType(),
            True,
        )
    ]
)


# =========================================================
# Create SparkSession
# =========================================================

def create_spark_session():

    spark = (
        SparkSession
        .builder
        .appName(
            "MidtermDataPipeline-SparkRawLoader"
        )
        .master(
            "local[*]"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    return spark


# =========================================================
# Count documents belonging to one run in MongoDB
# =========================================================

def count_loaded_raw(run_id):

    client = MongoClient(
        MONGO_URI
    )

    try:

        database = client[
            DATABASE_NAME
        ]

        collection = database[
            RAW_COLLECTION
        ]

        return collection.count_documents(
            {
                "run_id": run_id
            }
        )

    finally:

        client.close()


# =========================================================
# Spark Raw Loader
# =========================================================

def load_csv_with_spark(
    file_path,
    run_id=None,
):

    input_path = Path(
        file_path
    ).resolve()

    if not input_path.exists():

        raise FileNotFoundError(
            f"Input file not found: "
            f"{input_path}"
        )

    if run_id is None:

        run_id = str(
            uuid.uuid4()
        )

    spark = None

    start_time = time.time()

    try:

        # -------------------------------------------------
        # Start Spark
        # -------------------------------------------------

        spark = create_spark_session()

        print(
            "\n===================================="
        )

        print(
            "Spark Raw Loading Started"
        )

        print(
            "===================================="
        )

        print(
            f"Run ID: {run_id}"
        )

        print(
            f"Input file: {input_path}"
        )

        print(
            f"File size: "
            f"{get_file_size_mb(input_path):.2f} MB"
        )

        print(
            "Engine: pyspark"
        )

        print(
            f"Spark version: "
            f"{spark.version}"
        )

        print(
            f"Spark UI: "
            f"{spark.sparkContext.uiWebUrl}"
        )

        # -------------------------------------------------
        # Read CSV using FIXED schema
        # -------------------------------------------------

        source_df = (
            spark.read
            .option(
                "header",
                "true",
            )
            .option(
                "encoding",
                "UTF-8",
            )
            .option(
                "mode",
                "PERMISSIVE",
            )
            .option(
                "columnNameOfCorruptRecord",
                "_corrupt_record",
            )
            .option(
                "quote",
                '"',
            )
            .option(
                "escape",
                '"',
            )
            .schema(
                CSV_SCHEMA
            )
            .csv(
                str(input_path)
            )
        )

        # -------------------------------------------------
        # Number of Spark input partitions
        # -------------------------------------------------

        input_partitions = (
            source_df
            .rdd
            .getNumPartitions()
        )

        print(
            f"Input partitions: "
            f"{input_partitions}"
        )

        # -------------------------------------------------
        # Create Raw document structure.
        #
        # IMPORTANT:
        # No cleaning or conversion happens here.
        # Original CSV values remain Strings.
        # -------------------------------------------------

        raw_record_struct = F.struct(
            *[
                F.col(
                    field_name
                ).alias(
                    field_name
                )
                for field_name
                in RAW_FIELDS
            ],
            F.col(
                "_corrupt_record"
            ).alias(
                "_corrupt_record"
            ),
        )

        raw_df = (
            source_df
            .select(
                F.lit(
                    run_id
                ).alias(
                    "run_id"
                ),

                F.lit(
                    str(input_path)
                ).alias(
                    "source_file"
                ),

                # Spark does not guarantee the
                # original physical CSV line number.
                # Do not invent an incorrect row number.
                F.lit(
                    None
                ).cast(
                    "long"
                ).alias(
                    "source_row_number"
                ),

                F.current_timestamp().alias(
                    "ingested_at"
                ),

                F.lit(
                    "pyspark"
                ).alias(
                    "engine_used"
                ),

                raw_record_struct.alias(
                    "raw_record"
                ),
            )
        )

        output_partitions = (
            raw_df
            .rdd
            .getNumPartitions()
        )

        print(
            f"Output partitions: "
            f"{output_partitions}"
        )

        # -------------------------------------------------
        # Show execution plan.
        #
        # We intentionally do NOT call repartition()
        # without evidence that it is needed.
        # -------------------------------------------------

        print(
            "\nSpark execution plan:"
        )

        raw_df.explain(
            mode="formatted"
        )

        # -------------------------------------------------
        # Parallel write to MongoDB
        # -------------------------------------------------

        print(
            "\nWriting Raw records "
            "to MongoDB..."
        )

        (
            raw_df
            .write
            .format(
                "mongodb"
            )
            .mode(
                "append"
            )
            .option(
                "connection.uri",
                MONGO_URI,
            )
            .option(
                "database",
                DATABASE_NAME,
            )
            .option(
                "collection",
                RAW_COLLECTION,
            )
            .save()
        )

        # -------------------------------------------------
        # Verify number written using MongoDB
        # -------------------------------------------------

        loaded_raw = count_loaded_raw(
            run_id
        )

        elapsed_seconds = (
            time.time()
            - start_time
        )

        throughput = (
            loaded_raw
            / elapsed_seconds
            if elapsed_seconds > 0
            else 0
        )

        # -------------------------------------------------
        # Final output
        # -------------------------------------------------

        print(
            "\n===================================="
        )

        print(
            "Spark Raw Loading Completed"
        )

        print(
            "===================================="
        )

        print(
            f"Run ID: "
            f"{run_id}"
        )

        print(
            f"Rows loaded to Raw: "
            f"{loaded_raw}"
        )

        print(
            f"Input partitions: "
            f"{input_partitions}"
        )

        print(
            f"Elapsed time: "
            f"{elapsed_seconds:.2f} seconds"
        )

        print(
            f"Throughput: "
            f"{throughput:.2f} records/sec"
        )

        # -------------------------------------------------
        # Save metrics
        # -------------------------------------------------

        metrics = {

            "stage":
                "raw_load",

            "run_id":
                run_id,

            "file_name":
                input_path.name,

            "file_size_mb":
                get_file_size_mb(
                    input_path
                ),

            "used_engine":
                "pyspark",

            "read_rows":
                loaded_raw,

            "loaded_raw":
                loaded_raw,

            # These belong to the later
            # Quality / Transform stage.
            "count_valid":
                None,

            "count_corrected":
                None,

            "count_quarantine":
                None,

            "count_inserted":
                None,

            "count_updated":
                None,

            "count_unchanged":
                None,

            "seconds_elapsed":
                elapsed_seconds,

            "throughput":
                throughput,

            "batch_size":
                None,

            "partitions":
                input_partitions,

            "counts_case_error":
                {},
        }

        save_run_metrics(
            metrics
        )

        return {
            "run_id":
                run_id,

            "loaded_raw":
                loaded_raw,

            "partitions":
                input_partitions,

            "elapsed_seconds":
                elapsed_seconds,

            "throughput":
                throughput,
        }

    except Exception as error:

        print(
            "\nSpark loading FAILED."
        )

        print(
            f"Reason: {error}"
        )

        raise

    finally:

        if spark is not None:

            spark.stop()


# =========================================================
# Command-line entry point
# =========================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Load a CSV file into MongoDB Raw "
            "using Apache Spark."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help=(
            "Path to the CSV input file."
        ),
    )

    parser.add_argument(
        "--run-id",
        required=False,
        default=None,
        help=(
            "Optional run ID. "
            "A UUID is generated automatically "
            "when omitted."
        ),
    )

    args = parser.parse_args()

    load_csv_with_spark(
        file_path=args.input,
        run_id=args.run_id,
    )


if __name__ == "__main__":
    main()