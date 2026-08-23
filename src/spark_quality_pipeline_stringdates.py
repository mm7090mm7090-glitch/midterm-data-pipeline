import argparse
import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window
from pymongo import MongoClient

from config.settings import (
    DATABASE_NAME,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
)
from src.metrics import save_run_metrics

PIPELINE_VERSION = "2026-08-18-string-date-v2"


RAW_FIELDS = [
    "order_id", "order_date", "status", "customer_id", "customer_name",
    "customer_phone", "customer_email", "city", "district", "delivery_type",
    "delivery_cost", "payment_method", "payment_status", "payment_amount",
    "currency", "total_amount", "items_json", "_corrupt_record",
]

BUSINESS_FIELDS = [
    "order_id", "order_date", "status", "customer_id", "customer_name",
    "customer_phone", "customer_email", "city", "district", "delivery_type",
    "delivery_cost", "payment_method", "payment_status", "payment_amount",
    "currency", "total_amount", "items_json",
]

RAW_RECORD_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in RAW_FIELDS]
)

RAW_MONGO_SCHEMA = StructType([
    StructField("raw_document_id", StringType(), False),
    StructField("run_id", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("source_row_number", LongType(), True),
    StructField("ingested_at", TimestampType(), True),
    StructField("engine_used", StringType(), True),
    StructField("raw_record", RAW_RECORD_SCHEMA, True),
])

CORRECTION_SCHEMA = StructType([
    StructField("field", StringType(), False),
    StructField("original_value", StringType(), True),
    StructField("corrected_value", StringType(), True),
    StructField("rule_code", StringType(), False),
])

ITEMS_SCHEMA = ArrayType(
    StructType([
        StructField("qty", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("total", StringType(), True),
    ]),
    True,
)

EXISTING_SCHEMA = StructType([
    StructField("existing_order_id", StringType(), True),
    StructField("existing_business_hash", StringType(), True),
    StructField("existing_first_run_id", StringType(), True),
    StructField("existing_created_at", TimestampType(), True),
])


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("MidtermDataPipeline-SparkQuality")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "99")
        .config("spark.sql.adaptive.enabled", "true")
        # Spark 3.x defaults to EXCEPTION for some cross-version datetime
        # parsing differences. CORRECTED makes non-matching patterns return
        # null so our explicit validation/correction rules can handle them.
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def correction(field, old_col, new_col, rule_code):
    changed = ~old_col.eqNullSafe(new_col)
    return F.when(
        changed,
        F.struct(
            F.lit(field).alias("field"),
            old_col.cast("string").alias("original_value"),
            new_col.cast("string").alias("corrected_value"),
            F.lit(rule_code).alias("rule_code"),
        ),
    ).otherwise(F.lit(None).cast(CORRECTION_SCHEMA))


def normalize_digits(col):
    return F.translate(
        col,
        "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹",
        "01234567890123456789",
    )


def remove_thousands(col):
    return F.when(
        col.rlike(r"^[+-]?\d{1,3}(,\d{3})+(\.\d+)?$"),
        F.regexp_replace(col, ",", ""),
    ).otherwise(col)


def normalize_price_words(col):
    normalized = F.regexp_replace(
        F.trim(col.cast("string")),
        r"\s+",
        " ",
    )

    price_words = F.create_map(
        F.lit("\u0623\u0644\u0641"), F.lit("1000"),
        F.lit("\u0627\u0644\u0641"), F.lit("1000"),

        F.lit("\u0623\u0644\u0641\u0627\u0646"), F.lit("2000"),
        F.lit("\u0627\u0644\u0641\u0627\u0646"), F.lit("2000"),
        F.lit("\u0623\u0644\u0641\u064a\u0646"), F.lit("2000"),
        F.lit("\u0627\u0644\u0641\u064a\u0646"), F.lit("2000"),

        F.lit("\u062b\u0644\u0627\u062b\u0629 \u0622\u0644\u0627\u0641"), F.lit("3000"),
        F.lit("\u062b\u0644\u0627\u062b\u0647 \u0622\u0644\u0627\u0641"), F.lit("3000"),

        F.lit("\u0623\u0631\u0628\u0639\u0629 \u0622\u0644\u0627\u0641"), F.lit("4000"),
        F.lit("\u0627\u0631\u0628\u0639\u0629 \u0622\u0644\u0627\u0641"), F.lit("4000"),
        F.lit("\u0623\u0631\u0628\u0639\u0647 \u0622\u0644\u0627\u0641"), F.lit("4000"),

        F.lit("\u062e\u0645\u0633\u0629 \u0622\u0644\u0627\u0641"), F.lit("5000"),
        F.lit("\u062e\u0645\u0633\u0647 \u0622\u0644\u0627\u0641"), F.lit("5000"),

        F.lit("\u0633\u062a\u0629 \u0622\u0644\u0627\u0641"), F.lit("6000"),
        F.lit("\u0633\u062a\u0647 \u0622\u0644\u0627\u0641"), F.lit("6000"),

        F.lit("\u0633\u0628\u0639\u0629 \u0622\u0644\u0627\u0641"), F.lit("7000"),
        F.lit("\u0633\u0628\u0639\u0647 \u0622\u0644\u0627\u0641"), F.lit("7000"),

        F.lit("\u062b\u0645\u0627\u0646\u064a\u0629 \u0622\u0644\u0627\u0641"), F.lit("8000"),
        F.lit("\u062b\u0645\u0627\u0646\u064a\u0647 \u0622\u0644\u0627\u0641"), F.lit("8000"),

        F.lit("\u062a\u0633\u0639\u0629 \u0622\u0644\u0627\u0641"), F.lit("9000"),
        F.lit("\u062a\u0633\u0639\u0647 \u0622\u0644\u0627\u0641"), F.lit("9000"),

        F.lit("\u0639\u0634\u0631\u0629 \u0622\u0644\u0627\u0641"), F.lit("10000"),
        F.lit("\u0639\u0634\u0631\u0647 \u0622\u0644\u0627\u0641"), F.lit("10000"),
    )

    mapped = F.element_at(
        price_words,
        normalized,
    )

    return F.when(
        mapped.isNotNull(),
        mapped,
    ).otherwise(col)


def normalize_currency(col):
    trimmed = F.trim(col)
    lowered = F.lower(trimmed)
    known = (lowered == "yer") | trimmed.isin("ريال", "ريال يمني", "ريال يمنى")
    return F.when(known, F.lit("YER")).otherwise(col)


def normalize_phone(col):
    cleaned = F.regexp_replace(col, r"[\s\-\(\)]", "")
    return (
        F.when(
            cleaned.rlike(r"^00967\d{9}$"),
            F.concat(F.lit("+967"), F.substring(cleaned, 6, 9)),
        )
        .when(
            cleaned.rlike(r"^967\d{9}$"),
            F.concat(F.lit("+967"), F.substring(cleaned, 4, 9)),
        )
        .when(cleaned.rlike(r"^\+967\d{9}$"), cleaned)
        .when(cleaned.rlike(r"^\d{9}$"), F.concat(F.lit("+967"), cleaned))
        .otherwise(col)
    )


def normalize_email(col):
    candidate = F.regexp_replace(
        F.regexp_replace(col, r"@+", "@"),
        r"\.{2,}",
        ".",
    )
    valid = candidate.rlike(
        r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
    )
    return F.when(valid, candidate).otherwise(col)


def normalize_date(col):
    """
    Normalize known date formats using STRING operations only.
    No to_date / to_timestamp / try_to_timestamp is used.
    This avoids Spark's datetime parser completely.
    """
    value = normalize_digits(F.trim(col))

    return (
        # Already normalized ISO.
        F.when(
            value.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"),
            value,
        )
        .when(
            value.rlike(r"^\d{4}-\d{2}-\d{2}$"),
            value,
        )

        # yyyy-MM-dd HH:mm:ss -> yyyy-MM-ddTHH:mm:ss
        .when(
            value.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"),
            F.regexp_replace(
                value,
                r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})$",
                "$1T$2",
            ),
        )

        # dd-MM-yyyy HH:mm:ss -> yyyy-MM-ddTHH:mm:ss
        .when(
            value.rlike(r"^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$"),
            F.regexp_replace(
                value,
                r"^(\d{2})-(\d{2})-(\d{4}) (\d{2}:\d{2}:\d{2})$",
                "$3-$2-$1T$4",
            ),
        )

        # dd/MM/yyyy HH:mm:ss -> yyyy-MM-ddTHH:mm:ss
        .when(
            value.rlike(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$"),
            F.regexp_replace(
                value,
                r"^(\d{2})/(\d{2})/(\d{4}) (\d{2}:\d{2}:\d{2})$",
                "$3-$2-$1T$4",
            ),
        )

        # yyyy/MM/dd HH:mm:ss -> yyyy-MM-ddTHH:mm:ss
        .when(
            value.rlike(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$"),
            F.regexp_replace(
                value,
                r"^(\d{4})/(\d{2})/(\d{2}) (\d{2}:\d{2}:\d{2})$",
                "$1-$2-$3T$4",
            ),
        )

        # dd/MM/yyyy -> yyyy-MM-dd
        .when(
            value.rlike(r"^\d{2}/\d{2}/\d{4}$"),
            F.regexp_replace(
                value,
                r"^(\d{2})/(\d{2})/(\d{4})$",
                "$3-$2-$1",
            ),
        )

        # dd-MM-yyyy -> yyyy-MM-dd
        .when(
            value.rlike(r"^\d{2}-\d{2}-\d{4}$"),
            F.regexp_replace(
                value,
                r"^(\d{2})-(\d{2})-(\d{4})$",
                "$3-$2-$1",
            ),
        )

        # yyyy/MM/dd -> yyyy-MM-dd
        .when(
            value.rlike(r"^\d{4}/\d{2}/\d{2}$"),
            F.regexp_replace(
                value,
                r"^(\d{4})/(\d{2})/(\d{2})$",
                "$1-$2-$3",
            ),
        )
        .otherwise(value)
    )


def is_valid_normalized_date(col):
    """
    Strict Gregorian calendar validation using integer/string operations.
    It detects impossible dates such as 2025-02-30 without Spark date parsing.
    """
    value = F.trim(col)

    is_date = value.rlike(r"^\d{4}-\d{2}-\d{2}$")
    is_datetime = value.rlike(
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"
    )

    year = F.substring(value, 1, 4).cast("int")
    month = F.substring(value, 6, 2).cast("int")
    day = F.substring(value, 9, 2).cast("int")

    hour = F.substring(value, 12, 2).cast("int")
    minute = F.substring(value, 15, 2).cast("int")
    second = F.substring(value, 18, 2).cast("int")

    leap_year = (
        ((year % F.lit(4)) == 0)
        & (
            ((year % F.lit(100)) != 0)
            | ((year % F.lit(400)) == 0)
        )
    )

    days_in_month = (
        F.when(month.isin(1, 3, 5, 7, 8, 10, 12), F.lit(31))
        .when(month.isin(4, 6, 9, 11), F.lit(30))
        .when(
            month == 2,
            F.when(leap_year, F.lit(29)).otherwise(F.lit(28)),
        )
        .otherwise(F.lit(0))
    )

    calendar_ok = (
        year.between(1, 9999)
        & month.between(1, 12)
        & day.between(1, days_in_month)
    )

    time_ok = (
        hour.between(0, 23)
        & minute.between(0, 59)
        & second.between(0, 59)
    )

    result = (
        (is_date & calendar_ok)
        | (is_datetime & calendar_ok & time_ok)
    )

    return F.coalesce(result, F.lit(False))


def normalize_text(col, field):
    value = F.trim(F.regexp_replace(col, r"\s+", " "))
    if field == "payment_status":
        return F.when(value == "مدفوع", F.lit("تم الدفع")).otherwise(value)
    if field == "status":
        return F.when(value == "مؤكدة", F.lit("مؤكد")).otherwise(value)
    return value


def count_raw_run(run_id):
    client = MongoClient(MONGO_URI)
    try:
        return client[DATABASE_NAME][RAW_COLLECTION].count_documents({"run_id": run_id})
    finally:
        client.close()


def count_validated_collection():
    client = MongoClient(MONGO_URI)
    try:
        return client[DATABASE_NAME][VALIDATED_COLLECTION].estimated_document_count()
    finally:
        client.close()


def read_raw_run(spark, run_id, raw_count_hint):
    # Keep MongoDB _id available to the partitioner.
    # The previous version projected _id away before partition planning,
    # which previously caused duplicate
    # empty partition filters.
    pipeline = json.dumps([
        {"$match": {"run_id": run_id}},
        {"$set": {"raw_document_id": {"$toString": "$_id"}}},
    ])

    reader = (
        spark.read
        .format("mongodb")
        .option("connection.uri", MONGO_URI)
        .option("database", DATABASE_NAME)
        .option("collection", RAW_COLLECTION)
        .option("aggregation.pipeline", pipeline)
    )

    # Small validation runs should use a single MongoDB input partition.
    # Large production runs use parallel MongoDB partitions.
    if raw_count_hint <= 100000:
        reader = reader.option(
            "partitioner",
            "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner",
        )
    else:
        # Large production run: partition on MongoDB's unique _id.
        # Use a size-based sampling strategy instead of forcing a fixed
        # number of MongoDB partitions.
        reader = (
            reader
            .option(
                "partitioner",
                "com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner",
            )
            .option("partitioner.options.partition.field", "_id")
            .option("partitioner.options.partition.size", "128")
            .option("partitioner.options.samples.per.partition", "10")
        )

    return reader.schema(RAW_MONGO_SCHEMA).load()


def read_existing_validated(spark):
    # Keep MongoDB _id available for partition planning.
    pipeline = json.dumps([
        {
            "$set": {
                "existing_order_id": "$order_id",
                "existing_business_hash": "$business_hash",
                "existing_first_run_id": "$first_run_id",
                "existing_created_at": "$created_at",
            }
        }
    ])

    validated_count_hint = count_validated_collection()

    reader = (
        spark.read
        .format("mongodb")
        .option("connection.uri", MONGO_URI)
        .option("database", DATABASE_NAME)
        .option("collection", VALIDATED_COLLECTION)
        .option("aggregation.pipeline", pipeline)
    )

    if validated_count_hint <= 500000:
        reader = reader.option(
            "partitioner",
            "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner",
        )
    else:
        reader = (
            reader
            .option(
                "partitioner",
                "com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner",
            )
            .option("partitioner.options.partition.field", "_id")
            .option("partitioner.options.partition.size", "128")
            .option("partitioner.options.samples.per.partition", "10")
        )

    return reader.schema(EXISTING_SCHEMA).load()


def build_classified_dataframe(raw_df):
    df = raw_df

    # Pull nested raw fields into Spark columns.
    for name in RAW_FIELDS:
        df = df.withColumn(f"raw_{name}", F.col(f"raw_record.{name}"))

    # Rule 1: Arabic/Persian digits -> Latin.
    for name in ["delivery_cost", "payment_amount", "total_amount", "customer_phone"]:
        df = df.withColumn(f"digits_{name}", normalize_digits(F.col(f"raw_{name}")))

    # Rule 2: known price words -> numeric values.
    for name in ["delivery_cost", "payment_amount", "total_amount"]:
        df = df.withColumn(
            f"words_{name}",
            normalize_price_words(F.col(f"digits_{name}")),
        )

    # Rule 3: thousands separators in monetary fields.
    for name in ["delivery_cost", "payment_amount", "total_amount"]:
        df = df.withColumn(
            f"clean_{name}",
            remove_thousands(F.col(f"words_{name}")),
        )

    # Rules 3-6.
    df = (
        df
        .withColumn("clean_currency", normalize_currency(F.col("raw_currency")))
        .withColumn("clean_customer_phone", normalize_phone(F.col("digits_customer_phone")))
        .withColumn("clean_customer_email", normalize_email(F.col("raw_customer_email")))
        .withColumn("clean_order_date", normalize_date(F.col("raw_order_date")))
    )

    # Rule 7: whitespace and known text synonyms.
    text_fields = [
        "status", "payment_status", "customer_name", "city",
        "district", "delivery_type", "payment_method",
    ]
    for name in text_fields:
        df = df.withColumn(f"clean_{name}", normalize_text(F.col(f"raw_{name}"), name))

    df = (
        df
        .withColumn("clean_order_id", F.col("raw_order_id"))
        .withColumn("clean_customer_id", F.col("raw_customer_id"))
        .withColumn("clean_items_json", F.col("raw_items_json"))
        .withColumn("_items", F.from_json(F.col("raw_items_json"), ITEMS_SCHEMA))
    )

    # Rule 8: recalculate total when components are known and safe.
    df = (
        df
        .withColumn(
            "_items_total",
            F.aggregate(
                F.col("_items"),
                F.lit(0.0),
                lambda acc, item: acc + item["total"].cast("double"),
            ),
        )
        .withColumn("_delivery_number", F.col("clean_delivery_cost").cast("double"))
        .withColumn("_old_total_number", F.col("clean_total_amount").cast("double"))
        .withColumn("_expected_total", F.col("_items_total") + F.col("_delivery_number"))
    )

    total_recalc = (
        F.col("_items").isNotNull()
        & (F.size("_items") > 0)
        & F.col("_items_total").isNotNull()
        & F.col("_delivery_number").isNotNull()
        & F.col("_old_total_number").isNotNull()
        & (F.abs(F.col("_expected_total") - F.col("_old_total_number")) > F.lit(0.000001))
    )

    df = df.withColumn(
        "final_total_amount",
        F.when(total_recalc, F.col("_expected_total").cast("string"))
        .otherwise(F.col("clean_total_amount")),
    )

    # Full correction audit trail.
    correction_exprs = []

    for name in ["delivery_cost", "payment_amount", "total_amount", "customer_phone"]:
        correction_exprs.append(
            correction(
                name,
                F.col(f"raw_{name}"),
                F.col(f"digits_{name}"),
                "ARABIC_DIGITS_TO_LATIN",
            )
        )

    for name in ["delivery_cost", "payment_amount", "total_amount"]:
        correction_exprs.append(
            correction(
                name,
                F.col(f"digits_{name}"),
                F.col(f"words_{name}"),
                "PRICE_WORD_TO_NUMBER",
            )
        )

    for name in ["delivery_cost", "payment_amount", "total_amount"]:
        correction_exprs.append(
            correction(
                name,
                F.col(f"words_{name}"),
                F.col(f"clean_{name}"),
                "REMOVE_THOUSANDS_SEPARATOR",
            )
        )

    correction_exprs.extend([
        correction("currency", F.col("raw_currency"), F.col("clean_currency"), "NORMALIZE_CURRENCY_YER"),
        correction(
            "customer_phone",
            F.col("digits_customer_phone"),
            F.col("clean_customer_phone"),
            "NORMALIZE_PHONE_FORMAT",
        ),
        correction(
            "customer_email",
            F.col("raw_customer_email"),
            F.col("clean_customer_email"),
            "EMAIL_REPEATED_SYMBOLS",
        ),
        correction(
            "order_date",
            F.col("raw_order_date"),
            F.col("clean_order_date"),
            "NORMALIZE_DATE_ISO",
        ),
    ])

    for name in text_fields:
        correction_exprs.append(
            correction(
                name,
                F.col(f"raw_{name}"),
                F.col(f"clean_{name}"),
                "NORMALIZE_TEXT_VALUE",
            )
        )

    correction_exprs.append(
        F.when(
            total_recalc,
            F.struct(
                F.lit("total_amount").alias("field"),
                F.col("clean_total_amount").cast("string").alias("original_value"),
                F.col("final_total_amount").cast("string").alias("corrected_value"),
                F.lit("RECALCULATE_TOTAL_AMOUNT").alias("rule_code"),
            ),
        ).otherwise(F.lit(None).cast(CORRECTION_SCHEMA))
    )

    df = df.withColumn(
        "corrections",
        F.filter(F.array(*correction_exprs), lambda x: x.isNotNull()),
    )

    # Distributed duplicate detection. Missing IDs get a unique key.
    duplicate_key = F.when(
        F.col("clean_order_id").isNotNull()
        & (F.length(F.trim(F.col("clean_order_id"))) > 0),
        F.col("clean_order_id"),
    ).otherwise(F.concat(F.lit("__RAW__"), F.col("raw_document_id")))

    duplicate_window = Window.partitionBy(duplicate_key)
    df = df.withColumn("_duplicate_count", F.count(F.lit(1)).over(duplicate_window))

    missing_order = (
        F.col("clean_order_id").isNull()
        | (F.length(F.trim(F.col("clean_order_id"))) == 0)
    )
    missing_customer = (
        F.col("clean_customer_id").isNull()
        | (F.length(F.trim(F.col("clean_customer_id"))) == 0)
    )

    valid_date = is_valid_normalized_date(
        F.col("clean_order_date")
    )

    json_corrupted = F.col("_items").isNull()
    items_empty = F.col("_items").isNotNull() & (F.size("_items") == 0)

    negative_value = F.coalesce(
        F.exists(
            F.col("_items"),
            lambda item: (
                (item["qty"].cast("double") < 0)
                | (item["unit_price"].cast("double") < 0)
                | (item["total"].cast("double") < 0)
            ),
        ),
        F.lit(False),
    )

    price_unknown = (
        F.col("final_total_amount").isNull()
        | F.col("final_total_amount").cast("double").isNull()
    )
    duplicate_order = F.col("_duplicate_count") > 1
    csv_corrupted = (
        F.col("raw__corrupt_record").isNotNull()
        & (F.length(F.trim(F.col("raw__corrupt_record"))) > 0)
    )

    error_exprs = [
        F.when(missing_order, F.lit("ID_ORDER_MISSING")),
        F.when(missing_customer, F.lit("ID_CUSTOMER_MISSING")),
        F.when(~valid_date, F.lit("DATE_IMPOSSIBLE_INVALID")),
        F.when(json_corrupted, F.lit("JSON_ITEMS_CORRUPTED")),
        F.when(items_empty, F.lit("ITEMS_EMPTY")),
        F.when(price_unknown, F.lit("PRICE_UNKNOWN")),
        F.when(negative_value, F.lit("VALUE_NEGATIVE_AMBIGUOUS")),
        F.when(duplicate_order, F.lit("ID_ORDER_DUPLICATE")),
        F.when(csv_corrupted, F.lit("CSV_ROW_CORRUPTED")),
    ]

    detail_exprs = [
        F.when(missing_order, F.lit("Order ID is missing and cannot be inferred safely.")),
        F.when(missing_customer, F.lit("Customer ID is missing.")),
        F.when(~valid_date, F.lit("Order date is invalid or impossible.")),
        F.when(json_corrupted, F.lit("items_json is corrupted or cannot be parsed.")),
        F.when(items_empty, F.lit("The order contains no usable items.")),
        F.when(price_unknown, F.lit("Total amount is missing or cannot be determined.")),
        F.when(negative_value, F.lit("Negative quantity or amount cannot be interpreted safely.")),
        F.when(duplicate_order, F.lit("Duplicate order_id found inside the same run.")),
        F.when(csv_corrupted, F.lit("Spark detected a malformed CSV row.")),
    ]

    df = (
        df
        .withColumn(
            "_initial_error_codes",
            F.filter(F.array(*error_exprs), lambda x: x.isNotNull()),
        )
        .withColumn(
            "_initial_error_details",
            F.filter(F.array(*detail_exprs), lambda x: x.isNotNull()),
        )
    )

    df = (
        df
        .withColumn(
            "error_codes",
            F.when(
                F.size("_initial_error_codes") > 1,
                F.concat(
                    F.col("_initial_error_codes"),
                    F.array(F.lit("ERRORS_CONFLICTING_MULTIPLE")),
                ),
            ).otherwise(F.col("_initial_error_codes")),
        )
        .withColumn(
            "error_details",
            F.when(
                F.size("_initial_error_codes") > 1,
                F.concat(
                    F.col("_initial_error_details"),
                    F.array(F.lit("Multiple independent validation errors exist in this record.")),
                ),
            ).otherwise(F.col("_initial_error_details")),
        )
        .withColumn(
            "quality_status",
            F.when(F.size("error_codes") > 0, F.lit("quarantined"))
            .when(F.size("corrections") > 0, F.lit("corrected"))
            .otherwise(F.lit("valid")),
        )
    )

    final_map = {
        "order_id": "clean_order_id",
        "order_date": "clean_order_date",
        "status": "clean_status",
        "customer_id": "clean_customer_id",
        "customer_name": "clean_customer_name",
        "customer_phone": "clean_customer_phone",
        "customer_email": "clean_customer_email",
        "city": "clean_city",
        "district": "clean_district",
        "delivery_type": "clean_delivery_type",
        "delivery_cost": "clean_delivery_cost",
        "payment_method": "clean_payment_method",
        "payment_status": "clean_payment_status",
        "payment_amount": "clean_payment_amount",
        "currency": "clean_currency",
        "total_amount": "final_total_amount",
        "items_json": "clean_items_json",
    }

    for output_name, source_name in final_map.items():
        df = df.withColumn(output_name, F.col(source_name))

    return df.select(
        "raw_document_id",
        "run_id",
        "source_file",
        "source_row_number",
        "ingested_at",
        "engine_used",
        "raw_record",
        *BUSINESS_FIELDS,
        "quality_status",
        "corrections",
        "error_codes",
        "error_details",
    )


def add_business_hash(df):
    fields = [F.col(name) for name in BUSINESS_FIELDS]
    fields += [F.col("quality_status"), F.col("corrections")]
    return df.withColumn(
        "business_hash",
        F.sha2(F.to_json(F.struct(*fields)), 256),
    )


def prepare_validated_changes(spark, accepted_df, run_id):
    accepted_df = add_business_hash(accepted_df)

    existing_df = read_existing_validated(spark).dropDuplicates(["existing_order_id"])

    joined = accepted_df.join(
        existing_df,
        accepted_df["order_id"] == existing_df["existing_order_id"],
        "left",
    )

    joined = joined.withColumn(
        "change_action",
        F.when(F.col("existing_order_id").isNull(), F.lit("inserted"))
        .when(
            F.col("existing_business_hash") == F.col("business_hash"),
            F.lit("unchanged"),
        )
        .otherwise(F.lit("updated")),
    )

    now = F.current_timestamp()

    return (
        joined
        .withColumn(
            "first_run_id",
            F.coalesce(F.col("existing_first_run_id"), F.lit(run_id)),
        )
        .withColumn(
            "created_at",
            F.coalesce(F.col("existing_created_at"), now),
        )
        .withColumn("last_run_id", F.lit(run_id))
        .withColumn("updated_at", now)
    )


def write_validated_changes(changes_df):
    output_fields = BUSINESS_FIELDS + [
        "quality_status",
        "corrections",
        "business_hash",
        "first_run_id",
        "created_at",
        "last_run_id",
        "updated_at",
    ]

    to_write = (
        changes_df
        .filter(F.col("change_action") != "unchanged")
        .select(*output_fields)
    )

    (
        to_write.write
        .format("mongodb")
        .mode("append")
        .option("connection.uri", MONGO_URI)
        .option("database", DATABASE_NAME)
        .option("collection", VALIDATED_COLLECTION)
        .option("idFieldList", "order_id")
        .option("operationType", "replace")
        .option("upsertDocument", "true")
        .option("ordered", "false")
        .save()
    )


def write_quarantine(quarantine_df):
    output = quarantine_df.select(
        "raw_document_id",
        "run_id",
        "source_file",
        "source_row_number",
        "engine_used",
        F.current_timestamp().alias("processed_at"),
        "error_codes",
        "error_details",
        "corrections",
        "raw_record",
    )

    (
        output.write
        .format("mongodb")
        .mode("append")
        .option("connection.uri", MONGO_URI)
        .option("database", DATABASE_NAME)
        .option("collection", QUARANTINE_COLLECTION)
        .option("idFieldList", "raw_document_id")
        .option("operationType", "replace")
        .option("upsertDocument", "true")
        .option("ordered", "false")
        .save()
    )


def process_spark_run(run_id):
    spark = None
    classified_df = None
    changes_df = None
    started = time.time()

    try:
        spark = create_spark_session()

        print("\n====================================")
        print("Spark Quality Pipeline Started")
        print("====================================")
        print(f"Run ID: {run_id}")
        print(f"Pipeline version: {PIPELINE_VERSION}")
        print("Date strategy: STRING_ONLY_NO_SPARK_DATETIME_PARSER")
        print(f"Script path: {Path(__file__).resolve()}")
        print(f"Spark version: {spark.version}")
        print(f"Spark UI: {spark.sparkContext.uiWebUrl}")

        raw_count_hint = count_raw_run(run_id)
        print(f"Raw records expected from MongoDB: {raw_count_hint}")

        if raw_count_hint == 0:
            raise ValueError(f"No Raw records found for run_id: {run_id}")

        raw_df = read_raw_run(spark, run_id, raw_count_hint)
        input_partitions = raw_df.rdd.getNumPartitions()
        print(f"Input partitions: {input_partitions}")

        classified_df = (
            build_classified_dataframe(raw_df)
            .persist(StorageLevel.DISK_ONLY)
        )
        output_partitions = classified_df.rdd.getNumPartitions()
        print(f"Output partitions: {output_partitions}")

        print("\nSpark execution plan:")
        classified_df.explain(mode="formatted")

        summary = classified_df.agg(
            F.count(F.lit(1)).alias("raw_count"),
            F.sum(F.when(F.col("quality_status") == "valid", 1).otherwise(0)).alias("valid"),
            F.sum(F.when(F.col("quality_status") == "corrected", 1).otherwise(0)).alias("corrected"),
            F.sum(F.when(F.col("quality_status") == "quarantined", 1).otherwise(0)).alias("quarantine"),
        ).first()

        raw_count = int(summary["raw_count"] or 0)
        count_valid = int(summary["valid"] or 0)
        count_corrected = int(summary["corrected"] or 0)
        count_quarantine = int(summary["quarantine"] or 0)

        if raw_count != count_valid + count_corrected + count_quarantine:
            raise ValueError("Consistency check failed before writing.")

        error_rows = (
            classified_df
            .select(F.explode("error_codes").alias("error_code"))
            .groupBy("error_code")
            .count()
            .collect()
        )
        counts_case_error = {row["error_code"]: int(row["count"]) for row in error_rows}

        accepted_df = classified_df.filter(
            F.col("quality_status").isin("valid", "corrected")
        )
        quarantine_df = classified_df.filter(
            F.col("quality_status") == "quarantined"
        )

        print("\nComparing accepted records with orders_validated...")
        changes_df = (
            prepare_validated_changes(spark, accepted_df, run_id)
            .persist(StorageLevel.DISK_ONLY)
        )

        change_summary = changes_df.agg(
            F.sum(F.when(F.col("change_action") == "inserted", 1).otherwise(0)).alias("inserted"),
            F.sum(F.when(F.col("change_action") == "updated", 1).otherwise(0)).alias("updated"),
            F.sum(F.when(F.col("change_action") == "unchanged", 1).otherwise(0)).alias("unchanged"),
        ).first()

        count_inserted = int(change_summary["inserted"] or 0)
        count_updated = int(change_summary["updated"] or 0)
        count_unchanged = int(change_summary["unchanged"] or 0)

        print("\nWriting Valid/Corrected records with idempotent Upsert...")
        write_validated_changes(changes_df)

        print("\nWriting Quarantine records...")
        write_quarantine(quarantine_df)

        elapsed = time.time() - started
        throughput = raw_count / elapsed if elapsed > 0 else 0

        print("\n====================================")
        print("Spark Quality Pipeline Completed")
        print("====================================")
        print(f"Run ID: {run_id}")
        print(f"Raw count: {raw_count}")
        print(f"Valid: {count_valid}")
        print(f"Corrected: {count_corrected}")
        print(f"Quarantine: {count_quarantine}")
        print(f"Inserted: {count_inserted}")
        print(f"Updated: {count_updated}")
        print(f"Unchanged: {count_unchanged}")
        print(f"Input partitions: {input_partitions}")
        print(f"Output partitions: {output_partitions}")
        print(f"Elapsed time: {elapsed:.2f} seconds")
        print(f"Throughput: {throughput:.2f} records/sec")

        print("\nConsistency check:")
        print(f"{raw_count} = {count_valid} + {count_corrected} + {count_quarantine}")
        print("Consistency check PASSED.")

        print("\nError counts:")
        for code in sorted(counts_case_error):
            print(f"{code}: {counts_case_error[code]}")

        save_run_metrics({
            "run_id": run_id,
            "stage": "spark_quality",
            "file_name": "orders_huge_mixed_quality.csv",
            "used_engine": "pyspark",
            "read_rows": raw_count,
            "loaded_raw": raw_count,
            "count_valid": count_valid,
            "count_corrected": count_corrected,
            "count_quarantine": count_quarantine,
            "count_inserted": count_inserted,
            "count_updated": count_updated,
            "count_unchanged": count_unchanged,
            "seconds_elapsed": elapsed,
            "throughput": throughput,
            "batch_size": None,
            "partitions": input_partitions,
            "counts_case_error": counts_case_error,
        })

    finally:
        if changes_df is not None:
            changes_df.unpersist()
        if classified_df is not None:
            classified_df.unpersist()
        if spark is not None:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Spark cleaning, validation, quarantine, and MongoDB Upsert."
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="Existing run_id already loaded into orders_raw.",
    )
    args = parser.parse_args()
    process_spark_run(args.run_id)


if __name__ == "__main__":
    main()
