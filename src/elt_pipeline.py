import json
import time
from datetime import datetime, timezone

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)

from src.quality_rules import (
    normalize_arabic_digits,
    normalize_price_words,
    remove_thousands_separators,
    normalize_currency,
    normalize_phone,
    normalize_email,
    normalize_date,
    normalize_text_value,
    recalculate_total_amount,
)


# =========================================================
# Process and classify one raw record
# =========================================================

def process_record(raw_record):
    record = dict(raw_record)

    corrections = []
    error_codes = []
    error_details = []

    # -----------------------------------------------------
    # Spark may add this technical field while parsing CSV.
    # We use it for validation, but it must NOT become part
    # of the final business record in orders_validated.
    # -----------------------------------------------------

    corrupt_record = record.pop(
        "_corrupt_record",
        None,
    )

    if (
        corrupt_record is not None
        and str(corrupt_record).strip()
    ):
        error_codes.append(
            "CSV_ROW_CORRUPTED"
        )

        error_details.append(
            "Spark detected a malformed CSV row."
        )

    # -----------------------------------------------------
    # Helper for simple cleaning rules
    # -----------------------------------------------------

    def apply_rule(field, rule_function):
        old_value = record.get(field)

        new_value, correction = rule_function(
            old_value,
            field,
        )

        record[field] = new_value

        if correction is not None:
            corrections.append(
                correction
            )

    # =====================================================
    # CLEANING RULES
    # =====================================================

    # -----------------------------------------------------
    # 1. Arabic digits
    # -----------------------------------------------------

    for field in [
        "delivery_cost",
        "payment_amount",
        "total_amount",
        "customer_phone",
    ]:
        apply_rule(
            field,
            normalize_arabic_digits,
        )

    # -----------------------------------------------------
    # 2. Known price words -> numeric values
    # -----------------------------------------------------

    for field in [
        "delivery_cost",
        "payment_amount",
        "total_amount",
    ]:
        apply_rule(
            field,
            normalize_price_words,
        )

    # -----------------------------------------------------
    # 3. Thousands separators
    # -----------------------------------------------------

    for field in [
        "delivery_cost",
        "payment_amount",
        "total_amount",
    ]:
        apply_rule(
            field,
            remove_thousands_separators,
        )

    # -----------------------------------------------------
    # 3. Currency normalization
    # -----------------------------------------------------

    new_value, correction = normalize_currency(
        record.get("currency")
    )

    record["currency"] = new_value

    if correction is not None:
        corrections.append(
            correction
        )

    # -----------------------------------------------------
    # 4. Phone normalization
    # -----------------------------------------------------

    new_value, correction = normalize_phone(
        record.get("customer_phone")
    )

    record["customer_phone"] = new_value

    if correction is not None:
        corrections.append(
            correction
        )

    # -----------------------------------------------------
    # 5. Email normalization
    # -----------------------------------------------------

    new_value, correction = normalize_email(
        record.get("customer_email")
    )

    record["customer_email"] = new_value

    if correction is not None:
        corrections.append(
            correction
        )

    # -----------------------------------------------------
    # 6. Date normalization
    # -----------------------------------------------------

    new_value, correction = normalize_date(
        record.get("order_date")
    )

    record["order_date"] = new_value

    if correction is not None:
        corrections.append(
            correction
        )

    # -----------------------------------------------------
    # 7. Text normalization
    # -----------------------------------------------------

    for field in [
        "status",
        "payment_status",
        "customer_name",
        "city",
        "district",
        "delivery_type",
        "payment_method",
    ]:
        apply_rule(
            field,
            normalize_text_value,
        )

    # -----------------------------------------------------
    # 8. Recalculate total amount
    # -----------------------------------------------------

    new_value, correction = recalculate_total_amount(
        record.get("items_json"),
        record.get("delivery_cost"),
        record.get("total_amount"),
    )

    record["total_amount"] = new_value

    if correction is not None:
        corrections.append(
            correction
        )

    # =====================================================
    # QUARANTINE VALIDATION RULES
    # =====================================================

    # -----------------------------------------------------
    # Missing order ID
    # -----------------------------------------------------

    order_id_value = record.get(
        "order_id"
    )

    if (
        order_id_value is None
        or not str(
            order_id_value
        ).strip()
    ):
        error_codes.append(
            "ID_ORDER_MISSING"
        )

        error_details.append(
            "Order ID is missing and cannot be inferred safely."
        )

    # -----------------------------------------------------
    # Missing customer ID
    # -----------------------------------------------------

    customer_id_value = record.get(
        "customer_id"
    )

    if (
        customer_id_value is None
        or not str(
            customer_id_value
        ).strip()
    ):
        error_codes.append(
            "ID_CUSTOMER_MISSING"
        )

        error_details.append(
            "Customer ID is missing."
        )

    # -----------------------------------------------------
    # Validate order date
    # -----------------------------------------------------

    order_date_value = record.get(
        "order_date"
    )

    if order_date_value is None:
        order_date = ""
    else:
        order_date = str(
            order_date_value
        ).strip()

    date_valid = False

    for date_format in [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
    ]:
        try:
            datetime.strptime(
                order_date,
                date_format,
            )

            date_valid = True
            break

        except ValueError:
            continue

    if not date_valid:
        error_codes.append(
            "DATE_IMPOSSIBLE_INVALID"
        )

        error_details.append(
            f"Invalid or impossible order date: {order_date}"
        )

    # -----------------------------------------------------
    # Validate items_json
    # -----------------------------------------------------

    items = None

    items_json_value = record.get(
        "items_json"
    )

    try:
        if items_json_value is None:
            raise TypeError(
                "items_json is None"
            )

        items = json.loads(
            items_json_value
        )

    except (
        json.JSONDecodeError,
        TypeError,
    ):
        error_codes.append(
            "JSON_ITEMS_CORRUPTED"
        )

        error_details.append(
            "items_json is corrupted or cannot be parsed."
        )

    # -----------------------------------------------------
    # Empty items
    # -----------------------------------------------------

    if items is not None:

        if (
            not isinstance(
                items,
                list,
            )
            or len(items) == 0
        ):
            error_codes.append(
                "ITEMS_EMPTY"
            )

            error_details.append(
                "The order contains no usable items."
            )

        # -------------------------------------------------
        # Negative ambiguous values
        # -------------------------------------------------

        elif isinstance(
            items,
            list,
        ):
            for item in items:

                if not isinstance(
                    item,
                    dict,
                ):
                    continue

                for field in [
                    "qty",
                    "unit_price",
                    "total",
                ]:
                    value = item.get(
                        field
                    )

                    try:
                        if (
                            value is not None
                            and float(value) < 0
                        ):
                            if (
                                "VALUE_NEGATIVE_AMBIGUOUS"
                                not in error_codes
                            ):
                                error_codes.append(
                                    "VALUE_NEGATIVE_AMBIGUOUS"
                                )

                                error_details.append(
                                    "Negative quantity or amount "
                                    "cannot be interpreted safely."
                                )

                    except (
                        TypeError,
                        ValueError,
                    ):
                        pass

    # -----------------------------------------------------
    # Validate total amount
    # -----------------------------------------------------

    try:
        total_amount_value = record.get(
            "total_amount"
        )

        if total_amount_value is None:
            raise TypeError(
                "total_amount is None"
            )

        float(
            total_amount_value
        )

    except (
        TypeError,
        ValueError,
    ):
        error_codes.append(
            "PRICE_UNKNOWN"
        )

        error_details.append(
            "Total amount is missing or cannot be determined."
        )

    # =====================================================
    # FINAL CLASSIFICATION
    # =====================================================

    if error_codes:
        quality_status = "quarantined"

    elif corrections:
        quality_status = "corrected"

    else:
        quality_status = "valid"

    return {
        "quality_status": quality_status,
        "clean_record": record,
        "corrections": corrections,
        "error_codes": error_codes,
        "error_details": error_details,
    }


# =========================================================
# Process all records belonging to one run_id
# =========================================================

def process_run(run_id):

    client = MongoClient(
        MONGO_URI
    )

    database = client[
        DATABASE_NAME
    ]

    raw_collection = database[
        RAW_COLLECTION
    ]

    validated_collection = database[
        VALIDATED_COLLECTION
    ]

    quarantine_collection = database[
        QUARANTINE_COLLECTION
    ]

    # -----------------------------------------------------
    # Counters
    # -----------------------------------------------------

    count_valid = 0
    count_corrected = 0
    count_quarantine = 0

    count_inserted = 0
    count_updated = 0
    count_unchanged = 0

    counts_case_error = {}

    processed_count = 0

    seen_order_ids = set()

    start_time = time.time()

    try:

        # -------------------------------------------------
        # Count Raw records for this run
        # -------------------------------------------------

        raw_count = (
            raw_collection
            .count_documents(
                {
                    "run_id": run_id
                }
            )
        )

        print(
            f"Run ID: {run_id}"
        )

        print(
            f"Raw records found: {raw_count}"
        )

        print(
            "Processing started...\n"
        )

        # -------------------------------------------------
        # Read Raw documents
        # -------------------------------------------------

        cursor = (
            raw_collection
            .find(
                {
                    "run_id": run_id
                }
            )
            .sort(
                "source_row_number",
                1,
            )
        )

        for raw_document in cursor:

            raw_record = (
                raw_document[
                    "raw_record"
                ]
            )

            # ---------------------------------------------
            # Clean + validate + classify
            # ---------------------------------------------

            result = process_record(
                raw_record
            )

            # ---------------------------------------------
            # Safe order_id handling
            # ---------------------------------------------

            raw_order_id = raw_record.get(
                "order_id"
            )

            order_id = (
                str(
                    raw_order_id
                ).strip()
                if raw_order_id is not None
                else ""
            )

            # ---------------------------------------------
            # Duplicate order ID in same run
            # ---------------------------------------------

            if (
                order_id
                and order_id in seen_order_ids
            ):
                result[
                    "quality_status"
                ] = "quarantined"

                if (
                    "ID_ORDER_DUPLICATE"
                    not in result[
                        "error_codes"
                    ]
                ):
                    result[
                        "error_codes"
                    ].append(
                        "ID_ORDER_DUPLICATE"
                    )

                    result[
                        "error_details"
                    ].append(
                        "Duplicate order_id found "
                        "inside the same run."
                    )

            elif order_id:

                seen_order_ids.add(
                    order_id
                )

            quality_status = result[
                "quality_status"
            ]

            # Count final error codes for this record
            for error_code in result["error_codes"]:
                counts_case_error[error_code] = (
                    counts_case_error.get(error_code, 0) + 1
                )

            processed_count += 1

            # =================================================
            # VALID / CORRECTED
            # =================================================

            if quality_status in [
                "valid",
                "corrected",
            ]:

                if quality_status == "valid":
                    count_valid += 1

                else:
                    count_corrected += 1

                clean_record = result[
                    "clean_record"
                ]

                clean_order_id = clean_record.get(
                    "order_id"
                )

                # Safety guard
                if (
                    clean_order_id is None
                    or not str(
                        clean_order_id
                    ).strip()
                ):
                    raise ValueError(
                        "A record with missing order_id "
                        "reached orders_validated."
                    )

                business_document = {
                    **clean_record,
                    "quality_status":
                        quality_status,
                    "corrections":
                        result[
                            "corrections"
                        ],
                }

                # ---------------------------------------------
                # TRUE MONGODB UPSERT
                # ---------------------------------------------

                upsert_result = (
                    validated_collection
                    .update_one(
                        {
                            "order_id":
                                clean_order_id
                        },
                        {
                            "$set":
                                business_document,

                            "$setOnInsert": {
                                "first_run_id":
                                    run_id,

                                "created_at":
                                    datetime.now(
                                        timezone.utc
                                    ),
                            },
                        },
                        upsert=True,
                    )
                )

                # ---------------------------------------------
                # Insert / Update / Unchanged
                # ---------------------------------------------

                if (
                    upsert_result.upserted_id
                    is not None
                ):
                    count_inserted += 1

                elif (
                    upsert_result.modified_count
                    > 0
                ):
                    count_updated += 1

                else:
                    count_unchanged += 1

            # =================================================
            # QUARANTINED
            # =================================================

            else:

                count_quarantine += 1

                raw_document_id = str(
                    raw_document["_id"]
                )

                quarantine_document = {

                    "run_id":
                        run_id,

                    "raw_document_id":
                        raw_document_id,

                    "source_file":
                        raw_document.get(
                            "source_file"
                        ),

                    "source_row_number":
                        raw_document.get(
                            "source_row_number"
                        ),

                    "engine_used":
                        raw_document.get(
                            "engine_used"
                        ),

                    "processed_at":
                        datetime.now(
                            timezone.utc
                        ),

                    "error_codes":
                        result[
                            "error_codes"
                        ],

                    "error_details":
                        result[
                            "error_details"
                        ],

                    "corrections":
                        result[
                            "corrections"
                        ],

                    # Keep the ORIGINAL Raw record here,
                    # including Spark's technical field
                    # if it existed.
                    "raw_record":
                        raw_record,
                }

                quarantine_collection.update_one(
                    {
                        "raw_document_id":
                            raw_document_id
                    },
                    {
                        "$set":
                            quarantine_document
                    },
                    upsert=True,
                )

            # -------------------------------------------------
            # Progress
            # -------------------------------------------------

            if (
                processed_count
                % 5000 == 0
            ):
                print(
                    f"Processed: "
                    f"{processed_count}"
                    f"/{raw_count}"
                )

        # =====================================================
        # FINAL METRICS
        # =====================================================

        elapsed_seconds = (
            time.time()
            - start_time
        )

        throughput = (
            processed_count
            / elapsed_seconds
            if elapsed_seconds > 0
            else 0
        )

        classified_count = (
            count_valid
            + count_corrected
            + count_quarantine
        )

        print(
            "\nProcessing completed."
        )

        print(
            f"Raw count: {raw_count}"
        )

        print(
            f"Processed: {processed_count}"
        )

        print(
            f"Valid: {count_valid}"
        )

        print(
            f"Corrected: {count_corrected}"
        )

        print(
            f"Quarantine: {count_quarantine}"
        )

        print(
            f"Inserted: {count_inserted}"
        )

        print(
            f"Updated: {count_updated}"
        )

        print(
            f"Unchanged: {count_unchanged}"
        )

        print(
            f"Elapsed time: "
            f"{elapsed_seconds:.2f} seconds"
        )

        print(
            f"Throughput: "
            f"{throughput:.2f} records/sec"
        )

        # =====================================================
        # CONSISTENCY CHECK
        # =====================================================

        print(
            "\nConsistency check:"
        )

        print(
            f"{raw_count} = "
            f"{count_valid} + "
            f"{count_corrected} + "
            f"{count_quarantine}"
        )

        if (
            raw_count
            != classified_count
        ):
            raise ValueError(
                "Consistency check failed."
            )

        print(
            "Consistency check PASSED."
        )

        return {
            "run_id":
                run_id,

            "raw_count":
                raw_count,

            "count_valid":
                count_valid,

            "count_corrected":
                count_corrected,

            "count_quarantine":
                count_quarantine,

            "count_inserted":
                count_inserted,

            "count_updated":
                count_updated,

            "count_unchanged":
                count_unchanged,

            "elapsed_seconds":
                elapsed_seconds,

            "throughput":
                throughput,

            "counts_case_error":
                counts_case_error,
        }

    finally:
        client.close()