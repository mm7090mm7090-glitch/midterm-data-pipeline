import csv
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    RAW_COLLECTION,
    BATCH_SIZE,
)


def load_csv_to_raw(file_path, run_id=None, batch_size=BATCH_SIZE):
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if run_id is None:
        run_id = str(uuid.uuid4())

    client = MongoClient(MONGO_URI)
    database = client[DATABASE_NAME]
    raw_collection = database[RAW_COLLECTION]

    total_read = 0
    total_loaded = 0
    batch_number = 0

    start_time = time.time()

    try:
        with file_path.open(
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as csv_file:

            reader = csv.DictReader(csv_file)
            batch = []

            for row_number, row in enumerate(reader, start=2):
                total_read += 1

                raw_document = {
                    "run_id": run_id,
                    "source_file": str(file_path),
                    "source_row_number": row_number,
                    "ingested_at": datetime.now(timezone.utc),
                    "engine_used": "python_batch",
                    "raw_record": row,
                }

                batch.append(raw_document)

                if len(batch) >= batch_size:
                    batch_number += 1
                    batch_start = time.time()

                    try:
                        result = raw_collection.insert_many(batch)
                        inserted_count = len(result.inserted_ids)
                        total_loaded += inserted_count

                        batch_elapsed = time.time() - batch_start
                        batch_throughput = (
                            inserted_count / batch_elapsed
                            if batch_elapsed > 0
                            else 0
                        )

                        print(
                            f"Batch {batch_number}: "
                            f"{inserted_count} records | "
                            f"{batch_elapsed:.2f} sec | "
                            f"{batch_throughput:.2f} records/sec"
                        )

                    except Exception as error:
                        print(
                            f"Batch {batch_number} failed: {error}"
                        )
                        raise

                    batch = []

            if batch:
                batch_number += 1
                batch_start = time.time()

                try:
                    result = raw_collection.insert_many(batch)
                    inserted_count = len(result.inserted_ids)
                    total_loaded += inserted_count

                    batch_elapsed = time.time() - batch_start
                    batch_throughput = (
                        inserted_count / batch_elapsed
                        if batch_elapsed > 0
                        else 0
                    )

                    print(
                        f"Batch {batch_number}: "
                        f"{inserted_count} records | "
                        f"{batch_elapsed:.2f} sec | "
                        f"{batch_throughput:.2f} records/sec"
                    )

                except Exception as error:
                    print(
                        f"Batch {batch_number} failed: {error}"
                    )
                    raise

        total_elapsed = time.time() - start_time

        throughput = (
            total_loaded / total_elapsed
            if total_elapsed > 0
            else 0
        )

        print("\nBatch loading completed.")
        print(f"Run ID: {run_id}")
        print(f"Rows read: {total_read}")
        print(f"Rows loaded to raw: {total_loaded}")
        print(f"Number of batches: {batch_number}")
        print(f"Elapsed time: {total_elapsed:.2f} seconds")
        print(f"Throughput: {throughput:.2f} records/sec")

        return {
            "run_id": run_id,
            "read_rows": total_read,
            "loaded_raw": total_loaded,
            "batch_count": batch_number,
            "elapsed_seconds": total_elapsed,
            "throughput": throughput,
        }

    finally:
        client.close()


if __name__ == "__main__":
    load_csv_to_raw(
        "data/orders_small_sample.csv"
    )