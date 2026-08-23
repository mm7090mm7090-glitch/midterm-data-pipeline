import argparse
import csv
import time
from pathlib import Path


def create_small_sample(input_file, output_file, rows):
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    copied_rows = 0

    with input_path.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as source_file, output_path.open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as destination_file:

        reader = csv.reader(source_file)
        writer = csv.writer(destination_file)

        header = next(reader, None)

        if header is None:
            raise ValueError("The input CSV file is empty.")

        writer.writerow(header)

        for row in reader:
            if copied_rows >= rows:
                break

            writer.writerow(row)
            copied_rows += 1

    elapsed_time = time.time() - start_time

    print(f"Input file: {input_path}")
    print(f"Output file: {output_path}")
    print(f"Rows copied: {copied_rows}")
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
    print("Small sample created successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="Create a reproducible small CSV sample."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the large input CSV file."
    )

    parser.add_argument(
        "--output",
        default="data/orders_small_sample.csv",
        help="Path for the generated sample CSV."
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=100000,
        help="Number of data rows to copy."
    )

    args = parser.parse_args()

    create_small_sample(
        input_file=args.input,
        output_file=args.output,
        rows=args.rows
    )


if __name__ == "__main__":
    main()