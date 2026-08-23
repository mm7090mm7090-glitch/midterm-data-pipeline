from pathlib import Path

from config.settings import SMALL_FILE_THRESHOLD_MB


def get_file_size_mb(file_path):
    file_size_bytes = Path(file_path).stat().st_size
    file_size_mb = file_size_bytes / (1024 * 1024)
    return file_size_mb


def choose_engine(file_path):
    file_size_mb = get_file_size_mb(file_path)

    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = (
            f"File size is {file_size_mb:.2f} MB, "
            f"which is less than or equal to the threshold "
            f"of {SMALL_FILE_THRESHOLD_MB} MB."
        )
    else:
        engine = "pyspark"
        reason = (
            f"File size is {file_size_mb:.2f} MB, "
            f"which is greater than the threshold "
            f"of {SMALL_FILE_THRESHOLD_MB} MB."
        )

    print(f"File size: {file_size_mb:.2f} MB")
    print(f"Selected engine: {engine}")
    print(f"Reason: {reason}")

    return {
        "file_size_mb": file_size_mb,
        "engine": engine,
        "reason": reason,
    }