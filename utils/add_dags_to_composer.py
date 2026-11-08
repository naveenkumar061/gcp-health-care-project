import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from pathlib import Path
import tempfile
from shutil import copytree, ignore_patterns
from google.cloud import storage


def upload_single_file(
    file_path: Path,
    temp_path: Path,
    bucket_name: str,
    destination_prefix: str,
    dry_run: bool,
) -> str:
    """Worker function to upload an individual file to GCS."""
    relative_path = file_path.relative_to(temp_path)
    file_gcs_path = f"{destination_prefix}{relative_path.as_posix()}"

    if dry_run:
        return f"👀 [DRY-RUN] Would upload {file_path.name} -> gs://{bucket_name}/{file_gcs_path}"

    # Initialize the client per thread to ensure thread-safety
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_gcs_path)
    blob.upload_from_filename(str(file_path))
    return f"✅ Uploaded {file_path.name} -> gs://{bucket_name}/{file_gcs_path}"


def upload_to_composer(
    directory_path: str,
    bucket_name: str,
    destination_prefix: str,
    dry_run: bool,
    max_workers: int,
) -> None:
    """Copies relevant files to a temp directory and uploads them concurrently using a thread pool."""
    local_dir = Path(directory_path)

    if not local_dir.exists():
        print(
            f"⚠️ Warning: Directory '{directory_path}' does not exist. Skipping upload."
        )
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Exclude tests and compiled garbage files
        files_to_ignore = ignore_patterns("*_test.py", "__pycache__", "*.pyc")
        copytree(local_dir, temp_path, ignore=files_to_ignore, dirs_exist_ok=True)

        # Gather all files recursively
        files = [f for f in temp_path.rglob("*") if f.is_file()]

        if not files:
            print(f"⚠️ No matching files found in '{directory_path}'. Skipping.")
            return

        action_msg = (
            "Previewing uploads (Dry-Run mode)"
            if dry_run
            else "Starting parallel upload"
        )
        print(
            f"🚀 {action_msg} to gs://{bucket_name}/{destination_prefix} using up to {max_workers} threads..."
        )

        # Process uploads concurrently using a ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    upload_single_file,
                    file,
                    temp_path,
                    bucket_name,
                    destination_prefix,
                    dry_run,
                )
                for file in files
            ]

            # Print status updates as threads finish their tasks
            for future in as_completed(futures):
                try:
                    result_message = future.result()
                    print(result_message)
                except Exception as e:
                    print(f"❌ Thread execution error encountered: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parallel and safe upload of DAGs and data to Composer."
    )
    parser.add_argument("--dags_directory", help="Path to local DAGs directory.")
    parser.add_argument("--data_directory", help="Path to local data directory.")
    parser.add_argument(
        "--composer_bucket", required=True, help="Target GCS bucket name."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview target paths without executing uploads.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=8,
        help="Number of concurrent upload worker threads (default: 8).",
    )

    args = parser.parse_args()

    if args.dags_directory:
        upload_to_composer(
            args.dags_directory,
            args.composer_bucket,
            "dags/",
            args.dry_run,
            args.threads,
        )
    else:
        print("ℹ️ Skipping DAGs upload: flag '--dags_directory' not provided.")

    if args.data_directory:
        upload_to_composer(
            args.data_directory,
            args.composer_bucket,
            "data/",
            args.dry_run,
            args.threads,
        )
    else:
        print("ℹ️ Skipping Data upload: flag '--data_directory' not provided.")
