from pathlib import Path
import boto3

# --------------------------------------------------
# AWS / PROJECT CONFIGURATION
# --------------------------------------------------

BUCKET_NAME = "sai-rohith-olist-data-pipeline-2026"
S3_PREFIX = "raw-data/"

# Correct project path inside the Airflow Docker container
PROJECT_ROOT = Path("/opt/airflow")
DOWNLOAD_DIR = PROJECT_ROOT / "Data" / "olist"


# --------------------------------------------------
# DOWNLOAD RAW OLIST DATA FROM S3
# --------------------------------------------------

def download_raw_data():

    print("=" * 50)
    print("S3 DOWNLOAD - RAW OLIST DATA")
    print("=" * 50)

    # Create the local directory if it does not exist
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Uses AWS credentials mounted/configured outside source code.
    # Never hard-code AWS access keys here.
    s3 = boto3.client("s3")

    print(f"\nSource: s3://{BUCKET_NAME}/{S3_PREFIX}")
    print(f"Destination: {DOWNLOAD_DIR}\n")

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=S3_PREFIX
    )

    objects = response.get("Contents", [])

    csv_objects = [
        obj for obj in objects
        if obj["Key"].lower().endswith(".csv")
    ]

    if not csv_objects:
        raise FileNotFoundError(
            f"No CSV files found at "
            f"s3://{BUCKET_NAME}/{S3_PREFIX}"
        )

    for obj in csv_objects:

        s3_key = obj["Key"]
        filename = Path(s3_key).name
        local_path = DOWNLOAD_DIR / filename

        print(f"Downloading: {filename}")

        s3.download_file(
            BUCKET_NAME,
            s3_key,
            str(local_path)
        )

    print("\nDownload completed successfully.")
    print(f"CSV files downloaded: {len(csv_objects)}")


if __name__ == "__main__":
    download_raw_data()