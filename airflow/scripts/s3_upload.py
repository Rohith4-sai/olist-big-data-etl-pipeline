from pathlib import Path
import boto3


# ==================================================
# S3 UPLOAD - PROCESSED OLIST DATA
# ==================================================


# --------------------------------------------------
# 1. AWS / PROJECT CONFIGURATION
# --------------------------------------------------

BUCKET_NAME = "sai-rohith-olist-data-pipeline-2026"

# Inside the Docker/Airflow container, the project's
# Data folder is mounted at /opt/airflow/Data
PROJECT_ROOT = Path("/opt/airflow")

LOCAL_FILE = (
    PROJECT_ROOT
    / "Data"
    / "output"
    / "missed_shipping_limit_orders"
    / "late_shipments.csv"
)

S3_KEY = "processed-data/late_shipments.csv"


# --------------------------------------------------
# 2. UPLOAD PROCESSED DATA TO S3
# --------------------------------------------------

def upload_processed_data():

    print("=" * 50)
    print("S3 UPLOAD - PROCESSED OLIST DATA")
    print("=" * 50)

    # Verify that the PySpark output exists
    if not LOCAL_FILE.exists():

        raise FileNotFoundError(
            f"Processed file not found:\n{LOCAL_FILE}\n"
            "Run the PySpark ETL job before uploading."
        )

    print(
        f"\nLocal file:\n{LOCAL_FILE}"
    )

    print(
        f"\nDestination:\n"
        f"s3://{BUCKET_NAME}/{S3_KEY}"
    )

    # Boto3 uses the AWS credentials mounted into
    # the Airflow Docker container.
    # Never hard-code AWS access keys in this file.
    s3 = boto3.client(
        "s3",
        region_name="ap-south-1"
    )

    print("\nUploading...")

    s3.upload_file(
        str(LOCAL_FILE),
        BUCKET_NAME,
        S3_KEY
    )

    print("\nUpload completed successfully.")

    print(
        f"s3://{BUCKET_NAME}/{S3_KEY}"
    )


# --------------------------------------------------
# 3. RUN SCRIPT
# --------------------------------------------------

if __name__ == "__main__":

    upload_processed_data()