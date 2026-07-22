from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="olist_data_pipeline",
    description="Download Olist data from S3, process late shipments with PySpark, and upload results to S3",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "s3", "pyspark", "etl"],
) as dag:

    download_raw_data = BashOperator(
        task_id="download_raw_data_from_s3",
        bash_command="python /opt/airflow/scripts/s3_download.py",
    )

    process_late_shipments = BashOperator(
        task_id="process_late_shipments",
        bash_command="python /opt/airflow/scripts/spark_missed_deadline_job.py",
    )

    upload_processed_data = BashOperator(
        task_id="upload_processed_data_to_s3",
        bash_command="python /opt/airflow/scripts/s3_upload.py",
    )

    download_raw_data >> process_late_shipments >> upload_processed_data