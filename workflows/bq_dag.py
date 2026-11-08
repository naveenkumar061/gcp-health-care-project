from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Define constants
PROJECT_ID = "project-5d0d5583-f7fc-4cf4-9ce"
LOCATION = "US"

# Define default arguments
DEFAULT_ARGS = {
    "owner": "D Naveen Kumar GCP Data Engineering",
    "start_date": datetime(
        2026, 11, 1
    ),  # Explicit start date to prevent scheduling lockups
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="bigquery_dag",
    schedule_interval=None,  # Triggered manually, via Cloud Build, or API calls
    description="Optimized Medallion ETL architecture via BigQuery",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["gcs", "bq", "etl", "marvel"],
    template_searchpath=[
        "/home/airflow/gcs/data/BQ/"
    ],  # Resolves the Top-Level I/O scheduler performance bug
) as dag:
    # Task to create bronze table
    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": "bronze.sql",  # Loaded dynamically by Airflow renderer during task runtime execution
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create silver table
    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": "silver.sql",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create gold table
    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": "gold.sql",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

# Define pipeline execution sequence
bronze_tables >> silver_tables >> gold_tables
