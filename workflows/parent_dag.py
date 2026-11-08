from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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

# Define the parent orchestration DAG
with DAG(
    dag_id="parent_dag",
    schedule_interval="0 5 * * *",  # Runs daily at 05:00 AM UTC
    description="Parent DAG to orchestrate PySpark and BigQuery ETL pipelines sequentially",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["parent", "orchestration", "etl"],
) as dag:
    # Task to trigger the PySpark data ingestion pipeline
    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id="trigger_pyspark_dag",
        trigger_dag_id="pyspark_dag",
        wait_for_completion=True,
        poke_interval=30,  # Checks status every 30 seconds rather than flooding the database
        reset_dag_run=True,  # Clear and rerun the target DAG if it already ran for this execution date
    )

    # Task to trigger the BigQuery data warehouse refinement pipeline
    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id="trigger_bigquery_dag",
        trigger_dag_id="bigquery_dag",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

# Define sequential execution dependencies
trigger_pyspark_dag >> trigger_bigquery_dag
