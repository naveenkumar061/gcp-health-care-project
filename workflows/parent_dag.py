import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago

# Note: Modern Airflow versions recommend importing from .trigger_dagrun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Define default arguments
ARGS = {
    "owner": "D Naveen Kumar GCP Data Engineering",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the parent DAG
with DAG(
    dag_id="parent_dag",
    schedule_interval="0 5 * * *",
    description="Parent DAG to trigger PySpark and BigQuery DAGs",
    default_args=ARGS,
    tags=["parent", "orchestration", "etl"],
) as dag:
    # Task to trigger PySpark DAG
    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id="trigger_pyspark_dag",
        trigger_dag_id="pyspark_dag",
        wait_for_completion=True,
        poke_interval=60,  # Check the status of the downstream DAG every 60 seconds
        execution_timeout=timedelta(
            hours=2
        ),  # Prevents SIGTERM by allowing the task to run up to 2 hours
    )

    # Task to trigger BigQuery DAG
    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id="trigger_bigquery_dag",
        trigger_dag_id="bigquery_dag",
        wait_for_completion=True,
        poke_interval=30,
        execution_timeout=timedelta(hours=1),
    )

# Define dependencies
trigger_pyspark_dag >> trigger_bigquery_dag
