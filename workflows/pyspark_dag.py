from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

# Define variables using your updated project variables
PROJECT_ID = "project-5d0d5583-f7fc-4cf4-9ce"
REGION = "us-east1"
CLUSTER_NAME = "my-demo-cluster"
COMPOSER_BUCKET = "us-east4-demo-instance-186ddff6-bucket"

# Define cluster configuration structure required by modern Airflow operators
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_size_gb": 50},
    },
    "software_config": {"image_version": "2.0-debian10"},
}

# Define the PySpark job payloads
PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalA_mysqlToLanding.py"
    },
}

PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalB_mysqlToLanding.py"
    },
}

PYSPARK_JOB_3 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{COMPOSER_BUCKET}/data/INGESTION/claims.py"
    },
}

PYSPARK_JOB_4 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{COMPOSER_BUCKET}/data/INGESTION/cpt_codes.py"
    },
}

# Define default arguments matching your specifications
DEFAULT_ARGS = {
    "owner": "D Naveen Kumar GCP Data Engineering",
    "start_date": datetime(2026, 11, 1),
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
    dag_id="pyspark_dag",
    schedule_interval=None,
    description="Orchestrates Dataproc lifecycle and runs PySpark ingestion jobs",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["pyspark", "dataproc", "etl", "marvel"],
) as dag:
    # Replaced deprecated StartCluster with CreateCluster mapping config structure
    start_cluster = DataprocCreateClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id="pyspark_task_1",
        job=PYSPARK_JOB_1,
        region=REGION,
        project_id=PROJECT_ID,
    )

    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id="pyspark_task_2",
        job=PYSPARK_JOB_2,
        region=REGION,
        project_id=PROJECT_ID,
    )

    pyspark_task_3 = DataprocSubmitJobOperator(
        task_id="pyspark_task_3",
        job=PYSPARK_JOB_3,
        region=REGION,
        project_id=PROJECT_ID,
    )

    pyspark_task_4 = DataprocSubmitJobOperator(
        task_id="pyspark_task_4",
        job=PYSPARK_JOB_4,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Replaced deprecated StopCluster with DeleteCluster to release resources
    stop_cluster = DataprocDeleteClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",  # Guarantees the cluster is deleted even if prior jobs fail
    )

# Define task execution workflow
(
    start_cluster
    >> pyspark_task_1
    >> pyspark_task_2
    >> pyspark_task_3
    >> pyspark_task_4
    >> stop_cluster
)
