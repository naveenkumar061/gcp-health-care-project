# import all modules
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

# define the variables
PROJECT_ID = "project-5d0d5583-f7fc-4cf4-9ce"
REGION = "us-east1"
CLUSTER_NAME = "my-demo-cluster"
COMPOSER_BUCKET = "us-central1-demo-instance-e3396285-bucket"

# Clean configuration layout compatible with your environment version
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
    },
    "software_config": {
        "image_version": "2.0-debian10",
    },
}

# define PySpark job configurations
GCS_JOB_FILE_1 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalA_mysqlToLanding.py"
PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_1},
}

GCS_JOB_FILE_2 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalB_mysqlToLanding.py"
PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_2},
}

GCS_JOB_FILE_3 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/claims.py"
PYSPARK_JOB_3 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_3},
}

GCS_JOB_FILE_4 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/cpt_codes.py"
PYSPARK_JOB_4 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_4},
}

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

# CRITICAL FIX: Explicit global instantiation to force Airflow parsing engine recognition
dag = DAG(
    dag_id="pyspark_dag",
    schedule_interval=None,
    description="DAG to start a Dataproc cluster, run PySpark jobs, and stop the cluster",
    default_args=ARGS,
    catchup=False,
    tags=["pyspark", "dataproc", "etl", "marvel"],
)

# 1. Start Cluster Task
start_cluster = DataprocStartClusterOperator(
    task_id="start_cluster",
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
)

# 2. PySpark Execution Jobs
pyspark_task_1 = DataprocSubmitJobOperator(
    task_id="pyspark_task_1",
    job=PYSPARK_JOB_1,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

pyspark_task_2 = DataprocSubmitJobOperator(
    task_id="pyspark_task_2",
    job=PYSPARK_JOB_2,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

pyspark_task_3 = DataprocSubmitJobOperator(
    task_id="pyspark_task_3",
    job=PYSPARK_JOB_3,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

pyspark_task_4 = DataprocSubmitJobOperator(
    task_id="pyspark_task_4",
    job=PYSPARK_JOB_4,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# 3. Delete Cluster Task
stop_cluster = DataprocDeleteClusterOperator(
    task_id="stop_cluster",
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag,
)

# define the linear task dependencies
(
    start_cluster
    >> pyspark_task_1
    >> pyspark_task_2
    >> pyspark_task_3
    >> pyspark_task_4
    >> stop_cluster
)
