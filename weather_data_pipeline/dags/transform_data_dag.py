import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from dataproc_helpers import get_batch_id, get_batch_details

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 5, 15),
}

with DAG(
    dag_id="transformed_weather_data_to_bq",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    pyspark_task = DataprocCreateBatchOperator(
        task_id="spark_job_on_dataproc_serverless",
        batch=get_batch_details(),
        batch_id=get_batch_id(),
        project_id="fluted-cogency-403608",
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    pyspark_task
