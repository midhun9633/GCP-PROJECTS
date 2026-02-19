from datetime import datetime, timedelta
import uuid
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 15),
}

# Define the DAG
with DAG(
    dag_id="transformed_weather_data_to_bq",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Generate a unique batch ID using UUID
    batch_id = f"weather-data-batch-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity

    # Submit PySpark job to Dataproc Serverless
    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://weather-data-mid/script/weather_data_processing.py",
            "python_file_uris": [],
            "jar_file_uris": [],
            "args": []
        },
        "runtime_config": {
            "version": "2.2",
        },
        "environment_config": {
            "execution_config": {
                "service_account": "dev-user@fluted-cogency-403608.iam.gserviceaccount.com",
                "network_uri": "projects/fluted-cogency-403608/global/networks/default",
                "subnetwork_uri": "projects/fluted-cogency-403608/regions/us-central1/subnetworks/default",
            }
        },
    }

    pyspark_task = DataprocCreateBatchOperator(
        task_id="spark_job_on_dataproc_serverless",
        batch=batch_details,
        batch_id=batch_id,
        project_id="fluted-cogency-403608",
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    # Task Dependencies
    pyspark_task