from datetime import timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
from airflow.models import Variable
from openweather_helpers import extract_openweather, upload_to_gcs

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="openweather_api_to_gcs",
    default_args=default_args,
    description="Fetch OpenWeather with Pandas+Requests in a venv, upload to GCS, trigger downstream DAG",
    schedule_interval=None,
    catchup=False,
    tags=["weather", "gcs"],
) as dag:

    extract_weather = PythonVirtualenvOperator(
        task_id="extract_weather_data",
        python_callable=extract_openweather,
        requirements=["pandas==1.5.3", "requests==2.31.0"],
        system_site_packages=True,
        op_kwargs={"api_key": Variable.get("openweather_api_key")},
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={"ds": "{{ ds }}"},
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_data_transform_dag",
        trigger_dag_id="transformed_weather_data_to_bq",
        wait_for_completion=False,
    )

    extract_weather >> upload_to_gcs_task >> trigger_transform
