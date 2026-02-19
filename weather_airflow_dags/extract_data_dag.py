from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from datetime import timedelta

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

    # -------------------------------------------------------------------
    # Task 1: Extract data in a venv that REUSES system site-packages
    # -------------------------------------------------------------------
    def _extract_openweather(api_key: str) -> str:
        import requests
        import pandas as pd

        endpoint = "https://api.openweathermap.org/data/2.5/forecast"
        params = {"q": "Toronto,CA", "appid": api_key}

        resp = requests.get(endpoint, params=params)
        resp.raise_for_status()

        df = pd.json_normalize(resp.json()["list"])
        return df.to_csv(index=False)

    extract_weather = PythonVirtualenvOperator(
        task_id="extract_weather_data",
        python_callable=_extract_openweather,
        # install pandas & requests, but reuse system numpy
        requirements=[
            "pandas==1.5.3",
            "requests==2.31.0",
        ],
        system_site_packages=True,   # <-- avoids the numpy ABI mismatch
        op_kwargs={"api_key": Variable.get("openweather_api_key")},
    )

    # -------------------------------------------------------------------
    # Task 2: Upload CSV string from XCom into GCS
    # -------------------------------------------------------------------
    def _upload_to_gcs(ds: str, **kwargs):
        ti = kwargs["ti"]
        csv_data = ti.xcom_pull(task_ids="extract_weather_data")
        hook = GCSHook()
        hook.upload(
            bucket_name="weather-data-gds",
            object_name=f"weather/{ds}/forecast.csv",
            data=csv_data,
            mime_type="text/csv",
        )

    upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=_upload_to_gcs,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # -------------------------------------------------------------------
    # Task 3: Trigger downstream DAG
    # -------------------------------------------------------------------
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_data_transform_dag",
        trigger_dag_id="transformed_weather_data_to_bq",
        wait_for_completion=False,
    )

    # -------------------------------------------------------------------
    # DAG dependencies
    # -------------------------------------------------------------------
    extract_weather >> upload_to_gcs >> trigger_transform