from airflow.providers.google.cloud.hooks.gcs import GCSHook

BUCKET   = "weather-data-mid"
CITY     = "Toronto,CA"
ENDPOINT = "https://api.openweathermap.org/data/2.5/forecast"


def extract_openweather(api_key: str) -> str:
    """Fetch 5-day forecast from OpenWeatherMap and return as a CSV string."""
    import requests
    import pandas as pd

    params = {"q": CITY, "appid": api_key}
    resp = requests.get(ENDPOINT, params=params)
    resp.raise_for_status()

    df = pd.json_normalize(resp.json()["list"])
    return df.to_csv(index=False)


def upload_to_gcs(ds: str, **kwargs) -> None:
    """Pull CSV from XCom and upload to GCS under weather/{ds}/forecast.csv."""
    ti = kwargs["ti"]
    csv_data = ti.xcom_pull(task_ids="extract_weather_data")
    hook = GCSHook()
    hook.upload(
        bucket_name=BUCKET,
        object_name=f"weather/{ds}/forecast.csv",
        data=csv_data,
        mime_type="text/csv",
    )
