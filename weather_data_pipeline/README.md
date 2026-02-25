# Weather Data Pipeline

Fetches a 5-day weather forecast for **Toronto, CA** from the OpenWeatherMap API, stores raw data in GCS, transforms it with PySpark on Dataproc Serverless, and loads the result into BigQuery.

---

## Architecture

```
OpenWeatherMap API
       │
       ▼
DAG 1: openweather_api_to_gcs  (Cloud Composer)
  ├── extract_weather_data      → fetch & flatten forecast JSON to CSV
  ├── upload_to_gcs             → gs://weather-data-mid/weather/{date}/forecast.csv
  └── trigger_data_transform_dag
       │
       ▼
DAG 2: transformed_weather_data_to_bq  (Cloud Composer)
  └── spark_job_on_dataproc_serverless
       │
       ▼
Dataproc Serverless (PySpark)
  └── Read CSV → Cast & Rename → Write to BigQuery
       │
       ▼
BigQuery: fluted-cogency-403608.forecast.weather_data
```

---

## Project Structure

```
weather_data_pipeline/
├── dags/
│   ├── extract_data_dag.py      # DAG 1 — extract + upload + trigger
│   ├── transform_data_dag.py    # DAG 2 — submit Dataproc batch
│   ├── openweather_helpers.py   # extract_openweather(), upload_to_gcs()
│   └── dataproc_helpers.py      # get_batch_id(), get_batch_details()
└── spark_job/
    └── weather_data_processing.py  # PySpark: read GCS → transform → write BQ
```

---

## GCP Resources

| Resource | Value |
|---|---|
| Project | `fluted-cogency-403608` |
| Region | `us-central1` |
| GCS Bucket | `weather-data-mid` |
| Composer Environment | `airflow-qa` |
| BigQuery Dataset | `forecast` |
| BigQuery Table | `weather_data` |
| Service Account | `dev-user@fluted-cogency-403608.iam.gserviceaccount.com` |
| Spark Script (GCS) | `gs://weather-data-mid/script/weather_data_processing.py` |
| Raw Data (GCS) | `gs://weather-data-mid/weather/{YYYY-MM-DD}/forecast.csv` |

---

## DAGs

### DAG 1 — `openweather_api_to_gcs`

| Task | Operator | Description |
|---|---|---|
| `extract_weather_data` | `PythonVirtualenvOperator` | Calls `/forecast` API, flattens JSON to CSV via `pd.json_normalize` |
| `upload_to_gcs` | `PythonOperator` | Uploads CSV to `gs://weather-data-mid/weather/{ds}/forecast.csv` |
| `trigger_data_transform_dag` | `TriggerDagRunOperator` | Fires DAG 2 |

### DAG 2 — `transformed_weather_data_to_bq`

| Task | Operator | Description |
|---|---|---|
| `spark_job_on_dataproc_serverless` | `DataprocCreateBatchOperator` | Submits PySpark batch on Dataproc Serverless 2.2 |

---

## Setup

**Airflow Variable required:**
```
Key:   openweather_api_key
Value: <your OpenWeatherMap API key>
```

**Trigger:** Run DAG 1 (`openweather_api_to_gcs`) manually. It automatically triggers DAG 2 on completion.

---

## Deployment

Handled by CI/CD on push to `main`. See [../../.github/workflows/ci-cd.yaml](../../.github/workflows/ci-cd.yaml).
