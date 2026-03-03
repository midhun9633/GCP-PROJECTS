# Weather Data Processing Pipeline — End-to-End Workflow

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [GCP Resources](#3-gcp-resources)
4. [OpenWeatherMap API Review](#4-openweathermap-api-review)
5. [DAG 1 — Extract (openweather_api_to_gcs)](#5-dag-1--extract)
6. [DAG 2 — Transform (transformed_weather_data_to_bq)](#6-dag-2--transform)
7. [PySpark Job — weather_data_processing.py](#7-pyspark-job)
8. [CI/CD Pipeline](#8-cicd-pipeline)
9. [Incident Log](#9-incident-log)
10. [Known Risks](#10-known-risks)

---

## 1. Project Overview

This pipeline fetches a 5-day weather forecast for **Toronto, CA** from the OpenWeatherMap API every time it is triggered, stores the raw data in Google Cloud Storage, transforms it using a PySpark job running on **Dataproc Serverless**, and loads the cleaned data into **BigQuery** for analysis.

| Layer | Tool |
|---|---|
| Orchestration | Cloud Composer (Airflow) |
| Raw storage | Google Cloud Storage |
| Transformation | Dataproc Serverless (PySpark) |
| Warehouse | BigQuery |
| CI/CD | GitHub Actions |

---

## 2. Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                        GitHub (main branch)                        │
│                     CI/CD: .github/workflows/ci-cd.yaml            │
└──────────────────────┬─────────────────────┬───────────────────────┘
                       │ gsutil cp            │ gcloud composer import
                       ▼                      ▼
          ┌────────────────────┐   ┌─────────────────────────────┐
          │  GCS               │   │  Cloud Composer (Airflow)    │
          │  weather-data-mid/ │   │  environment: airflow-qa     │
          │  script/           │   │  region: us-central1         │
          └────────────────────┘   └──────────┬──────────────────┘
                                              │
                              ┌───────────────▼───────────────┐
                              │  DAG 1: openweather_api_to_gcs │
                              │                               │
                              │  [extract_weather_data]       │
                              │       │ PythonVirtualenvOp    │
                              │       │ OpenWeatherMap API    │
                              │       ▼                       │
                              │  [upload_to_gcs]             │
                              │       │ PythonOperator        │
                              │       │ → GCS weather/{ds}/  │
                              │       │   forecast.csv        │
                              │       ▼                       │
                              │  [trigger_data_transform_dag] │
                              └───────────────┬───────────────┘
                                              │ TriggerDagRunOperator
                              ┌───────────────▼───────────────────────┐
                              │  DAG 2: transformed_weather_data_to_bq │
                              │                                        │
                              │  [spark_job_on_dataproc_serverless]   │
                              │       │ DataprocCreateBatchOperator    │
                              └───────────────┬────────────────────────┘
                                              │
                              ┌───────────────▼────────────────────────┐
                              │  Dataproc Serverless Batch             │
                              │  Runtime: 2.2 | Region: us-central1   │
                              │                                        │
                              │  Reads:  GCS weather/{today}/          │
                              │          forecast.csv                  │
                              │                                        │
                              │  Transforms: cast, rename columns      │
                              │                                        │
                              │  Writes: BigQuery                      │
                              │  fluted-cogency-403608.forecast        │
                              │          .weather_data                 │
                              └────────────────────────────────────────┘
```

---

## 3. GCP Resources

| Resource | Name / ID |
|---|---|
| Project | `fluted-cogency-403608` |
| GCS bucket | `weather-data-mid` |
| Composer environment | `airflow-qa` (us-central1) |
| Dataproc region | `us-central1` |
| BigQuery dataset | `forecast` |
| BigQuery table | `weather_data` |
| Service account | `dev-user@fluted-cogency-403608.iam.gserviceaccount.com` |
| Spark script path | `gs://weather-data-mid/script/weather_data_processing.py` |
| Raw data path | `gs://weather-data-mid/weather/{YYYY-MM-DD}/forecast.csv` |

---

## 4. OpenWeatherMap API Review

**Endpoint used:**
```
GET https://api.openweathermap.org/data/2.5/forecast
```

**Parameters:**
| Parameter | Value in this project |
|---|---|
| `q` | `Toronto,CA` |
| `appid` | Stored as Airflow Variable `openweather_api_key` |
| `units` | Not set (defaults to Kelvin) |

**Response structure:**

The API returns a `list` array of forecast objects at **3-hour intervals** covering the next **5 days** (40 data points total). Each object is flattened by `pd.json_normalize()` before being written to CSV.

**Fields captured by this pipeline:**

| API field | CSV column | BigQuery column | Type | Notes |
|---|---|---|---|---|
| `dt` | `dt` | `dt` | timestamp | Unix epoch, converted via `from_unixtime` |
| `dt_txt` | `dt_txt` | `forecast_time` | timestamp | Human-readable datetime string |
| `weather` | `weather` | `weather` | string | Array serialized as string |
| `visibility` | `visibility` | `visibility` | int | Max 10,000 metres |
| `pop` | `pop` | `pop` | double | Probability of precipitation, 0–1 |
| `main.temp` | `main.temp` | `temp` | double | Temperature in Kelvin |
| `main.feels_like` | `main.feels_like` | `feels_like` | double | |
| `main.temp_min` | `main.temp_min` | `min_temp` | double | |
| `main.temp_max` | `main.temp_max` | `max_temp` | double | |
| `main.pressure` | `main.pressure` | `pressure` | long | hPa |
| `main.sea_level` | `main.sea_level` | `sea_level` | long | hPa |
| `main.grnd_level` | `main.grnd_level` | `ground_level` | long | hPa |
| `main.humidity` | `main.humidity` | `humidity` | long | % |
| `main.temp_kf` | `main.temp_kf` | `temp_kf` | double | Internal parameter |
| `clouds.all` | `clouds.all` | `clouds_all` | long | Cloudiness % |
| `wind.speed` | `wind.speed` | `wind_speed` | double | m/s |
| `wind.deg` | `wind.deg` | `wind_deg` | long | Direction in degrees |
| `wind.gust` | `wind.gust` | `wind_gust` | double | m/s |
| `sys.pod` | `sys.pod` | `sys_pod` | string | `d` = day, `n` = night |
| `rain.3h` | `rain.3h` | `rain_3h` | double | **Optional** — only present when it is raining |

> **Important:** `rain.3h` is **not always present** in the API response. It only appears in forecast entries where precipitation is expected. This means the column may be absent from the CSV entirely when there is no rain in the 5-day forecast — which will cause the PySpark job to fail at the select step.

---

## 5. DAG 1 — Extract

**File:** [weather_airflow_dags/extract_data_dag.py](weather_airflow_dags/extract_data_dag.py)
**DAG ID:** `openweather_api_to_gcs`
**Schedule:** Manual / triggered
**Retries:** 1 (5-minute delay)

### Tasks

```
extract_weather_data  ──►  upload_to_gcs  ──►  trigger_data_transform_dag
```

#### Task 1: `extract_weather_data` (PythonVirtualenvOperator)

Runs inside an isolated Python virtualenv with `pandas==1.5.3` and `requests==2.31.0` installed, while reusing system-level packages (to avoid numpy ABI conflicts).

- Calls `GET /data/2.5/forecast?q=Toronto,CA&appid=<key>`
- Flattens the nested JSON response using `pd.json_normalize(resp.json()["list"])`
- Returns the result as a CSV string via Airflow XCom

#### Task 2: `upload_to_gcs` (PythonOperator)

- Pulls the CSV string from XCom
- Uploads to `gs://weather-data-mid/weather/{ds}/forecast.csv`
- `{ds}` = Airflow's logical execution date (e.g. `2026-02-19`)

#### Task 3: `trigger_data_transform_dag` (TriggerDagRunOperator)

- Fires DAG 2 (`transformed_weather_data_to_bq`) without waiting for it to complete (`wait_for_completion=False`)

---

## 6. DAG 2 — Transform

**File:** [weather_airflow_dags/transform_data_dag.py](weather_airflow_dags/transform_data_dag.py)
**DAG ID:** `transformed_weather_data_to_bq`
**Schedule:** Manual / triggered by DAG 1
**Retries:** 1 (5-minute delay)

### Tasks

```
spark_job_on_dataproc_serverless
```

#### Task: `spark_job_on_dataproc_serverless` (DataprocCreateBatchOperator)

Submits a Dataproc Serverless batch job with the following config:

| Config | Value |
|---|---|
| Script | `gs://weather-data-mid/script/weather_data_processing.py` |
| Runtime | Dataproc Serverless 2.2 |
| Region | `us-central1` |
| Network | `default` VPC |
| Service account | `dev-user@fluted-cogency-403608.iam.gserviceaccount.com` |
| Batch ID | `weather-data-batch-<uuid[:8]>` (unique per run) |

The operator waits for the batch to complete and raises `AirflowException` if it fails.

---

## 7. PySpark Job

**File:** [spark_job/weather_data_processing.py](spark_job/weather_data_processing.py)
**Deployed to:** `gs://weather-data-mid/script/weather_data_processing.py`

### What it does

**Step 1 — Config**
```
input:  gs://weather-data-mid/weather/{today}/forecast.csv
output: fluted-cogency-403608.forecast.weather_data (BigQuery)
temp:   gs://weather-data-mid/ (used as staging area for BQ write)
```

**Step 2 — Read**
Reads the raw CSV with schema inference, double-quote handling, and comma separator.

**Step 3 — Transform**
- Converts `dt` from Unix epoch integer to a proper timestamp using `from_unixtime`
- Parses `dt_txt` string into a timestamp
- Casts and renames all 20 columns to clean BigQuery-friendly names
- Drops all other flattened API fields not in the select list

**Step 4 — Write to BigQuery**
Uses the Spark BigQuery connector with:
- `writeDisposition: WRITE_APPEND` — new rows are added each run
- `createDisposition: CREATE_IF_NEEDED` — table is created automatically
- Staging area: `gs://weather-data-mid/` (temp files cleaned up post-write)

---

## 8. CI/CD Pipeline

**File:** [.github/workflows/ci-cd.yaml](.github/workflows/ci-cd.yaml)
**Trigger:** Push to `main` branch

### Steps

```
1. Checkout code
2. Authenticate to GCP  (using GCP_SA_KEY secret)
3. Setup gcloud SDK     (using GCP_PROJECT_ID secret)
4. gsutil cp spark_job/weather_data_processing.py → gs://weather-data-mid/script/
5. gcloud composer environments storage dags import → airflow-qa (us-central1)
```

This means any merge to `main` automatically:
- Updates the live PySpark script in GCS
- Deploys both DAGs to the Cloud Composer environment

**Required GitHub Secrets:**
| Secret | Purpose |
|---|---|
| `GCP_SA_KEY` | JSON key for the service account used by CI/CD |
| `GCP_PROJECT_ID` | `fluted-cogency-403608` |

---

## 9. Incident Log

### Run 1 — `weather-data-batch-6ee35eae` (2026-02-19 11:42 UTC)

**Symptom:** Dataproc batch failed after ~2 minutes.

**Root cause from driver logs:**
```
403 Forbidden — The billing account for the owning project is disabled in state absent
GET gs://bq-temp-mid/.spark-bigquery-app-...
```

The `temporaryGcsBucket` was set to `bq-temp-mid`, a bucket in a GCP project with **no active billing account**. The Spark BigQuery connector could not access this bucket for staging, causing the write step to fail.

**Fix:** Changed `temp_bucket` in `weather_data_processing.py` from `bq-temp-mid` to `weather-data-mid` (the same bucket already used for raw data, which is in the active billing project).

---

### Run 2 — `weather-data-batch-26f07b87` (2026-02-19 11:55 UTC)

**Symptom:** Same error as Run 1.

**Root cause:** The fix from Run 1 had not yet been uploaded to GCS. Dataproc was still executing the old script containing `bq-temp-mid`.

**Fix:** Script was uploaded to GCS via:
```bash
gsutil cp spark_job/weather_data_processing.py gs://weather-data-mid/script/weather_data_processing.py
```

The pipeline should succeed on the next run.

---

## 10. Known Risks

### Risk 1 — `rain.3h` column may be absent

**Severity: High**

The OpenWeatherMap API only includes `rain.3h` in forecast entries where precipitation is expected. If no rain is forecast over the 5-day window, the column will be completely absent from the CSV. The PySpark job unconditionally selects it at [spark_job/weather_data_processing.py:59](spark_job/weather_data_processing.py#L59), which will cause a Spark `AnalysisException` at runtime.

**Recommended fix:** Make the column conditional:
```python
from pyspark.sql.functions import lit

rain_col = col("`rain.3h`").cast("double") if "`rain.3h`" in df.columns else lit(None).cast("double")
```

---

### Risk 2 — Date mismatch between DAG 1 and the Spark job

**Severity: Medium**

- DAG 1 uploads to `gs://weather-data-mid/weather/{ds}/forecast.csv` where `{ds}` is Airflow's **logical date**.
- The Spark job reads from `gs://weather-data-mid/weather/{datetime.date.today()}/forecast.csv` using **today's actual date at runtime**.

If DAG 2 is triggered manually on a different day than DAG 1 ran, the Spark job will look for a file that doesn't exist and fail. The correct fix is to pass the date as an argument from the DAG into the Spark job rather than calling `datetime.date.today()` inside the script.

---

### Risk 3 — WRITE_APPEND causes duplicates on retry

**Severity: Low**

If the Spark job partially succeeds and then retries (Airflow retries once on failure), rows may be written twice to BigQuery since `writeDisposition` is `WRITE_APPEND`. Consider adding a deduplication step or switching to `WRITE_TRUNCATE` for daily loads.
