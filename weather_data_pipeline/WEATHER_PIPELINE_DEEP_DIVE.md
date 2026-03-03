# Weather Data Pipeline — Deep Dive (Tech Architect Reference)

> **Audience**: Tech Architects
> **Project**: `fluted-cogency-403608` (GCP)
> **Last Updated**: 2026-02-25

---

## Table of Contents

1. [What This Pipeline Does (30-second pitch)](#1-what-this-pipeline-does)
2. [High-Level Architecture Diagram](#2-high-level-architecture-diagram)
3. [GCP Services Used & Why](#3-gcp-services-used--why)
4. [Repository Structure](#4-repository-structure)
5. [Data Flow — Step by Step](#5-data-flow--step-by-step)
6. [DAG 1 — Extract & Upload](#6-dag-1--extract--upload-openweather_api_to_gcs)
7. [DAG 2 — Transform & Load](#7-dag-2--transform--load-transformed_weather_data_to_bq)
8. [PySpark Transformation Job](#8-pyspark-transformation-job)
9. [Configuration & Secrets Management](#9-configuration--secrets-management)
10. [CI/CD Pipeline](#10-cicd-pipeline)
11. [Error Handling & Retry Logic](#11-error-handling--retry-logic)
12. [Scheduling & Triggers](#12-scheduling--triggers)
13. [BigQuery Schema (Final Output)](#13-bigquery-schema-final-output)
14. [Known Risks & Gaps](#14-known-risks--gaps)
15. [Incident Log](#15-incident-log)
16. [Key Design Decisions](#16-key-design-decisions)
17. [Quick Reference Card](#17-quick-reference-card)

---

## 1. What This Pipeline Does

This is an **ELT (Extract → Load → Transform)** pipeline that:

1. **Fetches** a 5-day weather forecast for **Toronto, CA** from the [OpenWeatherMap API](https://openweathermap.org/api/hourly-forecast)
2. **Stores** the raw JSON-flattened data as a CSV in Google Cloud Storage (GCS)
3. **Transforms** the CSV using PySpark on Dataproc Serverless (type casting, column renaming, timestamp parsing)
4. **Loads** the clean data into a BigQuery table for analytics

> The pipeline produces **40 forecast data points** (one per 3-hour interval over 5 days) per run.

---

## 2. High-Level Architecture Diagram

```
                     ┌─────────────────────────────────┐
                     │   Developer pushes to `main`    │
                     └────────────┬────────────────────┘
                                  │ GitHub Actions CI/CD
                                  │ (deploys code only, does NOT run pipeline)
                     ┌────────────▼────────────────────┐
                     │      Google Cloud Storage       │
                     │  gs://weather-data-mid/script/  │  ← PySpark script lives here
                     │  weather_data_processing.py     │
                     └─────────────────────────────────┘

                     ┌─────────────────────────────────┐
                     │   Manual Trigger (Airflow UI)   │
                     └────────────┬────────────────────┘
                                  │
                     ┌────────────▼────────────────────┐
                     │      Cloud Composer             │
                     │  (Managed Apache Airflow)       │
                     │                                 │
                     │  DAG 1: openweather_api_to_gcs  │
                     │  ┌───────────────────────────┐  │
                     │  │  Task 1: extract_weather  │  │  ← Calls OpenWeatherMap API
                     │  │  (PythonVirtualenvOperator│  │    Returns CSV via XCom
                     │  └──────────────┬────────────┘  │
                     │                 │               │
                     │  ┌──────────────▼────────────┐  │
                     │  │  Task 2: upload_to_gcs    │  │  ← Writes CSV to GCS
                     │  │  (PythonOperator)         │  │
                     │  └──────────────┬────────────┘  │
                     │                 │               │
                     │  ┌──────────────▼────────────┐  │
                     │  │  Task 3: trigger_dag_2    │  │  ← Fires DAG 2 (non-blocking)
                     │  │  (TriggerDagRunOperator)  │  │
                     │  └───────────────────────────┘  │
                     └─────────────────────────────────┘
                                  │
          ┌───────────────────────▼──────────────────────────┐
          │            OpenWeatherMap API                    │
          │  GET /data/2.5/forecast?q=Toronto,CA&appid=...   │
          │  Returns 40 forecast objects (3-hr intervals)    │
          └───────────────────────┬──────────────────────────┘
                                  │
          ┌───────────────────────▼──────────────────────────┐
          │            Google Cloud Storage                  │
          │  gs://weather-data-mid/weather/{date}/forecast.csv│
          └───────────────────────┬──────────────────────────┘
                                  │
          ┌───────────────────────▼──────────────────────────┐
          │      Cloud Composer (DAG 2 triggered)            │
          │  transformed_weather_data_to_bq                  │
          │  ┌────────────────────────────────────────────┐  │
          │  │  Task: spark_job_on_dataproc_serverless    │  │
          │  │  (DataprocCreateBatchOperator)             │  │
          │  └────────────────────┬───────────────────────┘  │
          └───────────────────────┼──────────────────────────┘
                                  │
          ┌───────────────────────▼──────────────────────────┐
          │          Dataproc Serverless (PySpark)           │
          │  - Reads CSV from GCS                            │
          │  - Casts types, renames columns                  │
          │  - Parses timestamps                             │
          │  - Writes to BigQuery via Spark connector        │
          └───────────────────────┬──────────────────────────┘
                                  │
          ┌───────────────────────▼──────────────────────────┐
          │                  BigQuery                        │
          │  fluted-cogency-403608.forecast.weather_data     │
          │  (20 columns, WRITE_APPEND mode)                 │
          └──────────────────────────────────────────────────┘
```

---

## 3. GCP Services Used & Why

| Service | Role | Config |
|---------|------|--------|
| **Cloud Composer** | Orchestrates both DAGs | Env: `airflow-qa`, Region: `us-central1` |
| **Google Cloud Storage** | Stores raw CSV + PySpark script | Bucket: `weather-data-mid` |
| **Dataproc Serverless** | Runs PySpark batch job | Runtime: 2.2, Region: `us-central1` |
| **BigQuery** | Data warehouse / final destination | Dataset: `forecast`, Table: `weather_data` |
| **IAM Service Account** | Auth for all GCP calls | `dev-user@fluted-cogency-403608.iam.gserviceaccount.com` |
| **GitHub Actions** | CI/CD deployment only | Triggered on push to `main` |

### Why These Choices?

- **Cloud Composer over Cloud Workflows/Scheduler**: Full Airflow semantics, retry logic, XCom, operator ecosystem
- **Dataproc Serverless over Dataflow**: No cluster management, PySpark familiarity, simpler setup
- **BigQuery over Cloud SQL**: Columnar, serverless, perfect for time-series forecast analytics
- **GCS as staging**: Decouples extract from transform — if transform fails, raw data is preserved

---

## 4. Repository Structure

```
GCP-PROJECTS/
├── README.md                              ← Top-level project overview
├── WORKFLOW.md                            ← Detailed incident log + architecture notes
├── WEATHER_PIPELINE_DEEP_DIVE.md          ← This document
├── .github/
│   └── workflows/
│       └── ci-cd.yaml                    ← GitHub Actions CI/CD pipeline
└── weather_data_pipeline/
    ├── README.md                          ← Pipeline-specific docs
    ├── dags/
    │   ├── extract_data_dag.py            ← DAG 1 definition
    │   ├── openweather_helpers.py         ← API fetch + GCS upload functions
    │   ├── transform_data_dag.py          ← DAG 2 definition
    │   └── dataproc_helpers.py            ← Dataproc batch config builder
    └── spark_job/
        └── weather_data_processing.py     ← PySpark ETL job
```

---

## 5. Data Flow — Step by Step

```
Step 1: Manual trigger of DAG 1 in Cloud Composer Airflow UI
         │
Step 2: extract_weather_data task runs in an isolated Python virtualenv
         │  Installs: pandas==1.5.3, requests==2.31.0
         │  Calls: GET https://api.openweathermap.org/data/2.5/forecast?q=Toronto,CA&appid=<key>
         │  Receives: JSON with 40 forecast objects for next 5 days
         │  Flattens: pd.json_normalize(response["list"]) → CSV string
         │  Pushes: CSV string to Airflow XCom
         │
Step 3: upload_to_gcs task runs
         │  Pulls: CSV string from XCom
         │  Writes: gs://weather-data-mid/weather/2026-02-25/forecast.csv
         │  MIME: text/csv
         │
Step 4: trigger_data_transform_dag task fires
         │  Triggers: DAG 2 (transformed_weather_data_to_bq)
         │  Mode: Non-blocking (DAG 1 marks complete without waiting for DAG 2)
         │
Step 5: DAG 2 — spark_job_on_dataproc_serverless task
         │  Submits batch to Dataproc Serverless
         │  Batch ID: weather-data-batch-<uuid[:8]> (unique per run)
         │  Waits for batch completion (synchronous)
         │
Step 6: Dataproc Serverless executes PySpark job
         │  Reads: gs://weather-data-mid/weather/2026-02-25/forecast.csv
         │  Transforms: 20 columns selected, typed, renamed
         │  Stages: temp data in gs://weather-data-mid/ (Spark BQ connector requirement)
         │
Step 7: Data written to BigQuery
         Table: fluted-cogency-403608.forecast.weather_data
         Mode: WRITE_APPEND
         Rows: ~40 new rows per pipeline run
```

---

## 6. DAG 1 — Extract & Upload (`openweather_api_to_gcs`)

**File**: [weather_data_pipeline/dags/extract_data_dag.py](weather_data_pipeline/dags/extract_data_dag.py)
**Helpers**: [weather_data_pipeline/dags/openweather_helpers.py](weather_data_pipeline/dags/openweather_helpers.py)

### DAG Properties

```python
dag_id          = "openweather_api_to_gcs"
schedule_interval = None          # Manual trigger only
catchup           = False
retries           = 1
retry_delay       = timedelta(minutes=5)
```

### Task 1 — `extract_weather_data`

**Operator**: `PythonVirtualenvOperator`

**Why a virtualenv operator?**
Cloud Composer's base Airflow environment may not have `pandas` or `requests` pinned to the correct versions. `PythonVirtualenvOperator` creates an **isolated Python subprocess** with the exact dependencies specified, avoiding conflicts with the Airflow environment.

```python
PythonVirtualenvOperator(
    task_id="extract_weather_data",
    python_callable=extract_openweather,       # from openweather_helpers.py
    requirements=["pandas==1.5.3", "requests==2.31.0"],
    system_site_packages=True,
    op_kwargs={"api_key": Variable.get("openweather_api_key")},
)
```

**What `extract_openweather()` does:**
```python
def extract_openweather(api_key: str) -> str:
    params = {"q": "Toronto,CA", "appid": api_key}
    resp = requests.get("https://api.openweathermap.org/data/2.5/forecast", params=params)
    resp.raise_for_status()                         # Fails fast on API errors
    df = pd.json_normalize(resp.json()["list"])     # Flattens nested JSON to flat table
    return df.to_csv(index=False)                   # Returns CSV string
```

**API Response Structure (OpenWeatherMap `/data/2.5/forecast`):**
- Returns `{"list": [...40 items...]}`
- Each item is a 3-hour interval forecast with nested fields:
  - `main.temp`, `main.feels_like`, `main.humidity`, etc.
  - `wind.speed`, `wind.deg`, `wind.gust`
  - `clouds.all`
  - `weather` (array of condition objects)
  - `rain.3h` (optional — only present if rain forecasted)
  - `dt` (Unix timestamp), `dt_txt` (human-readable string)
- `pd.json_normalize()` flattens all nested keys with dot notation

**XCom**: The CSV string is **returned** from the function and automatically stored in Airflow XCom by `PythonVirtualenvOperator`.

---

### Task 2 — `upload_to_gcs`

**Operator**: `PythonOperator`

```python
PythonOperator(
    task_id="upload_to_gcs",
    python_callable=upload_to_gcs,
    op_kwargs={"ds": "{{ ds }}"},      # Airflow templated date string (YYYY-MM-DD)
)
```

**What `upload_to_gcs()` does:**
```python
def upload_to_gcs(ds: str, **kwargs) -> None:
    ti = kwargs["ti"]
    csv_data = ti.xcom_pull(task_ids="extract_weather_data")   # Gets CSV from XCom
    hook = GCSHook()                                            # Uses Airflow GCP connection
    hook.upload(
        bucket_name="weather-data-mid",
        object_name=f"weather/{ds}/forecast.csv",              # Date-partitioned path
        data=csv_data,
        mime_type="text/csv",
    )
```

**Storage path pattern**: `gs://weather-data-mid/weather/YYYY-MM-DD/forecast.csv`
This is **date-partitioned** using Airflow's `{{ ds }}` template (logical execution date).

---

### Task 3 — `trigger_data_transform_dag`

**Operator**: `TriggerDagRunOperator`

```python
TriggerDagRunOperator(
    task_id="trigger_data_transform_dag",
    trigger_dag_id="transformed_weather_data_to_bq",
    wait_for_completion=False,       # Non-blocking: DAG 1 completes immediately
)
```

**Why non-blocking?**
DAG 1 is only responsible for extraction. It delegates transformation to DAG 2 without waiting. This decouples the two phases and allows DAG 2 to fail/retry independently.

### Task Dependency Chain (DAG 1)

```
extract_weather_data  ──►  upload_to_gcs  ──►  trigger_data_transform_dag
```

---

## 7. DAG 2 — Transform & Load (`transformed_weather_data_to_bq`)

**File**: [weather_data_pipeline/dags/transform_data_dag.py](weather_data_pipeline/dags/transform_data_dag.py)
**Helpers**: [weather_data_pipeline/dags/dataproc_helpers.py](weather_data_pipeline/dags/dataproc_helpers.py)

### DAG Properties

```python
dag_id          = "transformed_weather_data_to_bq"
schedule_interval = None           # Triggered by DAG 1 only
catchup           = False
start_date        = datetime(2025, 5, 15)
retries           = 1
retry_delay       = timedelta(minutes=5)
```

### Task — `spark_job_on_dataproc_serverless`

**Operator**: `DataprocCreateBatchOperator`

This operator submits a **Dataproc Serverless batch** (PySpark job) and waits for it to complete.

```python
DataprocCreateBatchOperator(
    task_id="spark_job_on_dataproc_serverless",
    batch=get_batch_details(),       # dict describing the Spark job
    batch_id=get_batch_id(),         # unique ID for this batch run
    project_id="fluted-cogency-403608",
    region="us-central1",
    gcp_conn_id="google_cloud_default",
)
```

### Batch ID Generation

```python
def get_batch_id() -> str:
    return f"weather-data-batch-{str(uuid.uuid4())[:8]}"
    # Example: weather-data-batch-a3f1c9e2
```

Each run gets a **unique batch ID** to avoid collisions and allow per-run tracking in the Dataproc UI.

### Batch Configuration

```python
def get_batch_details() -> dict:
    return {
        "pyspark_batch": {
            "main_python_file_uri": "gs://weather-data-mid/script/weather_data_processing.py",
            "python_file_uris": [],
            "jar_file_uris": [],
            "args": [],                # No runtime args — config is hardcoded in script
        },
        "runtime_config": {
            "version": "2.2",          # Dataproc Serverless runtime version
        },
        "environment_config": {
            "execution_config": {
                "service_account": "dev-user@fluted-cogency-403608.iam.gserviceaccount.com",
                "network_uri": "projects/fluted-cogency-403608/global/networks/default",
                "subnetwork_uri": "projects/fluted-cogency-403608/regions/us-central1/subnetworks/default",
            }
        },
    }
```

**Key points:**
- **Dataproc Serverless 2.2**: No cluster to manage. Google handles compute provisioning.
- **Service account**: The Spark job runs under `dev-user@...` which needs BigQuery write + GCS read/write permissions.
- **Network**: Uses the default VPC. Private Google Access must be enabled on the subnet for Dataproc Serverless to reach GCP APIs.
- **No args passed**: The PySpark script uses `datetime.date.today()` internally — this is a known risk (see Section 14).

---

## 8. PySpark Transformation Job

**File**: [weather_data_pipeline/spark_job/weather_data_processing.py](weather_data_pipeline/spark_job/weather_data_processing.py)
**Deployed to**: `gs://weather-data-mid/script/weather_data_processing.py`

### Job Config (Hardcoded)

```python
project     = "fluted-cogency-403608"
dataset     = "forecast"
table       = "weather_data"
temp_bucket = "weather-data-mid"
bucket      = "weather-data-mid"
today       = datetime.date.today().strftime("%Y-%m-%d")     # ← Dynamic, computed at runtime
input_path  = f"gs://{bucket}/weather/{today}/forecast.csv"
```

### Step 1 — Create SparkSession

```python
spark = SparkSession.builder.appName("WeatherDataProcessing").getOrCreate()
```

Dataproc Serverless automatically configures the Spark BigQuery connector. No additional JARs needed at runtime.

### Step 2 — Read CSV from GCS

```python
df = (
    spark.read
         .option("header", True)
         .option("quote", '"')
         .option("sep", ",")
         .option("inferSchema", True)    # Spark guesses column types automatically
         .csv(input_path)
)
```

`inferSchema=True` does a 2-pass read (first pass infers types, second pass reads data). For large files this is expensive, but for 40-row CSVs it's negligible.

### Step 3 — Transform: Cast & Rename (20 Columns)

```python
df2 = (
    df
    # Convert Unix epoch to Spark timestamp
    .withColumn("dt", from_unixtime(col("dt")).cast("timestamp"))

    # Parse human-readable datetime string
    .withColumn("dt_txt", to_timestamp(col("dt_txt"), "yyyy-MM-dd HH:mm:ss"))

    .select(
        col("dt").alias("dt"),
        col("dt_txt").alias("forecast_time"),
        col("weather").alias("weather"),                         # STRING (JSON array as string)
        col("visibility").cast("int").alias("visibility"),
        col("pop").cast("double").alias("pop"),                  # Probability of precipitation
        col("`main.temp`").cast("double").alias("temp"),         # Note backtick escaping for dots!
        col("`main.feels_like`").cast("double").alias("feels_like"),
        col("`main.temp_min`").cast("double").alias("min_temp"),
        col("`main.temp_max`").cast("double").alias("max_temp"),
        col("`main.pressure`").cast("long").alias("pressure"),
        col("`main.sea_level`").cast("long").alias("sea_level"),
        col("`main.grnd_level`").cast("long").alias("ground_level"),
        col("`main.humidity`").cast("long").alias("humidity"),
        col("`main.temp_kf`").cast("double").alias("temp_kf"),
        col("`clouds.all`").cast("long").alias("clouds_all"),
        col("`wind.speed`").cast("double").alias("wind_speed"),
        col("`wind.deg`").cast("long").alias("wind_deg"),
        col("`wind.gust`").cast("double").alias("wind_gust"),
        col("`sys.pod`").alias("sys_pod"),                       # 'd' or 'n' (day/night)
        col("`rain.3h`").cast("double").alias("rain_3h"),        # ← OPTIONAL FIELD (RISK!)
    )
)
```

**Backtick notation** (`\`main.temp\``): PySpark requires backticks to reference column names that contain dots. `pd.json_normalize` produces these dotted names from nested JSON.

### Step 4 — Write to BigQuery

```python
(
    df2.write
       .format("bigquery")
       .option("table", "fluted-cogency-403608.forecast.weather_data")
       .option("temporaryGcsBucket", "weather-data-mid")         # Staging area for BQ connector
       .option("createDisposition", "CREATE_IF_NEEDED")          # Auto-create table
       .option("writeDisposition", "WRITE_APPEND")               # Append rows each run
       .save()
)
```

**How the Spark BigQuery connector works:**
1. Spark writes the transformed data to a temp location in GCS (`gs://weather-data-mid/`)
2. BigQuery's load job picks up from GCS and appends to the table
3. Temp files are cleaned up automatically

**`WRITE_APPEND`**: Each pipeline run **adds ~40 new rows**. Rows accumulate over time (good for historical trend analysis). Downside: retries cause duplicates (see Known Risks).

---

## 9. Configuration & Secrets Management

### Where Config Lives

| Config Item | Where Stored | How Accessed |
|------------|-------------|-------------|
| OpenWeatherMap API Key | Airflow Variable (`openweather_api_key`) | `Variable.get("openweather_api_key")` |
| GCP Project ID | Hardcoded in Python files | Direct string `"fluted-cogency-403608"` |
| GCS Bucket Name | Hardcoded in Python files | Direct string `"weather-data-mid"` |
| Service Account | Hardcoded in `dataproc_helpers.py` | Direct string |
| GCP Auth (CI/CD) | GitHub Secret `GCP_SA_KEY` | Service account JSON key |
| GCP Project (CI/CD) | GitHub Secret `GCP_PROJECT_ID` | Used by `setup-gcloud` action |

### Airflow Variable Setup

The API key must be manually set in Cloud Composer before the pipeline runs:

1. Open Airflow UI in Cloud Composer (`airflow-qa`)
2. Go to `Admin → Variables`
3. Create: Key = `openweather_api_key`, Value = `<your API key>`

### Important Note on Hardcoding

Most configuration is hardcoded in Python files rather than loaded from environment variables or a config file. This is a simplicity trade-off but means changing the project/bucket/region requires code changes and a redeploy.

---

## 10. CI/CD Pipeline

**File**: [.github/workflows/ci-cd.yaml](.github/workflows/ci-cd.yaml)

### Trigger

```yaml
on:
  push:
    branches:
      - main
```

Runs automatically on every push/merge to the `main` branch.

### Steps

```
1. Checkout Code
   └── actions/checkout@v3

2. Authenticate to GCP
   └── google-github-actions/auth@v1
       └── Uses: secrets.GCP_SA_KEY (service account JSON)

3. Setup Google Cloud SDK
   └── google-github-actions/setup-gcloud@v1
       └── Uses: secrets.GCP_PROJECT_ID

4. Upload Spark Job to GCS
   └── gsutil cp weather_data_pipeline/spark_job/weather_data_processing.py
               gs://weather-data-mid/script/

5. Upload Airflow DAGs to Composer
   └── gcloud composer environments storage dags import
         --environment airflow-qa
         --location us-central1
         --source weather_data_pipeline/dags
```

### What CI/CD Does vs. Does Not Do

| Action | CI/CD Does This? |
|--------|----------------|
| Deploy PySpark script to GCS | YES |
| Deploy DAGs to Cloud Composer | YES |
| Trigger pipeline execution | NO |
| Run unit tests | NO |
| Create GCP infrastructure | NO (manual/pre-provisioned) |
| Validate DAG syntax | NO |

**No Terraform or IaC**: All GCP resources (Composer environment, GCS bucket, BigQuery dataset) are manually provisioned. Only code deployment is automated.

---

## 11. Error Handling & Retry Logic

### Retry Configuration (Both DAGs)

```python
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
```

This means: if any task fails, Airflow waits 5 minutes and tries **once more**. Maximum 2 total attempts per task.

### Per-Task Error Behavior

| Task | Failure Cause | Airflow Behavior |
|------|-------------|-----------------|
| `extract_weather_data` | API error (`resp.raise_for_status()`) | Task marked FAILED, retries after 5 min |
| `extract_weather_data` | Network timeout | Task marked FAILED, retries after 5 min |
| `upload_to_gcs` | GCS permission error | Task marked FAILED, retries after 5 min |
| `trigger_data_transform_dag` | DAG 2 not found | Task marked FAILED, retries after 5 min |
| `spark_job_on_dataproc_serverless` | Spark job fails | Task marked FAILED, retries after 5 min |

### What Happens When All Retries Fail

- DAG run marked as `FAILED`
- Airflow sends alert (if email/Slack configured — not currently configured in this pipeline)
- Raw CSV in GCS may or may not exist depending on which task failed
- No automatic cleanup of partial data

### What is NOT Handled

- **No idempotency**: Retries can produce duplicate rows in BigQuery
- **No dead-letter queue**: Failed API responses are not captured
- **No data quality checks**: No validation that the CSV has the expected number of rows
- **No alerting**: No email/Slack notification on failure

---

## 12. Scheduling & Triggers

### Current Setup: Manual Only

```
┌─────────────────────┐
│  schedule_interval  │    Both DAGs: None
│  = None             │    → No automatic scheduling
└─────────────────────┘
```

### Trigger Chain

```
User (Manual)
    │
    │  Triggers via Airflow UI
    ▼
DAG 1: openweather_api_to_gcs
    │
    │  Task 3: TriggerDagRunOperator (wait_for_completion=False)
    ▼
DAG 2: transformed_weather_data_to_bq
    │
    │  Submits Dataproc batch, waits for completion
    ▼
BigQuery write complete
```

### How to Trigger Manually

1. Open Cloud Composer `airflow-qa` environment in GCP Console
2. Open Airflow UI (link in Console)
3. Find `openweather_api_to_gcs` in DAGs list
4. Click the ▶ (Play) button → "Trigger DAG"
5. DAG 1 runs → auto-triggers DAG 2

### To Add Automatic Scheduling (Future)

Change `schedule_interval` in DAG 1:

```python
# Run daily at 6 AM UTC
schedule_interval="0 6 * * *"
```

---

## 13. BigQuery Schema (Final Output)

**Table**: `fluted-cogency-403608.forecast.weather_data`

| Column | Type | Source Field | Notes |
|--------|------|-------------|-------|
| `dt` | TIMESTAMP | `dt` (Unix epoch) | Converted via `from_unixtime()` |
| `forecast_time` | TIMESTAMP | `dt_txt` | Parsed from `"yyyy-MM-dd HH:mm:ss"` |
| `weather` | STRING | `weather` | JSON array string (e.g., `[{"id":800,...}]`) |
| `visibility` | INTEGER | `visibility` | Meters (max 10,000) |
| `pop` | FLOAT64 | `pop` | Probability of precipitation (0.0 – 1.0) |
| `temp` | FLOAT64 | `main.temp` | Kelvin (raw from API) |
| `feels_like` | FLOAT64 | `main.feels_like` | Kelvin |
| `min_temp` | FLOAT64 | `main.temp_min` | Kelvin |
| `max_temp` | FLOAT64 | `main.temp_max` | Kelvin |
| `pressure` | INTEGER | `main.pressure` | hPa |
| `sea_level` | INTEGER | `main.sea_level` | hPa |
| `ground_level` | INTEGER | `main.grnd_level` | hPa |
| `humidity` | INTEGER | `main.humidity` | Percentage (0–100) |
| `temp_kf` | FLOAT64 | `main.temp_kf` | Internal API parameter |
| `clouds_all` | INTEGER | `clouds.all` | Cloud coverage % |
| `wind_speed` | FLOAT64 | `wind.speed` | m/s |
| `wind_deg` | INTEGER | `wind.deg` | Meteorological degrees |
| `wind_gust` | FLOAT64 | `wind.gust` | m/s |
| `sys_pod` | STRING | `sys.pod` | `"d"` = day, `"n"` = night |
| `rain_3h` | FLOAT64 | `rain.3h` | mm of rain in last 3 hours (nullable) |

> **Note**: Temperatures are in **Kelvin** as returned by OpenWeatherMap. No conversion is applied in this pipeline. Subtract 273.15 for Celsius.

---

## 14. Known Risks & Gaps

### Risk 1 — `rain.3h` Missing Column (HIGH)

**What happens**: When no rain is forecast in the 5-day window, the OpenWeatherMap API **omits** the `rain` key entirely from the JSON. After `pd.json_normalize()`, the `rain.3h` column does not exist in the CSV.

**Failure mode**: PySpark throws `AnalysisException: Column 'rain.3h' not found` and the Spark job fails.

**Current mitigation**: None.

**Recommended fix**:
```python
from pyspark.sql.functions import lit

# Conditionally add rain_3h if column is absent
if "`rain.3h`" not in df.columns:
    df = df.withColumn("rain_3h", lit(None).cast("double"))
else:
    df = df.withColumn("rain_3h", col("`rain.3h`").cast("double"))
```

---

### Risk 2 — Date Mismatch Between DAG 1 and PySpark Job (MEDIUM)

**What happens**: The PySpark job uses `datetime.date.today()` at **Spark job runtime** to compute the input GCS path. If DAG 2 runs on a different date than DAG 1 (e.g., past midnight), it looks for a file that doesn't exist in GCS.

**Failure mode**: Spark throws `FileNotFoundException` (no CSV for today's date).

**Current mitigation**: None.

**Recommended fix**: Pass the execution date as a Spark argument from the Airflow DAG:
```python
# In dataproc_helpers.py
"args": ["--date", "{{ ds }}"],

# In weather_data_processing.py
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--date")
args = parser.parse_args()
today = args.date  # Use passed date instead of datetime.date.today()
```

---

### Risk 3 — Duplicate Rows on Retry (LOW)

**What happens**: If the Spark job writes to BigQuery and then fails (e.g., network blip), Airflow retries it. The second attempt appends rows again, creating duplicates.

**Current mitigation**: None.

**Recommended fix**: Use `WRITE_TRUNCATE` for daily loads (since we always want today's forecast, not accumulation):
```python
.option("writeDisposition", "WRITE_TRUNCATE")
```
Or implement deduplication in BigQuery using `ROW_NUMBER()` partitioned by `dt`.

---

### Risk 4 — No Infrastructure as Code

**Issue**: GCS bucket, Cloud Composer environment, BigQuery dataset are manually created. If the GCP project needs to be recreated, there is no automated way to reproduce the infrastructure.

**Recommended fix**: Add Terraform configs for all GCP resources.

---

### Risk 5 — Hardcoded Configuration

**Issue**: Project ID, bucket name, service account, etc. are hardcoded in Python files. Different environments (dev/staging/prod) cannot be targeted without code changes.

**Recommended fix**: Use Airflow Variables or environment-specific config files.

---

## 15. Incident Log

### Incident 1 — Billing Account Error (2026-02-19 11:42 UTC)

| Field | Value |
|-------|-------|
| Batch ID | `weather-data-batch-6ee35eae` |
| Error | `403 Forbidden — billing account is disabled` |
| Root Cause | `temporaryGcsBucket` was set to `bq-temp-mid` — a separate project with no active billing |
| Impact | Spark job failed, no data written to BigQuery |
| Fix Applied | Changed `temp_bucket` to `weather-data-mid` in `weather_data_processing.py` |
| Status | Resolved |

### Incident 2 — Stale Script in GCS (2026-02-19 11:55 UTC)

| Field | Value |
|-------|-------|
| Batch ID | `weather-data-batch-26f07b87` |
| Error | Same `403 Forbidden` billing error |
| Root Cause | Code was fixed locally but the old script was still in GCS. Dataproc runs from GCS, not local |
| Impact | Same as Incident 1 |
| Fix Applied | Manually uploaded updated script: `gsutil cp ... gs://weather-data-mid/script/` |
| Status | Resolved — CI/CD now handles script deployment automatically |

**Lesson**: Always deploy code changes via CI/CD. Manual GCS uploads are error-prone and temporary.

---

## 16. Key Design Decisions

### Decision 1: Two-DAG Architecture

**Why two DAGs instead of one?**

- Separation of concerns: Extract (DAG 1) vs. Transform (DAG 2)
- Independent failure domains: If transformation fails, extraction doesn't need to re-run
- DAG 2 can theoretically be triggered independently if the CSV already exists

**Trade-off**: Date synchronization between DAGs becomes a manual concern (Risk 2).

---

### Decision 2: PythonVirtualenvOperator for Extraction

**Why not a simple PythonOperator?**

Cloud Composer's managed Airflow environment has pinned dependency versions. Using `PythonVirtualenvOperator` creates an isolated subprocess with exactly `pandas==1.5.3` and `requests==2.31.0`, avoiding version conflicts.

**Trade-off**: Slower task startup (virtualenv creation time per run).

---

### Decision 3: XCom for Data Passing

**Why pass CSV via XCom?**

Simple and Airflow-native. The CSV is ~40 rows and stays small, making XCom appropriate.

**Trade-off**: XCom has a size limit (~48KB in some backends). If the API response grew significantly, this would break. A more robust approach would write directly to GCS in Task 1 and skip XCom entirely.

---

### Decision 4: Dataproc Serverless (Batch Mode)

**Why not Dataflow (Apache Beam)?**

- Team familiarity with PySpark
- No streaming requirement (this is a batch job)
- Simpler job structure
- Lower operational overhead (no cluster management)

**Trade-off**: Cold start latency for Dataproc Serverless (first batch of the day may take 1–2 minutes longer).

---

### Decision 5: WRITE_APPEND to BigQuery

**Why WRITE_APPEND?**

Preserves historical data — every pipeline run adds today's forecast, building a historical record over time.

**Trade-off**: Retries cause duplicate rows. No deduplication logic exists today.

---

## 17. Quick Reference Card

```
┌─────────────────────────────────────────────────────┐
│            WEATHER DATA PIPELINE — QUICK REF        │
├─────────────────────────┬───────────────────────────┤
│ GCP Project             │ fluted-cogency-403608      │
│ Region                  │ us-central1                │
│ GCS Bucket              │ weather-data-mid           │
│ Composer Environment    │ airflow-qa                 │
│ BigQuery Table          │ forecast.weather_data      │
│ Service Account         │ dev-user@fluted-cogency... │
├─────────────────────────┼───────────────────────────┤
│ Data Source             │ OpenWeatherMap /forecast   │
│ City                    │ Toronto, CA                │
│ Forecast Window         │ 5 days, 40 data points     │
│ Interval                │ 3 hours                    │
├─────────────────────────┼───────────────────────────┤
│ DAG 1                   │ openweather_api_to_gcs     │
│ DAG 2                   │ transformed_weather_data   │
│                         │ _to_bq                     │
│ Spark Runtime           │ Dataproc Serverless 2.2    │
│ Trigger                 │ Manual (Airflow UI)        │
│ Schedule                │ None (manual only)         │
├─────────────────────────┼───────────────────────────┤
│ Raw CSV Path            │ gs://weather-data-mid/     │
│                         │ weather/{date}/forecast.csv│
│ Spark Script Path       │ gs://weather-data-mid/     │
│                         │ script/weather_data_       │
│                         │ processing.py              │
├─────────────────────────┼───────────────────────────┤
│ Retries                 │ 1 (5-minute delay)         │
│ Write Mode              │ WRITE_APPEND               │
│ BigQuery Columns        │ 20                         │
├─────────────────────────┼───────────────────────────┤
│ HIGH Risk               │ rain.3h missing column     │
│ MEDIUM Risk             │ Date mismatch (DAG1/DAG2)  │
│ LOW Risk                │ Duplicate rows on retry    │
├─────────────────────────┼───────────────────────────┤
│ CI/CD Trigger           │ Push to `main` branch      │
│ CI/CD Deploys           │ Spark script + DAG files   │
│ CI/CD Does NOT Deploy   │ GCP infrastructure         │
└─────────────────────────┴───────────────────────────┘
```

---

*Generated from codebase analysis. Cross-reference with [WORKFLOW.md](WORKFLOW.md) for additional context.*
