# GCP Data Engineering Projects

A collection of end-to-end data engineering pipelines built on Google Cloud Platform using Cloud Composer (Airflow), Dataproc Serverless, BigQuery, and GCS.

---

## Projects

| Project | Description |
|---|---|
| [weather_data_pipeline](weather_data_pipeline/) | Fetches OpenWeatherMap forecasts, transforms with PySpark, loads into BigQuery |

---

## Repository Structure

```
GCP-PROJECTS/
├── weather_data_pipeline/                        # OpenWeatherMap → GCS → Dataproc → BigQuery
│   ├── dags/                                     # Airflow DAGs and callables
│   ├── spark_job/                                # PySpark transformation job
│   ├── architecture.png                          # Architecture diagram
│   ├── Weather Data Processing Architecture.drawio
│   ├── WORKFLOW.md                               # End-to-end workflow documentation
│   └── README.md
└── .github/
    └── workflows/
        └── weather-pipeline-ci-cd.yaml           # CI/CD: triggers only on weather_data_pipeline/** changes
```

---

## CI/CD

Each project has its own workflow file under `.github/workflows/` with path-based triggers, so only the relevant project deploys when its files change.

**Required GitHub Secrets:**

| Secret | Purpose |
|---|---|
| `GCP_SA_KEY` | Service account JSON key used for GCP authentication |
| `GCP_PROJECT_ID` | Target GCP project ID |

---

## GCP Stack

| Service | Purpose |
|---|---|
| Cloud Composer (Airflow) | Pipeline orchestration |
| Google Cloud Storage | Raw and staging data storage |
| Dataproc Serverless | Distributed PySpark transformation |
| BigQuery | Data warehouse |
