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
├── weather_data_pipeline/       # OpenWeatherMap → GCS → Dataproc → BigQuery
│   ├── dags/                    # Airflow DAGs and callables
│   ├── spark_job/               # PySpark transformation job
│   └── README.md
└── .github/
    └── workflows/
        └── ci-cd.yaml           # CI/CD: deploy to GCS + Cloud Composer on push to main
```

---

## CI/CD

All projects are deployed via GitHub Actions on push to `main`.

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
