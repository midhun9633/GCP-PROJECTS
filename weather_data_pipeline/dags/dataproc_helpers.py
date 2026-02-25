import uuid

PROJECT_ID      = "fluted-cogency-403608"
REGION          = "us-central1"
BUCKET          = "weather-data-mid"
SERVICE_ACCOUNT = "dev-user@fluted-cogency-403608.iam.gserviceaccount.com"


def get_batch_id() -> str:
    return f"weather-data-batch-{str(uuid.uuid4())[:8]}"


def get_batch_details() -> dict:
    return {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{BUCKET}/script/weather_data_processing.py",
            "python_file_uris": [],
            "jar_file_uris": [],
            "args": [],
        },
        "runtime_config": {
            "version": "2.2",
        },
        "environment_config": {
            "execution_config": {
                "service_account": SERVICE_ACCOUNT,
                "network_uri": f"projects/{PROJECT_ID}/global/networks/default",
                "subnetwork_uri": f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default",
            }
        },
    }
