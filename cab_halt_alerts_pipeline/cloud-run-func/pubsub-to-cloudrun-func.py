import base64
import json
import logging

import functions_framework
from google.cloud import bigquery

BQ = bigquery.Client()
DATASET = "fleet_analytics"
TABLE = "cab_idle_events"
TABLE_ID = f"{BQ.project}.{DATASET}.{TABLE}"
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

@functions_framework.cloud_event
def idle_alert(cloud_event):
    """
    Triggered by a Pub/Sub push to 'cab-idle-alerts'.
    Uses a BigQuery MERGE to insert the row only if it does not already exist.
    """
    msg = cloud_event.data.get("message", {})
    raw = msg.get("data")
    if not raw:
        LOGGER.error("No data in Pub/Sub message")
        return {"status": "error", "message": "empty payload"}, 400

    try:
        payload = json.loads(base64.b64decode(raw).decode("utf-8"))
    except Exception as e:
        LOGGER.exception("Bad JSON payload")
        return {"status": "error", "message": str(e)}, 400

    cab_id = payload["cab_id"]
    window_start = payload["window_start"]
    window_end = payload["window_end"]
    distance_m = payload["distance_m"]

    merge_sql = f"""
    MERGE `{TABLE_ID}` T
    USING (
      SELECT
        @cab_id        AS cab_id,
        TIMESTAMP(@ws) AS window_start,
        TIMESTAMP(@we) AS window_end,
        @dist          AS distance_m
    ) S
    ON T.cab_id = S.cab_id
      AND T.window_start = S.window_start
      AND T.window_end   = S.window_end
    WHEN NOT MATCHED THEN
      INSERT (cab_id, window_start, window_end, distance_m)
      VALUES (S.cab_id, S.window_start, S.window_end, S.distance_m)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cab_id", "STRING", cab_id),
            bigquery.ScalarQueryParameter("ws",     "STRING", window_start),
            bigquery.ScalarQueryParameter("we",     "STRING", window_end),
            bigquery.ScalarQueryParameter("dist",   "FLOAT",  distance_m),
        ]
    )
    try:
        query_job = BQ.query(merge_sql, job_config=job_config)
        query_job.result()
    except Exception as e:
        LOGGER.exception("BigQuery MERGE failed")
        return {"status": "error", "message": str(e)}, 500

    alert_msg = (
        f"CAB {cab_id} idle from {window_start} "
        f"to {window_end} ({distance_m:.1f} m)"
    )
    LOGGER.info(alert_msg)
    return {"status": "inserted", "alert": alert_msg}, 200
