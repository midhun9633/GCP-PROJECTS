import json
import math
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import SlidingWindows

TelemetryRecord = Dict[str, Any]
IdleAlertRecord = Dict[str, Any]
EARTH_RADIUS_M = 6_371_000
RAW_SCHEMA = "cab_id:STRING,timestamp:TIMESTAMP,lat:FLOAT,lon:FLOAT,speed:FLOAT"
TEMP_GCS_LOCATION = "gs://bq-temp-mid/temp"

class IdleOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--inputTopic",
            help="Pub/Sub topic for raw telemetry, e.g. projects/PROJECT/topics/cab-telemetry"
        )
        parser.add_value_provider_argument(
            "--alertTopic",
            help="Pub/Sub topic for idle alerts, e.g. projects/PROJECT/topics/cab-idle-alerts"
        )
        parser.add_value_provider_argument(
            "--rawTable",
            help="BigQuery table for raw telemetry, e.g. PROJECT:dataset.table"
        )
        parser.add_value_provider_argument("--windowSize", type=int, help="Sliding window size in seconds")
        parser.add_value_provider_argument("--windowPeriod", type=int, help="Sliding window period in seconds")


def parse_message(msg: bytes) -> TelemetryRecord:
    data = json.loads(msg.decode("utf-8"))
    ts = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
    return {
        "cab_id": data["cab_id"],
        "timestamp": ts.isoformat(),
        "lat": data["lat"],
        "lon": data["lon"],
        "speed": data["speed"],
    }


def haversine_meters(p1: TelemetryRecord, p2: TelemetryRecord) -> float:
    lat1, lat2 = math.radians(p1["lat"]), math.radians(p2["lat"])
    dlat = math.radians(p2["lat"] - p1["lat"])
    dlon = math.radians(p2["lon"] - p1["lon"])
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def compute_idle_alerts(kv: Tuple[str, Iterable[TelemetryRecord]]) -> List[IdleAlertRecord]:
    cab_id, recs = kv
    recs = sorted(recs, key=lambda r: r["timestamp"])
    if len(recs) < 2:
        return []

    distance_m = haversine_meters(recs[0], recs[-1])
    if distance_m == 0:
        return [{
            "cab_id": cab_id,
            "window_start": recs[0]["timestamp"],
            "window_end": recs[-1]["timestamp"],
            "distance_m": distance_m,
        }]
    return []


def run() -> None:
    opts = IdleOptions()
    opts.view_as(StandardOptions).streaming = True

    input_topic = opts.inputTopic
    alert_topic = opts.alertTopic
    raw_table = opts.rawTable
    window_size = opts.windowSize
    window_period = opts.windowPeriod

    with beam.Pipeline(options=opts) as p:
        raw = (
            p
            | "ReadPubSub" >> ReadFromPubSub(topic=input_topic.get())
            | "ParseJSON" >> beam.Map(parse_message)
        )

        raw | "WriteRawBQ" >> WriteToBigQuery(
            raw_table.get(),
            schema=RAW_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location=TEMP_GCS_LOCATION,
        )

        (
            raw
            | "KeyByCab" >> beam.Map(lambda r: (r["cab_id"], r))
            | "SlidingWindow" >> beam.WindowInto(
                SlidingWindows(window_size.get(), window_period.get())
            )
            | "Group" >> beam.GroupByKey()
            | "DetectIdle" >> beam.FlatMap(compute_idle_alerts)
            | "ToJSON" >> beam.Map(lambda a: json.dumps(a).encode("utf-8"))
            | "WriteAlerts" >> WriteToPubSub(topic=alert_topic.get())
        )

if __name__ == "__main__":
    run()
