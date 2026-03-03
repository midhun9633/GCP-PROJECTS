# Cab Halt Alerts Pipeline — Plain-English Deep Dive

> **Goal:** Detect when a cab has been sitting still (halted/idle) and raise an alert — in real time.

---

## Table of Contents

1. [Big Picture — What Does This Pipeline Do?](#1-big-picture)
2. [All the Moving Parts](#2-all-the-moving-parts)
3. [Step-by-Step Data Flow with a Real Example](#3-step-by-step-with-example)
4. [Key Concept — Sliding Windows Explained](#4-sliding-windows-explained)
5. [Key Concept — Haversine Formula Explained](#5-haversine-formula-explained)
6. [Code Walkthrough — File by File](#6-code-walkthrough)
7. [BigQuery Tables](#7-bigquery-tables)
8. [How to Run It](#8-how-to-run-it)
9. [End-to-End Summary Diagram](#9-end-to-end-summary-diagram)

---

## 1. Big Picture

Imagine you manage a fleet of 50 cabs. Each cab sends its **GPS location + speed every few seconds**. You want to know:

> "Which cab has been parked / stuck at the same spot for more than N minutes?"

That's exactly what this pipeline does. Here's the one-liner:

```
Cab GPS pings → Pub/Sub → Dataflow (Beam) → [detect halt] → Pub/Sub alert → Cloud Run Function → BigQuery
```

If a cab **hasn't moved at all** inside a time window, it gets flagged as **IDLE/HALTED** and an alert record is written to BigQuery.

---

## 2. All the Moving Parts

| Component | GCP Service | What It Does |
|---|---|---|
| `mock_data_gen.py` | runs locally / Cloud Shell | Simulates 50 cabs publishing GPS pings |
| `cab-telemetry` | **Cloud Pub/Sub** (topic) | Receives raw GPS messages from all cabs |
| `halt_detection_beam_job.py` | **Cloud Dataflow** (Apache Beam) | Reads pings, detects halts, raises alerts |
| `cab-idle-alerts` | **Cloud Pub/Sub** (topic) | Receives halt-alert messages from Dataflow |
| `pubsub-to-cloudrun-func.py` | **Cloud Run Function** | Reads alerts and writes them to BigQuery |
| `fleet_analytics.cab_telemetry_raw` | **BigQuery** | Stores every raw GPS ping |
| `fleet_analytics.cab_idle_events` | **BigQuery** | Stores confirmed halt events |

---

## 3. Step-by-Step with Example

Let's trace a single cab — **CAB007** — through the entire system.

---

### Step 1 — Mock Data Generator Publishes GPS Pings

`mock_data_gen.py` runs in a loop. Every 10 seconds it publishes one message per cab to Pub/Sub.

**Example messages for CAB007 (parked at a traffic light for 60 seconds):**

```json
{ "cab_id": "CAB007", "timestamp": "2024-01-15T10:00:00Z", "lat": 40.0512, "lon": -74.0231, "speed": 0.0 }
{ "cab_id": "CAB007", "timestamp": "2024-01-15T10:00:10Z", "lat": 40.0512, "lon": -74.0231, "speed": 0.0 }
{ "cab_id": "CAB007", "timestamp": "2024-01-15T10:00:20Z", "lat": 40.0512, "lon": -74.0231, "speed": 0.0 }
{ "cab_id": "CAB007", "timestamp": "2024-01-15T10:00:30Z", "lat": 40.0512, "lon": -74.0231, "speed": 0.0 }
{ "cab_id": "CAB007", "timestamp": "2024-01-15T10:00:40Z", "lat": 40.0512, "lon": -74.0231, "speed": 0.0 }
{ "cab_id": "CAB007", "timestamp": "2024-01-15T10:00:50Z", "lat": 40.0512, "lon": -74.0231, "speed": 0.0 }
```

Notice: **lat and lon are identical across all 6 messages** — the cab hasn't moved.

---

### Step 2 — Dataflow Reads and Parses the Messages

The Beam job reads these raw bytes from Pub/Sub and parses them:

```python
def parse(msg):
    data = json.loads(msg.decode("utf-8"))
    ts = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
    return {
        "cab_id":    data["cab_id"],   # "CAB007"
        "timestamp": ts.isoformat(),   # "2024-01-15T10:00:00+00:00"
        "lat":       data["lat"],      # 40.0512
        "lon":       data["lon"],      # -74.0231
        "speed":     data["speed"]     # 0.0
    }
```

**Also at this stage:** every parsed message is written directly to `cab_telemetry_raw` in BigQuery as a historical record.

---

### Step 3 — Key By Cab ID

Each parsed record is tagged with its cab_id so that data for the same cab can be grouped together:

```
("CAB007", { timestamp: "10:00:00", lat: 40.0512, lon: -74.0231 })
("CAB007", { timestamp: "10:00:10", lat: 40.0512, lon: -74.0231 })
...
```

---

### Step 4 — Sliding Windows Group the Pings

A **sliding window** (say, 60 seconds wide, sliding every 30 seconds) continuously groups the pings.

For CAB007, the window from `10:00:00` → `10:01:00` collects **6 pings** (one every 10 seconds).

```
Window [10:00:00 → 10:01:00] for CAB007:
  ping 1: lat=40.0512, lon=-74.0231
  ping 2: lat=40.0512, lon=-74.0231
  ping 3: lat=40.0512, lon=-74.0231
  ping 4: lat=40.0512, lon=-74.0231
  ping 5: lat=40.0512, lon=-74.0231
  ping 6: lat=40.0512, lon=-74.0231
```

---

### Step 5 — Detect Halt (compute_idle)

The pipeline sorts the pings by time, takes the **first** and **last** ping in the window, and calculates the GPS distance between them using the **Haversine formula**.

```
First ping → lat=40.0512, lon=-74.0231
Last  ping → lat=40.0512, lon=-74.0231

Distance = haversine(first, last) = 0.0 metres  ← EXACTLY ZERO!
```

Since `distance == 0`, CAB007 is flagged as **HALTED** and an alert is produced:

```json
{
  "cab_id":       "CAB007",
  "window_start": "2024-01-15T10:00:00+00:00",
  "window_end":   "2024-01-15T10:00:50+00:00",
  "distance_m":   0.0
}
```

> **Note:** If a different cab (say CAB012) moved from lat=40.01 to lat=40.02 in the same window, its distance would be ~1.1 km — so NO alert is raised for it.

---

### Step 6 — Alert Published to Pub/Sub

The alert JSON is serialised to bytes and published to the `cab-idle-alerts` Pub/Sub topic.

```
cab-idle-alerts topic receives:
  b'{"cab_id": "CAB007", "window_start": "...", "window_end": "...", "distance_m": 0.0}'
```

---

### Step 7 — Cloud Run Function Picks Up the Alert

The Cloud Run function (`pubsub-to-cloudrun-func.py`) is subscribed to `cab-idle-alerts` via a **Pub/Sub push subscription**. Every time a message arrives, it:

1. **Decodes** the base64-encoded Pub/Sub payload.
2. **Runs a BigQuery MERGE** — inserts the row only if it's not already in the table (deduplication!).
3. **Logs the alert message.**

```
MERGE result: CAB007 idle from 10:00:00 to 10:00:50 (0.0 m) → INSERTED into cab_idle_events
```

---

### Step 8 — Alert Lives in BigQuery

Anyone can now query BigQuery to see which cabs are halted:

```sql
SELECT cab_id, window_start, window_end, distance_m
FROM fleet_analytics.cab_idle_events
ORDER BY window_start DESC;
```

| cab_id | window_start | window_end | distance_m |
|---|---|---|---|
| CAB007 | 2024-01-15 10:00:00 | 2024-01-15 10:00:50 | 0.0 |

---

## 4. Sliding Windows Explained

A **sliding window** is like a moving spotlight over the stream of data.

```
Timeline: ──────────────────────────────────────────────────────►
Pings:       P1   P2   P3   P4   P5   P6   P7   P8
             |    |    |    |    |    |    |    |
             10s  20s  30s  40s  50s  60s  70s  80s

Window size = 60s, Period = 30s

Window 1:  [0s ──────────────── 60s]  → contains P1,P2,P3,P4,P5,P6
Window 2:  [30s ─────────────── 90s] → contains P3,P4,P5,P6,P7,P8
Window 3:  [60s ────────────── 120s] → contains P5,P6,P7,P8
```

- **Window Size** = how long a window lasts (e.g. 60 seconds).
- **Window Period** = how often a new window starts (e.g. every 30 seconds).

Because windows **overlap**, a ping can appear in multiple windows. This means you get more frequent checks — every 30 seconds you get a fresh answer to "has this cab moved in the last 60 seconds?"

---

## 5. Haversine Formula Explained

The Earth is a sphere, so you can't just subtract lat/lon values to get distance. The **Haversine formula** gives the great-circle distance between two GPS coordinates.

```python
def haversine(p1, p2):
    R = 6_371_000          # Earth's radius in metres
    lat1 = radians(p1["lat"])
    lat2 = radians(p2["lat"])
    dlat = radians(p2["lat"] - p1["lat"])
    dlon = radians(p2["lon"] - p1["lon"])
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))
```

**Example:**

| | Latitude | Longitude |
|---|---|---|
| Point A (start) | 40.0512 | -74.0231 |
| Point B (end)   | 40.0512 | -74.0231 |

→ Distance = **0.0 m** (same point) → **HALT DETECTED**

| | Latitude | Longitude |
|---|---|---|
| Point A (start) | 40.0100 | -74.0100 |
| Point B (end)   | 40.0200 | -74.0200 |

→ Distance ≈ **1,450 m** → **No halt, cab is moving**

> **Current logic:** The halt is only detected if distance is **exactly 0**. This means the first and last GPS point in the window must be bit-for-bit identical. This works well with the mock data generator which keeps lat/lon unchanged when a cab is idle.

---

## 6. Code Walkthrough

### `mock_data_gen.py` — The Fake Cab Fleet

```python
cab_ids = [f"CAB{i:03d}" for i in range(1, 51)]   # CAB001 … CAB050
cab_states = { cab: {"lat": 40.0 + random()*0.1, "lon": -74.0 + random()*0.1}
               for cab in cab_ids }

while True:
    for cab in cab_ids:
        if random() < 0.8:           # 80% chance: move a tiny bit
            state["lat"] += (random()-0.5) * 0.0005
            state["lon"] += (random()-0.5) * 0.0005
        # else: stays put → halt candidate

        speed = random.choice([0.0, round(uniform(5,60), 1)])
        # publish message to Pub/Sub ...

    time.sleep(10)   # next batch in 10 seconds
```

Key insight: **20% of the time a cab doesn't update its position** → that's your halt candidate.

---

### `halt_detection_beam_job.py` — The Brain

```
ReadFromPubSub(cab-telemetry)
    │
    ▼
parse()              ← decode JSON, normalise timestamp
    │
    ├──► WriteToBigQuery(cab_telemetry_raw)   ← store every ping
    │
    ▼
KeyByCab             ← tag each record with its cab_id
    │
    ▼
SlidingWindows(size, period)   ← group by time window
    │
    ▼
GroupByKey           ← collect all pings for a cab in a window
    │
    ▼
compute_idle()       ← haversine(first, last) == 0 → emit alert
    │
    ▼
WriteToPubSub(cab-idle-alerts)
```

---

### `pubsub-to-cloudrun-func.py` — The Alert Handler

```
Pub/Sub push → Cloud Run Function
    │
    ▼
Decode base64 payload
    │
    ▼
BigQuery MERGE
    ├── If row (cab_id + window_start + window_end) already exists → SKIP
    └── If new → INSERT into cab_idle_events
    │
    ▼
Log alert message + return HTTP 200
```

The **MERGE** (not a plain INSERT) prevents duplicate rows if the same alert fires multiple times due to Pub/Sub at-least-once delivery.

---

### `Dockerfile` — Packaging for Dataflow Flex Template

```dockerfile
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:latest

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY halt_detection_beam_job.py .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/templates/halt_detection_beam_job.py"
ENV FLEX_TEMPLATE_PYTHON_CONTAINER_IMAGE="gcr.io/.../cab-idle:latest"
```

This Docker image is what Dataflow uses to run your Beam job in the cloud. The `FLEX_TEMPLATE_PYTHON_PY_FILE` env var tells Dataflow which Python file is the entry point.

---

## 7. BigQuery Tables

### `fleet_analytics.cab_telemetry_raw`
Every single GPS ping from every cab, forever.

| Column | Type | Example |
|---|---|---|
| cab_id | STRING | `CAB007` |
| timestamp | TIMESTAMP | `2024-01-15 10:00:00 UTC` |
| lat | FLOAT64 | `40.0512` |
| lon | FLOAT64 | `-74.0231` |
| speed | FLOAT64 | `0.0` |

Partitioned by `DATE(timestamp)` — so queries like "show me all pings from yesterday" are fast and cheap.

---

### `fleet_analytics.cab_idle_events`
Confirmed halt events only.

| Column | Type | Example |
|---|---|---|
| cab_id | STRING | `CAB007` |
| window_start | TIMESTAMP | `2024-01-15 10:00:00 UTC` |
| window_end | TIMESTAMP | `2024-01-15 10:00:50 UTC` |
| distance_m | FLOAT64 | `0.0` |

Partitioned by `DATE(window_start)`.

---

## 8. How to Run It

### A. Start the Mock Data Generator

```bash
cd cab_halt_alerts_pipeline
pip install google-cloud-pubsub
python mock_data_gen.py
```

This will print something like:
```
Published: {'cab_id': 'CAB001', 'timestamp': '...', 'lat': 40.0312, 'lon': -74.0187, 'speed': 23.4}
Published: {'cab_id': 'CAB002', 'timestamp': '...', 'lat': 40.0451, 'lon': -74.0091, 'speed': 0.0}
...
Cycle complete — sleeping 10s before next cycle
```

### B. Create BigQuery Tables

```bash
bq query --use_legacy_sql=false < bigquery_commands.sql
```

### C. Build and Push the Dataflow Flex Template

```bash
# (see flex-template-build-command.txt for the full command)
gcloud dataflow flex-template build gs://YOUR_BUCKET/templates/cab-idle.json \
  --image-gcr-path gcr.io/YOUR_PROJECT/cab-idle:latest \
  --sdk-language PYTHON \
  --flex-template-base-image PYTHON3_9 \
  --metadata-file metadata.json \
  --py-path halt_detection_beam_job.py \
  --requirements-file requirements.txt
```

### D. Run the Dataflow Job

```bash
gcloud dataflow flex-template run cab-idle-detection \
  --template-file-gcs-location gs://YOUR_BUCKET/templates/cab-idle.json \
  --region us-central1 \
  --parameters inputTopic=projects/PROJECT/topics/cab-telemetry \
  --parameters alertTopic=projects/PROJECT/topics/cab-idle-alerts \
  --parameters rawTable=PROJECT:fleet_analytics.cab_telemetry_raw \
  --parameters windowSize=60 \
  --parameters windowPeriod=30
```

### E. Deploy the Cloud Run Function

```bash
cd cloud-run-func
gcloud functions deploy idle_alert \
  --runtime python311 \
  --trigger-topic cab-idle-alerts \
  --entry-point idle_alert
```

---

## 9. End-to-End Summary Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                        CAB HALT ALERTS PIPELINE                      │
└──────────────────────────────────────────────────────────────────────┘

  [50 Cabs / mock_data_gen.py]
         │
         │  GPS ping every 10s:
         │  { cab_id, timestamp, lat, lon, speed }
         │
         ▼
  ┌──────────────────┐
  │  Pub/Sub Topic   │  ← cab-telemetry
  │  (raw telemetry) │
  └────────┬─────────┘
           │
           ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │                 DATAFLOW (Apache Beam Streaming)                  │
  │                                                                   │
  │  ReadFromPubSub → parse() → ──────────────────► WriteToBigQuery  │
  │                              │                  (raw_telemetry)  │
  │                              ▼                                    │
  │                        KeyByCab("CAB007")                        │
  │                              │                                    │
  │                              ▼                                    │
  │                   SlidingWindow(60s, every 30s)                  │
  │                              │                                    │
  │                              ▼                                    │
  │                        GroupByKey                                 │
  │                              │                                    │
  │                   [CAB007 → 6 pings in window]                   │
  │                              │                                    │
  │                              ▼                                    │
  │                   compute_idle()                                  │
  │                   haversine(ping1, ping6) = 0m  → HALT!          │
  │                              │                                    │
  │                              ▼                                    │
  │                   { cab_id, window_start,                        │
  │                     window_end, distance_m: 0 }                  │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │
                                 ▼
                  ┌──────────────────────────┐
                  │  Pub/Sub Topic           │  ← cab-idle-alerts
                  │  (halt alerts)           │
                  └─────────────┬────────────┘
                                │  push subscription
                                ▼
                  ┌──────────────────────────┐
                  │  Cloud Run Function      │
                  │  (pubsub-to-cloudrun)    │
                  │                          │
                  │  1. Decode payload       │
                  │  2. MERGE into BQ        │
                  │     (dedup by key)       │
                  │  3. Log alert            │
                  └─────────────┬────────────┘
                                │
                                ▼
                  ┌──────────────────────────┐
                  │        BigQuery           │
                  │  fleet_analytics          │
                  │  .cab_idle_events         │
                  │                           │
                  │  CAB007 | 10:00 | 10:01  │
                  │  distance_m = 0.0         │
                  └───────────────────────────┘
```

---

## Quick Reference — Key Numbers

| Parameter | Default / Example | Meaning |
|---|---|---|
| Window Size | 60 seconds | How long to observe each cab before checking if it moved |
| Window Period | 30 seconds | How often a fresh window starts (overlapping) |
| Halt threshold | distance == 0 m | Cab hasn't moved at all between first and last ping |
| Cab count (mock) | 50 cabs | CAB001 to CAB050 |
| Publish cycle | every 10 seconds | One message per cab per cycle |
| Idle probability | ~20% per cycle | Random chance a cab doesn't update its GPS |
