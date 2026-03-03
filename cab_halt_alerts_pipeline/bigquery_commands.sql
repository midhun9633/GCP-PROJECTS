CREATE TABLE IF NOT EXISTS fleet_analytics.cab_telemetry_raw (
  cab_id     STRING,
  timestamp  TIMESTAMP,
  lat        FLOAT64,
  lon        FLOAT64,
  speed      FLOAT64
) PARTITION BY DATE(timestamp);

CREATE TABLE IF NOT EXISTS fleet_analytics.cab_idle_events (
  cab_id       STRING,
  window_start TIMESTAMP,
  window_end   TIMESTAMP,
  distance_m   FLOAT64
) PARTITION BY DATE(window_start);