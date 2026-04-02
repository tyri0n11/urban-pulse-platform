-- silver → gold: per-route hourly aggregates
SELECT
  route_id,
  origin,
  destination,
  DATE_TRUNC('hour', timestamp_utc)                                      AS hour_utc,
  COUNT(*)                                                                AS observation_count,
  AVG(duration_minutes)                                                   AS avg_duration_minutes,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_minutes)         AS p95_duration_minutes,
  AVG(congestion_heavy_ratio)                                             AS avg_heavy_ratio,
  AVG(congestion_moderate_ratio)                                          AS avg_moderate_ratio,
  MAX(congestion_severe_segments)                                         AS max_severe_segments
FROM read_parquet(:silver_path, hive_partitioning := true)
WHERE timestamp_utc IS NOT NULL
GROUP BY route_id, origin, destination, DATE_TRUNC('hour', timestamp_utc);
