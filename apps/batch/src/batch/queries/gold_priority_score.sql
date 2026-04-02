-- gold hourly → incident priority scores for downstream ranking
SELECT
  route_id,
  hour_utc,
  avg_duration_minutes,
  p95_duration_minutes,
  avg_heavy_ratio,
  max_severe_segments,
  (
    0.4 * avg_heavy_ratio
    + 0.3 * (avg_duration_minutes / NULLIF(p95_duration_minutes, 0))
    + 0.3 * (max_severe_segments::DOUBLE / NULLIF(MAX(max_severe_segments) OVER (), 0))
  ) AS priority_score
FROM read_parquet(:gold_hourly_path, hive_partitioning := true)
WHERE observation_count >= 3;
