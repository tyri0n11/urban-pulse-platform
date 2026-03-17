-- gold hourly → baseline feature vectors for anomaly detection
SELECT
  route_id,
  EXTRACT(DOW  FROM hour_utc) AS day_of_week,
  EXTRACT(HOUR FROM hour_utc) AS hour_of_day,
  AVG(avg_duration_minutes)   AS baseline_duration_mean,
  STDDEV(avg_duration_minutes) AS baseline_duration_stddev,
  AVG(avg_heavy_ratio)        AS baseline_heavy_ratio_mean,
  COUNT(*)                    AS sample_count
FROM read_parquet(:gold_hourly_path, hive_partitioning := true)
GROUP BY route_id, day_of_week, hour_of_day;
