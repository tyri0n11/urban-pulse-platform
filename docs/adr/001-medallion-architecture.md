# ADR 001: Medallion Architecture (Bronze → Silver → Gold)

**Status:** Accepted  
**Date:** 2024-Q4  
**Context:** Data pipeline design for Urban Pulse lakehouse

---

## Context

Urban Pulse ingests raw traffic data from VietMap API every 5 minutes across 20 routes. The system needs to support:

1. Raw data preservation for replayability and debugging
2. Cleaned, typed data for ad-hoc analysis and anomaly baseline computation
3. Hourly aggregates for ML training and RAG indexing
4. Incremental processing (only new data each cycle, not full recompute)

Two main alternatives were considered:

**Option A**: Write directly to a single normalized table in Postgres. Simple, no lakehouse complexity.

**Option B**: Medallion architecture — three layers in a data lakehouse (Parquet on MinIO + Iceberg catalog).

---

## Decision

Adopt **Option B** — the medallion architecture with three layers:

- **Bronze**: Raw Parquet files on MinIO, partitioned by `year/month/day/hour`. Schema-exact copy of the Kafka messages. Written by the `streaming` service. Never modified after write.
- **Silver**: Cleaned, typed, deduplicated Iceberg table (`silver.traffic_route`). Written by the `microbatch` Prefect flow every 5 minutes from newly arrived bronze files. Handles type casting, null coalescing, and deduplication by `(route_id, event_ts)`.
- **Gold**: Hourly aggregations (`gold.traffic_hourly`) and derived tables (`gold.traffic_baseline`). Written by the `hourly-gold` Prefect flow. Source for ML training, baseline computation, and RAG indexing.

---

## Consequences

### Benefits

**Raw data preservation**: Bronze Parquet files are immutable. If there is a bug in the silver or gold transformation logic, we can re-run the pipeline from bronze without any data loss. This was used during development to fix a duration unit bug (API returns seconds, not minutes).

**Incremental processing**: `microbatch` reads only new bronze files (tracked by high-water mark). `hourly-gold` reads only the current hour's silver data. Neither flow rescans the full dataset.

**Separation of concerns**: Schema changes in the gold layer (e.g., adding `max_severe_segments`) do not require re-ingesting from the API. Re-running `hourly-gold` from silver is sufficient.

**Independent ML training**: `gold.traffic_hourly` is the single source of truth for ML features. The ML service queries this table directly via DuckDB without needing to touch raw Parquet or Postgres.

**Weather context for RAG**: Weather data for the RAG pipeline is fetched directly from the Open-Meteo archive API by the `rag_indexer` job and written to ChromaDB (`external_context` collection). This avoids a separate medallion pipeline for weather while still providing the LLM with hourly weather history.

### Costs

**Operational complexity**: Running Iceberg requires a catalog (Project Nessie). Running Nessie adds one more container. DuckDB + PyIceberg integration requires careful version management.

**Latency floor**: Data must traverse bronze → silver → gold before being available for ML training. The floor is ~5 minutes (microbatch interval). This is acceptable because the ML model is retrained every 6 hours — per-minute freshness of training data is not required.

**DuckDB in-process limitation**: DuckDB does not support concurrent writers. The Prefect batch service is the sole writer; this works because all batch flows run within the same process (Prefect `serve()`). If batch tasks ever needed to run in parallel processes, a switch to a JDBC-based engine (Spark, Trino) would be necessary.

---

## Alternatives Rejected

**Single Postgres table**: No raw data preservation; reprocessing a bug requires re-ingesting from the API (requires API access and historical data availability). Not suitable for a research platform where reproducibility matters.

**Delta Lake**: Functionally equivalent to Iceberg but stronger vendor association with Databricks. Iceberg has broader ecosystem support (Nessie, Dremio, DuckDB) and is the open standard preferred for academic work.

**Direct S3 + Athena / Hive**: Adds AWS dependency. The thesis requirement is a system that runs entirely on homelab hardware.
