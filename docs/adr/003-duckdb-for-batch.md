# ADR 003: DuckDB for Batch Processing

**Status:** Accepted  
**Date:** 2024-Q4  
**Context:** Query engine selection for the batch/medallion pipeline

---

## Context

The `batch` service needs to:
1. Read new bronze Parquet files from MinIO (incremental)
2. Write cleaned data to Iceberg Silver table
3. Aggregate Silver to Gold (hourly metrics per route)
4. Build the ML training feature matrix from Gold
5. Run baseline statistics (GROUP BY route × day_of_week × hour_of_day)
6. Process weather data (Open-Meteo JSON → Silver → Gold)

All of these are structured SQL operations on columnar data. The question is which query engine to use inside the Prefect flows.

Options considered:

**Option A**: Apache Spark — the standard for "big data" medallion pipelines.

**Option B**: DuckDB — an in-process OLAP engine that reads Parquet and Iceberg natively.

**Option C**: pandas — in-memory DataFrames for small data.

---

## Decision

Use **DuckDB** for all batch query operations.

Key usage pattern:
```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
result = con.execute("""
    SELECT route_id, DATE_TRUNC('hour', event_ts) AS hour_utc,
           AVG(duration_minutes), AVG(heavy_ratio)
    FROM read_parquet('s3://urban-pulse/bronze/**/*.parquet')
    GROUP BY 1, 2
""").fetch_arrow_table()
```

DuckDB reads Parquet directly from MinIO (configured as S3 endpoint). PyIceberg handles Iceberg catalog reads/writes; DuckDB handles the query execution.

---

## Consequences

### Benefits

**Zero cluster overhead**: DuckDB runs in-process inside the Prefect worker. No YARN, no Spark driver/executor separation, no JVM. Starting a DuckDB query takes milliseconds, not the 30–60 seconds a Spark session requires.

**Parquet-native**: DuckDB reads Parquet with predicate pushdown and column pruning without requiring conversion to a DataFrame. A query like `WHERE hour_utc >= '2024-01-01'` against partitioned Parquet only scans relevant files.

**Arrow integration**: DuckDB returns results as Apache Arrow `Table` objects (via `.fetch_arrow_table()`), which PyIceberg accepts directly for writes. No pandas intermediate step needed.

**SQL expressiveness**: Complex aggregations (percentile, window functions, cyclical sin/cos) are expressed cleanly in SQL rather than procedural pandas code.

**Low memory footprint**: DuckDB processes data in streaming chunks internally. Aggregating a month of bronze Parquet (several GB) on a 24 GB homelab machine is feasible.

**Fast iteration**: DuckDB SQL is easy to test interactively in a notebook or the Dremio UI before embedding in production flow code.

### Costs

**Single-writer only**: DuckDB does not support concurrent write sessions to the same database file. The batch service uses DuckDB in-memory (no persistent `.duckdb` file) and all batch flows run within the same Prefect `serve()` process — this is safe. If flows were ever parallelized across separate processes, a different engine would be required.

**Not a distributed system**: DuckDB runs on one machine. If the bronze Parquet dataset grows beyond what a single machine can process (estimated at several TB for this use case), a distributed engine (Spark, Trino) would be needed. For 20 routes at 5-minute intervals, the dataset grows at roughly 5 MB/day — DuckDB is appropriate for the thesis evaluation window and beyond.

**Iceberg write via PyIceberg**: DuckDB cannot write to Iceberg tables directly (only read via the `iceberg` extension). Writes are handled by PyIceberg using the Arrow table output from DuckDB. This two-step pattern (DuckDB for compute, PyIceberg for write) adds some boilerplate but is straightforward.

**`.arrow()` vs `.fetch_arrow_table()`**: `duckdb.execute().arrow()` returns a `RecordBatchReader` (lazy iterator), not a materialized table. Calling `.num_rows` on a `RecordBatchReader` raises `AttributeError`. Always use `.fetch_arrow_table()` when the row count is needed before passing to PyIceberg.

---

## Alternatives Rejected

**Apache Spark**: Startup time (30–60s per session), JVM dependency, Spark cluster overhead (even in local mode). The Urban Pulse dataset is small enough that Spark's parallelism provides no benefit on a single machine, and its startup cost dominates.

**pandas**: pandas reads entire Parquet files into memory before filtering. DuckDB's predicate pushdown is significantly more memory-efficient for large historical scans. pandas also lacks native Iceberg integration.

**Polars**: A viable alternative to DuckDB for DataFrame operations. DuckDB was chosen because it supports SQL directly (easier for analysts to read/audit) and has better Iceberg read integration. Polars would be a reasonable substitution if SQL syntax was not a priority.
