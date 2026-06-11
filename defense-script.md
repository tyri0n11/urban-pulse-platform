# Defense Script — UrbanPulse

---

## Opening

*"As Ho Chi Minh City's urban density continues to grow, existing traffic monitoring infrastructure has proven insufficient for real-time incident response and proactive congestion management."*

*"This thesis addresses that gap. Let me walk you through the context, the challenge, and how this platform addresses them."*

---

## Slide: Context & Challenge

**Bối cảnh:**
HCMC's road network serves over 9 million vehicles, but urban traffic management still relies entirely on scheduled processing and manual reports — with no capability to detect or respond to incidents in the moment they occur.

**What current systems cannot do:**

- **Delayed Awareness:** By the time reports are generated, the incident window has already passed — there is nothing left to act on.
- **No Selective Priority:** Every route is processed equally on a fixed schedule — critical corridors get no faster treatment than low-traffic roads.

**The Approach:**
Stream only high-impact corridors in real time — enabling authorities to detect anomalies within minutes, understand why they occur, and act before the situation escalates.

---

## Slide: Solution Overview

3 tầng contribution:

1. **Selective streaming** — stream các tuyến trọng điểm thay vì toàn mạng (trade-off: coverage vs. latency)
2. **Dual-signal anomaly detection** — Z-score (real-time, statistical) + IsolationForest (ML, batch-trained)
3. **AI-powered BI layer** — RAG + local LLM cho root cause analysis và conversational interface

*Verbal mention dual-signal khi nói về solution — không cần slide riêng.*

---

## Slide: System Architecture

*"The architecture is organized around a single source of truth — Redpanda — from which all downstream processing branches."*

**Ingestion:**
Both the VietMap traffic ingestion service and the Open-Meteo weather ingestion service publish message events into Redpanda. From there, the system splits into two independent paths.

**Path 1 — Streaming & Batch (lakehouse path):**
The streaming service consumes events from Redpanda and sinks them to the Bronze layer in MinIO as Parquet files, partitioned by `topic/year/month/day/uuid.parquet`. From there, the Batch service — orchestrated by Prefect — runs scheduled flows that promote Bronze through Silver and then Gold in the Apache Iceberg lakehouse. The Gold layer serves three purposes: analysts can query and validate data directly via Dremio; the ML service retrains one IsolationForest model per route every 6 hours, registering artifacts in MLflow backed by MinIO storage; and the RAG pipeline re-indexes ChromaDB with fresh anomaly events, traffic patterns, and weather context.

**Path 2 — Online service (speed layer):**
The Online service runs a separate consumer on the same Redpanda topic. For each incoming event, it applies Welford's online algorithm to incrementally compute the Z-score of the `heavy_ratio` metric — no batch recomputation required. The result is written immediately to PostgreSQL. PostgreSQL's Listen/Notify mechanism is used here: after each write, the Online service emits a notification on a dedicated channel. The Prediction service subscribes to that channel — so it knows exactly when new features have arrived and triggers a fresh IsolationForest score immediately, without polling.

**Prediction & Serving:**
The Prediction service loads the latest `@champion` model per route from MLflow — artifacts stored in MinIO — and caches them in-process. On a Listen/Notify trigger, it scores the new features, merges the IsolationForest result with the Z-score signal from PostgreSQL, and exposes the dual-signal output through the serving API.

For conversational queries and root cause analysis requests, the serving layer forwards the request to Ollama. Before generating a response, Ollama retrieves relevant context from ChromaDB — which holds pre-embedded documents across the three collections: anomaly events, traffic patterns, and weather context. The embedded context is injected into the prompt, and Ollama streams the LLM-generated response back to the client via SSE.

**Infrastructure & Deployment:**
The entire backend runs on a self-hosted Linux server at home. CI/CD is handled by GitHub Actions building Docker images; Traefik acts as the reverse proxy with self-signed TLS and rate-limiting middleware. The Next.js frontend is hosted separately on Vercel.

---

## Slide: Impact — Real-Time Anomaly Monitoring

*"This slide shows the system in action."*

By decoupling the ingestion path from batch processing, Urban Pulse processes traffic events immediately as they arrive at the Redpanda broker — bypassing batch latency entirely. The dashboard here shows 14 active anomalies detected in real time across the monitored corridors.

Each anomaly entry shows the route, Z-score, travel time, and — critically — which signal flagged it:

- **BOTH** — flagged by both Z-score and IsolationForest. This is the highest-confidence alert. The "Urban Core to Coastal" route with a Z-score of +3.56 is a clear example — both detectors agree, which means this is almost certainly a genuine congestion event.
- **IFOREST only** — the ML model detected a structural anomaly that the Z-score missed. This typically means the pattern is unusual relative to historical behavior for that route, even if the magnitude alone doesn't cross the statistical threshold.

This is exactly why the dual-signal design matters — each signal captures anomaly characteristics the other misses. A single-detector system would either over-alert or under-alert depending on which signal you chose.

---

## Slide: Impact — System-Wide Heatmap Analysis

*"The previous slide showed individual anomaly alerts. This slide shows the system operating at a higher level — across all routes simultaneously."*

Instead of operators manually inspecting hundreds of independent route links, Urban Pulse allows them to trigger a full AI Heatmap Interpretation with a single click. The system scans the entire Z-score matrix across all routes and time windows, then produces a structured executive summary with four sections:

**1 — Mẫu bất thường nổi bật (Prominent anomaly patterns):** The LLM identifies the highest-severity routes for the week — in this case, Cảng Phú Mỹ → Cảng Cát Lái peaking at 05:00 UTC+7, and KCN Lê Minh Xuân → Cảng Phú Mỹ spiking at 23:00. Critically, it notes these are confirmed by both detection signals and fall within the 04:00–07:00 and 22:00–01:00 alert windows.

**2 — Nguyên nhân gốc rễ (Root cause):** The LLM correlates the anomaly clusters with logistics infrastructure — port and industrial zone corridors showing increased vehicle movement during early morning hours, pointing to freight and supply chain activity as the underlying driver.

**3 — Xu hướng và tương quan (Trends and correlations):** Cross-route patterns are identified — industrial-to-port corridors as a cluster, versus stable routes like Bến Thành → Khu CNC Sài Gòn–SHTP with no significant deterioration.

**4 — Khuyến nghị (Recommendations):** Actionable output — monitor port and logistics corridors during peak windows and coordinate freight scheduling to reduce congestion.

The bar chart at the bottom ranks routes by peak Z-score, all flagged by both signals — giving operators an immediate prioritization view without any manual analysis.

---

## Slide: AI Layer — Grounded RAG Pipeline

*"Numerical scores alone don't explain traffic context. This slide covers how the AI layer bridges that gap."*

Urban Pulse grounds a local **Qwen2.5:3b** model — running entirely via Ollama — using vector-mapped evidence retrieved from ChromaDB. Two categories of context are injected into every prompt:

- **Historical Patterns:** Typical congestion levels for that specific route, day-of-week, and hour — drawn from the `traffic_patterns` collection indexed from `gold.traffic_hourly`.
- **Meteorological Context:** Historical weather data from the lakehouse combined with live weather fetched asynchronously from Open-Meteo — stored in the `external_context` collection.

The log snippet on the slide is a real system output. A user asked *"giao thông hiện tại"* — the model returned a grounded, route-specific response citing actual `heavy_ratio` values (21.4% and 16.1%) for the Southern Coastal corridors, not a generic answer.

**Local Infrastructure Decision:**
Running the LLM entirely on local hardware via Ollama was a deliberate architectural choice — not a compromise. It gives three concrete guarantees: absolute municipal data privacy with no traffic data leaving the server; zero per-token API billing costs; and offline deployment capability on city-controlled hardware, which is a hard requirement for public infrastructure use cases.

---

## Slide: System Performance Results

*"Moving to experimental results — this slide covers end-to-end ingestion latency."*

The primary latency metric is the full traversal from raw VietMap API poll to database feature persistence — covering Redpanda publish, Online service consume, Welford computation, and PostgreSQL write.

The median (p50) ingestion lag is **137ms**. At p95 it reaches **214ms**, and at p99 **289ms** — all well within the NFR target of 500ms. Both p95 and p99 pass.

To put this in context: the VietMap API is polled every 5 minutes, so the bottleneck is the poll interval, not the pipeline. Once an event enters Redpanda, the system processes and persists it in under 300ms even at the 99th percentile. This confirms the speed layer adds negligible overhead relative to the data source cadence.

---

## Slide: Data Integrity & Scale

Validated over **57,600 hourly observations** collected across a 120-day observation window (Jan–Jun 2026).

*Without ground-truth labels, traditional metrics (precision, recall, F1) are inapplicable. Instead, we validate using proxy metrics that measure internal consistency between the two detectors:*

- **Model Agreement Rate (96.3%):** Each hourly observation is independently labeled by two detectors — the Z-score classifier, which flags if the heavy_ratio z-score exceeds the per-route dynamic threshold, and IsolationForest, which flags if the decision function is negative. Agreement rate measures the proportion of observations where both detectors produce the same label — both anomaly or both normal. At 96.3% across 57,000 observations, this confirms both detectors have learned the same baseline traffic behavior.

- **Anomaly Jaccard Overlap (12.1%):** Jaccard similarity measures the overlap between the two anomaly sets: the intersection divided by the union — routes flagged by both, over routes flagged by either. At 12.1%, most anomalies are detected by only one detector. Z-score catches sharp heavy_ratio spikes that clearly cross the statistical threshold; IsolationForest catches structural deviations in the 7-dimensional feature space even when heavy_ratio alone does not breach the threshold. Low Jaccard is the expected result — it confirms the two signals are complementary rather than redundant, and directly justifies the dual-signal design.

*This is a known constraint of unsupervised anomaly detection — future work includes collecting operator-labeled incident data for supervised validation.*

---

## Slide: MLOps & RAG Ops Pipeline

*"This slide covers how the system keeps itself current — both the ML models and the LLM context — without any manual intervention."*

**Automated Model Lifecycle (MLOps)**

Urban Pulse manages its anomaly detection models through a fully automated MLOps loop. Prefect triggers the retrain flow every 6 hours, scanning the full `gold.traffic_hourly` Iceberg table to train one IsolationForest per route. Each run logs metrics and registers a per-route artifact in MLflow under `iforest-{route_id}@champion`. The serving API caches each model in-process with a 1-hour TTL — on expiry it hot-swaps the new champion version without restarting.

**Adaptive Knowledge Base (RAG Ops)**

The LLM context is kept fresh through three ChromaDB collections: `anomaly_events` is re-indexed every hour with a rolling 7-day window of confirmed anomalies from PostgreSQL; `traffic_patterns` is re-indexed every 6 hours from the full `gold.traffic_hourly` aggregation by route × day-of-week × hour; and `external_context` is re-indexed every hour with city-level weather from Open-Meteo, embedded using nomic-embed-text at 768 dimensions via Ollama.

**Self-Healing Pipeline**

The Prefect screenshot on the right shows this in production — a retrain run completing on 2026/06/07 at 07:42, with three tasks executing in sequence: baseline learning, ML retrain trigger, and RAG re-index. The entire cycle takes under 7 minutes and runs on schedule with no human involvement.

---

## Slide: Demo

*"Let me show the system running live."*

---

## Slide: Conclusion

Urban Pulse demonstrates that selective real-time streaming on critical urban corridors — combined with dual-signal anomaly detection and AI-powered explanation — can close the latency gap in metropolitan traffic management without requiring ground-truth labels or full-network coverage.

**System Strengths:**
- Real-time anomaly detection with reliable dual-signal prediction — Z-score and IsolationForest operating as complementary detectors
- Continuous ML retraining and enriched LLM context updated on every pipeline cycle — no manual intervention
- Consistent medallion data pipeline with end-to-end data governance and clear lineage

**Future Extensions:**
- Integrate a BI Dashboard layer for city planner reporting
- Migrate from 30 per-route IsolationForest models to a single global prediction model
- Enrich the RAG knowledge base with verified documents: road closures, event calendars, traffic bulletins
- Scale route coverage via dynamic discovery from VietMap's full road graph

---

## Potential Q&A

- *"Không có ground truth thì sao biết model đúng?"* → dùng internal consistency metrics (Agreement + Jaccard) thay vì external validation
- *"Threshold Z-score 2.0 chọn dựa trên gì?"* → không phải fixed — dynamic per-route từ p99 historical Z-score của chính route đó; 2.0 chỉ là fallback
- *"Jaccard thấp có nghĩa là 2 model mâu thuẫn?"* → không, chúng bắt different anomaly characteristics — đó là lý do dùng dual-signal
- *"Agreement cao chỉ vì majority normal cases?"* → đúng, đó là lý do cần Jaccard để complete bức tranh
- *"Tại sao không stream tất cả?"* → computationally prohibitive ở city scale; selective streaming là trade-off có chủ đích giữa coverage và latency
