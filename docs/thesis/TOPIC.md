# Thesis Topic

## Title

**Urban Pulse: A Real-Time Traffic Anomaly Detection Platform for Ho Chi Minh City**

*(Hệ thống phát hiện bất thường giao thông thời gian thực cho Thành phố Hồ Chí Minh)*

---

## Problem Statement

Ho Chi Minh City (HCMC) is one of Southeast Asia's most congested urban environments, with over 9 million registered vehicles sharing a road network not designed for this scale. Traffic incidents — accidents, flooding, lane closures — cause cascading congestion that standard monitoring systems detect only after delays of 15–30 minutes, too late for effective operator response.

Existing solutions in Vietnam either rely on manual observation (camera operators), static threshold alerts (speed < X km/h), or expensive proprietary systems with no anomaly context. None provide:
- Automated anomaly detection across a city-wide route network
- Real-time explanations grounded in live and historical data
- An open, extensible architecture suitable for research and iteration

This thesis designs, implements, and evaluates an end-to-end platform — **Urban Pulse** — that addresses these gaps.

---

## Research Questions

1. **Detection quality**: Can a dual-signal approach (statistical z-score + IsolationForest) meaningfully improve anomaly detection precision over single-signal baselines on real HCMC traffic data?

2. **Latency feasibility**: Is it possible to achieve sub-60-second pipeline processing latency from raw API poll to scored anomaly alert on commodity homelab hardware?

3. **LLM-augmented explanation**: Does RAG-enhanced LLM explanation (traffic history + live weather context) produce more actionable anomaly rationale than rule-based templates?

4. **Data freshness vs. system overhead**: How does the externally-imposed 5-minute VietMap poll interval constrain overall end-to-end data freshness, and what is the system's own contribution to latency?

---

## Scope

### In scope

- 20 inter-zone routes across 6 HCMC zones (Urban Core, Eastern Innovation, Northern Industrial, Southern Port, Western Periurban, Southern Coastal)
- Real-time data ingestion from VietMap Traffic API (congestion ratios, duration, severe segments)
- Weather data from Open-Meteo API (free, no key required)
- Medallion lakehouse: Bronze → Silver → Gold pipeline using Apache Iceberg
- Dual-signal anomaly detection: Z-Score (online) + IsolationForest (batch)
- RAG-enhanced LLM explanation via Ollama (qwen2.5:3b) + ChromaDB
- Real-time dashboard with SSE, interactive map, anomaly drill-down, conversational AI assistant
- End-to-end latency measurement split into two SLOs

### Out of scope

- Individual vehicle tracking or plate recognition
- Predictive traffic forecasting (this thesis focuses on anomaly detection, not prediction)
- Integration with government VMS (Variable Message Signs)
- Mobile application

---

## Contributions

1. **System design**: An open-source, containerized, monorepo reference architecture for near-real-time urban traffic monitoring, deployable on a single homelab machine.

2. **Dual-signal detector**: Empirical comparison of Z-Score (speed layer) vs. IsolationForest (batch layer) on 20 HCMC routes, with majority-vote aggregation to reduce false positives.

3. **RAG-LLM explanation pipeline**: A three-layer retrieval design (live weather → ChromaDB weather history → ChromaDB traffic patterns) that grounds LLM responses in evidence rather than parametric knowledge.

4. **E2E latency decomposition**: A principled split of end-to-end latency into (a) pipeline processing latency (system overhead, SLO target p95 < 60 s) and (b) data freshness (externally bounded by poll interval, SLO target p95 < 310 s), with per-tick measurement stored in `prediction_history`.

5. **Production deployment**: Full TLS deployment on homelab hardware via Traefik + Cloudflare DNS, demonstrating feasibility at scale.

---

## Evaluation Plan

| Dimension | Metric | Target |
|-----------|--------|--------|
| Anomaly detection | Precision / Recall / F1 on labelled ground truth | Dual-signal > single-signal |
| Pipeline processing latency | p95 of `ingest_lag_ms + scoring_ms` | < 15 s |
| Data freshness | p95 of `staleness_ms` | < 15 s |
| IForest scoring speed | p95 of `scoring_ms` | < 500 ms |
| LLM explanation quality | Human evaluation (Likert 1–5) | Mean ≥ 3.5 |
| System availability | Uptime over evaluation window | ≥ 99% |

---

## Timeline

| Phase | Period | Deliverables |
|-------|--------|-------------|
| Data collection & pipeline | Month 1–2 | Working ingestion + medallion lakehouse |
| Anomaly detection | Month 2–3 | Z-score + IForest + dual-signal merge |
| LLM integration | Month 3–4 | RAG pipeline + /explain + /rca + /chat |
| Dashboard | Month 4–5 | Next.js UI + SSE + anomaly drill-down |
| Evaluation & writeup | Month 5–6 | Thesis document + defence |
