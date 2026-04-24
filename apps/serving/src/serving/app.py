"""FastAPI application factory and middleware configuration."""

import asyncio
import logging
import time

import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from serving.dependencies import postgres_dsn
from serving.routers import anomalies, chat, explain, health, metrics, online, predict, rca, telegram, ws as sse_router

logger = logging.getLogger(__name__)

_SCORER_INTERVAL = 15  # seconds — matches SSE poll interval


async def _iforest_scorer_loop(pool: asyncpg.Pool) -> None:
    """Background task: score all routes with IForest every 15s and persist to route_iforest_scores.

    Runs independently of SSE connections so that anomaly_summary/history always
    has IForest data for past hours, not just the current live window.
    """
    from serving.services import prediction_service

    while True:
        await asyncio.sleep(_SCORER_INTERVAL)
        try:
            async with pool.acquire() as conn:
                fetch_ts = time.time()
                rows = await conn.fetch(
                    """
                    SELECT DISTINCT ON (route_id)
                        route_id, window_start, is_anomaly,
                        mean_heavy_ratio,
                        COALESCE(mean_moderate_ratio, 0.0)  AS mean_moderate_ratio,
                        COALESCE(max_severe_segments, 0.0)  AS max_severe_segments,
                        duration_zscore,
                        mean_duration_minutes,
                        last_ingest_lag_ms,
                        updated_at
                    FROM online_route_features
                    ORDER BY route_id, updated_at DESC
                    """
                )
                if not rows:
                    continue

                row_dicts = [dict(r) for r in rows]
                t_score = time.monotonic()
                try:
                    predictions = prediction_service.score_rows(row_dicts)
                except Exception as exc:
                    logger.debug("background scorer: score_rows failed — %s", exc)
                    continue
                scoring_ms = int((time.monotonic() - t_score) * 1000)

                window_by_route     = {r["route_id"]: r["window_start"]          for r in row_dicts}
                zscore_by_route     = {r["route_id"]: bool(r["is_anomaly"])       for r in row_dicts}
                zscore_val_by_route = {r["route_id"]: r["duration_zscore"]       for r in row_dicts}
                duration_by_route   = {r["route_id"]: r["mean_duration_minutes"]  for r in row_dicts}
                heavy_by_route      = {r["route_id"]: r["mean_heavy_ratio"]       for r in row_dicts}
                ingest_lag_by_route = {r["route_id"]: r["last_ingest_lag_ms"]    for r in row_dicts}
                updated_at_by_route = {r["route_id"]: r["updated_at"]            for r in row_dicts}

                scored = [p for p in predictions if p.route_id in window_by_route]

                upsert_rows = [
                    (p.route_id, window_by_route[p.route_id],
                     p.iforest_anomaly, p.iforest_score, p.both_anomaly)
                    for p in scored
                ]
                await conn.executemany(
                    """
                    INSERT INTO route_iforest_scores
                        (route_id, window_start, scored_at, iforest_anomaly, iforest_score, both_anomaly,
                         score_count, anomaly_count, both_count)
                    VALUES ($1, $2, NOW(), $3, $4, $5,
                            1,
                            CASE WHEN $3 THEN 1 ELSE 0 END,
                            CASE WHEN $5 THEN 1 ELSE 0 END)
                    ON CONFLICT (route_id, window_start) DO UPDATE SET
                        scored_at       = NOW(),
                        iforest_anomaly = EXCLUDED.iforest_anomaly,
                        iforest_score   = EXCLUDED.iforest_score,
                        both_anomaly    = EXCLUDED.both_anomaly,
                        score_count     = route_iforest_scores.score_count + 1,
                        anomaly_count   = route_iforest_scores.anomaly_count
                                          + CASE WHEN EXCLUDED.iforest_anomaly THEN 1 ELSE 0 END,
                        both_count      = route_iforest_scores.both_count
                                          + CASE WHEN EXCLUDED.both_anomaly THEN 1 ELSE 0 END
                    """,
                    upsert_rows,
                )

                history_rows = []
                for p in scored:
                    ingest_lag = ingest_lag_by_route.get(p.route_id) or 0
                    updated_at = updated_at_by_route.get(p.route_id)
                    staleness_ms = int((fetch_ts - updated_at.timestamp()) * 1000) if updated_at else 0
                    full_e2e_ms = ingest_lag + staleness_ms + scoring_ms
                    history_rows.append((
                        p.route_id,
                        window_by_route[p.route_id],
                        p.iforest_score,
                        p.iforest_anomaly,
                        zscore_by_route.get(p.route_id, False),
                        p.both_anomaly,
                        zscore_val_by_route.get(p.route_id),
                        duration_by_route.get(p.route_id),
                        heavy_by_route.get(p.route_id),
                        ingest_lag,
                        staleness_ms,
                        scoring_ms,
                        full_e2e_ms,
                    ))

                await conn.executemany(
                    """
                    INSERT INTO prediction_history
                        (scored_at, route_id, window_start,
                         iforest_score, iforest_anomaly, zscore_anomaly, both_anomaly,
                         duration_zscore, mean_duration_minutes, mean_heavy_ratio,
                         ingest_lag_ms, staleness_ms, scoring_ms, full_e2e_ms)
                    VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    """,
                    history_rows,
                )

                logger.info(
                    "scorer: %d routes scored — scoring_ms=%d p50_e2e_ms=%d",
                    len(scored),
                    scoring_ms,
                    sorted(r[12] for r in history_rows)[len(history_rows) // 2] if history_rows else 0,
                )
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("background IForest scorer error: %s", exc)


def create_app() -> FastAPI:
    app = FastAPI(
        title="Urban Pulse Serving API",
        description="Near-real-time online features and anomaly insights for the Urban Pulse platform.",
        version="0.1.0",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "https://tyr1on.io.vn",
            "https://www.tyr1on.io.vn",
        ],
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    app.include_router(health.router)
    app.include_router(online.router)
    app.include_router(anomalies.router)
    app.include_router(metrics.router)
    app.include_router(predict.router)
    app.include_router(explain.router)
    app.include_router(chat.router)
    app.include_router(rca.router)
    app.include_router(telegram.router)
    app.include_router(sse_router.router)

    _CREATE_IFOREST_TABLE = """
        CREATE TABLE IF NOT EXISTS route_iforest_scores (
            route_id        TEXT             NOT NULL,
            window_start    TIMESTAMPTZ      NOT NULL,
            scored_at       TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
            iforest_anomaly BOOLEAN          NOT NULL,
            iforest_score   DOUBLE PRECISION,
            both_anomaly    BOOLEAN          NOT NULL,
            score_count     INT              NOT NULL DEFAULT 0,
            anomaly_count   INT              NOT NULL DEFAULT 0,
            both_count      INT              NOT NULL DEFAULT 0,
            PRIMARY KEY (route_id, window_start)
        );
        CREATE INDEX IF NOT EXISTS idx_iforest_window_start
            ON route_iforest_scores (window_start DESC);

        CREATE TABLE IF NOT EXISTS prediction_history (
            id                    BIGSERIAL        PRIMARY KEY,
            scored_at             TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
            route_id              TEXT             NOT NULL,
            window_start          TIMESTAMPTZ      NOT NULL,
            iforest_score         DOUBLE PRECISION,
            iforest_anomaly       BOOLEAN          NOT NULL,
            zscore_anomaly        BOOLEAN          NOT NULL,
            both_anomaly          BOOLEAN          NOT NULL,
            duration_zscore       DOUBLE PRECISION,
            mean_duration_minutes DOUBLE PRECISION,
            mean_heavy_ratio      DOUBLE PRECISION,
            ingest_lag_ms         BIGINT,
            staleness_ms          BIGINT,
            scoring_ms            BIGINT,
            full_e2e_ms           BIGINT
        );
        ALTER TABLE prediction_history
            ADD COLUMN IF NOT EXISTS ingest_lag_ms  BIGINT,
            ADD COLUMN IF NOT EXISTS staleness_ms   BIGINT,
            ADD COLUMN IF NOT EXISTS scoring_ms     BIGINT,
            ADD COLUMN IF NOT EXISTS full_e2e_ms    BIGINT;
        CREATE INDEX IF NOT EXISTS idx_ph_route_time
            ON prediction_history (route_id, scored_at DESC);
        CREATE INDEX IF NOT EXISTS idx_ph_time
            ON prediction_history (scored_at DESC);

        CREATE TABLE IF NOT EXISTS rag_interaction_log (
            id               SERIAL PRIMARY KEY,
            ts               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            query_type       TEXT        NOT NULL,  -- 'explain' | 'chat' | 'rca'
            route_id         TEXT,
            query            TEXT        NOT NULL,
            retrieved_chunks JSONB,
            response         TEXT,
            feedback         SMALLINT,              -- NULL | 1 (good) | -1 (bad)
            lang             TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_rag_log_ts
            ON rag_interaction_log (ts DESC);
    """

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.pg_pool = await asyncpg.create_pool(postgres_dsn(), min_size=2, max_size=10)
        async with app.state.pg_pool.acquire() as conn:
            await conn.execute(_CREATE_IFOREST_TABLE)
        app.state.scorer_task = asyncio.create_task(
            _iforest_scorer_loop(app.state.pg_pool),
            name="iforest-background-scorer",
        )

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        task: asyncio.Task[None] = app.state.scorer_task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await app.state.pg_pool.close()

    return app
