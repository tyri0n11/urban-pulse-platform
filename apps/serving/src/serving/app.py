"""FastAPI application factory and middleware configuration."""

import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from serving.dependencies import postgres_dsn
from serving.routers import anomalies, chat, explain, health, metrics, online, predict, rca, telegram, ws as sse_router


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
        ALTER TABLE route_iforest_scores ADD COLUMN IF NOT EXISTS score_count   INT NOT NULL DEFAULT 0;
        ALTER TABLE route_iforest_scores ADD COLUMN IF NOT EXISTS anomaly_count INT NOT NULL DEFAULT 0;
        ALTER TABLE route_iforest_scores ADD COLUMN IF NOT EXISTS both_count    INT NOT NULL DEFAULT 0;

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

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await app.state.pg_pool.close()

    return app
