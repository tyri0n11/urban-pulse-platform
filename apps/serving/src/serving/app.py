"""FastAPI application factory and middleware configuration."""

import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from serving.dependencies import postgres_dsn
from serving.routers import anomalies, chat, explain, health, metrics, online, predict, ws as sse_router


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
    app.include_router(sse_router.router)

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.pg_pool = await asyncpg.create_pool(postgres_dsn(), min_size=2, max_size=10)

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await app.state.pg_pool.close()

    return app
