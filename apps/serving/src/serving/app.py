"""FastAPI application factory and middleware configuration."""

import asyncpg
from fastapi import FastAPI

from serving.dependencies import postgres_dsn
from serving.routers import health, online


def create_app() -> FastAPI:
    app = FastAPI(
        title="Urban Pulse Serving API",
        description="Near-real-time online features and anomaly insights for the Urban Pulse platform.",
        version="0.1.0",
    )

    app.include_router(health.router)
    app.include_router(online.router)

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.pg_pool = await asyncpg.create_pool(postgres_dsn(), min_size=2, max_size=10)

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await app.state.pg_pool.close()

    return app
