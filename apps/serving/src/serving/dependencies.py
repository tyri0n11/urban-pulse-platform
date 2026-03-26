"""FastAPI dependency injection providers (DB clients, config, etc.)."""

import os
from typing import AsyncGenerator

import asyncpg
from fastapi import Request


async def get_db(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    """Yield a Postgres connection from the app-level pool."""
    async with request.app.state.pg_pool.acquire() as conn:
        yield conn


def postgres_dsn() -> str:
    return os.getenv(
        "POSTGRES_DSN",
        "postgresql://urbanpulse:urbanpulse@postgres:5432/urbanpulse",
    )
