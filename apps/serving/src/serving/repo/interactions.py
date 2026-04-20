"""DB access for rag_interaction_log table (chat, explain, rca query logs)."""

import json
from typing import Any

import asyncpg

from serving.utils.serializers import row_to_dict


async def log_interaction(
    conn: asyncpg.Connection,
    *,
    query_type: str,
    query: str,
    lang: str,
    route_id: str | None = None,
    chunks: list[dict[str, Any]] | None = None,
) -> int | None:
    """Insert a new interaction log row; returns the generated id."""
    try:
        return await conn.fetchval(  # type: ignore[no-any-return]
            """
            INSERT INTO rag_interaction_log
                (query_type, route_id, query, retrieved_chunks, lang)
            VALUES ($1, $2, $3, $4::jsonb, $5)
            RETURNING id
            """,
            query_type,
            route_id,
            query,
            json.dumps(chunks) if chunks else None,
            lang,
        )
    except Exception:
        return None


async def update_response(conn: asyncpg.Connection, log_id: int, response: str) -> None:
    try:
        await conn.execute(
            "UPDATE rag_interaction_log SET response = $1 WHERE id = $2",
            response, log_id,
        )
    except Exception:
        pass


async def update_feedback(conn: asyncpg.Connection, log_id: int, feedback: int) -> str:
    return await conn.execute(  # type: ignore[no-any-return]
        "UPDATE rag_interaction_log SET feedback = $1 WHERE id = $2",
        feedback, log_id,
    )


async def fetch_recent_logs(conn: asyncpg.Connection, limit: int) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT id, ts, query_type, route_id, query, response, feedback, lang
        FROM rag_interaction_log
        ORDER BY ts DESC
        LIMIT $1
        """,
        limit,
    )
    return [row_to_dict(r) for r in rows]
