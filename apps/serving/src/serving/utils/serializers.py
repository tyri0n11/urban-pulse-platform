"""Shared serialization helpers used across routers and services."""

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import asyncpg


def row_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    return dict(row)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Not JSON serializable: {type(obj)!r}")


def dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, default=_json_default)
