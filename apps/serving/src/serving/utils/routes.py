"""Cached loader for static routes.json (origin/destination coords)."""

import json
import os
from typing import Any

_routes_coords: dict[str, dict[str, Any]] | None = None


def load_routes_coords() -> dict[str, dict[str, Any]]:
    """Return route metadata keyed by route_id, loaded once from routes.json."""
    global _routes_coords
    if _routes_coords is None:
        routes_path = os.environ.get("ROUTES_JSON_PATH", "/app/routes.json")
        coords: dict[str, dict[str, Any]] = {}
        try:
            with open(routes_path) as f:
                for r in json.load(f):
                    coords[r["route_id"]] = {
                        "origin": r.get("origin"),
                        "destination": r.get("destination"),
                        "origin_anchor": r.get("origin_anchor"),
                        "destination_anchor": r.get("destination_anchor"),
                    }
        except FileNotFoundError:
            pass
        _routes_coords = coords
    return _routes_coords
