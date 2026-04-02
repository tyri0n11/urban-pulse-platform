"""Quick payload inspector for the VietMap Route v3 API.

Fetches a single route (Urban Core → Eastern Innovation) with all
annotation types and pretty-prints the raw response, stripping the
verbose `points` and `instructions` fields so the structure is readable.

Usage:
    uv run python api.py
"""

import json
import os

import dotenv
import requests

dotenv.load_dotenv()

API_KEY = os.environ["VIETMAP_API_KEY"]
BASE_URL = "https://maps.vietmap.vn/api/route/v3"

# Urban Core → Eastern Innovation (first route in routes.json)
ORIGIN = (10.7755334, 106.6989747)
DESTINATION = (10.8666897, 106.7793007)

url = (
    f"{BASE_URL}"
    f"?apikey={API_KEY}"
    f"&point={ORIGIN[0]},{ORIGIN[1]}"
    f"&point={DESTINATION[0]},{DESTINATION[1]}"
    f"&points_encoded=false"
    f"&vehicle=car"
    f"&annotations=congestion"
    f"&alternative=true"
)

print(f"GET {url}\n")
resp = requests.get(url, timeout=30)
resp.raise_for_status()
data = resp.json()


# ── Full payload (points/instructions stripped for readability) ──────────────
def strip_verbose(obj):
    """Recursively remove large fields that obscure the structure."""
    if isinstance(obj, dict):
        return {
            k: strip_verbose(v)
            for k, v in obj.items()
            if k not in ("points", "instructions", "snapped_waypoints")
        }
    if isinstance(obj, list):
        return [strip_verbose(i) for i in obj]
    return obj


print("=" * 60)
print("FULL RESPONSE (points/instructions stripped)")
print("=" * 60)
print(json.dumps(strip_verbose(data), indent=2, ensure_ascii=False))

# ── Annotation deep-dive ─────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("ANNOTATION DEEP-DIVE")
print("=" * 60)

for path_idx, path in enumerate(data.get("paths", [])):
    print(
        f"\n── path[{path_idx}]  dist={path.get('distance', 0)/1000:.1f} km  "
        f"time={path.get('time', 0)/1000:.0f} s ──"
    )

    # The API docs say `annotations`; test.py reads from `details` — check both
    for field in ("annotations", "details"):
        block = path.get(field)
        if block:
            print(
                f"\n  [{field}] keys: {list(block.keys()) if isinstance(block, dict) else type(block)}"
            )
            print(f"  [{field}] raw:")
            print(json.dumps(block, indent=4, ensure_ascii=False))
        else:
            print(f"\n  [{field}]: absent")
