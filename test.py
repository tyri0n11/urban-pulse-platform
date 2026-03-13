import os
from typing import Dict
from urllib import response
import dotenv
import requests
import json

dotenv.load_dotenv()
api_key = os.getenv("VIETMAP_API_KEY")
origin = "10.7755334,106.6989747"
destination = "10.8666897,106.7793007"
vehicle = "car"
annotations = "congestion"

ZONES: Dict[str, Dict[str, any]] = {
    "zone1_urban_core": {
        "name": "Urban Core",
        "anchor": (10.7755334, 106.6989747),  # Nhà hát TP
        "description": "CBD - hành chính - dịch vụ",
    },
    "zone2_eastern_innovation": {
        "name": "Eastern Innovation",
        "anchor": (10.8666897, 106.7793007),  # Nhà điều hành ĐHQG
        "description": "Thủ Đức - Dĩ An - Thuận An",
    },
    "zone3_northern_industrial": {
        "name": "Northern Industrial",
        "anchor": (11.1031199, 106.5495773),  # Khu công nghiệp Protrade
        "description": "Thủ Dầu Một - Bến Cát",
    },
    "zone4_southern_port": {
        "name": "Southern Port",
        "anchor": (10.7629962, 106.7722453),  # Cảng Cát Lái
        "description": "Quận 7 - Nhà Bè - Cát Lái - cảng",
    },
    "zone5_western_periurban": {
        "name": "Western Peri-urban",
        "anchor": (10.6866869, 106.5773591),  # Bệnh viện nhi đồng TP
        "description": "Bình Chánh - Hóc Môn - Củ Chi",
    },
    "zone6_southern_coastal": {
        "name": "Southern Coastal",
        "anchor": (10.5842275, 107.0367637),  # Cảng Phú Mỹ
        "description": "Phú Mỹ - Long Hải (cảng nước sâu, năng lượng)",
    },
}
def _get_a_route(origin: tuple[float, float], destination: tuple[float, float]):
    traffic_url = f"https://maps.vietmap.vn/api/route/v3?apikey={api_key}&point={origin[0]},{origin[1]}&point={destination[0]},{destination[1]}&points_encoded=false&vehicle={vehicle}&annotations={annotations}"
    response = requests.get(traffic_url)
    data = response.json()
    for path in data.get("paths", []):
        path.pop("points", None)
        path.pop("instructions", None)
        congestion_data = data.get("paths", [{}])[0].get("details", {}).get("congestion", [])
        if "details" in path and "congestion" in path.get("details", {}):
            path["congestion_ratio"] = _calc_congestion_for_interval(congestion_data, 0, len(congestion_data))
    with open("routes.json", "a") as f:
        json.dump(data, f)
        f.write("\n")
    yield data


def _calc_congestion_for_interval(congestion_data: list[dict], start_idx: int, end_idx: int):
    """
    Calculate congestion stats for a point interval

    Args:
        congestion_data: Full congestion data from API
        start_idx: Start point index
        end_idx: End point index

    Returns:
        CongestionMetrics instance or None
    """
    relevant_segments = [
        seg
        for seg in congestion_data
        if seg.get("first", 0) >= start_idx and seg.get("last", 0) <= end_idx
    ]

    if not relevant_segments:
        return None

    total_segments = len(relevant_segments)
    counts = {"heavy": 0, "moderate": 0, "low": 0, "severe": 0, "unknown": 0}

    for seg in relevant_segments:
        value = seg.get("value", "unknown")
        if value in counts:
            counts[value] += 1
        else:
            counts["unknown"] += 1

    return {
        "heavy_ratio": round(counts["heavy"] / total_segments, 2) if total_segments > 0 else 0.0,
        "moderate_ratio": round(counts["moderate"] / total_segments, 2) if total_segments > 0 else 0.0,
        "low_ratio": round(counts["low"] / total_segments, 2) if total_segments > 0 else 0.0,
        "severe_segments": counts["severe"],
        "total_segments": total_segments,
    }

def test_vietmap_api():
    for zone in ZONES.values():
        for zone_des in ZONES.values():
            if zone["name"] != zone_des["name"]:
                anchor = zone["anchor"]
                anchor_des = zone_des["anchor"]
                print(f"Testing traffic API for {zone['name']} at anchor {anchor}... to {zone_des['name']} at anchor {anchor_des}")
                for route_data in _get_a_route(anchor, anchor_des):
                    print(route_data)
            
def test_weather_api():
    pass
def main():
    test_vietmap_api()
    print("All tests passed!")
    
if __name__ == "__main__":
    main()