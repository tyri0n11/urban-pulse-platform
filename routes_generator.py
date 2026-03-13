import json
from typing import Dict, TypedDict


type Anchor = Dict[float, float]
class Route(TypedDict):
    route_id: str
    origin: str
    destination: str
    origin_anchor: Anchor
    destination_anchor: Anchor

def generate_routes(zones_file):
    with open(zones_file, 'r') as f:
        zones = json.load(f)

    zone_by_id = {zone['id']: zone for zone in zones}
    routes = []

    for zone in zones:
        for target_id in zone.get('connects_to', []):
            target = zone_by_id[target_id]
            routes.append({
                'route_id': f"{zone['id']}_to_{target_id}",
                'origin': zone['name'],
                'destination': target['name'],
                'origin_anchor': zone['anchor'],
                'destination_anchor': target['anchor'],
            })

    return routes

if __name__ == "__main__":
    routes = generate_routes('zones.json')
    count = len(routes)
    print(f"Generated {count} routes:")
    for route in routes:
        print(f"{route['origin_anchor']} -> {route['destination_anchor']}")
    with open('routes.json', 'w') as f:
        json.dump(routes, f, indent=2)