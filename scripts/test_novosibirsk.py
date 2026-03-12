import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import CONFIG
import json

# inject test data for Novosibirsk
CONFIG["cities"]["Новосибирск"]["preset_zones"] = [
    {"name": "Дзержинский", "type": "district", "osm_relation_id": 364776},
    {"name": "Железнодорожный", "type": "district", "osm_relation_id": 365341},
    {"name": "Заельцовский", "type": "district", "osm_relation_id": 365385},
    {"name": "Калининский", "type": "district", "osm_relation_id": 364762},
    {"name": "Кировский", "type": "district", "osm_relation_id": 365403},
    {"name": "Ленинский", "type": "district", "osm_relation_id": 365401},
    {"name": "Октябрьский", "type": "district", "osm_relation_id": 364764},
    {"name": "Первомайский", "type": "district", "osm_relation_id": 366541},
    {"name": "Советский", "type": "district", "osm_relation_id": 366519},
    {"name": "Центральный", "type": "district", "osm_relation_id": 364763},
]

from run_pipeline import _discover_preset_zones, _ensure_preset_presence, PipelineStats

def run_test():
    city_key = "Новосибирск"
    city_info = CONFIG["cities"][city_key]
    stats = PipelineStats(city=city_key)

    print("Running _discover_preset_zones for Novosibirsk...")
    zones = _discover_preset_zones(city_key, city_info, stats)
    
    if zones is not None:
        zones = _ensure_preset_presence(zones, city_key, city_info)
        
    print(f"Discovered {len(zones)} zones.")
    with open(f"data/{city_key}/raw/zones_test.json", "w", encoding="utf-8") as f:
        json.dump(zones, f, ensure_ascii=False, indent=2)
    print(f"Saved to data/{city_key}/raw/zones_test.json")
    
    # Validation
    for z in zones:
        if z.get("needs_geometry") or not z.get("geojson"):
            print(f"WARNING: Zone {z['name']} is missing geometry!")
        else:
            geom = z["geojson"]
            poly_type = geom.get("type")
            print(f"OK: Zone {z['name']} has geometry type {poly_type}")

if __name__ == "__main__":
    run_test()
