import time
import json
import requests
import re
import sys
from pathlib import Path

# Добавляем корень проекта
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import CONFIG

def get_relation_from_nominatim(district_name, city_name):
    # Try multiple variants for safety
    queries = [
        f"{district_name} внутригородской район, {city_name}",
        f"{district_name} район, {city_name}",
        f"{district_name} округ, {city_name}",
        f"{district_name}, {city_name}",
    ]
    
    # Simple clean function similar to the pipeline
    clean_target = district_name.lower().replace("район", "").strip()
    
    for query in queries:
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": query,
            "format": "json",
            "polygon_geojson": 0,
            "limit": 10,
            "accept-language": "ru",
        }
        
        headers = {"User-Agent": "UrbanPulseConfigBuilder/1.0"}
        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                for item in data:
                    if item.get("osm_type") == "relation" and item.get("class") == "boundary":
                        item_type = item.get("type")
                        disp = str(item.get("display_name", "")).lower()
                        if item_type in ("administrative", "political") and "район" in disp:
                            # Verify names
                            if clean_target in item.get("name", "").lower() or clean_target in disp.split(",")[0]:
                                return int(item["osm_id"]), float(item["lat"]), float(item["lon"])
        except Exception as e:
            print(f"Error querying {query}: {e}")
        time.sleep(1.2)

    return None, None, None

def main():
    updated_cities = {}
    found_count = 0
    total_count = 0
    
    for city, city_data in CONFIG["cities"].items():
        print(f"\nProcessing {city}...")
        preset_zones = city_data.get("preset_zones", [])
        new_preset_zones = []
        
        for zone in preset_zones:
            if isinstance(zone, dict):
                zone_copy = dict(zone)
                
                if zone_copy.get("type") == "district" and "osm_relation_id" not in zone_copy:
                    total_count += 1
                    rel_id, lat, lon = get_relation_from_nominatim(zone_copy["name"], city)
                    if rel_id:
                        zone_copy["osm_relation_id"] = rel_id
                        if "lat" not in zone_copy:
                            zone_copy["lat"] = lat
                            zone_copy["lon"] = lon
                        print(f"  [+] {zone_copy['name']}: found relation {rel_id}")
                        found_count += 1
                    else:
                        print(f"  [-] {zone_copy['name']}: not found")
                elif "osm_relation_id" in zone_copy:
                    print(f"  [✓] {zone_copy['name']}: already has relation {zone_copy['osm_relation_id']}")
                new_preset_zones.append(zone_copy)
            else:
                new_preset_zones.append(zone)
                
        city_data_copy = dict(city_data)
        if new_preset_zones:
            city_data_copy["preset_zones"] = new_preset_zones
        updated_cities[city] = city_data_copy
        
    print(f"\n=== SUMMARY: Found {found_count}/{total_count} missing districts ===")
    
    # Merge back to raw config text
    with open("config_updates.json", "w", encoding="utf-8") as f:
        json.dump(updated_cities, f, ensure_ascii=False, indent=4)
        print("\nSaved updates to config_updates.json")

if __name__ == "__main__":
    main()
