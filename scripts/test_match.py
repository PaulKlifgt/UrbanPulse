import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from scripts.patch_from_logs import parse_logs, LOGS
from config import CONFIG

updates = parse_logs(LOGS)
c = CONFIG["cities"]

print("c keys:", [repr(k) for k in c.keys()])
print("updates keys:", [repr(k) for k in updates.keys()])
        for zone in c[city].get("preset_zones", []):
            if isinstance(zone, dict) and zone.get("name") in districts:
                print(f"MATCH: {city} -> {zone['name']}")
