import ast
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import CONFIG
from scripts.patch_from_logs import parse_logs, LOGS

updates = parse_logs(LOGS)
c = CONFIG["cities"]

# We will patch `c` directly with our logs
for city, districts in updates.items():
    if city in c:
        if "preset_zones" not in c[city]:
            c[city]["preset_zones"] = []
            
        existing_zones = {z["name"]: z for z in c[city]["preset_zones"] if isinstance(z, dict)}
        
        for d_name, rel_id in districts.items():
            if d_name in existing_zones:
                existing_zones[d_name]["osm_relation_id"] = rel_id
            else:
                c[city]["preset_zones"].append({
                    "name": d_name,
                    "type": "district",
                    "osm_relation_id": rel_id
                })

# Formatter
def format_dict(d, indent=2):
    ind = "    " * indent
    lines = ["{"]
    for k, v in d.items():
        if k == "preset_zones" and isinstance(v, list):
            lines.append(f'{ind}    "{k}": [')
            for item in v:
                lines.append(f'{ind}        {repr(item)},')
            lines.append(f'{ind}    ],')
        elif isinstance(v, dict):
            lines.append(f'{ind}    "{k}": {format_dict(v, indent+1)},')
        else:
            lines.append(f'{ind}    "{k}": {repr(v)},')
    lines.append(f"{ind}}}")
    return "\n".join(lines)

cities_str = format_dict(c, indent=1)

with open("config.py", "r", encoding="utf-8") as f:
    source = f.read()

tree = ast.parse(source)
config_dict = None
for node in tree.body:
    if isinstance(node, ast.Assign) and len(node.targets) == 1 and getattr(node.targets[0], "id", "") == "CONFIG":
        config_dict = node.value
        break

for k, v in zip(config_dict.keys, config_dict.values):
    if isinstance(k, ast.Constant) and k.value == "cities":
        start_line = v.lineno - 1
        end_line = v.end_lineno
        lines = source.split("\n")
        prefix = lines[start_line][:v.col_offset]
        suffix = lines[end_line-1][v.end_col_offset:]
        new_lines = lines[:start_line] + [prefix + cities_str + suffix] + lines[end_line:]
        
        with open("config.py", "w", encoding="utf-8") as fw:
            fw.write("\n".join(new_lines))
        print("Restored and patched config.py entirely!")
        break
