import re
import ast

LOGS = """
Processing Владивосток...
  [✓] Ленинский: already has relation 1933824
  [✓] Первомайский: already has relation 1933826
  [✓] Первореченский: already has relation 1933862
  [✓] Советский: already has relation 2517328
  [✓] Фрунзенский: already has relation 1933812
  [-] Островные территории: not found

Processing Сочи...
  [✓] Центральный: already has relation 1116490
  [✓] Адлерский: already has relation 5650614
  [✓] Хостинский: already has relation 907728
  [✓] Лазаревский: already has relation 1116460
  [✓] Сириус: already has relation 11865377

Processing Новосибирск...
  [+] Дзержинский: found relation 364776
  [+] Железнодорожный: found relation 365341
  [+] Заельцовский: found relation 365385
  [+] Калининский: found relation 364762
  [+] Кировский: found relation 365403
  [+] Ленинский: found relation 365401
  [+] Октябрьский: found relation 364764
  [+] Первомайский: found relation 366541
  [+] Советский: found relation 366519
  [+] Центральный: found relation 364763

Processing Нижний Новгород...
  [+] Автозаводский: found relation 2006088
  [+] Канавинский: found relation 2006102
  [-] Ленинский: not found
  [+] Московский: found relation 2006066
  [+] Нижегородский: found relation 1203429
  [-] Приокский: not found
  [+] Советский: found relation 1203475
  [+] Сормовский: found relation 2006052

Processing Челябинск...
  [+] Калининский: found relation 1579611
  [+] Курчатовский: found relation 1579610
  [+] Ленинский: found relation 1581744
  [+] Металлургический: found relation 1581688
  [+] Советский: found relation 1581689
  [+] Тракторозаводский: found relation 1581743
  [+] Центральный: found relation 1579833

Processing Санкт-Петербург...
  [+] Адмиралтейский: found relation 1114193
  [+] Василеостровский: found relation 1114252
  [+] Выборгский: found relation 1114354
  [+] Калининский: found relation 1114806
  [+] Кировский: found relation 1114809
  [+] Колпинский: found relation 337424
  [+] Красногвардейский: found relation 1114895
  [+] Красносельский: found relation 363103
  [+] Кронштадтский: found relation 1115082
  [+] Курортный: found relation 1115366
  [+] Московский: found relation 338636
  [+] Невский: found relation 368287
  [+] Петроградский: found relation 1114905
  [+] Петродворцовый: found relation 367375
  [+] Приморский: found relation 1115367
  [+] Пушкинский: found relation 338635
  [+] Фрунзенский: found relation 369514
  [+] Центральный: found relation 1114902

Processing Екатеринбург...
  [✓] Верх-Исетский: already has relation 5803327
  [✓] Железнодорожный: already has relation 5818948
  [✓] Кировский: already has relation 5818883
  [✓] Ленинский: already has relation 5817698
  [✓] Октябрьский: already has relation 5803648
  [✓] Орджоникидзевский: already has relation 5819002
  [✓] Чкаловский: already has relation 5817295

Processing Казань...
  [+] Авиастроительный: found relation 2133461
  [+] Вахитовский: found relation 2133462
  [+] Кировский: found relation 2133463
  [+] Московский: found relation 2133464
  [+] Ново-Савиновский: found relation 2133465
  [+] Приволжский: found relation 2133466
  [+] Советский: found relation 2133467

Processing Самара...
  [+] Железнодорожный: found relation 283645
  [+] Кировский: found relation 285953
  [+] Красноглинский: found relation 285954
  [+] Куйбышевский: found relation 283540
  [+] Ленинский: found relation 283781
  [+] Октябрьский: found relation 284542
  [+] Промышленный: found relation 285136
  [+] Самарский: found relation 283541
  [+] Советский: found relation 284582

Processing Ростов-на-Дону...
  [+] Ворошиловский: found relation 2228519
  [+] Железнодорожный: found relation 2227607
  [+] Кировский: found relation 2228364
  [+] Ленинский: found relation 2227685
  [+] Октябрьский: found relation 2228342
  [+] Первомайский: found relation 2228520
  [+] Пролетарский: found relation 2228370
  [+] Советский: found relation 2227024

Processing Уфа...
  [+] Дёмский: found relation 5523261
  [+] Калининский: found relation 5523739
  [+] Кировский: found relation 5523570
  [+] Ленинский: found relation 5523346
  [+] Октябрьский: found relation 5493970
  [+] Орджоникидзевский: found relation 5523682
  [+] Советский: found relation 3856973
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import CONFIG
import json

def parse_logs(logs):
    updates = {}
    current_city = None
    
    for line in logs.split("\n"):
        line = line.strip()
        if line.startswith("Processing"):
            current_city = line.split(" ")[1].replace("...", "")
            updates[current_city] = {}
        elif line.startswith("[+]"):
            parts = line.split(":")
            district_name = parts[0].replace("[+]", "").strip()
            rel_id_str = parts[1].replace("found relation", "").strip()
            rel_id = int(rel_id_str)
            updates[current_city][district_name] = rel_id
            
    return updates

def apply_updates(updates):
    c = CONFIG["cities"]
    for city, districts in updates.items():
        if city in c:
            for zone in c[city].get("preset_zones", []):
                if isinstance(zone, dict) and zone.get("name") in districts:
                    zone["osm_relation_id"] = districts[zone["name"]]
                    print(f"Patched {city} - {zone['name']} -> {zone['osm_relation_id']}")

    # Apply manually
    # We will format this dictionary to string
    # A simple custom formatter to look like original
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

    import ast
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
            print("Successfully patched config.py")
            return

def main():
    updates = parse_logs(LOGS)
    apply_updates(updates)

if __name__ == "__main__":
    main()
