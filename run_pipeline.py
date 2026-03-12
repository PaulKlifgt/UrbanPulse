#!/usr/bin/env python3
# run_pipeline.py — UrbanPulse Real Data Pipeline v2.3
"""
v2.3 — Исправления:
  - Экология: зелень считается по ПЛОЩАДИ покрытия в радиусе, не по глобальному среднему
  - Шум: более плавная формула, потолок для типового города
  - AQI: учитывает фоновый уровень города
"""

import os
import sys
import time
import json
import re
import math
import logging
import tempfile
import shutil
import threading
import warnings
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from shapely.geometry import shape as shapely_shape
except ImportError:
    shapely_shape = None

warnings.filterwarnings("ignore")

try:
    import matplotlib
    matplotlib.use("Agg")
except ImportError:
    pass

# Попытка импорта ваших модулей
try:
    from parsers.realty_parser import RealtyParser  # Или как называется ваш класс
    HAS_REALTY_PARSER = True
except ImportError:
    HAS_REALTY_PARSER = False
    print("⚠️ Модуль parsers не найден. Сбор недвижимости будет пропущен.")

from config import CONFIG

# ============================================================
# ЛОГИРОВАНИЕ
# ============================================================

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("urbanpulse")
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)

# ============================================================
# КОНСТАНТЫ
# ============================================================

YANDEX_KEY = CONFIG.get("yandex_geocoder_key", "")
OWM_KEY = CONFIG.get("openweathermap_key", "") or CONFIG.get("openweather_api_key", "")
DGIS_KEY = CONFIG.get("dgis_key", "")

OVERPASS_SERVERS = [
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
]

_R_EARTH = 6371000
MAX_ZONE_DISTANCE_KM = 18
MIN_ZONE_POPULATION = 300

# ============================================================
# ФИЛЬТРЫ
# ============================================================

# ============================================================
# ФИЛЬТРЫ (ОБНОВЛЕНО)
# ============================================================

# ============================================================
# ФИЛЬТРЫ (ИСПРАВЛЕННЫЕ ДЛЯ ВЛАДИВОСТОКА)
# ============================================================

SKIP_WORDS_CONTAINS = [
    "платформа", "дендро", "терминал",
    "пост ", "грузовой", "депо", "завод", "фабрик",
    "кладбище", "садовод", "промзона", "магазин",
    "больница", "сортировочн", "сталь", "гаражн",
    "автовокзал", "аэропорт", "причал", "порт ",
    "проезд ", "переулок ", "тупик ", "км", "километр",
    "промузел", "тэц", "грэс", "подстанци", "очистн",
    "водозабор", "нефтебаз", "нефтеперер", "свалка", "полигон",
    "лесничество", "урочище", "казарма", "воинская", "часть",
    "склад", "база", "рудник", "шахта", "карьер",
    "трансформатор", "котельная", "гараж",
    # Дополнительные фильтры для точности
    "станция", "вокзал", "площадь", "пристань",
    "насосная", "водонасос", "канализац", "теплиц", "ангар",
    "остановк", "разъезд", "переезд", "мост ",
    "плотина", "дамба", "лесопарк", "заповедник",
    "кордон", "сторожка", "будка", "пожарн",
]

SKIP_PATTERNS = re.compile(
    r"|".join([
        r"^жк\b", r"^жилой\s+комплекс", r"жилой\s+район",
        r"^ст\b", r"^снт\b", r"^днт\b", r"^дск\b", r"^тсн\b",
        r"^кп\b", r"^гск\b", r"^бц\b", r"^тц\b", r"^тк\b",
        r"^парк\b", r"^сквер\b", r"^лагерь\b", r"^база\b",
        r"^пансионат", r"^санаторий",
        r"литер[а-я]?\b", r"\bочередь\b", r"\bкорпус\b",
        r"^посёлок-парк\b", r"^поселок-парк\b",
        r"коттеджн", r"таунхаус",
        r"дом\s+\d+", r"^ул\b", r"^улица\b",
        r"^пр-т\b", r"^проспект\b", r"^пер\b",
        r"^наб\b", r"^набережная\b", r"^ш\b", r"^шоссе\b",
        r"\d+-й\s+км", r"территория", r"квартал\s+\d+",
        # Дополнительные паттерны
        r"^площадь\b", r"^станция\b", r"^ост\.?\b",
        r"^пл\.\b", r"^ж[/.]?д\.?\b", r"^разъезд\b",
        r"^мост\b", r"^пляж\b", r"^роща\b",
    ]),
    re.IGNORECASE,
)

META_ZONE_PATTERNS = re.compile(
    r"|".join([
        r"городской\s+округ", r"муниципальн", r"^город\s+",
        r"городское\s+поселение", r"муниципальное\s+образование",
        r"^мо\s+", r"сельсовет", r"сельское\s+поселение",
        r"сельский\s+округ", r"волость", r"поссовет", r"администрация\b",
    ]),
    re.IGNORECASE,
)

# ============================================================
# NLP
# ============================================================

POS_W = {
    "отлично", "хорошо", "прекрасно", "замечательно", "красиво",
    "чисто", "тихо", "удобно", "рядом", "доступно", "зелёный",
    "зеленый", "парк", "уютно", "безопасно", "новый", "ухоженный",
    "приятно", "рекомендую", "нравится", "люблю", "комфортно",
    "развитая", "развитый", "спокойный", "дружная", "отличное",
    "хорошая", "отличная", "хороший", "красивые", "достаточно",
    "много", "работает", "доступен",
}
NEG_W = {
    "ужас", "плохо", "грязь", "грязно", "мусор", "шум", "шумно",
    "темно", "ямы", "яма", "пробки", "опасно", "страшно", "сломан",
    "разбит", "далеко", "никто", "свалка", "вонь", "запах",
    "завод", "старый", "разбитый", "негде", "невозможно", "ужасный",
    "ужасные", "плохая", "плохое", "грязные", "мало", "недоступен",
    "abandoned", "disused",
}
NEGATION_WORDS = {"не", "нет", "ни", "без", "нельзя", "никак", "некуда", "негде"}

PROBLEMS_MAP = {
    "Мусор и чистота": ["мусор", "грязь", "грязно", "свалка", "убирает"],
    "Дороги": ["яма", "ямы", "асфальт", "дорога", "тротуар", "разбит"],
    "Освещение": ["темно", "фонарь", "освещение", "свет"],
    "Шум": ["шум", "громко", "шумно", "трасса"],
    "Парковка": ["парковка", "газон", "стоянка"],
    "Озеленение": ["деревья", "зелень", "парк", "сквер", "зелени"],
    "Детская инфраструктура": ["площадка", "качели", "горка", "детям", "детских", "детей"],
    "Безопасность": ["опасно", "страшно", "камеры"],
    "Транспорт": ["автобус", "маршрутка", "метро", "пробка", "остановка", "транспорт", "остановок"],
    "ЖКХ": ["подъезд", "лифт", "двор", "ремонт"],
    "Медицина": ["поликлиника", "больница", "аптек", "врач", "медицина"],
    "Магазины": ["магазин", "супермаркет", "ехать", "ездить"],
}

# ============================================================
# СТАТИСТИКА
# ============================================================
def collect_realty_selenium(city_key, city_osm_name, zones, stats=None):
    """
    Интеграция с вашим realty_parser.py (Selenium).
    """
    if not HAS_REALTY_PARSER:
        return

    print(f"  🏠 Сбор недвижимости (Selenium)...")
    live_limit = int(CONFIG.get("realty_live_limit_per_type", 30) or 30)
    
    # Путь для сохранения
    os.makedirs(f"data/{city_key}/processed", exist_ok=True)
    out_file = f"data/{city_key}/processed/realty_offers.csv"

    # Инициализируем ваш класс
    # cache_dir можно настроить под структуру проекта
    parser = RealtyParser(cache_dir=f"data/{city_key}/raw/realty_cache")
    
    all_offers = []
    prog = Progress(len(zones), "Парсинг")
    
    try:
        for z in zones:
            zone_total = 0
            for deal_type in ("sale", "rent"):
                offers = parser.search(
                    lat=z["lat"],
                    lon=z["lon"],
                    city_name=city_osm_name,
                    deal_type=deal_type,
                    limit=max(10, live_limit),
                    radius_km=1.0  # Ищем в радиусе 1 км от центра зоны
                )

                # Важный момент: добавляем привязку к району!
                for offer in offers:
                    offer["district"] = z["name"]
                    offer["deal_type"] = offer.get("deal_type") or deal_type

                    # Для продажи считаем цену за м2, для аренды тоже можно хранить
                    if "price" in offer and "area" in offer and offer["area"]:
                        try:
                            offer["price_per_sqm"] = int(
                                offer["price"] / float(offer["area"])
                            )
                        except Exception:
                            offer["price_per_sqm"] = 0

                    all_offers.append(offer)
                zone_total += len(offers)

            prog.tick(f"{z['name']}: +{zone_total} (sale+rent)")
            
    except KeyboardInterrupt:
        print("\n🛑 Парсинг прерван пользователем. Сохраняем то, что есть...")
    except Exception as e:
        logger.error(f"Ошибка парсера: {e}")
        if stats: stats.inc_error("realty_parser_crash")
    finally:
        # Обязательно закрываем драйвер, чтобы не плодить процессы Chrome
        parser.close()

    # Сохраняем результат
    if all_offers:
        df = pd.DataFrame(all_offers)
        if "photo" in df.columns and "image_url" not in df.columns:
            df["image_url"] = df["photo"]
        elif "image_url" not in df.columns:
            df["image_url"] = ""
        # Удаляем дубликаты (одна квартира могла попасть в соседние зоны)
        if "url" in df.columns and "deal_type" in df.columns:
            df = df.drop_duplicates(subset=["url", "deal_type"])
        elif "link" in df.columns and "deal_type" in df.columns:
            df = df.drop_duplicates(subset=["link", "deal_type"])
        elif "url" in df.columns:
            df = df.drop_duplicates(subset=["url"])
        elif "link" in df.columns:
            df = df.drop_duplicates(subset=["link"])
            
        df.to_csv(out_file, index=False)
        print(f"    ✅ Сохранено {len(df)} объявлений в {out_file}")
    else:
        print("    ⚠️ Объявлений не найдено.")


@dataclass
class PipelineStats:
    city: str = ""
    zones_found: int = 0
    zones_by_source: Dict[str, int] = field(default_factory=dict)
    zones_filtered: int = 0
    api_calls: Dict[str, int] = field(default_factory=dict)
    api_errors: Dict[str, int] = field(default_factory=dict)
    cache_hits: int = 0
    duration_seconds: float = 0.0
    stages_completed: List[str] = field(default_factory=list)
    stages_failed: List[str] = field(default_factory=list)

    def inc_api(self, n: str):
        self.api_calls[n] = self.api_calls.get(n, 0) + 1

    def inc_error(self, n: str):
        self.api_errors[n] = self.api_errors.get(n, 0) + 1

    def summary(self) -> str:
        lines = [
            f"📊 Статистика: {self.city}",
            f"   Зоны: {self.zones_found} (отфильтровано: {self.zones_filtered})",
            f"   Источники: {self.zones_by_source}",
            f"   API: {self.api_calls}  Ошибки: {self.api_errors}",
            f"   Кэш: {self.cache_hits}  Время: {self.duration_seconds:.1f}с",
            f"   ✅ {self.stages_completed}",
        ]
        if self.stages_failed:
            lines.append(f"   ❌ {self.stages_failed}")
        return "\n".join(lines)


# ============================================================
# УТИЛИТЫ
# ============================================================


class RateLimiter:
    def __init__(self, rps: float = 1.0):
        self.interval = 1.0 / rps
        self.last = 0.0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            w = self.interval - (now - self.last)
            if w > 0:
                time.sleep(w)
            self.last = time.monotonic()


class Progress:
    def __init__(self, total: int, prefix: str = ""):
        self.total = max(total, 1)
        self.done = 0
        self.prefix = prefix
        self.t0 = time.monotonic()
        self._last_len = 0

    def tick(self, extra: str = ""):
        self.done += 1
        el = time.monotonic() - self.t0
        eta = el / self.done * (self.total - self.done) if self.done else 0
        pct = self.done / self.total * 100
        f = int(20 * self.done / self.total)
        line = (
            f"\r    {self.prefix} |{'█'*f}{'░'*(20-f)}| "
            f"{pct:5.1f}% ({self.done}/{self.total}) ETA {eta:.0f}s {extra[:30]}"
        )
        pad = max(0, self._last_len - len(line))
        sys.stdout.write(line + (" " * pad))
        sys.stdout.flush()
        self._last_len = len(line)
        if self.done >= self.total:
            print()


def safe_json_dump(data, path):
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        shutil.move(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def safe_json_load(path) -> Optional[Any]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning("Битый кэш %s: %s", path, e)
        os.unlink(path)
        return None


def _dist_m(lat1, lon1, lat2, lon2):
    r1, r2 = math.radians(lat1), math.radians(lat2)
    dl = r2 - r1
    do = math.radians(lon2 - lon1)
    a = math.sin(dl/2)**2 + math.cos(r1)*math.cos(r2)*math.sin(do/2)**2
    return _R_EARTH * 2 * math.asin(math.sqrt(min(1.0, a)))


def _dist_km(lat1, lon1, lat2, lon2):
    return _dist_m(lat1, lon1, lat2, lon2) / 1000.0


def _ring_signed_area(ring):
    if not ring or len(ring) < 3:
        return 0.0
    area = 0.0
    for i in range(len(ring)):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % len(ring)]
        area += x1 * y2 - x2 * y1
    return area / 2.0


def _ring_centroid(ring):
    area = _ring_signed_area(ring)
    if abs(area) < 1e-12:
        return None

    cx = 0.0
    cy = 0.0
    for i in range(len(ring)):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % len(ring)]
        cross = x1 * y2 - x2 * y1
        cx += (x1 + x2) * cross
        cy += (y1 + y2) * cross

    factor = 1.0 / (6.0 * area)
    return cx * factor, cy * factor


def _polygon_anchor_point(coords):
    if not coords:
        return None

    outer = coords[0]
    centroid = _ring_centroid(outer)
    if centroid is not None:
        lon, lat = centroid
        return {"lat": float(lat), "lon": float(lon)}

    pts = [pt for ring in coords for pt in ring]
    if not pts:
        return None

    lon = sum(pt[0] for pt in pts) / len(pts)
    lat = sum(pt[1] for pt in pts) / len(pts)
    return {"lat": float(lat), "lon": float(lon)}


def _geometry_anchor_point(geom):
    if not geom:
        return None

    if shapely_shape is not None:
        try:
            shp = shapely_shape(geom)
            point = shp.representative_point()
            return {"lat": float(point.y), "lon": float(point.x)}
        except Exception:
            pass

    gtype = geom.get("type")
    coords = geom.get("coordinates") or []
    if gtype == "Polygon":
        return _polygon_anchor_point(coords)
    if gtype == "MultiPolygon":
        best = None
        best_area = None
        for polygon in coords:
            if not polygon:
                continue
            area = abs(_ring_signed_area(polygon[0]))
            if best is None or area > best_area:
                best = polygon
                best_area = area
        if best is not None:
            return _polygon_anchor_point(best)
    return None


def clean_name(name):
    """Убирает лишние слова из названия района."""
    s = str(name).strip()
    s = re.sub(r"'''+", "", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\[[^\]]+\]", " ", s)
    s = re.sub(r"^\s*[\dIVXLCDM]+[.)'\"]*\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s{2,}", " ", s).strip()
    # Убираем кавычки
    s = s.strip('"').strip("'").strip("«").strip("»")
    
    # Убираем всё, что в скобках (например, "Трусовский район (Астрахань)" -> "Трусовский район")
    s = re.sub(r'\s*\(.*?\)\s*', '', s).strip()
    
    # Слова паразиты в начале или конце
    prefixes = ["микрорайон", "посёлок", "поселок", "квартал", "жилмассив", "район", "территория", "городок"]
    
    s_lower = s.lower()
    for p in prefixes:
        # Если название начинается с "микрорайон ...", убираем
        if s_lower.startswith(p + " "):
            s = s[len(p):].strip()
            s_lower = s.lower()
        # Если заканчивается (редко, но бывает "Бабаевского микрорайон")
        if s_lower.endswith(" " + p):
            s = s[:-len(p)].strip()
            s_lower = s.lower()
            
    # Если осталось только число или пустота, возвращаем исходное (чтобы не сломать "Микрорайон №6")
    if s.isdigit() or len(s) < 2:
        return name
        
    return s


def _zone_name_key(name):
    s = clean_name(name)
    s = str(s or "").lower().replace("ё", "е")
    s = re.sub(r"\bадминистратив\w*\b", " ", s)
    s = re.sub(r"\bвнутригород\w*\b", " ", s)
    s = re.sub(r"\bмуниципаль\w*\b", " ", s)
    s = re.sub(r"\bгорода\b", " ", s)
    s = re.sub(r"\bг\.\b", " ", s)
    s = re.sub(r"\b(район|района|районе|районом|районы|районов)\b", " ", s)
    s = re.sub(r"\b(округ|округа|округе|округом|округи|округов)\b", " ", s)
    s = re.sub(r"\bао\b", " ", s)
    s = re.sub(r"[^a-zа-я0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _city_stems(city):
    parts = [p for p in re.split(r"[\s\-]+", str(city or "").lower().strip()) if p]
    stems = []
    for p in parts:
        stems.append(p)
        if len(p) >= 5:
            stems.append(p[: max(4, len(p) - 2)])
    return list(dict.fromkeys(stems))


def _text_matches_city(text, city_name):
    stems = _city_stems(city_name)
    hay = str(text or "").lower()
    if not stems:
        return True
    return any(stem in hay for stem in stems)


def _extract_zone_city_from_query(addr):
    parts = [p.strip() for p in str(addr or "").split(",") if p.strip()]
    if len(parts) >= 2:
        zone = clean_name(parts[0])
        city = clean_name(parts[-1])
        return zone, city
    cleaned = clean_name(addr)
    return cleaned, ""


def _looks_like_admin_district_name(name):
    lo = clean_name(name).lower().strip()
    if not lo:
        return False
    if re.search(r"\b(район|микрорайон|округ|ао)\b", lo):
        return True
    if "административный округ" in lo:
        return True
    if re.search(r"(ский|ская|ское|ские|ских|скому|ском|ный|ная|ное|ные|ного|ном)$", lo):
        return True
    return False


def _district_queries(zone_name, city_name):
    raw_name = str(zone_name or "").strip()
    city_name = str(city_name or "").strip()
    base_name = re.sub(
        r"\b(административный|административного)\s+округ\b",
        " ",
        raw_name,
        flags=re.IGNORECASE,
    )
    base_name = re.sub(r"\bао\b", " ", base_name, flags=re.IGNORECASE)
    base_name = re.sub(r"\s+", " ", base_name).strip(" ,")

    queries = [
        f"{raw_name} внутригородской район города {city_name}",
        f"{raw_name} внутригородской район, {city_name}",
        f"{raw_name} район города {city_name}",
        f"{raw_name} район, {city_name}",
        f"{raw_name} административный район, {city_name}",
        f"{city_name}, {raw_name} район",
        f"{city_name}, {raw_name}",
    ]

    if base_name and base_name.lower() != raw_name.lower():
        queries.extend([
            f"{base_name} административный округ, {city_name}",
            f"{base_name} административный округ города {city_name}",
            f"{base_name} округ, {city_name}",
            f"{city_name}, {base_name} административный округ",
            f"{city_name}, {base_name} округ",
        ])

    if city_name.lower() == "москва":
        queries.extend([
            f"{base_name or raw_name} административный округ, Москва, Россия",
            f"{base_name or raw_name} административный округ Москвы",
            f"{base_name or raw_name} округ, Москва, Россия",
        ])

    deduped = []
    seen = set()
    for query in queries:
        q = re.sub(r"\s+", " ", query).strip(" ,")
        if not q:
            continue
        key = q.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(q)
    return deduped


def _geometry_to_polygons(geom):
    if not geom:
        return []
    gtype = geom.get("type")
    coords = geom.get("coordinates") or []
    if gtype == "Polygon":
        return [coords]
    if gtype == "MultiPolygon":
        return list(coords)
    return []


def _combine_geometries(geoms):
    polygons = []
    for geom in geoms:
        polygons.extend(_geometry_to_polygons(geom))
    if not polygons:
        return None
    if len(polygons) == 1:
        return {"type": "Polygon", "coordinates": polygons[0]}
    return {"type": "MultiPolygon", "coordinates": polygons}


def _fetch_geometry_by_queries(queries, zone_name, city_name, zone_type, center, max_dist_km, trusted_district):
    geoms = []
    for query in queries:
        geom = geo_polygon_nom(
            query,
            zone_name=zone_name,
            city_name=city_name,
            zone_type=zone_type,
        )
        if not geom or not geom.get("geojson"):
            continue
        if city_name and not _text_matches_city(geom.get("display_name", ""), city_name):
            point_limit_km = max(max_dist_km * 1.5, max_dist_km + 15.0)
            if not center or _dist_km(center[0], center[1], geom["lat"], geom["lon"]) > point_limit_km:
                continue
        point_limit_km = max_dist_km if not trusted_district else max(max_dist_km * 2.5, max_dist_km + 20.0)
        if center and _dist_km(center[0], center[1], geom["lat"], geom["lon"]) > point_limit_km:
            continue
        geoms.append(geom["geojson"])

    combined = _combine_geometries(geoms)
    if not combined:
        return None
    anchor = _geometry_anchor_point(combined)
    if not anchor:
        return None
    return {
        "lat": float(anchor["lat"]),
        "lon": float(anchor["lon"]),
        "geojson": combined,
    }


def _lookup_relation_geometry(osm_rel_id, fallback_lat=None, fallback_lon=None):
    try:
        r = _session.get(
            "https://nominatim.openstreetmap.org/lookup",
            params={"osm_ids": f"R{osm_rel_id}", "format": "json", "polygon_geojson": 1},
            headers={"User-Agent": "UrbanPulseApp/1.0"},
            timeout=20,
        )
        data = r.json()
        if data and data[0].get("geojson"):
            item = data[0]
            return {
                "geojson": item["geojson"],
                "lat": float(item.get("lat", fallback_lat or 0.0) or 0.0),
                "lon": float(item.get("lon", fallback_lon or 0.0) or 0.0),
                "osm_id": osm_rel_id,
            }
    except Exception:
        pass
    return None


def _apply_relation_overrides(base_geojson, extra_relation_ids=None, subtract_relation_ids=None, fallback_lat=None, fallback_lon=None):
    extra_relation_ids = list(extra_relation_ids or [])
    subtract_relation_ids = list(subtract_relation_ids or [])
    if not (extra_relation_ids or subtract_relation_ids) or shapely_shape is None:
        return base_geojson

    try:
        from shapely.geometry import mapping
    except Exception:
        return base_geojson

    geom = shapely_shape(base_geojson) if base_geojson else None

    for rel_id in extra_relation_ids:
        rel_data = _lookup_relation_geometry(rel_id, fallback_lat=fallback_lat, fallback_lon=fallback_lon)
        if not rel_data:
            continue
        rel_geom = shapely_shape(rel_data["geojson"])
        geom = rel_geom if geom is None else geom.union(rel_geom)

    if geom is None:
        return base_geojson

    for rel_id in subtract_relation_ids:
        rel_data = _lookup_relation_geometry(rel_id, fallback_lat=fallback_lat, fallback_lon=fallback_lon)
        if not rel_data:
            continue
        rel_geom = shapely_shape(rel_data["geojson"])
        geom = geom.difference(rel_geom)

    if geom.is_empty:
        return base_geojson
    return mapping(geom)


def _get_zone_max_dist(city_info):
    try:
        return float(city_info.get("max_zone_distance_km") or MAX_DIST_KM)
    except Exception:
        return float(MAX_DIST_KM)


def _bbox(lat, lon, r_m):
    dl = r_m / 111000
    do = r_m / (111000 * max(0.01, math.cos(math.radians(lat))))
    return f"{lat-dl},{lon-do},{lat+dl},{lon+do}"


def _build_idx(objects, gs=0.015):
    idx = {}
    for o in objects:
        k = (round(o["lat"]/gs), round(o["lon"]/gs))
        idx.setdefault(k, []).append(o)
    return idx, gs


def _count_near(idx, gs, lat, lon, r_m=1500):
    cells = int(r_m / (gs * 111000)) + 2
    ck = (round(lat/gs), round(lon/gs))
    c = 0
    for di in range(-cells, cells+1):
        for dj in range(-cells, cells+1):
            for o in idx.get((ck[0]+di, ck[1]+dj), []):
                if _dist_m(lat, lon, o["lat"], o["lon"]) <= r_m:
                    c += 1
    return c


def _near_with_dist(idx, gs, lat, lon, r_m=1500):
    """Объекты в радиусе с расстояниями."""
    cells = int(r_m / (gs * 111000)) + 2
    ck = (round(lat/gs), round(lon/gs))
    result = []
    for di in range(-cells, cells+1):
        for dj in range(-cells, cells+1):
            for o in idx.get((ck[0]+di, ck[1]+dj), []):
                d = _dist_m(lat, lon, o["lat"], o["lon"])
                if d <= r_m:
                    result.append((o, d))
    return result


# ============================================================
# ФИЛЬТРЫ НАЗВАНИЙ
# ============================================================


# Известные аббревиатуры районов, которые не нужно отфильтровывать
_KNOWN_ABBREVS = {
    "виз", "чтз", "чмз", "амз", "жби", "ваи", "обьгэс",
}

def is_junk(name):
    if not name or len(name.strip()) < 2:
        return True
    lo = name.lower().strip()
    # Слишком короткие имена (1-2 символа) — мусор, кроме известных аббревиатур
    if len(lo) <= 2 and lo not in _KNOWN_ABBREVS:
        return True
    if any(w in lo for w in SKIP_WORDS_CONTAINS):
        return True
    if SKIP_PATTERNS.search(lo):
        return True
    s = name.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("«") and s.endswith("»")):
        return True
    if re.match(r"^[A-Za-z\s]+$", s):
        return True
    if re.search(r"дом\s*\d+", lo):
        return True
    if len(name) > 40:
        return True
    return False


def is_meta(name, tags=None):
    if META_ZONE_PATTERNS.search(name):
        return True
    if tags:
        al = tags.get("admin_level", "")
        if al in ("4", "5"):
            return True
        if al == "6" and tags.get("boundary") == "administrative":
            if tags.get("place", "") not in ("suburb", "quarter", "neighbourhood"):
                return True
    return False


def is_rc(name, tags=None):
    lo = name.lower().strip()
    if '"' in name or "«" in name or "»" in name or "'" in name:
        return True
    if re.search(r"\d+-\d+", name):
        return True
    if re.search(r"\bна\s+[А-Я]", name):
        return True
    if tags:
        if tags.get("building") or tags.get("operator"):
            return True
        if tags.get("landuse") in ("construction", "commercial", "industrial"):
            return True
        if tags.get("website") or tags.get("brand"):
            return True
        if any(tags.get(k) for k in ("highway", "public_transport", "amenity", "shop", "office")):
            return True
    return False


def is_tiny(name, tags=None):
    if tags and tags.get("place", "") in ("hamlet", "isolated_dwelling", "farm", "allotments"):
        return True
    if any(w in name.lower() for w in ["хутор", "ферма", "дача", "садоводство"]):
        return True
    return False


def _fuzzy(n1, n2):
    a = _zone_name_key(n1)
    b = _zone_name_key(n2)
    if a == b:
        return True
    if len(a) > 4 and len(b) > 4 and (a in b or b in a):
        return True
    return False


# ============================================================
# HTTP
# ============================================================

_session = requests.Session()
_session.headers.update({"User-Agent": "UrbanPulse/2.3"})
_retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503],
               allowed_methods=["GET", "POST"])
_session.mount("https://", HTTPAdapter(max_retries=_retry))
_session.mount("http://", HTTPAdapter(max_retries=_retry))

_nom_rl = RateLimiter(1.0)
_ya_rl = RateLimiter(10.0)
_op_rl = RateLimiter(2.0)
_owm_rl = RateLimiter(5.0)

def _has_ya():
    return bool(YANDEX_KEY) and YANDEX_KEY != "YOUR_YANDEX_API_KEY"

def _has_owm():
    return bool(OWM_KEY) and OWM_KEY != "YOUR_OWM_API_KEY"


# ============================================================
# OVERPASS
# ============================================================


def _parse_count(data):
    if not data:
        return 0
    el = data.get("elements", [])
    if not el:
        return 0
    f = el[0]
    if f.get("type") == "count":
        try:
            return int(f.get("tags", {}).get("total", 0))
        except (ValueError, TypeError):
            return 0
    t = f.get("tags", {}).get("total")
    if t is not None and len(el) == 1:
        try:
            return int(t)
        except (ValueError, TypeError):
            pass
    return len(el)


def qop(query, timeout=60, stats=None):
    for srv in OVERPASS_SERVERS:
        short = srv.split("/")[2]
        _op_rl.wait()
        if stats:
            stats.inc_api("overpass")
        try:
            r = _session.post(srv, data={"data": query}, timeout=timeout)
            if "json" not in r.headers.get("Content-Type", "") and r.status_code == 200:
                continue
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 30)))
                continue
            r.raise_for_status()
            d = r.json()
            if "elements" in d:
                return d
        except requests.exceptions.Timeout:
            if stats: stats.inc_error("op_timeout")
        except requests.exceptions.ConnectionError:
            if stats: stats.inc_error("op_conn")
        except Exception:
            if stats: stats.inc_error("op_other")
    return None


# ============================================================
# GEOCODING
# ============================================================


def geo_nom(addr):
    _nom_rl.wait()
    try:
        r = _session.get("https://nominatim.openstreetmap.org/search",
                         params={"q": addr, "format": "json", "limit": 8,
                                 "accept-language": "ru"}, timeout=10)
        r.raise_for_status()
        res = r.json()
        if res:
            want, city = _extract_zone_city_from_query(addr)
            city_stems = _city_stems(city)
            best = None
            best_score = None
            for item in res:
                disp = str(item.get("display_name", "") or "")
                first = clean_name(disp.split(",")[0]).lower().strip()
                all_text = f"{disp} {item.get('type','')} {item.get('class','')}".lower()
                score = 0
                if want and first == want.lower().strip():
                    score += 12
                elif want and _fuzzy(first, want.lower().strip()):
                    score += 8
                elif want and want.lower().strip() in all_text:
                    score += 4
                city_match = any(stem in all_text for stem in city_stems) if city_stems else True
                if city_match:
                    score += 8
                elif city_stems:
                    score -= 10
                if item.get("class") == "boundary":
                    score += 4
                if item.get("type") in ("administrative", "suburb", "quarter", "neighbourhood", "residential"):
                    score += 4
                if item.get("type") in ("house", "residential", "street"):
                    score -= 3
                if city and first == clean_name(city).lower().strip():
                    score -= 8
                if best is None or score > best_score:
                    best = item
                    best_score = score
            if best:
                return {"lat": float(best["lat"]), "lon": float(best["lon"]),
                        "name": best.get("display_name", "").split(",")[0],
                        "display_name": best.get("display_name", "")}
    except Exception:
        pass
    return None

def geo_polygon_nom(addr, zone_name=None, city_name=None, zone_type=None):
    _nom_rl.wait()
    try:
        r = _session.get("https://nominatim.openstreetmap.org/search",
                         params={"q": addr, "format": "json", "limit": 8,
                                 "polygon_geojson": 1, "accept-language": "ru"}, timeout=12)
        r.raise_for_status()
        res = r.json()
        if res:
            want = clean_name(zone_name or addr).lower().strip()
            city_stems = _city_stems(city_name)
            best = None
            best_score = None
            for item in res:
                geom = item.get("geojson")
                if not geom or geom.get("type") not in ("Polygon", "MultiPolygon"):
                    continue
                disp = str(item.get("display_name", "") or "")
                first = clean_name(disp.split(",")[0]).lower().strip()
                all_text = f"{disp} {item.get('type','')} {item.get('class','')}".lower()
                score = 0
                if want and first == want:
                    score += 12
                elif want and _fuzzy(first, want):
                    score += 8
                elif want and want in all_text:
                    score += 5
                city_match = any(stem in all_text for stem in city_stems) if city_stems else True
                if city_match:
                    score += 8
                elif city_stems:
                    score -= 12
                if item.get("class") == "boundary":
                    score += 4
                if item.get("type") in ("administrative", "suburb", "quarter", "neighbourhood", "residential"):
                    score += 4
                if zone_type == "district" or _looks_like_admin_district_name(zone_name or addr):
                    item_type = item.get("type")
                    if item_type == "administrative":
                        score += 8
                    elif item_type in ("quarter", "neighbourhood", "residential"):
                        score -= 6
                    elif item_type == "suburb":
                        score -= 2
                    if item.get("class") == "boundary":
                        score += 4
                    if "внутригород" in all_text:
                        score += 8
                    if " район" in all_text or all_text.startswith("район "):
                        score += 4
                    if "микрорайон" in all_text:
                        score -= 4
                    if "городской округ" in all_text:
                        score -= 6
                if city_name and first == clean_name(city_name).lower().strip():
                    score -= 10
                if best is None or score > best_score:
                    best = item
                    best_score = score

            if best and best.get("geojson"):
                anchor = _geometry_anchor_point(best["geojson"])
                return {
                    "lat": float(anchor["lat"]) if anchor else float(best["lat"]),
                    "lon": float(anchor["lon"]) if anchor else float(best["lon"]),
                    "name": best.get("display_name", "").split(",")[0],
                    "display_name": best.get("display_name", ""),
                    "geojson": best["geojson"],
                    "osm_type": best.get("osm_type"),
                    "osm_id": best.get("osm_id")
                }
    except Exception:
        pass
    return None




def geo_ya(addr):
    if not _has_ya():
        return None
    _ya_rl.wait()
    try:
        want, city = _extract_zone_city_from_query(addr)
        r = _session.get("https://geocode-maps.yandex.ru/1.x/",
                         params={"apikey": YANDEX_KEY, "geocode": addr,
                                 "format": "json", "results": 5}, timeout=10)
        if r.status_code == 403:
            return None
        r.raise_for_status()
        m = r.json()["response"]["GeoObjectCollection"]["featureMember"]
        if m:
            city_stems = _city_stems(city)
            best = None
            best_score = None
            for item in m:
                obj = item.get("GeoObject", {})
                meta = obj.get("metaDataProperty", {}).get("GeocoderMetaData", {})
                kind = str(meta.get("kind", "") or "")
                name = str(obj.get("name", "") or "")
                text = f"{name} {meta.get('text','')}".lower()
                score = 0
                if want and clean_name(name).lower().strip() == want.lower().strip():
                    score += 12
                elif want and _fuzzy(clean_name(name).lower().strip(), want.lower().strip()):
                    score += 8
                elif want and want.lower().strip() in text:
                    score += 4
                if city_stems and any(stem in text for stem in city_stems):
                    score += 4
                if kind in ("district", "locality"):
                    score += 5
                if kind in ("street", "house", "metro"):
                    score -= 4
                if city and clean_name(name).lower().strip() == clean_name(city).lower().strip():
                    score -= 8
                if best is None or score > best_score:
                    best = obj
                    best_score = score
            if best:
                p = best["Point"]["pos"]
                lo, la = p.split()
                return {"lat": float(la), "lon": float(lo),
                        "name": best.get("name", "")}
    except Exception:
        pass
    return None


def geo(addr):
    return geo_ya(addr) or geo_nom(addr)


def rev_ya(lat, lon):
    if not _has_ya():
        return None
    _ya_rl.wait()
    try:
        r = _session.get("https://geocode-maps.yandex.ru/1.x/",
                         params={"apikey": YANDEX_KEY, "geocode": f"{lon},{lat}",
                                 "format": "json", "results": 5,
                                 "kind": "district"},    # ← ДОБАВЛЕНО: ищем именно районы
                         timeout=10)
        if r.status_code == 403:
            logger.warning("Яндекс reverse: 403 Forbidden")
            return None
        r.raise_for_status()
        data = r.json()
        results = []
        seen = set()
        members = data.get("response", {}).get("GeoObjectCollection", {}).get("featureMember", [])
        if not members:
            logger.debug("Яндекс reverse: пустой ответ для %.4f,%.4f", lat, lon)
            return None  # ← не [] а None чтобы различать "пусто" и "ошибка"
        for m in members:
            o = m["GeoObject"]
            meta = o.get("metaDataProperty", {}).get("GeocoderMetaData", {})
            kind = meta.get("kind", "")
            name = o.get("name", "")
            if kind in ("district", "locality") and name and name not in seen:
                p = o["Point"]["pos"]
                lo2, la2 = p.split()
                results.append({"name": name, "lat": float(la2),
                                "lon": float(lo2), "kind": kind})
                seen.add(name)
            for c in meta.get("Address", {}).get("Components", []):
                if c.get("kind") == "district" and c.get("name") and c["name"] not in seen:
                    results.append({"name": c["name"], "lat": lat,
                                    "lon": lon, "kind": "addr"})
                    seen.add(c["name"])
        return results if results else None
    except Exception as e:
        logger.warning("Яндекс reverse error: %s", e)
        return None


def rev_nom(lat, lon):
    _nom_rl.wait()
    try:
        r = _session.get("https://nominatim.openstreetmap.org/reverse",
                         params={"lat": lat, "lon": lon, "format": "json", "zoom": 14,
                                 "accept-language": "ru", "addressdetails": 1}, timeout=10)
        r.raise_for_status()
        addr = r.json().get("address", {})
        return [{"name": addr[k], "level": k, "lat": lat, "lon": lon}
                for k in ["suburb", "quarter", "neighbourhood", "city_district"] if k in addr]
    except Exception:
        return None


# ============================================================
# НАСЕЛЕНИЕ
# ============================================================


def _pop_wd(city):
    # Если Викидата тормозит, лучше сразу сдаться и взять данные из OSM/Fallback
    q = f'SELECT ?p WHERE {{?c rdfs:label "{city}"@ru.?c wdt:P1082 ?p.}}ORDER BY DESC(?p)LIMIT 1'
    try:
        r = _session.get("https://query.wikidata.org/sparql",
                         params={"query": q, "format": "json"}, timeout=5) # Маленький таймаут
        r.raise_for_status()
        b = r.json()["results"]["bindings"]
        if b:
            return int(float(b[0]["p"]["value"]))
    except Exception:
        pass
    return None


def _pop_osm(name, lat, lon, stats=None):
    """Поиск населения с географической привязкой."""
    bb = _bbox(lat, lon, 10000)  # 10km radius
    q = (f'[out:json][timeout:15];'
         f'(relation["name"="{name}"]["population"]({bb});'
         f'node["name"="{name}"]["population"]({bb}););'
         f'out tags 1;')
    d = qop(q, 20, stats)
    if d:
        for e in d.get("elements", []):
            p = e.get("tags", {}).get("population")
            if p:
                try:
                    return int(p)
                except ValueError:
                    pass
    return None


def _pop_bld(lat, lon, stats=None):
    q = f'[out:json][timeout:30];way["building"~"residential|apartments|yes"]({_bbox(lat, lon, 1000)});out count;'
    c = _parse_count(qop(q, 35, stats))
    return c * 40 if c > 0 else None


def _residential_building_count(lat, lon, stats=None):
    q = f'[out:json][timeout:30];way["building"~"residential|apartments|yes"]({_bbox(lat, lon, 1000)});out count;'
    return _parse_count(qop(q, 35, stats))


def _pop_from_buildings(zone, building_count, city_population):
    if building_count <= 0:
        return None

    zone_type = str(zone.get("type", "") or "")
    if zone_type == "district":
        per_building = 32
        cap = min(220000, max(20000, int(city_population * 0.35)))
    else:
        per_building = 22
        cap = min(90000, max(12000, int(city_population * 0.12)))

    estimate = max(3000, int(building_count * per_building))
    return min(cap, estimate)


def _fallback_zone_population(zone, estimates_by_type):
    zone_type = str(zone.get("type", "") or "")
    pool = estimates_by_type.get(zone_type) or estimates_by_type.get("_all") or []
    if pool:
        return int(np.median(pool))
    return 12000 if zone_type == "district" else 6000


def est_pop(city, zones, stats=None):
    print(f"    📊 Население...")
    cp = _pop_wd(city)
    if not cp:
        q = f'[out:json][timeout:15];relation["name"="{city}"]["population"]["place"~"city|town"];out tags 1;'
        d = qop(q, 20, stats)
        if d:
            for e in d.get("elements", []):
                p = e.get("tags", {}).get("population")
                if p:
                    try:
                        cp = int(p)
                    except ValueError:
                        pass
    cp = cp or 500000
    print(f"      Город: {cp:,}")

    raw = {}
    estimates_by_type = {"district": [], "microdistrict": [], "_all": []}
    prog = Progress(len(zones), "Население")
    for z in zones:
        n = z["name"]
        p = _pop_osm(n, z["lat"], z["lon"], stats)
        if p and 100 <= p <= int(cp * 0.75):
            raw[n] = {"v": p, "s": "osm_tag"}
            estimates_by_type.setdefault(z.get("type", "") or "", []).append(p)
            estimates_by_type["_all"].append(p)
            prog.tick(n); continue
        bld = _residential_building_count(z["lat"], z["lon"], stats)
        p = _pop_from_buildings(z, bld, cp)
        if p and p > 100:
            raw[n] = {"v": p, "s": "building_density"}
            raw[n]["building_count"] = bld
            estimates_by_type.setdefault(z.get("type", "") or "", []).append(p)
            estimates_by_type["_all"].append(p)
            prog.tick(n); continue
        raw[n] = {"v": None, "s": "est"}
        prog.tick(n)
        time.sleep(0.3)

    for z in zones:
        if raw[z["name"]]["v"]:
            continue
        raw[z["name"]] = {
            "v": _fallback_zone_population(z, estimates_by_type),
            "s": "fallback_median",
        }

    if zones and all(z.get("type") == "district" for z in zones) and len(zones) >= 4:
        current_total = sum(max(0, int(v["v"] or 0)) for v in raw.values())
        if current_total > 0:
            scale = cp / current_total
            for z in zones:
                item = raw[z["name"]]
                item["v"] = max(3000, int(round(item["v"] * scale)))
                if item["s"] in {"building_density", "fallback_median"}:
                    item["s"] = "city_scaled"

    for z in zones:
        i = raw.get(z["name"], {})
        z["population"] = i.get("v", 5000)
        z["population_source"] = i.get("s", "?")
    return zones

# ============================================================
# ЗОНЫ
# ============================================================
# ============================================================
# 2. ОБНАРУЖЕНИЕ ЗОН — v6
# ============================================================

MAX_DIST_KM = 12
DEDUP_RADIUS_M = 500
MIN_ZONE_POPULATION = 1000

RURAL_PLACES = {
    "village", "hamlet", "isolated_dwelling", "farm",
    "allotments", "locality",
}


def _grid(center, radius_km, step_km):
    pts = []
    lc, loc = center
    cos = max(0.01, math.cos(math.radians(lc)))
    s = int(radius_km / step_km) + 1
    for i in range(-s, s + 1):
        for j in range(-s, s + 1):
            la = lc + i * step_km / 111.0
            lo = loc + j * step_km / (111.0 * cos)
            if _dist_km(lc, loc, la, lo) <= radius_km:
                pts.append((round(la, 5), round(lo, 5)))
    return pts


def _ok_zone(name, tags, city_lo, center, lat, lon, seen, max_dist_km=MAX_DIST_KM):
    if not name or not lat or not lon:
        return False
    if is_junk(name) or is_meta(name, tags):
        return False
    if name.lower().strip() in seen or name.lower().strip() == city_lo:
        return False
    if _dist_km(center[0], center[1], float(lat), float(lon)) > max_dist_km:
        return False
    if tags:
        if tags.get("natural") in {"cape", "bay", "strait", "peak", "cliff"}:
            return False
        if tags.get("place", "") in RURAL_PLACES:
            return False
        if is_tiny(name, tags):
            return False
        if tags.get("place", "") and is_rc(name, tags):
            return False
    return True


def _dedup(zones, center, max_dist_km=MAX_DIST_KM):
    # 1. Сначала чистим названия
    for z in zones:
        z["orig_name"] = z["name"]
        z["name"] = clean_name(z["name"])

    # Приоритеты источников
    _prio = {
        "preset": 10,
        "wikidata": 9,
        "osm_suburb": 8, "osm_quarter": 8, "osm_neighbourhood": 8,
        "osm_microdistrict": 8,
    }

    def _get_prio(z):
        src = z.get("source", "")
        for k, v in _prio.items():
            if src.startswith(k):
                return v
        return 2

    # 2. Обычная дедупликация (по расстоянию и имени)
    unique_zones = []
    # Сортируем: сначала важные источники, потом остальные
    zones.sort(key=lambda x: _get_prio(x), reverse=True)

    for z in zones:
        if z["lat"] == 0 or z["lon"] == 0: continue
        if _dist_km(center[0], center[1], z["lat"], z["lon"]) > max_dist_km: continue
        if is_junk(z["name"]) or is_meta(z["name"]): continue
        
        # Фильтр по населению (если уже есть, для мелких поселков)
        # (здесь пока пропускаем, фильтр pop в конце)

        dup = False
        for e in unique_zones:
            # Если имена похожи
            if _fuzzy(z["name"], e["name"]):
                dup = True; break
            # Если точки слишком близко (< 1200м)
            if _dist_m(z["lat"], z["lon"], e["lat"], e["lon"]) < 1200:
                # Если текущая зона "важнее" (например Wikidata против Grid), заменяем
                if _get_prio(z) > _get_prio(e):
                    unique_zones.remove(e)
                    unique_zones.append(z)
                dup = True
                break
        
        if not dup:
            unique_zones.append(z)

    # 3. ЛОГИКА "АНТИ-АДМИН": Удаляем огромные районы, если есть детальные
    # Считаем, сколько у нас "хороших" микрорайонов (не admin, не district)
    micro_count = sum(1 for z in unique_zones if z.get("type") != "district" and "admin" not in z.get("source", ""))
    
    if micro_count >= 5:
        print(f"    ✨ Найдено {micro_count} микрорайонов. Удаляем административные районы...")
        # Оставляем только те, что НЕ district (или source не osm_admin)
        # Но оставляем Wikidata, так как там часто хорошие исторические районы
        unique_zones = [z for z in unique_zones 
                        if not (z.get("source", "").startswith("osm_admin"))]

    return unique_zones


def _preset_zone_specs(city_key, city_info):
    """Возвращает список спецификаций preset-зон из config."""
    specs = []
    # Новый формат: city_info["preset_zones"]
    if city_info.get("preset_zones"):
        specs.extend(city_info.get("preset_zones", []))
    # Совместимый формат: CONFIG["preset_city_zones"][city_key]
    try:
        cfg_presets = (CONFIG.get("preset_city_zones") or {}).get(city_key, [])
        if cfg_presets:
            specs.extend(cfg_presets)
    except Exception:
        pass
    return specs


def _trusted_preset_zone(item):
    if not isinstance(item, dict):
        return False
    ztype = str(item.get("type", "") or "")
    name = str(item.get("name", "") or "")
    return ztype == "district" or _looks_like_admin_district_name(name)


def _district_preset_mode(city_key, city_info):
    preset_specs = _preset_zone_specs(city_key, city_info)
    if not preset_specs:
        return False
    typed_specs = [item for item in preset_specs if isinstance(item, dict)]
    return bool(typed_specs) and all(_trusted_preset_zone(item) for item in typed_specs)


def _preset_name_map(city_key, city_info):
    name_map = {}
    for item in _preset_zone_specs(city_key, city_info):
        if isinstance(item, str):
            nm = item.strip()
        elif isinstance(item, dict):
            nm = str(item.get("name", "")).strip()
        else:
            nm = ""
        if not nm:
            continue
        clean_nm = clean_name(nm) or nm
        name_map[clean_nm.lower()] = clean_nm
    return name_map


def _preset_spec_map(city_key, city_info):
    spec_map = {}
    for item in _preset_zone_specs(city_key, city_info):
        if not isinstance(item, dict):
            continue
        nm = str(item.get("name", "")).strip()
        clean_nm = clean_name(nm) or nm
        if clean_nm:
            spec_map[clean_nm.lower()] = item
    return spec_map


def _load_saved_zone_geometries(city_key):
    saved = {}

    raw_zones = safe_json_load(f"data/{city_key}/raw/zones.json") or []
    for zone in raw_zones:
        name = clean_name(str(zone.get("name", "")).strip()) or str(zone.get("name", "")).strip()
        if not name:
            continue
        entry = saved.setdefault(name.lower(), {})
        if zone.get("geojson") and not entry.get("geojson"):
            entry["geojson"] = zone["geojson"]
        if zone.get("lat") is not None and zone.get("lon") is not None:
            entry["lat"] = zone.get("lat")
            entry["lon"] = zone.get("lon")
        if zone.get("osm_relation_id") and not entry.get("osm_relation_id"):
            entry["osm_relation_id"] = zone.get("osm_relation_id")

    geojson_fc = safe_json_load(f"data/{city_key}/processed/districts_final.geojson") or {}
    for feature in geojson_fc.get("features", []):
        props = feature.get("properties", {}) or {}
        name = clean_name(str(props.get("district", "")).strip()) or str(props.get("district", "")).strip()
        geom = feature.get("geometry")
        if not name or not geom:
            continue
        entry = saved.setdefault(name.lower(), {})
        if not entry.get("geojson"):
            entry["geojson"] = geom
        anchor = _geometry_anchor_point(geom)
        if anchor:
            entry["lat"] = anchor["lat"]
            entry["lon"] = anchor["lon"]

    return saved


def _stabilize_preset_districts(zones, city_key, city_info):
    if not zones or not _district_preset_mode(city_key, city_info):
        return zones

    preset_specs = _preset_spec_map(city_key, city_info)
    saved_geometries = _load_saved_zone_geometries(city_key)
    restored = 0
    recentered = 0

    for zone in zones:
        name = clean_name(str(zone.get("name", "")).strip()) or str(zone.get("name", "")).strip()
        if not name:
            continue
        spec = preset_specs.get(name.lower())
        if not spec or not _trusted_preset_zone(spec):
            continue

        saved = saved_geometries.get(name.lower()) or {}
        if not zone.get("geojson") and saved.get("geojson"):
            zone["geojson"] = saved["geojson"]
            zone.setdefault("geometry_restored_from_cache", True)
            if str(zone.get("source", "")) == "preset_missing":
                zone["source"] = "preset_restored"
            restored += 1

        if zone.get("geojson"):
            anchor = _geometry_anchor_point(zone["geojson"])
            if anchor:
                current_lat = zone.get("lat")
                current_lon = zone.get("lon")
                needs_center_fix = (
                    current_lat is None
                    or current_lon is None
                    or zone.get("needs_geometry")
                    or str(zone.get("source", "")).startswith("preset_")
                    or _dist_km(float(current_lat), float(current_lon), anchor["lat"], anchor["lon"]) > 25.0
                )
                if needs_center_fix:
                    zone["lat"] = round(float(anchor["lat"]), 6)
                    zone["lon"] = round(float(anchor["lon"]), 6)
                    recentered += 1
                    zone.pop("needs_geometry", None)
                    continue

        if saved.get("lat") is not None and saved.get("lon") is not None:
            if zone.get("needs_geometry") or str(zone.get("source", "")) in {"preset_missing", "preset_restored"}:
                try:
                    zone["lat"] = round(float(saved["lat"]), 6)
                    zone["lon"] = round(float(saved["lon"]), 6)
                    recentered += 1
                    zone.pop("needs_geometry", None)
                except (TypeError, ValueError):
                    pass

    if restored or recentered:
        print(f"    ♻️ Stabilize preset districts: restored={restored}, recentered={recentered}")
    return zones


def _finalize_curated_district_zones(zones, city_key, city_info, stats=None):
    if not zones:
        return []

    cn = city_info["osm_name"]
    ct = tuple(city_info["center"])

    zones = _restrict_to_preset_names(zones, city_key, city_info)
    zones = _ensure_preset_presence(zones, city_key, city_info)
    if any("population" not in z for z in zones):
        zones = est_pop(cn, zones, stats)

    zones = _final_zone_quality_filter(city_key, city_info, zones)
    zones = _fetch_geometries(zones, cn, city_center=ct, stats=stats)
    zones = _stabilize_preset_districts(zones, city_key, city_info)

    valid_zones = []
    removed_no_geom = 0
    for z in zones:
        if not z.get("geojson"):
            if z.get("lat") is not None and z.get("lon") is not None:
                z = dict(z)
                z["source"] = str(z.get("source", "") or "preset")
                if not z["source"].startswith("preset"):
                    z["source"] = f"preset_point::{z['source']}"
                valid_zones.append(z)
                print(f"    ⚠️ Оставлена preset-зона без контура: {z['name']}")
                continue
            removed_no_geom += 1
            print(f"    ❌ Убрана зона без контура: {z['name']}")
            continue
        valid_zones.append(z)
    if removed_no_geom:
        print(f"    🧹 Убрано зон без контура: {removed_no_geom}")

    if stats:
        stats.zones_found = len(valid_zones)
        stats.zones_by_source = dict(Counter(z.get("source", "?") for z in valid_zones))

    return valid_zones


def _cached_preset_zones_incomplete(cached, city_key, city_info):
    preset_specs = _preset_zone_specs(city_key, city_info)
    if not preset_specs or not cached:
        return False

    preset_name_map = _preset_name_map(city_key, city_info)
    cached_by_name = {str(z.get("name", "")).strip(): z for z in cached}
    if _district_preset_mode(city_key, city_info):
        for zone in cached:
            zone_name = str(zone.get("name", "")).strip().lower()
            if zone_name and zone_name not in preset_name_map:
                return True

    for spec in preset_specs:
        if isinstance(spec, str):
            clean_nm = clean_name(spec) or spec
            if clean_nm not in cached_by_name:
                return True
            continue

        if not isinstance(spec, dict):
            continue

        nm = str(spec.get("name", "")).strip()
        if not nm:
            continue
        clean_nm = clean_name(nm) or nm
        zone = cached_by_name.get(clean_nm)
        if zone is None:
            return True
        if (
            _trusted_preset_zone(spec)
            and not zone.get("geojson")
            and str(zone.get("source", "")) != "preset_missing"
        ):
            return True
    return False


def _restrict_to_preset_names(zones, city_key, city_info):
    name_map = _preset_name_map(city_key, city_info)
    if not name_map:
        return zones

    restricted = []
    seen = set()
    for zone in zones:
        zone_name = str(zone.get("name", "")).strip()
        canon = name_map.get(zone_name.lower())
        if not canon or canon in seen:
            continue
        zone = dict(zone)
        zone["name"] = canon
        restricted.append(zone)
        seen.add(canon)
    return restricted


def _ensure_preset_presence(zones, city_key, city_info):
    specs = _preset_zone_specs(city_key, city_info)
    if not specs:
        return zones

    center = tuple(city_info["center"])
    existing = {str(z.get("name", "")).strip().lower() for z in zones}
    filled = list(zones)

    for idx, item in enumerate(specs):
        if isinstance(item, str):
            nm = item.strip()
            ztype = "microdistrict"
            meta = {}
        elif isinstance(item, dict):
            nm = str(item.get("name", "")).strip()
            ztype = str(item.get("type", "microdistrict") or "microdistrict")
            meta = item
        else:
            continue

        clean_nm = clean_name(nm) or nm
        key = clean_nm.lower()
        if not clean_nm or key in existing:
            continue

        lat = meta.get("lat")
        lon = meta.get("lon")
        if lat is None or lon is None:
            # Сохраняем preset даже без точной геометрии, чтобы район не пропадал из UI.
            lat = round(center[0] + (idx % 3 - 1) * 0.01, 6)
            lon = round(center[1] + ((idx // 3) % 3 - 1) * 0.01, 6)

        try:
            lat = float(lat)
            lon = float(lon)
        except (TypeError, ValueError):
            lat, lon = center

        zone_extra = {}
        if meta.get("geometry_queries"):
            zone_extra["geometry_queries"] = list(meta.get("geometry_queries", []))
        if meta.get("osm_relation_id"):
            zone_extra["osm_relation_id"] = meta["osm_relation_id"]
        if meta.get("extra_relation_ids"):
            zone_extra["extra_relation_ids"] = list(meta.get("extra_relation_ids", []))
        if meta.get("subtract_relation_ids"):
            zone_extra["subtract_relation_ids"] = list(meta.get("subtract_relation_ids", []))
        filled.append({
            "name": clean_nm,
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "source": "preset_missing",
            "type": ztype,
            "orig_name": nm,
            "needs_geometry": True,
            **zone_extra,
        })
        existing.add(key)

    return filled


def _discover_preset_zones(city_key, city_info, stats=None):
    """Собирает зоны из заранее заданного списка районов (с геокодированием)."""
    specs = _preset_zone_specs(city_key, city_info)
    if not specs:
        return None

    cn = city_info["osm_name"]
    ct = tuple(city_info["center"])
    max_dist_km = _get_zone_max_dist(city_info)
    print(f"  🗺️ Зоны {city_key} (preset)...")
    zones = []
    seen = set()

    for item in specs:
        if isinstance(item, str):
            nm = item.strip()
            ztype = "microdistrict"
            meta = {}
        elif isinstance(item, dict):
            nm = str(item.get("name", "")).strip()
            ztype = item.get("type", "microdistrict")
            meta = item
        else:
            continue

        if not nm:
            continue
        trusted_district = ztype == "district" or _looks_like_admin_district_name(nm)
        key = clean_name(nm).lower().strip()
        if not key or key in seen:
            continue

        la = meta.get("lat")
        lo = meta.get("lon")
        geometry_queries = meta.get("geometry_queries") or []
        osm_rel_id = meta.get("osm_relation_id")
        extra_rel_ids = meta.get("extra_relation_ids") or []
        subtract_rel_ids = meta.get("subtract_relation_ids") or []

        # Приоритетный путь: прямой Nominatim lookup по OSM relation ID
        if osm_rel_id and not meta.get("geojson"):
            try:
                r = _session.get(
                    "https://nominatim.openstreetmap.org/lookup",
                    params={"osm_ids": f"R{osm_rel_id}", "format": "json", "polygon_geojson": 1},
                    headers={"User-Agent": "UrbanPulseApp/1.0"},
                    timeout=20,
                )
                data = r.json()
                if data and data[0].get("geojson"):
                    raw_geoj = data[0]["geojson"]
                    # Apply urban_clip_bbox if defined (clip out mountains/sea)
                    urban_bbox = meta.get("urban_clip_bbox")
                    if urban_bbox:
                        try:
                            from shapely.geometry import shape, box, mapping
                            g = shape(raw_geoj)
                            clip = box(urban_bbox[0], urban_bbox[1], urban_bbox[2], urban_bbox[3])
                            clipped = g.intersection(clip)
                            if not clipped.is_empty and clipped.area > 0.0001:
                                raw_geoj = mapping(clipped)
                        except Exception:
                            pass
                    raw_geoj = _apply_relation_overrides(
                        raw_geoj,
                        extra_relation_ids=extra_rel_ids,
                        subtract_relation_ids=subtract_rel_ids,
                        fallback_lat=la or ct[0],
                        fallback_lon=lo or ct[1],
                    )
                    meta = dict(meta)
                    meta["geojson"] = raw_geoj
                    meta["osm_type"] = "relation"
                    meta["osm_id"] = osm_rel_id
                    if la is None:
                        la = float(data[0].get("lat", ct[0]))
                    if lo is None:
                        lo = float(data[0].get("lon", ct[1]))
            except Exception:
                pass
            time.sleep(0.5)

        if geometry_queries and not meta.get("geojson"):
            combined = _fetch_geometry_by_queries(
                geometry_queries,
                zone_name=nm,
                city_name=cn,
                zone_type=ztype,
                center=ct,
                max_dist_km=max_dist_km,
                trusted_district=trusted_district,
            )
            if combined:
                combined_geo = _apply_relation_overrides(
                    combined["geojson"],
                    extra_relation_ids=extra_rel_ids,
                    subtract_relation_ids=subtract_rel_ids,
                    fallback_lat=la or combined["lat"],
                    fallback_lon=lo or combined["lon"],
                )
                meta = dict(meta)
                meta["geojson"] = combined_geo
                if la is None:
                    la = combined["lat"]
                if lo is None:
                    lo = combined["lon"]
        if la is None or lo is None:
            geo_candidates = []
            geom_candidates = []

            queries = []
            if la is None or lo is None:
                if ztype == "district" or _looks_like_admin_district_name(nm):
                    queries.extend(_district_queries(nm, cn))
                queries.extend([
                    f"{nm}, микрорайон, {cn}",
                    f"{nm}, {cn}",
                    f"{cn}, {nm}",
                ])

            for query in queries:
                gc = geo(query)
                point_limit_km = max_dist_km if not trusted_district else max(max_dist_km * 2.5, max_dist_km + 20.0)
                if gc and (
                    _text_matches_city(gc.get("display_name", ""), cn)
                    or _dist_km(ct[0], ct[1], gc["lat"], gc["lon"]) <= point_limit_km
                ):
                    geo_candidates.append(gc)

                geom = geo_polygon_nom(
                    query,
                    zone_name=nm,
                    city_name=cn,
                    zone_type=ztype,
                )
                if geom and (
                    _text_matches_city(geom.get("display_name", ""), cn)
                    or (trusted_district and _dist_km(ct[0], ct[1], geom["lat"], geom["lon"]) <= max(max_dist_km * 1.5, max_dist_km + 15.0))
                    or (not trusted_district and _dist_km(ct[0], ct[1], geom["lat"], geom["lon"]) <= max_dist_km)
                ):
                    geom_candidates.append(geom)

            if la is not None and lo is not None:
                pass
            elif geom_candidates:
                geom_candidates.sort(
                    key=lambda g: _dist_km(ct[0], ct[1], g["lat"], g["lon"])
                )
                la = geom_candidates[0]["lat"]
                lo = geom_candidates[0]["lon"]
                meta = dict(meta)
                meta["geojson"] = geom_candidates[0].get("geojson")
                meta["osm_type"] = geom_candidates[0].get("osm_type")
                meta["osm_id"] = geom_candidates[0].get("osm_id")
            elif geo_candidates:
                geo_candidates.sort(
                    key=lambda g: _dist_km(ct[0], ct[1], g["lat"], g["lon"])
                )
                la = geo_candidates[0]["lat"]
                lo = geo_candidates[0]["lon"]
            else:
                logger.info("Preset-зона не геокодирована: %s (%s)", nm, city_key)
                zones.append({
                    "name": clean_name(nm) or nm,
                    "lat": round(ct[0], 6),
                    "lon": round(ct[1], 6),
                    "source": "preset_missing",
                    "type": ztype,
                    "orig_name": nm,
                    "needs_geometry": True,
                })
                seen.add(key)
                continue

        try:
            la = float(la)
            lo = float(lo)
        except (ValueError, TypeError):
            continue

        if not trusted_district and _dist_km(ct[0], ct[1], la, lo) > max_dist_km:
            continue

        seen.add(key)
        z = {
            "name": clean_name(nm) or nm,
            "lat": round(la, 6),
            "lon": round(lo, 6),
            "source": "preset",
            "type": ztype,
            "orig_name": nm,
        }
        if osm_rel_id:
            z["osm_relation_id"] = osm_rel_id
        if geometry_queries:
            z["geometry_queries"] = list(geometry_queries)
        if meta.get("extra_relation_ids"):
            z["extra_relation_ids"] = list(meta.get("extra_relation_ids", []))
        if meta.get("subtract_relation_ids"):
            z["subtract_relation_ids"] = list(meta.get("subtract_relation_ids", []))
        if "population" in meta:
            z["population"] = meta["population"]
            z["population_source"] = "preset"
        if meta.get("geojson"):
            z["geojson"] = meta["geojson"]
            z["osm_type"] = meta.get("osm_type")
            z["osm_id"] = meta.get("osm_id")
        zones.append(z)

    if not zones:
        return []

    zones = _fetch_geometries(zones, cn, city_center=ct, stats=stats)

    # Небольшая валидация/очистка перед остальным пайплайном
    zones = _dedup(zones, ct, max_dist_km=max_dist_km)
    zones = _check_urban(zones, ct, stats)

    # Если preset не содержит population — досчитаем как обычно
    need_pop = any("population" not in z for z in zones)
    if need_pop:
        zones = est_pop(cn, zones, stats)

    # Mood как в обычном discover_zones
    if zones:
        mx = max(_dist_km(ct[0], ct[1], z["lat"], z["lon"]) for z in zones) or 1
        for z in zones:
            d = _dist_km(ct[0], ct[1], z["lat"], z["lon"])
            z["mood"] = round(max(0.15, min(0.85, 0.8 - 0.5 * d / mx)), 2)

    if stats:
        stats.zones_found = len(zones)
        stats.zones_by_source = dict(Counter(z.get("source", "?") for z in zones))

    return zones


def _filter_hybrid_parsed_zones(parsed_zones, city_key=None, city_info=None):
    """Оставляем только качественные жилые источники для hybrid merge."""
    if not parsed_zones:
        return []

    district_only_mode = bool(city_key and city_info and _district_preset_mode(city_key, city_info))
    preset_name_map = _preset_name_map(city_key, city_info) if district_only_mode else {}

    trusted_prefixes = (
        "wikidata",
        "osm_suburb",
        "osm_quarter",
        "osm_neighbourhood",
        "osm_residential",
        "osm_bbox_suburb",
        "osm_bbox_quarter",
        "osm_bbox_neighbourhood",
    )
    generic_names = {
        "центральный", "северный", "южный", "западный", "восточный",
        "ленинский", "советский", "октябрьский", "кировский",
        "первомайский", "железнодорожный", "промышленный",
        "заводской", "калининский", "московский",
    }

    result = []
    for z in parsed_zones:
        src = str(z.get("source", ""))
        name = str(z.get("name", "")).strip()
        if not name:
            continue
        if not src.startswith(trusted_prefixes):
            continue
        if district_only_mode and str(z.get("type", "") or "") != "district":
            continue
        if district_only_mode and name.lower() not in preset_name_map:
            continue
        # Отсеиваем слишком общие названия без уточнения
        if name.lower() in generic_names and len(name.split()) == 1:
            continue
        result.append(z)
    return result


def _final_zone_quality_filter(city_key, city_info, zones):
    """Финальная очистка зон от нежилых/маргинальных топонимов."""
    if not zones:
        return []

    center = tuple(city_info["center"])
    city_bl = {
        "Владивосток": {
            "минка", "мелководный", "экипажный", "подножье",
            "кэт", "рында", "канал", "житкова", "шигино", "поспелово",
        }
    }
    city_allow = {
        "Владивосток": {
            "чуркин", "луговая", "первая речка", "вторая речка", "миллионка",
            "нейбута", "заря", "снеговая падь", "эгершельд", "академгородок",
            "минный городок", "аякс", "лесной", "седанка", "котеджи",
        }
    }

    topo_bad = re.compile(
        r"^(мыс|остров|бухта|пролив|канал|порт|причал|аэропорт|пляж|гора|сопка)\b",
        re.IGNORECASE,
    )
    generic_bad = {
        "центральный", "северный", "южный", "восточный", "западный",
        # Дополнительные бессмысленные/слишком общие названия
        "квартал", "победы", "правый берег", "левый берег",
        "центр", "новый", "старый", "городок",
        "привокзальный", "пристанционный", "промышленный",
        "заводской", "железнодорожный", "первомайский",
    }

    out = []
    removed = 0
    for z in zones:
        name = str(z.get("name", "")).strip()
        lo = name.lower()
        src = str(z.get("source", ""))
        ztype = str(z.get("type", "") or "")
        pop = int(z.get("population", 0) or 0)
        dist = _dist_km(center[0], center[1], z.get("lat", 0), z.get("lon", 0))

        if ztype == "district":
            out.append(z)
            continue
        if lo in city_allow.get(city_key, set()):
            out.append(z)
            continue
        if _looks_like_admin_district_name(name) and pop >= 5000:
            out.append(z)
            continue
        if lo in city_bl.get(city_key, set()):
            removed += 1
            continue
        if topo_bad.search(name):
            removed += 1
            continue
        if lo in generic_bad and not (src.startswith("preset") or _looks_like_admin_district_name(name)):
            removed += 1
            continue
        out.append(z)

    if removed:
        print(f"    🧹 Финальная очистка зон: убрано {removed}")
    return out


# ==================== ИСТОЧНИКИ ====================

def _src_wikidata(city, center, seen, stats, max_dist_km=MAX_DIST_KM):
    """
    Wikidata SPARQL — основной источник.
    Ищет районы, микрорайоны, исторические районы города.
    """
    print(f"    [1] Wikidata...", end=" ", flush=True)
    zones = []
    cl = city.lower().strip()

    # Q123705=neighbourhood Q15715406=district_of_city
    # Q3957=town Q12813115=микрорайон Q19953632=городской_район Q4286337=suburb
    # Q253019=жилой_район Q192078=адм_терр_ед Q3413999=район_Москвы Q4388406=адм_округ
    # (Исторические районы удалены по запросу пользователя)
    sparql = f'''SELECT ?item ?itemLabel ?coord WHERE {{
      ?city rdfs:label "{city}"@ru .
      ?city wdt:P31/wdt:P279* wd:Q515 .
      ?item wdt:P131+ ?city .
      ?item wdt:P31 ?type .
      VALUES ?type {{ wd:Q123705 wd:Q15715406
                      wd:Q3957 wd:Q12813115 wd:Q19953632 wd:Q4286337
                      wd:Q253019 wd:Q192078 wd:Q3413999 wd:Q4388406 }}
      OPTIONAL {{ ?item wdt:P625 ?coord . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ru,en" . }}
    }} LIMIT 300'''

    try:
        r = _session.get("https://query.wikidata.org/sparql",
                         params={"query": sparql, "format": "json"}, timeout=30)
        r.raise_for_status()

        for b in r.json().get("results", {}).get("bindings", []):
            nm = b.get("itemLabel", {}).get("value", "")
            key = nm.lower().strip()
            clean_key = clean_name(nm).lower().strip()

            if not nm or key in seen or clean_key in seen or key == cl:
                continue
            if is_junk(nm) or is_meta(nm):
                continue

            la = lo = None
            coord = b.get("coord", {}).get("value", "")
            if coord:
                m = re.search(r"Point\(([-\d.]+)\s+([-\d.]+)\)", coord)
                if m:
                    lo, la = float(m.group(1)), float(m.group(2))

            ztype = "district" if _looks_like_admin_district_name(nm) else "microdistrict"

            if not la or not lo or _dist_km(center[0], center[1], la, lo) > max_dist_km:
                gc = None
                geo_queries = []
                if ztype == "district":
                    geo_queries.append(f"{nm} район, {city}")
                geo_queries.extend([f"{nm}, {city}", f"{city}, {nm}"])
                for query in geo_queries:
                    gc = geo(query)
                    if gc and _dist_km(center[0], center[1], gc["lat"], gc["lon"]) <= max_dist_km:
                        la, lo = gc["lat"], gc["lon"]
                        break
                if not la or not lo:
                    continue

            seen.add(key)
            seen.add(clean_key)
            zones.append({"name": clean_name(nm) or nm,
                          "lat": round(la, 6), "lon": round(lo, 6),
                          "source": "wikidata", "type": ztype})

    except Exception as e:
        print(f"(ошибка: {str(e)[:50]})", end=" ")
        if stats: stats.inc_error("wikidata_error")

    print(f"→ {len(zones)}")
    return zones


def _wiki_parse_wikitext(title, stats=None):
    if stats:
        stats.inc_api("wikipedia")
    r = _session.get(
        "https://ru.wikipedia.org/w/api.php",
        params={
            "action": "parse",
            "page": title,
            "prop": "wikitext",
            "format": "json",
            "redirects": 1,
        },
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    return (data.get("parse", {}).get("wikitext", {}) or {}).get("*", "")


def _wiki_search_titles(query, stats=None, limit=10):
    if stats:
        stats.inc_api("wikipedia")
    r = _session.get(
        "https://ru.wikipedia.org/w/api.php",
        params={
            "action": "query",
            "list": "search",
            "srsearch": query,
            "srlimit": limit,
            "format": "json",
        },
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    return [str(x.get("title", "")).strip() for x in data.get("query", {}).get("search", [])]


def _wiki_is_list_title(title):
    lo = title.lower().strip()
    return bool(re.search(
        r"(районы|микрорайоны|административн.*деление|внутригородские районы|список районов)",
        lo,
        re.IGNORECASE,
    ))


def _wiki_title_matches_city(title, city):
    title_lo = str(title or "").lower().strip()
    city_lo = str(city or "").lower().strip()
    if not title_lo or not city_lo:
        return False
    
    # Исключаем страницы областей/краев/республик для обычных городов
    if re.search(r"(област|края|республик|округа|района)", title_lo):
        if city_lo not in ["москва", "санкт-петербург", "севастополь"]:
            return False

    city_words = [w for w in re.split(r"[\s\-]+", city_lo) if len(w) > 2]
    if city_lo in title_lo:
        return True
    city_stems = {w[: max(4, len(w) - 2)] for w in city_words if len(w) >= 5}
    if city_stems and all(any(stem in part for part in re.split(r"[\s\-]+", title_lo)) for stem in city_stems):
        return True
    return all(word in title_lo for word in city_words)


def _wiki_bad_zone_name(name, city=None):
    lo = str(name or "").lower().strip()
    if not lo:
        return True
    if city and lo == str(city).lower().strip():
        return True
    if _wiki_is_list_title(lo):
        return True
    if re.search(r"^\d+\s+район", lo):
        return True
    if re.search(r"^\d+\s+(района|районов|микрорайона|микрорайонов)$", lo):
        return True
    if re.search(r"(thumb\||файл:|file:|category:|категория:|commons:|px\||right\||left\|)", lo):
        return True
    if re.search(r"(век|год|года|уровнем моря|епархия|обсерватория|восстание|система высот)", lo):
        return True
    if re.search(r"(улица|проспект|переулок|набережн|мост|река|остров|озеро|площадь|село|деревн|поселок)", lo):
        return True
    if re.search(r"(районам|районов|районе|районах|микрорайонам|микрорайонов|внутригородск)", lo) and not _looks_like_admin_district_name(lo):
        return True
    if re.search(r"(тр[её]м|четыр[её]м|пяти|шести|семи|восьми|девяти)\b", lo):
        return True
    if re.search(r"^[\d\s\-–.,]+$", lo):
        return True
    return False


def _wiki_line_looks_like_zone(line):
    lo = str(line or "").lower().strip()
    if not lo:
        return False
    if re.search(r"\[\[[^\]]*(район|микрорайон|округ)", lo):
        return True
    if re.search(r"(район|микрорайон|округ|административн)", lo):
        return True
    return False


def _wiki_zone_name_looks_plausible(name):
    # Пользователь попросил мягкий фильтр: пропускаем всё,
    # что прошло предыдущие проверки как ссылка в разделе районов
    return True


def _wiki_extract_zone_names(wikitext, city=None, page_title=None):
    if not wikitext:
        return []

    zone_heading = re.compile(
        r"^==+\s*(административное деление|районы|микрорайоны|внутригородские районы|территориальное деление)\s*==+\s*$",
        re.IGNORECASE,
    )
    any_heading = re.compile(r"^==+.*==+\s*$")
    link_re = re.compile(r"\[\[([^\]|#]+)(?:#[^\]|]+)?(?:\|([^\]]+))?\]\]")

    names = []
    in_zone_section = False

    def _append_links(line):
        if "{{" in line and not re.search(r"^\s*[\*\#\|\!]", line):
            return
        if not _wiki_line_looks_like_zone(line):
            return
        appended = False
        for m in link_re.finditer(line):
            for raw in (m.group(2), m.group(1)):
                if not raw:
                    continue
                nm = raw.replace("«", "").replace("»", "").replace('"', '').replace("'", "").strip()
                if not nm or ":" in nm:
                    continue
                cleaned = clean_name(nm)
                if _wiki_bad_zone_name(cleaned, city=city):
                    continue
                if not _wiki_zone_name_looks_plausible(cleaned):
                    continue
                if page_title and cleaned.lower().strip() == page_title.lower().strip():
                    continue
                names.append(cleaned)
                appended = True
                break
            if appended:
                break

    for line in wikitext.splitlines():
        s = line.strip()
        if zone_heading.match(s):
            in_zone_section = True
            continue
        if any_heading.match(s):
            in_zone_section = False
            continue
        if in_zone_section and re.match(r"^[\*\#\|\!]", s):
            _append_links(s)

    # Для страниц-списков берём только строки списков/таблиц.
    if not names:
        for line in wikitext.splitlines():
            s = line.strip()
            if not re.match(r"^[\*\#\|\!]", s):
                continue
            if not _wiki_line_looks_like_zone(s):
                continue
            _append_links(s)

    out = []
    seen_local = set()
    for n in names:
        lo = n.lower().strip()
        if lo and lo not in seen_local and not _wiki_bad_zone_name(n, city=city):
            seen_local.add(lo)
            out.append(n)
    return out


def _wiki_candidate_titles(city, stats=None):
    queries = [
        f'intitle:"Административно-территориальное деление" "{city}"',
        f'intitle:"Районы" "{city}"',
        f'intitle:"Список районов" "{city}"',
        f'intitle:"Микрорайоны" "{city}"',
        f'intitle:"Административное деление" "{city}"',
        f'"районы {city}"',
        f'"микрорайоны {city}"',
    ]

    titles = []
    seen = set()
    for query in queries:
        try:
            found = _wiki_search_titles(query, stats=stats, limit=8)
        except Exception:
            continue
        for title in found:
            key = title.lower().strip()
            if not key or key in seen:
                continue
            if not _wiki_is_list_title(title):
                continue
            if not _wiki_title_matches_city(title, city):
                continue
            seen.add(key)
            titles.append(title)

    # Точный fallback на старые варианты именования страниц.
    for title in (
        f"Районы {city}",
        f"Список районов {city}",
        f"Микрорайоны {city}",
        f"Административное деление {city}",
    ):
        key = title.lower().strip()
        if key not in seen:
            titles.append(title)
            seen.add(key)
    return titles


def _src_wikipedia(city, center, seen, stats, max_dist_km=MAX_DIST_KM):
    """
    Wikipedia API — fallback/дополнение:
    парсим именно страницы-списки районов/микрорайонов.
    """
    print(f"    [2] Wikipedia...", end=" ", flush=True)
    zones = []
    city_key = city.lower().strip()

    titles = _wiki_candidate_titles(city, stats)
    if not titles:
        titles = [f"Районы {city}", f"Список районов {city}", f"Микрорайоны {city}"]

    candidate_names = []
    candidate_seen = set()
    for title in titles:
        try:
            wt = _wiki_parse_wikitext(title, stats)
            if wt:
                for nm in _wiki_extract_zone_names(wt, city=city, page_title=title):
                    key = clean_name(nm).lower().strip()
                    if not key or key in candidate_seen:
                        continue
                    candidate_seen.add(key)
                    candidate_names.append(nm)
        except Exception:
            continue

    if len(candidate_names) > 80:
        logger.info("Wikipedia candidates for %s trimmed: %d -> 80", city, len(candidate_names))
        candidate_names = candidate_names[:80]

    for nm in candidate_names:
        key = nm.lower().strip()
        clean_key = clean_name(nm).lower().strip()
        if not key or key == city_key or key in seen or clean_key in seen:
            continue
        if _wiki_bad_zone_name(nm, city=city):
            continue
        if is_junk(nm) or is_meta(nm):
            continue

        ztype = "district" if _looks_like_admin_district_name(nm) else "microdistrict"
        gc = None
        geo_queries = []
        if ztype == "district":
            geo_queries.append(f"{nm} район, {city}")
        geo_queries.extend([f"{nm}, {city}", f"{city}, {nm}"])
        for query in geo_queries:
            gc = geo(query)
            if gc and _dist_km(center[0], center[1], gc["lat"], gc["lon"]) <= max_dist_km:
                break
        if not gc:
            continue
        if _dist_km(center[0], center[1], gc["lat"], gc["lon"]) > max_dist_km:
            continue

        seen.add(key)
        seen.add(clean_key)
        zones.append({
            "name": clean_name(nm) or nm,
            "lat": round(gc["lat"], 6),
            "lon": round(gc["lon"], 6),
            "source": "wikipedia",
            "type": ztype,
        })

    print(f"→ {len(zones)}")
    return zones



def _fetch_geometries(zones, city_name, city_center=None, stats=None):
    """Выгружает полигоны районов через Nominatim."""
    print(f"    🗺️ Скачивание полигонов границ...", end=" ", flush=True)
    fetched = 0
    for z in zones:
        if z.get("geojson"):
            continue
        nm = z["name"]
        is_district = str(z.get("type", "") or "") == "district" or _looks_like_admin_district_name(nm)

        # Приоритетный путь: прямой lookup по OSM relation ID (точный высокодетальный полигон)
        osm_rel_id = z.get("osm_relation_id")
        extra_rel_ids = z.get("extra_relation_ids") or []
        subtract_rel_ids = z.get("subtract_relation_ids") or []
        if osm_rel_id:
            try:
                rel_data = _lookup_relation_geometry(osm_rel_id, fallback_lat=z["lat"], fallback_lon=z["lon"])
                if rel_data and rel_data.get("geojson"):
                    geom = rel_data["geojson"]
                    # Apply urban_clip_bbox if defined (clip out mountains/sea areas)
                    urban_bbox = z.get("urban_clip_bbox")
                    if urban_bbox:
                        try:
                            from shapely.geometry import shape, box, mapping
                            g = shape(geom)
                            clip = box(urban_bbox[0], urban_bbox[1], urban_bbox[2], urban_bbox[3])
                            clipped = g.intersection(clip)
                            if not clipped.is_empty and clipped.area > 0.0001:
                                geom = mapping(clipped)
                        except Exception:
                            pass
                    geom = _apply_relation_overrides(
                        geom,
                        extra_relation_ids=extra_rel_ids,
                        subtract_relation_ids=subtract_rel_ids,
                        fallback_lat=z["lat"],
                        fallback_lon=z["lon"],
                    )
                    lat = float(rel_data.get("lat", z["lat"]))
                    lon = float(rel_data.get("lon", z["lon"]))
                    z["geojson"] = geom
                    z["lat"] = lat
                    z["lon"] = lon
                    z["osm_type"] = "relation"
                    z["osm_id"] = osm_rel_id
                    fetched += 1
                    time.sleep(0.5)
                    continue
            except Exception:
                pass
            time.sleep(0.3)

        if z.get("geometry_queries"):
            combined = _fetch_geometry_by_queries(
                z["geometry_queries"],
                zone_name=nm,
                city_name=city_name,
                zone_type=z.get("type"),
                center=(z.get("lat"), z.get("lon")),
                max_dist_km=MAX_ZONE_DISTANCE_KM,
                trusted_district=is_district,
            )
            if combined:
                z["geojson"] = _apply_relation_overrides(
                    combined["geojson"],
                    extra_relation_ids=extra_rel_ids,
                    subtract_relation_ids=subtract_rel_ids,
                    fallback_lat=combined["lat"],
                    fallback_lon=combined["lon"],
                )
                z["lat"] = combined["lat"]
                z["lon"] = combined["lon"]
                fetched += 1
                continue
        queries = []
        if is_district:
            queries.extend(_district_queries(nm, city_name))
        queries.extend([
            f"{nm}, {city_name}",
            f"{city_name}, {nm}",
            nm,
        ])

        geom_data = None
        for query in queries:
            geom_data = geo_polygon_nom(
                query,
                zone_name=nm,
                city_name=city_name,
                zone_type=z.get("type"),
            )
            if not geom_data or not geom_data.get("geojson"):
                continue
            if city_name and not _text_matches_city(geom_data.get("display_name", ""), city_name):
                if city_center is None:
                    geom_data = None
                    continue
                city_limit_km = 20.0 if is_district else 12.0
                if _dist_km(city_center[0], city_center[1], geom_data["lat"], geom_data["lon"]) > city_limit_km:
                    geom_data = None
                    continue
            # Если полигон слишком далеко от исходной точки, скорее всего это не тот объект.
            max_geom_shift_km = 60.0 if is_district else 15.0
            if _dist_km(z["lat"], z["lon"], geom_data["lat"], geom_data["lon"]) > max_geom_shift_km:
                geom_data = None
                continue
            break

        if geom_data and geom_data.get("geojson"):
            z["geojson"] = geom_data["geojson"]
            z["osm_type"] = geom_data["osm_type"]
            z["osm_id"] = geom_data["osm_id"]
            z["lat"] = geom_data["lat"]
            z["lon"] = geom_data["lon"]
            fetched += 1
                
    print(f"загружено {fetched}/{len(zones)}")
    return zones



# ==================== КООРДИНАТЫ ====================

def _fix_coords(zones, city, center, max_dist_km=MAX_DIST_KM):
    """Исправление координат через геокодирование."""
    print(f"    📍 Координаты ({len(zones)})...", end=" ", flush=True)

    def _fix_one(z):
        gc = None
        queries = []
        if str(z.get("type", "")) == "district" or _looks_like_admin_district_name(z["name"]):
            queries.append(f"{z['name']} район, {city}")
        queries.extend([f"{z['name']}, {city}", f"{city}, {z['name']}"])
        for query in queries:
            gc = geo(query)
            if gc:
                break
        if not gc:
            return z
        if _dist_km(center[0], center[1], gc["lat"], gc["lon"]) > max_dist_km:
            return z
        diff = _dist_km(z["lat"], z["lon"], gc["lat"], gc["lon"])
        if diff > 1.5:
            z["lat"] = round(gc["lat"], 6)
            z["lon"] = round(gc["lon"], 6)
            z["coord_fixed"] = True
        return z

    workers = 4 if _has_ya() else 1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        zones = list(pool.map(_fix_one, zones))

    zones = [z for z in zones
             if _dist_km(center[0], center[1], z["lat"], z["lon"]) <= max_dist_km]
    corrected = sum(1 for z in zones if z.get("coord_fixed"))
    print(f"исправлено {corrected}")
    return zones


# ==================== УРБАНИЗАЦИЯ ====================

def _check_urban(zones, center, stats):
    """
    Проверяем наличие зданий.
    НЕ проверяем administrative districts (type=district) —
    их центр может быть в реке.
    """
    print(f"    🏠 Урбанизация...", end=" ", flush=True)
    result = []
    removed = 0

    for z in zones:
        # Адм. районы — НЕ проверяем
        if z.get("type") == "district":
            result.append(z)
            continue

        d = _dist_km(center[0], center[1], z["lat"], z["lon"])
        src = z.get("source", "")
        is_hub = src.startswith("osm_hub")
        # Ближе 5км — доверяем
        if d <= 5 and not is_hub:
            result.append(z)
            continue

        # Для hub-источников проверяем всегда (они часто не жилые ориентиры)
        check_radius = 700 if is_hub else 500
        min_buildings = 15 if is_hub else 10
        bb = _bbox(z["lat"], z["lon"], check_radius)
        q = f'[out:json][timeout:15];way["building"]({bb});out count;'
        data = qop(q, 20, stats)
        bld = _parse_count(data)

        if bld < min_buildings:
            removed += 1
            logger.info("Не город: %s (%d зданий, %.1fкм)", z["name"], bld, d)
            continue

        result.append(z)
        time.sleep(0.2)

    print(f"убрано {removed}")
    return result


# ==================== ГЛАВНАЯ ====================

def _discover_zones_parsed_only(city_info, stats=None):
    """Парсинг зон: Wikidata + Wikipedia fallback."""
    cn = city_info["osm_name"]
    ct = tuple(city_info["center"])
    max_dist_km = _get_zone_max_dist(city_info)
    seen = set()

    all_zones = []
    try:
        all_zones.extend(_src_wikidata(cn, ct, seen, stats, max_dist_km=max_dist_km))
    except Exception:
        pass
    try:
        all_zones.extend(_src_wikipedia(cn, ct, seen, stats, max_dist_km=max_dist_km))
    except Exception:
        pass

    print(f"    Сырых: {len(all_zones)}")

    all_zones = _fix_coords(all_zones, cn, ct, max_dist_km=max_dist_km)
    all_zones = _dedup(all_zones, ct, max_dist_km=max_dist_km)
    print(f"    После дедупа: {len(all_zones)}")
    all_zones = _check_urban(all_zones, ct, stats)
    all_zones = est_pop(cn, all_zones, stats)

    MIN_POP = 3000
    before = len(all_zones)
    all_zones = [z for z in all_zones if z.get("population", 0) >= MIN_POP]
    if len(all_zones) < before:
        print(f"    Убрано мелких (<{MIN_POP}): {before - len(all_zones)}")

    if all_zones:
        mx = max(_dist_km(ct[0], ct[1], z["lat"], z["lon"]) for z in all_zones) or 1
        for z in all_zones:
            d = _dist_km(ct[0], ct[1], z["lat"], z["lon"])
            z["mood"] = round(max(0.15, min(0.85, 0.8 - 0.5 * d / mx)), 2)

    return all_zones


def discover_zones(city_key, city_info, stats=None):
    """
    v6: OSM area + OSM bbox + Wikidata + сетка.
    """
    print(f"  🗺️ Зоны {city_key}...")
    os.makedirs(f"data/{city_key}/raw", exist_ok=True)
    cf = f"data/{city_key}/raw/zones.json"
    has_preset_specs = bool(_preset_zone_specs(city_key, city_info))
    zones_mode = city_info.get("zones_mode") or (
        "hybrid" if has_preset_specs else "parse"
    )
    ct = tuple(city_info["center"])
    cn = city_info["osm_name"]

    use_cache = not (zones_mode in ("hybrid", "preset_plus_parse") and has_preset_specs)

    if use_cache:
        cached = safe_json_load(cf)
        if cached is not None:
            if not cached:
                print("    Кэш пустой — пересобираю")
                try:
                    os.unlink(cf)
                except OSError:
                    pass
            elif any(is_meta(z["name"]) or is_junk(z["name"]) for z in cached):
                os.unlink(cf)
            elif any(
                str(z.get("source", "")).startswith("wikipedia")
                and _wiki_bad_zone_name(z.get("name", ""), city=cn)
                for z in cached
            ):
                print("    Кэш Wikipedia содержит мусор — пересобираю")
                os.unlink(cf)
            elif _cached_preset_zones_incomplete(cached, city_key, city_info):
                print("    Кэш preset-зон неполный — пересобираю")
                os.unlink(cf)
            else:
                print(f"    Кэш ({len(cached)})")
                if stats:
                    stats.cache_hits += 1
                return cached

    preset = None
    if zones_mode in ("preset", "preset_or_parse", "hybrid", "preset_plus_parse") and has_preset_specs:
        preset = _discover_preset_zones(city_key, city_info, stats)
        if _district_preset_mode(city_key, city_info) and preset:
            print(f"    ✅ curated district preset: {len(preset)}")
            finalized = _finalize_curated_district_zones(preset, city_key, city_info, stats)
            safe_json_dump(finalized, cf)
            return finalized
        if zones_mode == "preset":
            if preset:
                print(f"    ✅ preset-зоны: {len(preset)}")
                safe_json_dump(preset, cf)
                return preset
            print("    ⚠️ preset-зоны пустые")
            return []
        if zones_mode == "preset_or_parse" and preset:
            print(f"    ✅ preset-зоны: {len(preset)}")
            safe_json_dump(preset, cf)
            return preset

    parsed = _discover_zones_parsed_only(city_info, stats)
    all_zones = parsed
    if zones_mode in ("hybrid", "preset_plus_parse") and preset:
        parsed_filtered = _filter_hybrid_parsed_zones(parsed, city_key=city_key, city_info=city_info)
        print(
            f"    🔀 Гибрид: preset {len(preset)} + "
            f"parse {len(parsed)} -> trusted {len(parsed_filtered)}"
        )
        all_zones = _dedup((preset or []) + (parsed_filtered or []), ct)
        if _district_preset_mode(city_key, city_info):
            all_zones = _restrict_to_preset_names(all_zones, city_key, city_info)
            all_zones = _ensure_preset_presence(all_zones, city_key, city_info)
        # пересчитаем mood после merge
        if all_zones:
            mx = max(_dist_km(ct[0], ct[1], z["lat"], z["lon"]) for z in all_zones) or 1
            for z in all_zones:
                d = _dist_km(ct[0], ct[1], z["lat"], z["lon"])
                z["mood"] = round(max(0.15, min(0.85, 0.8 - 0.5 * d / mx)), 2)

    if _district_preset_mode(city_key, city_info):
        all_zones = _finalize_curated_district_zones(all_zones, city_key, city_info, stats)
    else:
        if any("population" not in z for z in all_zones):
            all_zones = est_pop(cn, all_zones, stats)

        all_zones = _final_zone_quality_filter(city_key, city_info, all_zones)
        all_zones = _fetch_geometries(all_zones, city_info["osm_name"], city_center=ct, stats=stats)
        all_zones = _stabilize_preset_districts(all_zones, city_key, city_info)

        # Обязательная валидация на наличие контура
        valid_zones = []
        removed_no_geom = 0
        for z in all_zones:
            if not z.get("geojson"):
                removed_no_geom += 1
                print(f"    ❌ Убрана зона без контура: {z['name']}")
                continue
            valid_zones.append(z)
        if removed_no_geom:
            print(f"    🧹 Убрано зон без контура: {removed_no_geom}")
        all_zones = valid_zones

    safe_json_dump(all_zones, cf)

    # Статистика
    sources = Counter(z.get("source", "?") for z in all_zones)
    if stats:
        stats.zones_found = len(all_zones)
        stats.zones_by_source = dict(sources)

    # Вывод красивой статистики в консоль
    center_z = sum(1 for z in all_zones
                   if _dist_km(ct[0], ct[1], z["lat"], z["lon"]) <= 3)
    mid_z = sum(1 for z in all_zones
                if 3 < _dist_km(ct[0], ct[1], z["lat"], z["lon"]) <= 7)
    outer_z = sum(1 for z in all_zones
                  if _dist_km(ct[0], ct[1], z["lat"], z["lon"]) > 7)

    print(f"    ✅ {len(all_zones)} зон [{dict(sources)}]")
    print(f"       Покрытие: 0-3км={center_z}, 3-7км={mid_z}, 7+км={outer_z}")
    for z in all_zones:
        d = _dist_km(ct[0], ct[1], z["lat"], z["lon"])
        fix = " 📍" if z.get("coord_fixed") else ""
        print(f"       • {z['name']} ({d:.1f}км, pop={z['population']:,}, "
              f"{z.get('source', '?')}){fix}")

    return all_zones




# ============================================================
# ИНФРАСТРУКТУРА (ОБНОВЛЕННАЯ)
# ============================================================

_AM = {
    "school": "schools", 
    "kindergarten": "kindergartens", 
    "hospital": "hospitals",
    "clinic": "clinics", 
    "pharmacy": "pharmacies", 
    "cafe": "cafes",
    "restaurant": "restaurants", 
    "bank": "banks", 
    "library": "libraries",
    "cinema": "cinemas", 
    "theatre": "theatres",
    "post_office": "post_offices",  # <--- НОВОЕ
    "arts_centre": "culture"        # <--- НОВОЕ
}
# Расширяем типы магазинов
_SH = {
    "convenience": "shops", 
    "supermarket": "supermarkets",
    "mall": "malls",                # <--- НОВОЕ
    "department_store": "malls"
}
_LE = {
    "park": "parks", 
    "playground": "playgrounds", 
    "fitness_centre": "fitness",
    "sports_centre": "fitness",     # <--- НОВОЕ
    "pitch": "sport_fields"         # <--- НОВОЕ
}


def collect_infra(city_key, zones, stats=None):
    print(f"  🏗️ Инфраструктура...")
    cf = f"data/{city_key}/raw/infrastructure.json"
    
    # Если кэш есть, загружаем
    cached = safe_json_load(cf)
    if cached is not None:
        print("    Кэш")
        if stats: stats.cache_hits += 1
        infra = cached
    else:
        # Собираем данные
        lats = [z["lat"] for z in zones]
        lons = [z["lon"] for z in zones]
        m = 0.03 # Чуть больше запас границ
        bb = f"{min(lats)-m},{min(lons)-m},{max(lats)+m},{max(lons)+m}"
        
        al = "|".join(_AM.keys())
        # Запрос стал сложнее, чтобы захватить всё
        q = f'''[out:json][timeout:120];
        (
         node["amenity"~"{al}"]({bb});
         way["amenity"~"{al}"]({bb});
         
         node["shop"~"convenience|supermarket|mall|department_store"]({bb});
         way["shop"~"convenience|supermarket|mall|department_store"]({bb});
         
         node["highway"="bus_stop"]({bb});
         
         way["leisure"~"park|playground|fitness_centre|sports_centre|pitch"]({bb});
         relation["leisure"="park"]({bb});
         node["leisure"~"playground|fitness_centre|sports_centre|pitch"]({bb});
        );
        out center;'''
        
        print(f"    Запрос...", end=" ", flush=True)
        data = qop(q, 150, stats) # Увеличенный таймаут
        
        # Инициализация списков
        infra = {v: [] for v in _AM.values()}
        infra.update({v: [] for v in _SH.values()})
        infra.update({v: [] for v in _LE.values()})
        infra["bus_stops"] = []
        
        if data:
            for e in data.get("elements", []):
                t = e.get("tags", {})
                la = e.get("lat") or e.get("center", {}).get("lat")
                lo = e.get("lon") or e.get("center", {}).get("lon")
                if not la or not lo: continue
                
                p = {"lat": round(float(la), 6), "lon": round(float(lo), 6), "name": t.get("name", "")}
                
                # Распределение по категориям
                a = t.get("amenity", "")
                if a in _AM: infra[_AM[a]].append(p)
                
                s = t.get("shop", "")
                if s in _SH: infra[_SH[s]].append(p)
                
                l = t.get("leisure", "")
                if l in _LE: infra[_LE[l]].append(p)
                
                if t.get("highway") == "bus_stop": infra["bus_stops"].append(p)
                
            print(f"→ {sum(len(v) for v in infra.values())}")
        else:
            print("✗")
        safe_json_dump(infra, cf)

    # Построение индексов для быстрого поиска (Grid Index вместо перебора всех точек)
    sp = {}
    for k, objs in infra.items():
        if objs: sp[k] = _build_idx(objs)

    rows = []
    for z in zones:
        row = {
            "district": z["name"],
            "lat": z["lat"],
            "lon": z["lon"],
            "population": z["population"],
            "zone_source": z.get("source", ""),
            "zone_type": z.get("type", ""),
            "zone_orig_name": z.get("orig_name", z.get("name", "")),
        }
        
        # Считаем объекты в радиусе 1.5 км
        for k in infra:
            c = _count_near(*sp[k], z["lat"], z["lon"], 1500) if k in sp else 0
            row[f"{k}_count"] = c
            # Плотность на 1000 человек (как в вашем коде)
            pop_k = max(1, z["population"] / 1000)
            row[f"{k}_per_1000"] = round(c / pop_k, 3)
            
        rows.append(row)
        
    df = pd.DataFrame(rows)
    df.to_csv(f"data/{city_key}/processed/infrastructure.csv", index=False)
    print(f"    ✅ {len(df)} зон")
    return df

# ============================================================
# ЭКОЛОГИЯ v4 — PERCENTILE-BASED ЗЕЛЕНЬ
# ============================================================


def _owm_air(lat, lon):
    if not _has_owm(): return None
    _owm_rl.wait()
    try:
        r = _session.get("https://api.openweathermap.org/data/2.5/air_pollution",
                         params={"lat": lat, "lon": lon, "appid": OWM_KEY}, timeout=10)
        r.raise_for_status()
        d = r.json()
        if d.get("list"):
            i = d["list"][0]
            c = i.get("components", {})
            return {"aqi": i["main"].get("aqi", 3), "pm2_5": round(c.get("pm2_5", 0), 1),
                    "pm10": round(c.get("pm10", 0), 1), "no2": round(c.get("no2", 0), 1)}
    except Exception:
        pass
    return None


def _eco_batch(zones, stats=None):
    lats = [z["lat"] for z in zones]
    lons = [z["lon"] for z in zones]
    m = 0.025
    bb = f"{min(lats)-m},{min(lons)-m},{max(lats)+m},{max(lons)+m}"
    q = f'''[out:json][timeout:120];
    (way["leisure"~"park|garden|nature_reserve"]({bb});
     way["landuse"~"forest|grass|meadow|recreation_ground|village_green|orchard"]({bb});
     way["natural"~"wood|grassland|scrub|wetland|water"]({bb});
     relation["leisure"="park"]({bb});
     relation["landuse"~"forest|grass|meadow"]({bb});
     relation["natural"~"wood|water"]({bb});
     way["landuse"="industrial"]({bb});
     node["man_made"~"chimney|works"]({bb});
     way["highway"~"motorway|trunk|primary"]({bb});
     way["highway"~"secondary|tertiary"]({bb});
     way["railway"~"rail|light_rail"]({bb});
    );out center tags;'''
    print(f"    Батч...", end=" ", flush=True)
    data = qop(q, 150, stats)
    if not data:
        print("✗"); return None
    r = {"green": [], "water": [], "industrial": [], "major": [], "secondary": []}
    for e in data.get("elements", []):
        t = e.get("tags", {})
        la = e.get("lat") or e.get("center", {}).get("lat")
        lo = e.get("lon") or e.get("center", {}).get("lon")
        if not la or not lo: continue
        p = {"lat": float(la), "lon": float(lo)}
        lei = t.get("leisure", "")
        nat = t.get("natural", "")
        lu = t.get("landuse", "")
        hw = t.get("highway", "")
        rw = t.get("railway", "")
        if lei in ("park", "garden", "nature_reserve") or nat in ("wood", "grassland", "scrub", "wetland") \
                or lu in ("forest", "grass", "meadow", "recreation_ground", "village_green", "orchard"):
            r["green"].append(p)
        elif nat == "water":
            r["water"].append(p)
        elif lu == "industrial" or t.get("man_made"):
            r["industrial"].append(p)
        elif hw in ("motorway", "trunk", "primary") or rw in ("rail", "light_rail"):
            r["major"].append(p)
        elif hw in ("secondary", "tertiary"):
            r["secondary"].append(p)
    cts = {k: len(v) for k, v in r.items()}
    print(f"→ {sum(cts.values())} ({cts})")
    return r


# ============================================================
# ЗАМЕНИТЕ _eco_zone и _green_percentile_normalize в вашем файле
# ============================================================


def _eco_zone(lat, lon, idxs, _unused):
    """Экология v7 — финальные правки шума и AQI."""
    gi, wi, ii, mi, si = idxs

    # === Зелёные объекты ===
    green_objs = _near_with_dist(gi[0], gi[1], lat, lon, 1500)
    water_objs = _near_with_dist(wi[0], wi[1], lat, lon, 1500)

    gs = 0.0
    for _, d in green_objs:
        gs += max(0.1, 1.0 - d / 1500)
    for _, d in water_objs:
        gs += max(0.1, 1.0 - d / 1500) * 0.7

    # === Загрязнители ===
    major_objs = _near_with_dist(mi[0], mi[1], lat, lon, 1200)
    sec_objs = _near_with_dist(si[0], si[1], lat, lon, 800)
    ind_objs = _near_with_dist(ii[0], ii[1], lat, lon, 1500)

    # === Штраф зелени ===
    road_penalty = 0.0
    for _, d in major_objs:
        road_penalty += 2.0 * math.exp(-d / 400)
    for _, d in ind_objs:
        road_penalty += 3.0 * math.exp(-d / 500)

    quality_factor = max(0.15, 1.0 - road_penalty / (road_penalty + 12.0))
    effective_green = gs * quality_factor

    # === Шум v7: двойное логарифмическое сжатие ===
    noise = 38.0
    road_noise_raw = 0.0
    for _, d in major_objs:
        road_noise_raw += 8.0 * math.exp(-d / 350)
    for _, d in sec_objs:
        road_noise_raw += 3.0 * math.exp(-d / 250)
    for _, d in ind_objs:
        road_noise_raw += 2.0 * math.exp(-d / 400)

    # Двойное сжатие: log(1 + log(1 + x))
    # raw=1→+5, raw=10→+16, raw=50→+24, raw=200→+30
    if road_noise_raw > 0:
        compressed = math.log1p(road_noise_raw / 3.0)
        noise += 10.0 * math.log1p(compressed)

    # Зелень демпфирует
    noise -= min(4.0, effective_green * 0.12)
    noise = round(max(35.0, min(72.0, noise + np.random.normal(0, 1.2))), 1)

    # === AQI v7: непрерывная шкала с округлением ===
    raw_pollution = 0.0
    for _, d in major_objs:
        raw_pollution += 1.8 * math.exp(-d / 400)
    for _, d in ind_objs:
        raw_pollution += 3.0 * math.exp(-d / 600)
    for _, d in sec_objs:
        raw_pollution += 0.5 * math.exp(-d / 300)

    # Двойное логарифмическое сжатие
    if raw_pollution > 0:
        pollution = 3.5 * math.log1p(math.log1p(raw_pollution / 2.0))
    else:
        pollution = 0.0

    # Зелень снижает
    pollution = max(0, pollution - effective_green * 0.04)

    # Фоновый уровень города (не бывает AQI=1 в городе с промышленностью)
    base_pollution = 0.3  # городской фон
    pollution = max(base_pollution, pollution)

    # Непрерывное AQI с округлением
    # pollution: 0..1 → AQI 1, 1..2.2 → AQI 2, 2.2..3.5 → AQI 3,
    #            3.5..5 → AQI 4, >5 → AQI 5
    if pollution <= 1.0:
        aqi = 1
    elif pollution <= 2.2:
        aqi = 2
    elif pollution <= 3.5:
        aqi = 3
    elif pollution <= 5.0:
        aqi = 4
    else:
        aqi = 5

    pm25_base = {1: 6, 2: 14, 3: 24, 4: 38, 5: 55}
    pm10_base = {1: 12, 2: 25, 3: 42, 4: 65, 5: 90}
    no2_base = {1: 10, 2: 22, 3: 42, 4: 68, 5: 100}

    return {
        "green_score_raw": round(gs, 2),
        "effective_green": round(effective_green, 2),
        "quality_factor": round(quality_factor, 3),
        "road_penalty": round(road_penalty, 2),
        "raw_pollution": round(raw_pollution, 2),
        "pollution_compressed": round(pollution, 2),
        "road_noise_raw": round(road_noise_raw, 2),
        "aqi": aqi,
        "pm2_5": max(1, round(pm25_base[aqi] + np.random.normal(0, 2), 1)),
        "pm10": max(2, round(pm10_base[aqi] + np.random.normal(0, 3), 1)),
        "no2": max(1, round(no2_base[aqi] + np.random.normal(0, 3), 1)),
        "noise_level_db": noise,
        "air_source": "v7",
        "green_obj": len(green_objs),
        "water_obj": len(water_objs),
        "major_roads": len(major_objs),
        "industrial": len(ind_objs),
    }


def _green_percentile_normalize(eco_data):
    """
    v7: Адаптивная нормализация с расширенным диапазоном.
    """
    if not eco_data:
        return eco_data

    eff_scores = {}
    for name, v in eco_data.items():
        eff_scores[name] = v.get("effective_green", 0)

    values = sorted(eff_scores.values())
    n = len(values)
    if n == 0:
        return eco_data

    median_eff = values[n // 2]
    p90 = values[int(n * 0.9)] if n >= 5 else values[-1]

    # Адаптивный потолок: учитывает и медиану, и p90
    # Лесистый город (p90=200+) → max_green=55-65%
    # Аридный город (p90=20) → max_green=30-40%
    max_green = min(65.0, 20.0 + p90 * 0.3 + median_eff * 0.5)
    max_green = max(30.0, max_green)  # минимум 30% для лидера
    min_green = 3.0

    for name, v in eco_data.items():
        eff = eff_scores[name]

        # Перцентильный ранг
        rank = sum(1 for x in values if x < eff) / max(n, 1)

        if eff <= 0.3:
            green_pct = min_green + rank * 5.0
        elif eff < 2.0:
            green_pct = 6.0 + rank * 12.0
        else:
            # Основной диапазон
            green_pct = 10.0 + rank * (max_green - 12.0)

            # Мягкий бонус за абсолютное значение
            if eff > p90 * 0.8 and p90 > 5:
                bonus = min(5.0, (eff / p90 - 0.8) * 10.0)
                green_pct += bonus

        v["green_coverage_pct"] = round(
            max(min_green, min(max_green, green_pct)), 1
        )

    return eco_data


def collect_eco(city_key, zones, stats=None):
    print(f"  🌿 Экология...")
    cf = f"data/{city_key}/raw/ecology_raw.json"
    cached = safe_json_load(cf)
    if cached is not None:
        sources = {v.get("air_source", "") for v in cached.values()}
        if not sources.intersection({"v7", "fallback", "openweathermap"}):
            os.unlink(cf)
        else:
            print("    Кэш")
            if stats: stats.cache_hits += 1
            return _eco_df(city_key, zones, cached)

    eco = {}
    has_owm = False
    if _has_owm():
        t = _owm_air(zones[0]["lat"], zones[0]["lon"])
        has_owm = t is not None
        print(f"      OWM: {'✅' if has_owm else '⚠️'}")

    eo = _eco_batch(zones, stats)
    if eo:
        gi = _build_idx(eo["green"])
        wi = _build_idx(eo.get("water", []))
        ii = _build_idx(eo["industrial"])
        mi = _build_idx(eo["major"])
        si = _build_idx(eo.get("secondary", []))
        idxs = (gi, wi, ii, mi, si)

        for z in zones:
            entry = _eco_zone(z["lat"], z["lon"], idxs, None)
            if has_owm:
                aq = _owm_air(z["lat"], z["lon"])
                if aq:
                    entry.update(aq)
                    entry["air_source"] = "openweathermap"
            eco[z["name"]] = entry

        # Нормализация зелени по перцентилям
        eco = _green_percentile_normalize(eco)

        for name, e in eco.items():
            print(f"    {name} → AQI={e['aqi']} "
                  f"green={e['green_coverage_pct']}% "
                  f"noise={e['noise_level_db']}dB "
                  f"🌳{e.get('green_obj', 0)} 💧{e.get('water_obj', 0)} "
                  f"🛣{e.get('major_roads', 0)} 🏭{e.get('industrial', 0)} "
                  f"penalty={e.get('road_penalty', 0):.1f}")
    else:
        print("    ⚠️ Фоллбэк...")
        for z in zones:
            eco[z["name"]] = _eco_fallback(z["lat"], z["lon"], stats)
        eco = _green_percentile_normalize(eco)

    safe_json_dump(eco, cf)
    return _eco_df(city_key, zones, eco)


def _eco_fallback(lat, lon, stats=None):
    bb = _bbox(lat, lon, 1000)
    q = f'[out:json][timeout:20];(way["leisure"~"park|garden"]({bb});way["natural"~"wood|water"]({bb}););out count;'
    gc = _parse_count(qop(q, 25, stats))
    time.sleep(0.3)
    bb2 = _bbox(lat, lon, 500)
    q2 = f'[out:json][timeout:15];way["highway"~"motorway|trunk|primary"]({bb2});out body;'
    rd = qop(q2, 20, stats)
    rc = len(rd.get("elements", [])) if rd else 0
    gs = gc * 1.5
    noise = round(max(35, min(66, 38 + rc * 3 + np.random.normal(0, 1))), 1)
    p = max(0, rc * 3 - gc * 0.5)
    aqi = 1 if p <= 1 else 2 if p <= 3 else 3 if p <= 7 else 4 if p <= 14 else 5
    pm = {1: 5, 2: 12, 3: 22, 4: 35, 5: 55}
    return {"green_score_raw": gs, "aqi": aqi,
            "pm2_5": max(1, round(pm[aqi] + np.random.normal(0, 2), 1)),
            "pm10": max(2, round(aqi * 12 + np.random.normal(0, 3), 1)),
            "no2": max(1, round(aqi * 15 + np.random.normal(0, 3), 1)),
            "noise_level_db": noise, "air_source": "fallback",
            "green_coverage_pct": 15.0}


def _eco_df(city_key, zones, eco):
    rows = []
    for z in zones:
        e = eco.get(z["name"], {})
        rows.append({"district": z["name"],
                     "air_quality_index": e.get("aqi", 3),
                     "pm2_5": e.get("pm2_5", 15), "pm10": e.get("pm10", 25),
                     "no2": e.get("no2", 20),
                     "noise_level_db": e.get("noise_level_db", 50),
                     "green_coverage_pct": e.get("green_coverage_pct", 15)})
    df = pd.DataFrame(rows)
    df.to_csv(f"data/{city_key}/processed/ecology.csv", index=False)
    print(f"    ✅ {len(df)} | AQI:{df['air_quality_index'].min()}-{df['air_quality_index'].max()} "
          f"(μ{df['air_quality_index'].mean():.1f}) | "
          f"Green:{df['green_coverage_pct'].min():.0f}-{df['green_coverage_pct'].max():.0f}% "
          f"(μ{df['green_coverage_pct'].mean():.0f}%) | "
          f"Noise:{df['noise_level_db'].min():.0f}-{df['noise_level_db'].max():.0f}dB")
    return df


# ============================================================
# ОТЗЫВЫ
# ============================================================


def _osm_rev(lat, lon, stats=None):
    revs = []
    q = f'''[out:json][timeout:20];
    (node["amenity"]["name"]({_bbox(lat, lon, 1500)});way["amenity"]["name"]({_bbox(lat, lon, 1500)});
     node["shop"]["name"]({_bbox(lat, lon, 1500)}););out tags;'''
    d = qop(q, 25, stats)
    if not d: return revs
    pos = {"park", "playground", "library", "community_centre", "theatre", "cinema"}
    neg = {"disused", "abandoned", "construction"}
    for e in d.get("elements", []):
        t = e.get("tags", {})
        nm = t.get("name", "")
        am = t.get("amenity", "")
        if t.get("opening_hours"):
            revs.append({"text": f"{nm} — работает ({t['opening_hours']})", "rating": 4, "source": "osm_active"})
        if am in pos:
            revs.append({"text": f"Есть {nm} ({am})", "rating": 4, "source": "osm_positive"})
        for n in neg:
            if t.get(n) or t.get(f"{n}:amenity"):
                revs.append({"text": f"{nm} — {n}", "rating": 2, "source": "osm_negative"})
        wc = t.get("wheelchair")
        if wc == "yes":
            revs.append({"text": f"{nm} — доступен", "rating": 5, "source": "osm_access"})
        elif wc == "no":
            revs.append({"text": f"{nm} — недоступен", "rating": 2, "source": "osm_access"})
    return revs


def _dgis_rev(city, zone, lat, lon):
    if not DGIS_KEY: return []
    revs = []
    try:
        r = _session.get("https://catalog.api.2gis.com/3.0/items",
                         params={"q": zone, "point": f"{lon},{lat}", "radius": 1500,
                                 "fields": "items.reviews", "key": DGIS_KEY,
                                 "locale": "ru_RU", "page_size": 10}, timeout=10)
        if r.status_code == 403: return revs
        r.raise_for_status()
        for i in r.json().get("result", {}).get("items", []):
            ir = i.get("reviews", {})
            if isinstance(ir, dict) and ir.get("general_rating"):
                revs.append({"text": f"Рейтинг {i.get('name','')}: {ir['general_rating']}/5",
                             "rating": round(float(ir["general_rating"])), "source": "2gis"})
    except Exception:
        pass
    return revs


def _ya_rev(lat, lon, zone):
    if not _has_ya(): return []
    revs = []
    for st in ["магазин", "поликлиника", "школа"]:
        _ya_rl.wait()
        try:
            r = _session.get("https://geocode-maps.yandex.ru/1.x/",
                             params={"apikey": YANDEX_KEY, "geocode": f"{st}, {zone}",
                                     "format": "json", "results": 5,
                                     "ll": f"{lon},{lat}", "spn": "0.015,0.015"}, timeout=10)
            if r.status_code == 403: return revs
            r.raise_for_status()
            for m in r.json()["response"]["GeoObjectCollection"]["featureMember"]:
                nm = m["GeoObject"].get("name", "")
                if nm:
                    revs.append({"text": f"В районе: {nm} ({st})", "rating": 4, "source": "yandex"})
        except Exception:
            pass
    return revs


def _infra_rev(zone, row):
    revs = []
    sh = row.get("shops_count", 0) + row.get("supermarkets_count", 0)
    if sh >= 10: revs.append({"text": f"Много магазинов ({sh})", "rating": 5, "source": "infra"})
    elif sh >= 5: revs.append({"text": f"Магазинов достаточно ({sh})", "rating": 4, "source": "infra"})
    elif sh >= 1: revs.append({"text": f"Мало магазинов ({sh})", "rating": 2, "source": "infra"})
    else: revs.append({"text": "Магазинов нет", "rating": 1, "source": "infra"})

    st = row.get("bus_stops_count", 0)
    if st >= 15: revs.append({"text": f"Отличный транспорт ({st})", "rating": 5, "source": "infra"})
    elif st >= 8: revs.append({"text": f"Транспорт норм ({st})", "rating": 4, "source": "infra"})
    elif st >= 3: revs.append({"text": f"Транспорт редкий ({st})", "rating": 3, "source": "infra"})
    else: revs.append({"text": f"Плохой транспорт", "rating": 1, "source": "infra"})

    pk = row.get("parks_count", 0)
    if pk >= 3: revs.append({"text": f"Парки ({pk})", "rating": 5, "source": "infra"})
    elif pk >= 1: revs.append({"text": "Есть парк", "rating": 4, "source": "infra"})
    else: revs.append({"text": "Парков нет", "rating": 2, "source": "infra"})

    pg = row.get("playgrounds_count", 0)
    if pg >= 5: revs.append({"text": f"Площадки ({pg})", "rating": 5, "source": "infra"})
    elif pg == 0: revs.append({"text": "Площадок нет", "rating": 1, "source": "infra"})

    h = row.get("hospitals_count", 0) + row.get("clinics_count", 0)
    if h >= 3: revs.append({"text": f"Медицина ({h})", "rating": 5, "source": "infra"})
    elif h >= 1: revs.append({"text": "Есть поликлиника", "rating": 4, "source": "infra"})
    else: revs.append({"text": "Нет поликлиники", "rating": 2, "source": "infra"})

    if row.get("pharmacies_count", 0) == 0:
        revs.append({"text": "Аптек нет", "rating": 1, "source": "infra"})

    sc = row.get("schools_count", 0)
    kg = row.get("kindergartens_count", 0)
    if sc >= 2 and kg >= 2:
        revs.append({"text": f"Школы ({sc}), садики ({kg})", "rating": 5, "source": "infra"})
    elif sc == 0 and kg == 0:
        revs.append({"text": "Нет школ/садиков", "rating": 1, "source": "infra"})

    ca = row.get("cafes_count", 0) + row.get("restaurants_count", 0)
    if ca >= 10: revs.append({"text": f"Кафе ({ca})", "rating": 5, "source": "infra"})
    elif ca == 0: revs.append({"text": "Кафе нет", "rating": 2, "source": "infra"})

    tot = sh + st + pk + h + sc
    if tot >= 30: revs.append({"text": "Развитый район", "rating": 5, "source": "infra"})
    elif tot <= 5: revs.append({"text": "Слабая инфраструктура", "rating": 2, "source": "infra"})

    return revs


def collect_rev(city_key, city_name, zones, infra_df, stats=None):
    print(f"  📝 Отзывы...")
    cf = f"data/{city_key}/raw/reviews_raw.json"
    cached = safe_json_load(cf)
    if cached is not None:
        print("    Кэш")
        if stats: stats.cache_hits += 1
        all_rev = cached
    else:
        all_rev = {}
        prog = Progress(len(zones), "Отзывы")
        for z in zones:
            zn = z["name"]
            zr = _osm_rev(z["lat"], z["lon"], stats)
            if DGIS_KEY: zr.extend(_dgis_rev(city_name, zn, z["lat"], z["lon"])); time.sleep(0.3)
            zr.extend(_ya_rev(z["lat"], z["lon"], zn))
            ir = infra_df[infra_df["district"] == zn]
            if not ir.empty: zr.extend(_infra_rev(zn, ir.iloc[0].to_dict()))
            all_rev[zn] = zr
            prog.tick(f"{zn} → {len(zr)}")
            time.sleep(0.2)
        safe_json_dump(all_rev, cf)

    rows = []
    for z in zones:
        zn = z["name"]
        zr = all_rev.get(zn, [])
        if not zr:
            ir = infra_df[infra_df["district"] == zn]
            if not ir.empty: zr = _infra_rev(zn, ir.iloc[0].to_dict())
        for rv in zr:
            rows.append({"district": zn, "text": rv.get("text", ""), "rating": rv.get("rating", 3),
                         "source": rv.get("source", "?"),
                         "lat": z["lat"] + np.random.normal(0, 0.003),
                         "lon": z["lon"] + np.random.normal(0, 0.003)})
    df = pd.DataFrame(rows)
    df.to_csv(f"data/{city_key}/processed/reviews.csv", index=False)
    print(f"    ✅ {len(df)} отзывов")
    return df


# ============================================================
# NLP
# ============================================================


def sent(text):
    words = re.findall(r"[а-яёА-ЯЁa-zA-Z]+", text.lower())
    p = n = 0.0
    for i, w in enumerate(words):
        neg = (i > 0 and words[i-1] in NEGATION_WORDS) or (i > 1 and words[i-2] in NEGATION_WORDS)
        if w in POS_W: n += 1 if neg else 0; p += 0 if neg else 1
        elif w in NEG_W: p += 0.5 if neg else 0; n += 0 if neg else 1
    t = p + n
    if t == 0: return "NEUTRAL", 0.5
    if p > n: return "POSITIVE", round(0.5 + 0.5*(p-n)/t, 3)
    if n > p: return "NEGATIVE", round(0.5 - 0.5*(n-p)/t, 3)
    return "NEUTRAL", 0.5


def find_probs(texts):
    c = Counter()
    for t in texts:
        lo = t.lower()
        for pr, kw in PROBLEMS_MAP.items():
            if any(k in lo for k in kw): c[pr] += 1
    return dict(c.most_common())


def nlp(city_key):
    print(f"  🧠 NLP...")
    df = pd.read_csv(f"data/{city_key}/processed/reviews.csv")
    if len(df) == 0:
        pd.DataFrame(columns=["district", "review_count", "avg_rating", "avg_sentiment",
                               "positive_share", "negative_share",
                               "top_problem_1", "top_problem_2", "top_problem_3"
                               ]).to_csv(f"data/{city_key}/processed/review_profiles.csv", index=False)
        return
    r = [sent(str(t)) for t in df["text"]]
    df["sentiment_label"] = [x[0] for x in r]
    df["sentiment_score"] = [x[1] for x in r]
    df["sentiment_numeric"] = df["sentiment_label"].map({"POSITIVE": 1.0, "NEUTRAL": 0.5, "NEGATIVE": 0.0})
    df.to_csv(f"data/{city_key}/processed/reviews_analyzed.csv", index=False)

    profiles = []
    for dn in df["district"].unique():
        dd = df[df["district"] == dn]
        pr = find_probs(dd["text"].tolist())
        pl = list(pr.keys())
        n = len(dd)
        profiles.append({
            "district": dn, "review_count": n,
            "avg_rating": round(dd["rating"].mean(), 2) if n else 3.0,
            "avg_sentiment": round(dd["sentiment_numeric"].mean(), 3) if n else 0.5,
            "positive_share": round((dd["sentiment_label"] == "POSITIVE").mean(), 3) if n else 0,
            "negative_share": round((dd["sentiment_label"] == "NEGATIVE").mean(), 3) if n else 0,
            "top_problem_1": pl[0] if len(pl) >= 1 else "",
            "top_problem_2": pl[1] if len(pl) >= 2 else "",
            "top_problem_3": pl[2] if len(pl) >= 3 else "",
        })
    pd.DataFrame(profiles).to_csv(f"data/{city_key}/processed/review_profiles.csv", index=False)
    print(f"    ✅ {len(df)} отзывов, {len(profiles)} профилей")


# ============================================================
# СКОРИНГ
# ============================================================


def _s(v, mid=5.0, st=0.3):
    try:
        return 1.0 / (1.0 + math.exp(-st * (v - mid)))
    except OverflowError:
        return 0.0 if v < mid else 1.0


def sc_infra(r):
    # Учитываем магазины, супермаркеты, ТЦ, аптеки, банки, почту
    shops = r.get("shops_count", 0) + r.get("supermarkets_count", 0) + r.get("malls_count", 0) * 5
    s = [
        _s(shops, 5, .2),                           # Магазины (абсолютное число)
        _s(r.get("shops_per_1000", 0), 1.0, 1.5),   # Магазины (плотность)
        _s(r.get("pharmacies_per_1000", 0), .5, 2), # Аптеки
        _s(r.get("banks_count", 0), 2, .6),         # Банки
        _s(r.get("post_offices_count", 0), 1, 1.0)  # Почта (НОВОЕ)
    ]
    return round(float(np.clip(np.mean(s)*100, 0, 100)), 1)

def sc_leisure(r):
    # Кафе, кино, театры, фитнес, парки
    food = r.get("cafes_count", 0) + r.get("restaurants_count", 0)
    sport = r.get("fitness_count", 0) + r.get("sport_fields_count", 0)
    
    s = [
        _s(food, 8, .15),
        _s(r.get("cinemas_count", 0) + r.get("culture_count", 0), 1, 1.5),
        _s(sport, 3, .4),                           # Спорт (НОВОЕ)
        _s(r.get("playgrounds_count", 0), 5, .3),
        _s(r.get("parks_count", 0), 2, .7)
    ]
    return round(float(np.clip(np.mean(s)*100, 0, 100)), 1)

def sc_edu(r):
    s = [_s(r.get("schools_per_1000", 0), .5, 2.5), _s(r.get("kindergartens_per_1000", 0), .5, 2.5),
         _s(r.get("libraries_count", 0), 2, .8)]
    return round(float(np.clip(np.mean(s)*100, 0, 100)), 1)

def sc_health(r):
    s = [_s(r.get("hospitals_count", 0), 1, 1.5), _s(r.get("clinics_count", 0), 2, .7),
         _s(r.get("pharmacies_count", 0), 4, .4)]
    return round(float(np.clip(np.mean(s)*100, 0, 100)), 1)

def sc_transport(r):
    s = [_s(r.get("bus_stops_count", 0), 8, .25), _s(r.get("bus_stops_per_1000", 0), 1, 1.2)]
    return round(float(np.clip(np.mean(s)*100, 0, 100)), 1)

def sc_eco(r):
    s = []
    aqi = r.get("air_quality_index", 3); s.append(max(0, (5-aqi)/4))
    pm = r.get("pm2_5", 15); s.append(max(0, min(1, (35-pm)/25)) if pm > 10 else 1.0)
    gr = r.get("green_coverage_pct", 15); s.append(_s(gr, 20, .08))
    no = r.get("noise_level_db", 50); s.append(max(0, min(1, (62-no)/22)) if no > 38 else 1.0)
    s.append(_s(r.get("parks_count", 0), 2, .6))
    return round(float(np.clip(np.mean(s)*100, 0, 100)), 1)

def sc_safety(r):
    b = r.get("bus_stops_count", 0); sh = r.get("shops_count", 0); ca = r.get("cafes_count", 0)
    s = [_s(b+sh+ca, 20, .08),
         _s(b+sh+r.get("pharmacies_count", 0)+r.get("banks_count", 0), 25, .06),
         _s(r.get("playgrounds_count", 0)+r.get("schools_count", 0)+r.get("kindergartens_count", 0), 6, .25)]
    tot = b+sh+ca+r.get("pharmacies_count", 0)+r.get("playgrounds_count", 0)
    s.append(.15 if tot < 8 else .35 if tot < 18 else .55 if tot < 30 else .75)
    s.append(max(0, 1-r.get("negative_share", .3)*2))
    return round(float(np.clip(np.mean(s)*100, 0, 100)), 1)

def sc_leisure(r):
    s = [_s(r.get("cafes_count", 0)+r.get("restaurants_count", 0), 6, .25),
         _s(r.get("cinemas_count", 0), 1, 1.5), _s(r.get("theatres_count", 0), 1, 1.5),
         _s(r.get("fitness_count", 0), 2, .6), _s(r.get("playgrounds_count", 0), 4, .35),
         _s(r.get("parks_count", 0), 2, .7)]
    return round(float(np.clip(np.mean(s)*100, 0, 100)), 1)

def sc_social(r):
    rc = r.get("review_count", 0)
    if rc == 0: return 50.0
    s = [r.get("avg_sentiment", .5), r.get("positive_share", 0),
         (r.get("avg_rating", 3)-1)/4, max(0, 1-r.get("negative_share", .3)*1.5)]
    conf = min(1.0, rc/50)
    raw = np.mean(s)
    adj = raw * conf + 0.5 * (1-conf)
    return round(float(np.clip(adj*100, 5, 95)), 1)


def to_grade(x):
    if x >= 68: return "A"
    elif x >= 53: return "B"
    elif x >= 38: return "C"
    elif x >= 26: return "D"
    else: return "F"


def norm_scores(df):
    cols = [c for c in df.columns if c.endswith("_score") and c != "total_index"]
    for c in cols:
        v = df[c]
        if len(v) < 3: continue
        sp = v.max() - v.min()
        if sp < 3:
            df[c] = (50 + (v-v.median())*3).clip(5, 95).round(1)
        elif sp < 15:
            p10, p90 = v.quantile(.1), v.quantile(.9)
            if p90 > p10:
                df[c] = ((v-p10)/(p90-p10)*70+15).clip(5, 95).round(1)
    return df


# ============================================================
# ИНДЕКС + ML
# ============================================================

_W = {"infrastructure": .15, "education": .10, "healthcare": .10,
      "transport": .15, "ecology": .12, "safety": .10,
      "leisure": .10, "social": .18}


def calc_idx(city_key):
    print(f"  📊 Индекс...")
    i = pd.read_csv(f"data/{city_key}/processed/infrastructure.csv")
    e = pd.read_csv(f"data/{city_key}/processed/ecology.csv")
    p = pd.read_csv(f"data/{city_key}/processed/review_profiles.csv")
    df = i.merge(e, on="district", how="left").merge(p, on="district", how="left").fillna(0)

    df["infrastructure_score"] = df.apply(sc_infra, axis=1)
    df["education_score"] = df.apply(sc_edu, axis=1)
    df["healthcare_score"] = df.apply(sc_health, axis=1)
    df["transport_score"] = df.apply(sc_transport, axis=1)
    df["ecology_score"] = df.apply(sc_eco, axis=1)
    df["safety_score"] = df.apply(sc_safety, axis=1)
    df["leisure_score"] = df.apply(sc_leisure, axis=1)
    df["social_score"] = df.apply(sc_social, axis=1)
    df = norm_scores(df)
    df["total_index"] = sum(df[f"{c}_score"]*w for c, w in _W.items()).round(1)
    df["grade"] = df["total_index"].apply(to_grade)
    df = df.sort_values("total_index", ascending=False)
    df.to_csv(f"data/{city_key}/processed/districts_final.csv", index=False)

    em = {"A": "🟢", "B": "🔵", "C": "🟡", "D": "🟠", "F": "🔴"}
    print(f"\n    {'─'*78}")
    print(f"    {'#':>3}  {'Зона':28s} │ {'Σ':>5} │ Инф Обр Здр Тр  Эко Без Дос Соц")
    print(f"    {'─'*78}")
    for j, (_, r) in enumerate(df.iterrows(), 1):
        print(f"    {j:2d}. {em.get(r['grade'],'⚪')} {r['district'][:25]:25s} │ "
              f"{r['total_index']:5.1f} │ "
              f"{r['infrastructure_score']:3.0f} {r['education_score']:3.0f} "
              f"{r['healthcare_score']:3.0f} {r['transport_score']:3.0f} "
              f"{r['ecology_score']:3.0f} {r['safety_score']:3.0f} "
              f"{r['leisure_score']:3.0f} {r['social_score']:3.0f}")
    print(f"    {'─'*78}")
    return df


def ml(city_key):
    print(f"  🤖 ML...")
    try:
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler
        from sklearn.decomposition import PCA
    except ImportError: return
    df = pd.read_csv(f"data/{city_key}/processed/districts_final.csv")
    cols = ["infrastructure_score", "education_score", "healthcare_score",
            "transport_score", "ecology_score", "safety_score",
            "leisure_score", "social_score"]
    av = [c for c in cols if c in df.columns]
    if len(df) < 3 or len(av) < 3:
        df["cluster"] = 0; df["pca_1"] = 0; df["pca_2"] = 0
        df.to_csv(f"data/{city_key}/processed/districts_final.csv", index=False); return
    X = StandardScaler().fit_transform(df[av].values)
    n = min(4, len(df)-1)
    df["cluster"] = KMeans(n_clusters=n, random_state=42, n_init=10).fit_predict(X)
    pc = PCA(n_components=2).fit_transform(X)
    df["pca_1"] = pc[:, 0]; df["pca_2"] = pc[:, 1]
    df.to_csv(f"data/{city_key}/processed/districts_final.csv", index=False)
    print(f"    ✅ {n} кл.")


# ============================================================
# ПАЙПЛАЙН
# ============================================================


def process(city_key, city_info):
    print(f"\n{'═'*60}\n🏙️  {city_key}\n{'═'*60}")
    for d in [f"data/{city_key}/raw", f"data/{city_key}/processed"]:
        os.makedirs(d, exist_ok=True)
    stats = PipelineStats(city=city_key)
    t0 = time.monotonic()

    zones = discover_zones(city_key, city_info, stats)
    if len(zones) < 2:
        print(f"  ❌ Мало зон"); return

    idf = None
    for nm, fn in [("infrastructure", lambda: collect_infra(city_key, zones, stats)),
                   ("ecology", lambda: collect_eco(city_key, zones, stats))]:
        try:
            r = fn()
            if nm == "infrastructure": idf = r
            stats.stages_completed.append(nm)
        except Exception as e:
            logger.exception("%s: %s", nm, e); stats.stages_failed.append(nm)
    if idf is None:
        idf = pd.DataFrame([{"district": z["name"]} for z in zones])
        
    # Сохраняем geojson
    # Build lookup for urban_clip_bbox from preset zone specs
    _clip_bbox_map = {}
    for spec in _preset_zone_specs(city_key, city_info):
        if isinstance(spec, dict) and spec.get("urban_clip_bbox"):
            sname = clean_name(str(spec.get("name", ""))) or str(spec.get("name", ""))
            _clip_bbox_map[sname.lower()] = spec["urban_clip_bbox"]

    features = []
    for z in zones:
        if "geojson" in z and z["geojson"]:
            geojson = z["geojson"]
            # Apply urban_clip_bbox if defined for this zone
            zname_key = (clean_name(z["name"]) or z["name"]).lower()
            urban_bbox = _clip_bbox_map.get(zname_key)
            if urban_bbox:
                try:
                    from shapely.geometry import shape, box, mapping
                    g = shape(geojson)
                    clip = box(urban_bbox[0], urban_bbox[1], urban_bbox[2], urban_bbox[3])
                    clipped = g.intersection(clip)
                    if not clipped.is_empty and clipped.area > 0.0001:
                        geojson = mapping(clipped)
                except Exception:
                    pass
            features.append({
                "type": "Feature",
                "properties": {"district": z["name"], "zone_source": z.get("source", "?")},
                "geometry": geojson
            })
    if features:
        with open(f"data/{city_key}/processed/districts_final.geojson", "w", encoding="utf-8") as f:
            json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False)
            
    for nm, fn in [("reviews", lambda: collect_rev(city_key, city_info["osm_name"], zones, idf, stats)),
                   ("nlp", lambda: nlp(city_key)),
                   ("index", lambda: calc_idx(city_key)),
                   ("ml", lambda: ml(city_key))]:
        try:
            fn(); stats.stages_completed.append(nm)
        except Exception as e:
            logger.exception("%s: %s", nm, e); stats.stages_failed.append(nm)
    stats.duration_seconds = time.monotonic() - t0
    print(f"\n{stats.summary()}")
    safe_json_dump({"city": stats.city, "zones": stats.zones_found,
                    "sources": stats.zones_by_source, "filtered": stats.zones_filtered,
                    "api": stats.api_calls, "errors": stats.api_errors,
                    "cache": stats.cache_hits, "time": round(stats.duration_seconds, 1),
                    "ok": stats.stages_completed, "fail": stats.stages_failed,
                    "ts": time.strftime("%Y-%m-%d %H:%M:%S")},
                   f"data/{city_key}/pipeline_stats.json")


def main():
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║    🏙️  UrbanPulse — Pipeline v2.3  🏙️                   ║
    ╚══════════════════════════════════════════════════════════╝""")
    for l, ok in [("Яндекс", _has_ya()), ("OWM", _has_owm()), ("2GIS", bool(DGIS_KEY))]:
        print(f"    {'✅' if ok else '⚠️'} {l}")
    print()
    t0 = time.time()
    if len(sys.argv) > 1:
        a = " ".join(sys.argv[1:]).strip().replace("--city", "").strip().strip("=").strip()
        if a in CONFIG["cities"]:
            process(a, CONFIG["cities"][a])
        else:
            gc = geo(a)
            if not gc:
                print(f"❌ Не удалось определить город: '{a}'")
                print(f"   Доступные в config: {', '.join(CONFIG['cities'].keys())}")
                return
            city_key = a.strip()
            custom_info = {
                "full_name": city_key,
                "osm_name": city_key,
                "center": [round(gc["lat"], 6), round(gc["lon"], 6)],
                "zoom": 12,
                "admin_level": "9",
                "zones_mode": "parse",
            }
            print(
                f"ℹ️ Город '{city_key}' не найден в config — "
                f"используем пользовательский ввод и парсинг зон"
            )
            process(city_key, custom_info)
    else:
        for k, v in CONFIG["cities"].items():
            process(k, v)
    print(f"\n{'═'*60}\n✅ {time.time()-t0:.1f}с\n🚀 streamlit run app.py")


if __name__ == "__main__":
    main()
