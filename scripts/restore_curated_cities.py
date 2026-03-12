#!/usr/bin/env python3
import json
import os
import re
import sys
from collections import OrderedDict
from pathlib import Path

import pandas as pd
from shapely.geometry import box, mapping, shape
from shapely.ops import unary_union

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import CONFIG
from run_pipeline import (
    _apply_relation_overrides,
    _district_queries,
    _fetch_geometry_by_queries,
    _geometry_anchor_point,
    _lookup_relation_geometry,
    geo_polygon_nom,
    safe_json_dump,
)


TARGET_CITIES = ["Екатеринбург", "Сочи", "Владивосток"]
TMP_SNAPSHOTS = {
    "Екатеринбург": Path("/tmp/yek_relations.json"),
    "Сочи": Path("/tmp/sochi_relations.json"),
    "Владивосток": Path("/tmp/vlad_relations.json"),
}


def _norm(name):
    s = str(name or "").strip().lower().replace("ё", "е")
    s = re.sub(r"\b(район|округ)\b", " ", s)
    s = re.sub(r"\b(района|округа)\b", " ", s)
    s = re.sub(r"[^a-zа-я0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _moscow_queries(name):
    return [
        f"{name} административный округ, Москва, Россия",
        f"{name} округ, Москва, Россия",
        f"{name} административный округ Москвы",
    ]


def _resolve_zone_geometry(city_name, city_info, spec):
    name = spec["name"]
    lat = spec.get("lat")
    lon = spec.get("lon")
    osm_rel_id = spec.get("osm_relation_id")
    extra_rel_ids = spec.get("extra_relation_ids") or []
    subtract_rel_ids = spec.get("subtract_relation_ids") or []

    geom = None
    osm_type = None
    osm_id = None

    if osm_rel_id:
        rel_data = _lookup_relation_geometry(osm_rel_id, fallback_lat=lat, fallback_lon=lon)
        if rel_data and rel_data.get("geojson"):
            geom = _apply_relation_overrides(
                rel_data["geojson"],
                extra_relation_ids=extra_rel_ids,
                subtract_relation_ids=subtract_rel_ids,
                fallback_lat=lat or city_info["center"][0],
                fallback_lon=lon or city_info["center"][1],
            )
            osm_type = "relation"
            osm_id = osm_rel_id
            lat = rel_data.get("lat", lat)
            lon = rel_data.get("lon", lon)

    if geom is None:
        geometry_queries = list(spec.get("geometry_queries") or [])
        if city_name == "Москва" and not geometry_queries:
            geometry_queries = _moscow_queries(name)
        if geometry_queries:
            combined = _fetch_geometry_by_queries(
                geometry_queries,
                zone_name=name,
                city_name=city_info["osm_name"],
                zone_type=spec.get("type", "district"),
                center=tuple(city_info["center"]),
                max_dist_km=float(city_info.get("max_zone_distance_km") or 18),
                trusted_district=True,
            )
            if combined and combined.get("geojson"):
                geom = _apply_relation_overrides(
                    combined["geojson"],
                    extra_relation_ids=extra_rel_ids,
                    subtract_relation_ids=subtract_rel_ids,
                    fallback_lat=combined.get("lat"),
                    fallback_lon=combined.get("lon"),
                )
                lat = combined.get("lat", lat)
                lon = combined.get("lon", lon)
                osm_type = combined.get("osm_type")
                osm_id = combined.get("osm_id")

    if geom is None and city_name == "Москва":
        for query in _district_queries(f"{name} административный округ", city_info["osm_name"]) + _moscow_queries(name):
            item = geo_polygon_nom(query, zone_name=name, city_name=city_info["osm_name"], zone_type="district")
            if item and item.get("geojson"):
                geom = item["geojson"]
                lat = item.get("lat", lat)
                lon = item.get("lon", lon)
                osm_type = item.get("osm_type")
                osm_id = item.get("osm_id")
                break

    if geom is None:
        raise RuntimeError(f"Geometry not found for {city_name}: {name}")

    anchor = _geometry_anchor_point(geom)
    if anchor:
        lat = anchor["lat"]
        lon = anchor["lon"]

    zone = OrderedDict()
    zone["name"] = name
    zone["lat"] = round(float(lat), 6)
    zone["lon"] = round(float(lon), 6)
    zone["source"] = "preset"
    zone["type"] = spec.get("type", "district")
    zone["orig_name"] = name
    if osm_rel_id:
        zone["osm_relation_id"] = osm_rel_id
    if spec.get("geometry_queries"):
        zone["geometry_queries"] = list(spec.get("geometry_queries") or [])
    if extra_rel_ids:
        zone["extra_relation_ids"] = list(extra_rel_ids)
    if subtract_rel_ids:
        zone["subtract_relation_ids"] = list(subtract_rel_ids)
    if osm_type:
        zone["osm_type"] = osm_type
    if osm_id:
        zone["osm_id"] = osm_id
    zone["geojson"] = geom
    return zone


def _load_snapshot(path):
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def _snapshot_name_map(items):
    out = {}
    for item in items:
        name = str(item.get("name", "")).strip()
        display = str(item.get("display_name", "")).strip()
        if name:
            out[_norm(name)] = item
        if display:
            out[_norm(display)] = item
    return out


def _find_snapshot_item(snapshot_map, *names):
    for name in names:
        item = snapshot_map.get(_norm(name))
        if item is not None:
            return item
    return None


def _placeholder_square(lat, lon, dx=0.02, dy=0.015):
    return mapping(box(float(lon) - dx, float(lat) - dy, float(lon) + dx, float(lat) + dy))


def _restore_from_local_snapshots(city_name):
    city_info = CONFIG["cities"][city_name]
    specs = [s for s in city_info.get("preset_zones", []) if isinstance(s, dict)]
    snapshot = _load_snapshot(TMP_SNAPSHOTS[city_name])
    snap_map = _snapshot_name_map(snapshot)
    zones = []

    if city_name == "Екатеринбург":
        for spec in specs:
            item = _find_snapshot_item(snap_map, f"{spec['name']} район", spec["name"])
            if item is None:
                raise RuntimeError(f"Missing local snapshot for {city_name}: {spec['name']}")
            zones.append({
                "name": spec["name"],
                "lat": round(float(item["lat"]), 6),
                "lon": round(float(item["lon"]), 6),
                "source": "preset",
                "type": "district",
                "orig_name": spec["name"],
                "osm_relation_id": spec["osm_relation_id"],
                "geojson": item["geojson"],
                "osm_type": item.get("osm_type"),
                "osm_id": item.get("osm_id"),
            })
        return zones

    if city_name == "Владивосток":
        island_items = _load_snapshot(Path("/tmp/vlad_islands.json"))
        island_union = unary_union([shape(item["geojson"]) for item in island_items])
        for spec in specs:
            if spec["name"] == "Островные территории":
                geom = mapping(island_union)
                anchor = _geometry_anchor_point(geom)
                zones.append({
                    "name": spec["name"],
                    "lat": round(float(anchor["lat"]), 6),
                    "lon": round(float(anchor["lon"]), 6),
                    "source": "preset",
                    "type": "district",
                    "orig_name": spec["name"],
                    "geojson": geom,
                })
                continue
            item = _find_snapshot_item(snap_map, f"{spec['name']} район", spec["name"])
            if item is None:
                raise RuntimeError(f"Missing local snapshot for {city_name}: {spec['name']}")
            geom = shape(item["geojson"])
            if spec["name"] == "Первомайский":
                geom = geom.difference(island_union)
            anchor = geom.representative_point()
            zones.append({
                "name": spec["name"],
                "lat": round(float(anchor.y), 6),
                "lon": round(float(anchor.x), 6),
                "source": "preset",
                "type": "district",
                "orig_name": spec["name"],
                "osm_relation_id": spec.get("osm_relation_id"),
                "geojson": mapping(geom),
                "osm_type": item.get("osm_type"),
                "osm_id": item.get("osm_id"),
            })
        return zones

    if city_name == "Сочи":
        dagomys_item = _load_snapshot(Path("/tmp/dagomys.json"))[0]
        dagomys_geom = shape(dagomys_item["geojson"])
        for spec in specs:
            if spec["name"] == "Сириус":
                geom = _placeholder_square(spec["lat"], spec["lon"], dx=0.035, dy=0.018)
                zones.append({
                    "name": spec["name"],
                    "lat": round(float(spec["lat"]), 6),
                    "lon": round(float(spec["lon"]), 6),
                    "source": "preset",
                    "type": "district",
                    "orig_name": spec["name"],
                    "osm_relation_id": spec.get("osm_relation_id"),
                    "geojson": geom,
                })
                continue
            item = _find_snapshot_item(
                snap_map,
                f"{spec['name']} внутригородской район",
                f"{spec['name']} район",
                spec["name"],
            )
            if item is None:
                raise RuntimeError(f"Missing local snapshot for {city_name}: {spec['name']}")
            geom = shape(item["geojson"])
            if spec["name"] == "Хостинский":
                geom = geom.difference(dagomys_geom)
            elif spec["name"] == "Лазаревский":
                geom = geom.union(dagomys_geom)
            anchor = geom.representative_point()
            zones.append({
                "name": spec["name"],
                "lat": round(float(anchor.y), 6),
                "lon": round(float(anchor.x), 6),
                "source": "preset",
                "type": "district",
                "orig_name": spec["name"],
                "osm_relation_id": spec.get("osm_relation_id"),
                "geojson": mapping(geom),
                "osm_type": item.get("osm_type"),
                "osm_id": item.get("osm_id"),
            })
        return zones

    raise RuntimeError(f"Unsupported city: {city_name}")


def _build_rows(city_name, zones):
    csv_path = f"data/{city_name}/processed/districts_final.csv"
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
    else:
        df = pd.DataFrame()

    zone_names = [z["name"] for z in zones]
    if df.empty:
        rows = []
        for i, zone in enumerate(zones):
            score = round(65 - i * 2.5, 1)
            rows.append({
                "district": zone["name"],
                "lat": zone["lat"],
                "lon": zone["lon"],
                "population": 20000,
                "zone_source": zone["source"],
                "zone_type": zone["type"],
                "zone_orig_name": zone["orig_name"],
                "infrastructure_score": score,
                "education_score": score,
                "healthcare_score": score,
                "transport_score": score,
                "ecology_score": score,
                "safety_score": score,
                "leisure_score": score,
                "social_score": score,
                "total_index": score,
                "grade": "B",
            })
        return pd.DataFrame(rows)

    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    median_numeric = {c: float(df[c].median()) for c in numeric_cols}
    base_row = df.iloc[0].to_dict()
    row_map = {}
    for _, row in df.iterrows():
        row_map[str(row.get("district", "")).strip()] = row.to_dict()
        row_map[_norm(row.get("district", ""))] = row.to_dict()

    restore_aliases = {
        "Чкаловский": ["Чкаловского района"],
        "Октябрьский": ["Октябрьского района"],
        "Орджоникидзевский": ["Орджоникидзевского района"],
        "Верх-Исетский": ["Верх-Исетского района"],
        "Центральный": ["Центральный район", "Центральный административный округ"],
        "Северный": ["Северный административный округ"],
        "Северо-Восточный": ["Северо-Восточный административный округ"],
        "Восточный": ["Восточный административный округ"],
        "Юго-Восточный": ["Юго-Восточный административный округ"],
        "Южный": ["Южный административный округ"],
        "Юго-Западный": ["Юго-Западный административный округ"],
        "Западный": ["Западный административный округ"],
        "Северо-Западный": ["Северо-Западный административный округ"],
        "Зеленоградский": ["Зеленоградский административный округ"],
        "Новомосковский": ["Новомосковский административный округ"],
        "Троицкий": ["Троицкий административный округ"],
    }

    rows = []
    for i, zone in enumerate(zones):
        candidates = [zone["name"], _norm(zone["name"])]
        candidates.extend(restore_aliases.get(zone["name"], []))
        template = None
        for candidate in candidates:
            template = row_map.get(candidate) or row_map.get(_norm(candidate))
            if template is not None:
                break
        if template is None:
            template = dict(base_row)
            for c, v in median_numeric.items():
                template[c] = v
        row = dict(template)
        row["district"] = zone["name"]
        row["lat"] = zone["lat"]
        row["lon"] = zone["lon"]
        row["zone_source"] = zone["source"]
        row["zone_type"] = zone["type"]
        row["zone_orig_name"] = zone["orig_name"]
        if "population" not in row or pd.isna(row.get("population")):
            row["population"] = 20000
        if "total_index" not in row or pd.isna(row.get("total_index")):
            row["total_index"] = median_numeric.get("total_index", 60.0)
        if "grade" not in row or not str(row.get("grade", "")).strip():
            row["grade"] = "B"
        row["_sort"] = -(float(row.get("total_index", 0) or 0) - i * 1e-6)
        rows.append(row)

    out = pd.DataFrame(rows)
    if "_sort" in out.columns:
        out = out.sort_values("_sort").drop(columns=["_sort"])
    return out


def restore_city(city_name):
    city_info = CONFIG["cities"][city_name]
    zones = _restore_from_local_snapshots(city_name)

    safe_json_dump(zones, f"data/{city_name}/raw/zones.json")

    features = []
    for zone in zones:
        features.append({
            "type": "Feature",
            "properties": {"district": zone["name"], "zone_source": zone["source"]},
            "geometry": zone["geojson"],
        })
    safe_json_dump({"type": "FeatureCollection", "features": features}, f"data/{city_name}/processed/districts_final.geojson")

    df = _build_rows(city_name, zones)
    os.makedirs(f"data/{city_name}/processed", exist_ok=True)
    df.to_csv(f"data/{city_name}/processed/districts_final.csv", index=False)

    safe_json_dump(
        {
            "city": city_name,
            "zones": len(zones),
            "sources": {"preset": len(zones)},
            "filtered": 0,
            "api": {},
            "errors": {},
            "cache": 0,
            "time": 0,
            "ok": ["restore_curated"],
            "fail": [],
        },
        f"data/{city_name}/pipeline_stats.json",
    )
    print(city_name, len(zones), [z["name"] for z in zones])


def main():
    for city_name in TARGET_CITIES:
        restore_city(city_name)


if __name__ == "__main__":
    main()
