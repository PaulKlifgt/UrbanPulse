# collectors/osm_collector.py

import osmnx as ox
import geopandas as gpd
import pandas as pd
import numpy as np
from geopy.distance import geodesic
import warnings
import json
import os

warnings.filterwarnings("ignore")


class OSMCollector:
    """Сбор данных инфраструктуры из OpenStreetMap"""

    def __init__(self, city_name: str):
        self.city = city_name
        self.cache_dir = "data/raw/osm"
        os.makedirs(self.cache_dir, exist_ok=True)

        self.infrastructure_tags = {
            "schools": {"amenity": "school"},
            "kindergartens": {"amenity": "kindergarten"},
            "hospitals": {"amenity": "hospital"},
            "clinics": {"amenity": "clinic"},
            "pharmacies": {"amenity": "pharmacy"},
            "shops": {"shop": True},
            "supermarkets": {"shop": "supermarket"},
            "bus_stops": {"highway": "bus_stop"},
            "parks": {"leisure": "park"},
            "playgrounds": {"leisure": "playground"},
            "fitness": {"leisure": "fitness_centre"},
            "cafes": {"amenity": "cafe"},
            "restaurants": {"amenity": "restaurant"},
            "banks": {"amenity": "bank"},
            "post_offices": {"amenity": "post_office"},
            "libraries": {"amenity": "library"},
            "cinemas": {"amenity": "cinema"},
            "theatres": {"amenity": "theatre"},
        }

    def collect_all(self) -> dict:
        """Собрать все данные об инфраструктуре"""
        cache_file = os.path.join(self.cache_dir, "infrastructure.json")

        if os.path.exists(cache_file):
            print("Загружаем из кэша...")
            with open(cache_file, "r") as f:
                return json.load(f)

        results = {}
        for name, tags in self.infrastructure_tags.items():
            print(f"Собираем {name}...")
            try:
                gdf = ox.features_from_place(self.city, tags=tags)

                points = []
                for idx, row in gdf.iterrows():
                    try:
                        centroid = row.geometry.centroid
                        points.append({
                            "lat": centroid.y,
                            "lon": centroid.x,
                            "name": row.get("name", ""),
                        })
                    except Exception:
                        continue

                results[name] = points
                print(f"  → {len(points)} объектов")

            except Exception as e:
                print(f"  ✗ Ошибка: {e}")
                results[name] = []

        with open(cache_file, "w") as f:
            json.dump(results, f, ensure_ascii=False)

        return results

    def count_objects_near_point(
        self, objects: list, lat: float, lon: float, radius_m: int = 1000
    ) -> int:
        """Подсчёт объектов в радиусе от точки"""
        count = 0
        center = (lat, lon)

        for obj in objects:
            try:
                dist = geodesic(center, (obj["lat"], obj["lon"])).meters
                if dist <= radius_m:
                    count += 1
            except Exception:
                continue

        return count

    def calculate_district_infrastructure(
        self, infrastructure: dict, districts: dict
    ) -> pd.DataFrame:
        """Расчёт инфраструктурных метрик для каждого района"""
        rows = []

        for district_name, district_info in districts.items():
            lat = district_info["lat"]
            lon = district_info["lon"]
            population = district_info["population"]

            row = {"district": district_name, "lat": lat, "lon": lon, "population": population}

            for infra_type, objects in infrastructure.items():
                count = self.count_objects_near_point(objects, lat, lon, radius_m=1500)
                row[f"{infra_type}_count"] = count
                row[f"{infra_type}_per_1000"] = round(count / (population / 1000), 3) if population > 0 else 0

            rows.append(row)

        return pd.DataFrame(rows)


if __name__ == "__main__":
    from config import CONFIG

    collector = OSMCollector(CONFIG["city"])
    infrastructure = collector.collect_all()

    df = collector.calculate_district_infrastructure(infrastructure, CONFIG["districts"])
    os.makedirs("data/processed", exist_ok=True)
    df.to_csv("data/processed/infrastructure.csv", index=False)
    print("\n✅ Данные сохранены в data/processed/infrastructure.csv")
    print(df.head())