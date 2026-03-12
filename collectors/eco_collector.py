# collectors/eco_collector.py

import requests
import pandas as pd
import os
import time


class EcoCollector:
    """Сбор экологических данных"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.cache_dir = "data/raw/eco"
        os.makedirs(self.cache_dir, exist_ok=True)

    def get_air_quality(self, lat: float, lon: float) -> dict:
        """Качество воздуха через OpenWeatherMap API"""
        url = "http://api.openweathermap.org/data/2.5/air_pollution"
        params = {"lat": lat, "lon": lon, "appid": self.api_key}

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if "list" in data and len(data["list"]) > 0:
                item = data["list"][0]
                return {
                    "aqi": item["main"]["aqi"],
                    "pm2_5": item["components"].get("pm2_5", 0),
                    "pm10": item["components"].get("pm10", 0),
                    "no2": item["components"].get("no2", 0),
                    "co": item["components"].get("co", 0),
                    "o3": item["components"].get("o3", 0),
                    "so2": item["components"].get("so2", 0),
                }
        except Exception as e:
            print(f"  ✗ Ошибка API воздуха: {e}")

        return {"aqi": 3, "pm2_5": 0, "pm10": 0, "no2": 0, "co": 0, "o3": 0, "so2": 0}

    def estimate_noise_level(self, infrastructure_df: pd.DataFrame, district: str) -> float:
        """Оценка уровня шума по косвенным данным"""
        row = infrastructure_df[infrastructure_df["district"] == district]
        if row.empty:
            return 55.0

        row = row.iloc[0]

        base_noise = 45.0
        bus_stops = row.get("bus_stops_count", 0)
        cafes = row.get("cafes_count", 0)
        restaurants = row.get("restaurants_count", 0)
        shops = row.get("shops_count", 0)

        noise = base_noise + bus_stops * 0.8 + (cafes + restaurants) * 0.5 + shops * 0.3
        return min(noise, 85.0)

    def estimate_green_coverage(self, infrastructure_df: pd.DataFrame, district: str) -> float:
        """Оценка процента озеленения"""
        row = infrastructure_df[infrastructure_df["district"] == district]
        if row.empty:
            return 15.0

        row = row.iloc[0]
        parks = row.get("parks_count", 0)
        base_green = 10.0
        green = base_green + parks * 8.0
        return min(green, 60.0)

    def collect_for_districts(self, districts: dict, infrastructure_df: pd.DataFrame) -> pd.DataFrame:
        """Сбор экоданных для всех районов"""
        rows = []

        for district_name, info in districts.items():
            print(f"Экоданные для {district_name}...")

            air = self.get_air_quality(info["lat"], info["lon"])
            noise = self.estimate_noise_level(infrastructure_df, district_name)
            green = self.estimate_green_coverage(infrastructure_df, district_name)

            row = {
                "district": district_name,
                "air_quality_index": air["aqi"],
                "pm2_5": air["pm2_5"],
                "pm10": air["pm10"],
                "no2": air["no2"],
                "noise_level_db": round(noise, 1),
                "green_coverage_pct": round(green, 1),
            }

            rows.append(row)
            time.sleep(1)  # не превышать лимит API

        return pd.DataFrame(rows)


if __name__ == "__main__":
    from config import CONFIG

    collector = EcoCollector(CONFIG["openweather_api_key"])
    infra_df = pd.read_csv("data/processed/infrastructure.csv")
    eco_df = collector.collect_for_districts(CONFIG["districts"], infra_df)
    eco_df.to_csv("data/processed/ecology.csv", index=False)
    print("\n✅ Экоданные сохранены")
    print(eco_df)