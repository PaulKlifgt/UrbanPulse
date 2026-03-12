# analyzers/comfort_index.py

import pandas as pd
import numpy as np
import os
import json


class ComfortIndexCalculator:
    """Расчёт интегрального индекса комфортности — 8 компонент"""

    def __init__(self):
        self.weights = {
            "infrastructure": 0.15,
            "education": 0.10,
            "healthcare": 0.10,
            "transport": 0.15,
            "ecology": 0.12,
            "safety": 0.10,
            "leisure": 0.10,
            "social": 0.18,
        }

    # ────────────────────────────────────────
    # 1. ИНФРАСТРУКТУРА (магазины, банки, аптеки)
    # ────────────────────────────────────────
    def calculate_infrastructure_score(self, row: pd.Series) -> float:
        scores = []

        # Магазины: норма ~3 на 1000
        shops = row.get("shops_per_1000", 0) + row.get("supermarkets_per_1000", 0)
        scores.append(self._sigmoid_score(shops, midpoint=3.0, steepness=0.8))

        # Аптеки: норма ~0.5 на 1000
        pharm = row.get("pharmacies_per_1000", 0)
        scores.append(self._sigmoid_score(pharm, midpoint=0.5, steepness=2.0))

        # Банки
        banks = row.get("banks_count", 0)
        scores.append(self._sigmoid_score(banks, midpoint=4, steepness=0.4))

        # Супермаркеты (отдельно — показатель качества торговли)
        super_count = row.get("supermarkets_count", 0)
        scores.append(self._sigmoid_score(super_count, midpoint=3, steepness=0.5))

        return self._weighted_mean_100(scores)

    # ────────────────────────────────────────
    # 2. ОБРАЗОВАНИЕ
    # ────────────────────────────────────────
    def calculate_education_score(self, row: pd.Series) -> float:
        scores = []

        # Школы: норма ~0.5 на 1000
        schools = row.get("schools_per_1000", 0)
        scores.append(self._sigmoid_score(schools, midpoint=0.5, steepness=2.5))

        # Детсады: норма ~0.5 на 1000
        kinder = row.get("kindergartens_per_1000", 0)
        scores.append(self._sigmoid_score(kinder, midpoint=0.5, steepness=2.5))

        # Библиотеки
        libs = row.get("libraries_count", 0)
        scores.append(self._sigmoid_score(libs, midpoint=2, steepness=0.8))

        return self._weighted_mean_100(scores)

    # ────────────────────────────────────────
    # 3. ЗДРАВООХРАНЕНИЕ
    # ────────────────────────────────────────
    def calculate_healthcare_score(self, row: pd.Series) -> float:
        scores = []

        # Больницы: даже 1 рядом — хорошо
        hosp = row.get("hospitals_count", 0)
        scores.append(self._sigmoid_score(hosp, midpoint=1.0, steepness=1.5))

        # Клиники/поликлиники
        clinics = row.get("clinics_count", 0)
        scores.append(self._sigmoid_score(clinics, midpoint=2.0, steepness=0.7))

        # Аптеки
        pharm = row.get("pharmacies_count", 0)
        scores.append(self._sigmoid_score(pharm, midpoint=4.0, steepness=0.4))

        return self._weighted_mean_100(scores)

    # ────────────────────────────────────────
    # 4. ТРАНСПОРТ
    # ────────────────────────────────────────
    def calculate_transport_score(self, row: pd.Series) -> float:
        scores = []

        # Остановки ОТ: норма ~8 в радиусе 1.5км
        stops = row.get("bus_stops_count", 0)
        scores.append(self._sigmoid_score(stops, midpoint=8, steepness=0.25))

        # Плотность остановок (на 1000 жителей)
        stops_per_1k = row.get("bus_stops_per_1000", 0)
        scores.append(self._sigmoid_score(stops_per_1k, midpoint=1.0, steepness=1.2))

        return self._weighted_mean_100(scores)

    # ────────────────────────────────────────
    # 5. ЭКОЛОГИЯ
    # ────────────────────────────────────────
    def calculate_ecology_score(self, row: pd.Series) -> float:
        scores = []

        # AQI: 1=отлично, 5=плохо
        aqi = row.get("air_quality_index", 3)
        scores.append(max(0, (5 - aqi) / 4))

        # PM2.5: <10 отлично, >25 плохо
        pm25 = row.get("pm2_5", 15)
        if pm25 <= 10:
            scores.append(1.0)
        elif pm25 >= 35:
            scores.append(0.0)
        else:
            scores.append((35 - pm25) / 25)

        # Озеленение
        green = row.get("green_coverage_pct", 15)
        scores.append(self._sigmoid_score(green, midpoint=25, steepness=0.1))

        # Шум
        noise = row.get("noise_level_db", 55)
        if noise <= 40:
            scores.append(1.0)
        elif noise >= 75:
            scores.append(0.0)
        else:
            scores.append((75 - noise) / 35)

        # Парки рядом (экологический фактор)
        parks = row.get("parks_count", 0)
        scores.append(self._sigmoid_score(parks, midpoint=3, steepness=0.5))

        return self._weighted_mean_100(scores)

    # ────────────────────────────────────────
    # 6. БЕЗОПАСНОСТЬ (переработанная!)
    # ────────────────────────────────────────
    def calculate_safety_score(self, row: pd.Series) -> float:
        """
        Безопасность — комбинация факторов:
        - Освещённость (косвенно: плотность остановок и магазинов)
        - Людность (чем больше людей — тем безопаснее)
        - Удалённость от центра (обычно окраины менее безопасны)
        - Наличие камер/полиции (нет данных → используем прокси)
        - Шум ночью (тихие пустые районы менее безопасны)
        
        КЛЮЧЕВОЕ: используем УБЫВАЮЩУЮ шкалу с высоким порогом,
        чтобы НЕ все районы получали 100%.
        """
        scores = []

        # 1. Людность улиц: остановки + магазины + кафе
        #    Но с ВЫСОКИМ порогом!
        bus = row.get("bus_stops_count", 0)
        shops = row.get("shops_count", 0)
        cafes = row.get("cafes_count", 0)
        activity = bus + shops + cafes

        # Сигмоида с серединой 25 — нужно МНОГО объектов для хорошей оценки
        scores.append(self._sigmoid_score(activity, midpoint=25, steepness=0.08))

        # 2. Освещённость (прокси: плотность инфраструктуры)
        total_infra = (bus + shops + row.get("pharmacies_count", 0) +
                       row.get("banks_count", 0) + row.get("restaurants_count", 0))
        scores.append(self._sigmoid_score(total_infra, midpoint=30, steepness=0.06))

        # 3. Жилая активность: площадки + школы + садики = семьи = безопаснее
        family_infra = (row.get("playgrounds_count", 0) +
                        row.get("schools_count", 0) +
                        row.get("kindergartens_count", 0))
        scores.append(self._sigmoid_score(family_infra, midpoint=8, steepness=0.2))

        # 4. Пустынность — ШТРАФ
        #    Если мало объектов вообще — это пустырь, небезопасно
        all_objects = activity + total_infra + family_infra
        if all_objects < 10:
            scores.append(0.15)  # Сильный штраф
        elif all_objects < 20:
            scores.append(0.35)
        elif all_objects < 35:
            scores.append(0.55)
        else:
            scores.append(0.75)

        # 5. Вечерняя жизнь (кафе + рестораны = люди вечером)
        evening = row.get("cafes_count", 0) + row.get("restaurants_count", 0)
        scores.append(self._sigmoid_score(evening, midpoint=8, steepness=0.2))

        # 6. Негативные отзывы о безопасности (из NLP)
        neg_share = row.get("negative_share", 0.3)
        # Чем больше негатива — тем ниже безопасность
        scores.append(max(0, 1.0 - neg_share * 2))

        return self._weighted_mean_100(scores)

    # ────────────────────────────────────────
    # 7. ДОСУГ И КУЛЬТУРА (НОВАЯ!)
    # ────────────────────────────────────────
    def calculate_leisure_score(self, row: pd.Series) -> float:
        scores = []

        # Кафе и рестораны
        cafes = row.get("cafes_count", 0) + row.get("restaurants_count", 0)
        scores.append(self._sigmoid_score(cafes, midpoint=8, steepness=0.2))

        # Кинотеатры
        cinema = row.get("cinemas_count", 0)
        scores.append(self._sigmoid_score(cinema, midpoint=1, steepness=1.5))

        # Театры
        theatre = row.get("theatres_count", 0)
        scores.append(self._sigmoid_score(theatre, midpoint=1, steepness=1.5))

        # Фитнес
        fitness = row.get("fitness_count", 0)
        scores.append(self._sigmoid_score(fitness, midpoint=3, steepness=0.5))

        # Площадки
        play = row.get("playgrounds_count", 0)
        scores.append(self._sigmoid_score(play, midpoint=5, steepness=0.3))

        # Парки (досуговый аспект)
        parks = row.get("parks_count", 0)
        scores.append(self._sigmoid_score(parks, midpoint=2, steepness=0.7))

        return self._weighted_mean_100(scores)

    # ────────────────────────────────────────
    # 8. СОЦИАЛЬНАЯ СРЕДА
    # ────────────────────────────────────────
    def calculate_social_score(self, row: pd.Series) -> float:
        scores = []

        # Средний сентимент (0-1)
        avg_sent = row.get("avg_sentiment", 0.5)
        scores.append(avg_sent)

        # Доля позитивных
        pos = row.get("positive_share", 0.4)
        scores.append(pos)

        # Средний рейтинг (1-5 → 0-1)
        rating = row.get("avg_rating", 3.0)
        scores.append((rating - 1) / 4.0)

        # Доля негативных (инверсия) — штраф за много негатива
        neg = row.get("negative_share", 0.3)
        scores.append(max(0, 1.0 - neg * 1.5))

        return self._weighted_mean_100(scores)

    # ────────────────────────────────────────
    # УТИЛИТЫ
    # ────────────────────────────────────────
    @staticmethod
    def _sigmoid_score(value, midpoint=5, steepness=0.3):
        """
        Сигмоидная функция оценки.
        
        При value = midpoint → результат = 0.5
        steepness контролирует крутизну:
          - больше = быстрее насыщается
          - меньше = плавнее рост
        
        Возвращает значение от 0 до 1.
        """
        return 1.0 / (1.0 + np.exp(-steepness * (value - midpoint)))

    @staticmethod
    def _weighted_mean_100(scores):
        """Среднее * 100, обрезанное до [0, 100]"""
        if not scores:
            return 50.0
        return round(np.clip(np.mean(scores) * 100, 0, 100), 1)

    def index_to_grade(self, index: float) -> str:
        if index >= 75:
            return "A"
        elif index >= 60:
            return "B"
        elif index >= 45:
            return "C"
        elif index >= 30:
            return "D"
        else:
            return "F"

    def grade_to_emoji(self, grade: str) -> str:
        return {"A": "🟢", "B": "🔵", "C": "🟡", "D": "🟠", "F": "🔴"}.get(grade, "⚪")

    def calculate_all(
        self,
        infrastructure_df: pd.DataFrame,
        eco_df: pd.DataFrame,
        review_profiles_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Объединение данных и расчёт итогового индекса (8 компонент)"""

        df = infrastructure_df.copy()
        df = df.merge(eco_df, on="district", how="left")
        df = df.merge(review_profiles_df, on="district", how="left")
        df = df.fillna(0)

        # 8 компонент
        df["infrastructure_score"] = df.apply(self.calculate_infrastructure_score, axis=1)
        df["education_score"] = df.apply(self.calculate_education_score, axis=1)
        df["healthcare_score"] = df.apply(self.calculate_healthcare_score, axis=1)
        df["transport_score"] = df.apply(self.calculate_transport_score, axis=1)
        df["ecology_score"] = df.apply(self.calculate_ecology_score, axis=1)
        df["safety_score"] = df.apply(self.calculate_safety_score, axis=1)
        df["leisure_score"] = df.apply(self.calculate_leisure_score, axis=1)
        df["social_score"] = df.apply(self.calculate_social_score, axis=1)

        # Итоговый индекс
        df["total_index"] = sum(
            df[f"{comp}_score"] * weight
            for comp, weight in self.weights.items()
        ).round(1)

        df["grade"] = df["total_index"].apply(self.index_to_grade)
        df["grade_emoji"] = df["grade"].apply(self.grade_to_emoji)

        return df


if __name__ == "__main__":
    infra_df = pd.read_csv("data/processed/infrastructure.csv")
    eco_df = pd.read_csv("data/processed/ecology.csv")
    profiles_df = pd.read_csv("data/processed/review_profiles.csv")

    calculator = ComfortIndexCalculator()
    result = calculator.calculate_all(infra_df, eco_df, profiles_df)
    result = result.sort_values("total_index", ascending=False)
    result.to_csv("data/processed/comfort_index.csv", index=False)

    print("\n" + "=" * 90)
    print("🏙️  URBANPULSE — ИНДЕКС КОМФОРТНОСТИ (8 компонент)")
    print("=" * 90)

    for _, row in result.iterrows():
        print(
            f"{row['grade_emoji']} {row['district']:20s} │ "
            f"Σ {row['total_index']:5.1f} ({row['grade']}) │ "
            f"Инфр {row['infrastructure_score']:4.0f} │ "
            f"Обр {row['education_score']:4.0f} │ "
            f"Здр {row['healthcare_score']:4.0f} │ "
            f"Тран {row['transport_score']:4.0f} │ "
            f"Эко {row['ecology_score']:4.0f} │ "
            f"Без {row['safety_score']:4.0f} │ "
            f"Дос {row['leisure_score']:4.0f} │ "
            f"Соц {row['social_score']:4.0f}"
        )

    print("=" * 90)