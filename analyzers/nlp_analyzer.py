# analyzers/nlp_analyzer.py

import pandas as pd
import numpy as np
from collections import Counter
import re
import os

# Попробуем использовать transformers, если установлен
try:
    from transformers import pipeline
    HAS_TRANSFORMERS = True
except ImportError:
    HAS_TRANSFORMERS = False


class NLPAnalyzer:
    """Анализ тональности отзывов"""

    def __init__(self, use_ml: bool = True):
        self.use_ml = use_ml and HAS_TRANSFORMERS

        if self.use_ml:
            print("Загружаем модель тональности (ruBERT)...")
            try:
                self.sentiment_pipeline = pipeline(
                    "sentiment-analysis",
                    model="blanchefort/rubert-base-cased-sentiment-rurewiews",
                    tokenizer="blanchefort/rubert-base-cased-sentiment-rurewiews",
                    device=-1,  # CPU
                )
                print("✅ Модель загружена")
            except Exception as e:
                print(f"✗ Не удалось загрузить модель: {e}")
                print("Используем словарный метод")
                self.use_ml = False
        else:
            print("Используем словарный метод анализа тональности")

        # Словари для простого метода
        self.positive_words = {
            "отлично", "хорошо", "прекрасно", "замечательно", "красиво",
            "чисто", "тихо", "удобно", "рядом", "доступно", "зелёный",
            "зеленый", "парк", "уютно", "безопасно", "новый", "ухоженный",
            "приятно", "рекомендую", "нравится", "люблю", "комфортно",
            "развитая", "спокойный", "дружная",
        }

        self.negative_words = {
            "ужас", "плохо", "грязь", "грязно", "мусор", "шум", "шумно",
            "темно", "ямы", "яма", "пробки", "опасно", "страшно",
            "сломан", "разбит", "далеко", "нет", "никто", "свалка",
            "вонь", "запах", "завод", "старый", "разбитый", "негде",
            "невозможно", "ужасный",
        }

        # Категории проблем
        self.problem_keywords = {
            "Мусор и чистота": ["мусор", "грязь", "грязно", "свалка", "помойка", "убирает", "уборка"],
            "Дороги и тротуары": ["яма", "ямы", "асфальт", "дорога", "тротуар", "лужи", "разбит"],
            "Освещение": ["темно", "фонарь", "освещение", "свет", "темнота"],
            "Шум": ["шум", "громко", "шумно", "грохот", "стройка", "трасса"],
            "Парковка": ["парковка", "парковать", "газон", "стоянка", "машины"],
            "Озеленение": ["деревья", "газон", "клумба", "зелень", "парк", "сквер", "зелёный"],
            "Детская инфраструктура": ["площадка", "качели", "горка", "песочница", "детская", "детям"],
            "Безопасность": ["опасно", "кража", "страшно", "небезопасно", "камеры"],
            "Транспорт": ["автобус", "маршрутка", "метро", "пробка", "остановка", "транспорт"],
            "ЖКХ": ["подъезд", "лифт", "двор", "управляющая", "ремонт"],
        }

    def analyze_sentiment_simple(self, text: str) -> dict:
        """Простой словарный анализ тональности"""
        words = set(re.findall(r"[а-яёА-ЯЁ]+", text.lower()))

        pos_count = len(words & self.positive_words)
        neg_count = len(words & self.negative_words)

        total = pos_count + neg_count
        if total == 0:
            return {"label": "NEUTRAL", "score": 0.5}

        if pos_count > neg_count:
            return {"label": "POSITIVE", "score": 0.5 + 0.5 * (pos_count - neg_count) / total}
        elif neg_count > pos_count:
            return {"label": "NEGATIVE", "score": 0.5 + 0.5 * (neg_count - pos_count) / total}
        else:
            return {"label": "NEUTRAL", "score": 0.5}

    def analyze_sentiment_ml(self, texts: list) -> list:
        """ML анализ тональности через ruBERT"""
        results = []
        batch_size = 16

        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch = [t[:512] for t in batch]

            try:
                predictions = self.sentiment_pipeline(batch)
                results.extend(predictions)
            except Exception as e:
                for t in batch:
                    results.append(self.analyze_sentiment_simple(t))

        return results

    def analyze_reviews(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
        """Полный анализ всех отзывов"""
        print(f"Анализируем {len(reviews_df)} отзывов...")

        texts = reviews_df["text"].tolist()

        if self.use_ml:
            sentiments = self.analyze_sentiment_ml(texts)
        else:
            sentiments = [self.analyze_sentiment_simple(t) for t in texts]

        reviews_df = reviews_df.copy()
        reviews_df["sentiment_label"] = [s["label"] for s in sentiments]
        reviews_df["sentiment_score"] = [s["score"] for s in sentiments]

        # Числовая оценка тональности
        label_map = {"POSITIVE": 1.0, "NEUTRAL": 0.5, "NEGATIVE": 0.0}
        # Для модели из transformers метки могут быть на русском
        label_map_ru = {"positive": 1.0, "neutral": 0.5, "negative": 0.0}

        def map_label(label):
            label_lower = label.lower()
            if label_lower in label_map_ru:
                return label_map_ru[label_lower]
            return label_map.get(label, 0.5)

        reviews_df["sentiment_numeric"] = reviews_df["sentiment_label"].apply(map_label)

        print("✅ Анализ тональности завершён")
        return reviews_df

    def extract_problems(self, texts: list) -> dict:
        """Извлечение проблем из текстов"""
        problem_counts = Counter()

        for text in texts:
            text_lower = text.lower()
            for problem, keywords in self.problem_keywords.items():
                if any(kw in text_lower for kw in keywords):
                    problem_counts[problem] += 1

        return dict(problem_counts.most_common())

    def get_district_profiles(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
        """Профили тональности по районам"""
        profiles = []

        for district in reviews_df["district"].unique():
            district_reviews = reviews_df[reviews_df["district"] == district]
            texts = district_reviews["text"].tolist()

            problems = self.extract_problems(texts)

            profile = {
                "district": district,
                "review_count": len(district_reviews),
                "avg_rating": round(district_reviews["rating"].mean(), 2),
                "avg_sentiment": round(district_reviews["sentiment_numeric"].mean(), 3),
                "positive_share": round(
                    (district_reviews["sentiment_label"].str.upper() == "POSITIVE").mean(), 3
                ),
                "negative_share": round(
                    (district_reviews["sentiment_label"].str.upper() == "NEGATIVE").mean(), 3
                ),
                "top_problem_1": list(problems.keys())[0] if len(problems) >= 1 else "",
                "top_problem_2": list(problems.keys())[1] if len(problems) >= 2 else "",
                "top_problem_3": list(problems.keys())[2] if len(problems) >= 3 else "",
                "problems_json": str(problems),
            }

            profiles.append(profile)

        return pd.DataFrame(profiles)


if __name__ == "__main__":
    reviews_df = pd.read_csv("data/processed/reviews.csv")

    analyzer = NLPAnalyzer(use_ml=False)  # True если хотите ruBERT
    analyzed = analyzer.analyze_reviews(reviews_df)
    analyzed.to_csv("data/processed/reviews_analyzed.csv", index=False)

    profiles = analyzer.get_district_profiles(analyzed)
    profiles.to_csv("data/processed/review_profiles.csv", index=False)

    print("\n✅ Результаты сохранены")
    print("\nПрофили районов:")
    print(profiles[["district", "review_count", "avg_rating", "avg_sentiment",
                     "positive_share", "top_problem_1"]].to_string(index=False))