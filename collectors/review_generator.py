# collectors/review_generator.py

import pandas as pd
import numpy as np
import os
import random

class ReviewGenerator:
    """
    Генератор реалистичных отзывов для демонстрации.
    В реальном проекте заменить на парсинг 2ГИС / Яндекс.Карт.
    """

    def __init__(self):
        self.positive_templates = [
            "Отличный район, очень зелёный, много парков и скверов",
            "Хорошая инфраструктура, рядом школа, садик, поликлиника",
            "Тихий спокойный район, приятно гулять вечером",
            "Много магазинов, всё в шаговой доступности",
            "Отличная транспортная доступность, до центра 15 минут",
            "Новые детские площадки, спортивные тренажёры во дворе",
            "Чисто, убирают регулярно, приятно жить",
            "Хороший район для семьи с детьми",
            "Красивые виды, ухоженные дворы",
            "Рядом большой парк, летом очень красиво",
            "Развитая инфраструктура, не надо никуда ездить",
            "Безопасный район, камеры видеонаблюдения везде",
            "Отличное место, переехали и не жалеем",
            "Много кафе и ресторанов рядом",
            "Хорошие соседи, дружная атмосфера",
        ]

        self.negative_templates = [
            "Ужасные дороги, ямы на каждом шагу",
            "Мусор никто не убирает, во дворе свалка",
            "Очень шумно из-за трассы рядом",
            "Темно вечером, фонари не работают",
            "Нет нормальных магазинов, за всем надо ездить",
            "Пробки утром и вечером, невозможно выехать",
            "Парковки не хватает, машины стоят на газонах",
            "Плохая экология, рядом завод, постоянный запах",
            "Нет детских площадок, детям негде играть",
            "Опасно ходить вечером, плохое освещение",
            "Общественный транспорт ходит редко",
            "Старые разбитые дворы, никто не ремонтирует",
            "Слишком далеко от центра, добираться долго",
            "Нет поликлиники рядом, приходится ездить в другой район",
            "Подъезды грязные, лифт постоянно сломан",
        ]

        self.neutral_templates = [
            "Нормальный район, ничего особенного",
            "Средний район, есть плюсы и минусы",
            "Район как район, жить можно",
            "Неплохо, но есть что улучшить",
            "В целом нормально, но хотелось бы больше зелени",
        ]

    def generate_for_districts(self, districts: dict, seed: int = 42) -> pd.DataFrame:
        """Генерация отзывов для всех районов"""
        np.random.seed(seed)
        random.seed(seed)

        # Районы с разным «настроением»
        district_mood = {
            "Центр": 0.7,
            "Ботанический": 0.6,
            "Академический": 0.75,
            "Уралмаш": 0.35,
            "Эльмаш": 0.4,
            "Пионерский": 0.65,
            "Вторчермет": 0.3,
            "Юго-Западный": 0.55,
            "Сортировка": 0.25,
            "Парковый": 0.7,
            "ЖБИ": 0.5,
            "Синие Камни": 0.45,
            "Широкая Речка": 0.6,
            "Компрессорный": 0.35,
            "Втузгородок": 0.6,
        }

        rows = []

        for district_name in districts:
            mood = district_mood.get(district_name, 0.5)
            n_reviews = random.randint(30, 100)

            for _ in range(n_reviews):
                r = random.random()

                if r < mood * 0.7:
                    text = random.choice(self.positive_templates)
                    rating = random.choice([4, 5])
                elif r < mood * 0.7 + 0.15:
                    text = random.choice(self.neutral_templates)
                    rating = random.choice([3, 4])
                else:
                    text = random.choice(self.negative_templates)
                    rating = random.choice([1, 2])

                # Добавляем немного вариативности
                additions = [
                    f" Живу здесь {random.randint(1,20)} лет.",
                    f" Оценка {rating} из 5.",
                    "",
                    "",
                    " Рекомендую!",
                    " Не рекомендую.",
                ]
                text += random.choice(additions)

                rows.append({
                    "district": district_name,
                    "text": text,
                    "rating": rating,
                    "lat": districts[district_name]["lat"] + np.random.normal(0, 0.005),
                    "lon": districts[district_name]["lon"] + np.random.normal(0, 0.005),
                })

        return pd.DataFrame(rows)


if __name__ == "__main__":
    from config import CONFIG

    generator = ReviewGenerator()
    reviews_df = generator.generate_for_districts(CONFIG["districts"])
    os.makedirs("data/processed", exist_ok=True)
    reviews_df.to_csv("data/processed/reviews.csv", index=False)
    print(f"✅ Сгенерировано {len(reviews_df)} отзывов")
    print(reviews_df.head(10))
    print(f"\nПо районам:")
    print(reviews_df.groupby("district").agg({"rating": ["count", "mean"]}).round(2))