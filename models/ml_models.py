# models/ml_models.py

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import os
import json

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


class DistrictAnalyzer:
    """ML-анализ районов"""

    def __init__(self):
        self.scaler = StandardScaler()
        self.pca = PCA(n_components=2)
        self.feature_cols = [
            "infrastructure_score",
            "transport_score",
            "ecology_score",
            "safety_score",
            "social_score",
        ]

        self.cluster_descriptions = {
            0: "Развитый комфортный район",
            1: "Спальный район среднего уровня",
            2: "Район с хорошей экологией",
            3: "Проблемный район",
            4: "Активно развивающийся район",
        }

    def cluster_districts(self, df: pd.DataFrame, n_clusters: int = 4) -> pd.DataFrame:
        """Кластеризация районов"""
        X = df[self.feature_cols].values
        X_scaled = self.scaler.fit_transform(X)

        # Подбор оптимального числа кластеров
        if len(df) < n_clusters:
            n_clusters = max(2, len(df) // 2)

        model = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        df = df.copy()
        df["cluster"] = model.fit_predict(X_scaled)

        # PCA для визуализации
        X_pca = self.pca.fit_transform(X_scaled)
        df["pca_1"] = X_pca[:, 0]
        df["pca_2"] = X_pca[:, 1]

        # Описания кластеров
        cluster_profiles = []
        for cluster_id in range(n_clusters):
            cluster_data = df[df["cluster"] == cluster_id]
            profile = {
                "cluster": cluster_id,
                "count": len(cluster_data),
                "districts": cluster_data["district"].tolist(),
                "avg_index": round(cluster_data["total_index"].mean(), 1),
                "avg_infrastructure": round(cluster_data["infrastructure_score"].mean(), 1),
                "avg_transport": round(cluster_data["transport_score"].mean(), 1),
                "avg_ecology": round(cluster_data["ecology_score"].mean(), 1),
                "avg_safety": round(cluster_data["safety_score"].mean(), 1),
                "avg_social": round(cluster_data["social_score"].mean(), 1),
            }

            # Определяем тип кластера
            if profile["avg_index"] >= 60:
                profile["type"] = "🟢 Комфортный"
            elif profile["avg_index"] >= 45:
                profile["type"] = "🟡 Средний"
            else:
                profile["type"] = "🔴 Проблемный"

            # Сильные и слабые стороны
            scores = {
                "Инфраструктура": profile["avg_infrastructure"],
                "Транспорт": profile["avg_transport"],
                "Экология": profile["avg_ecology"],
                "Безопасность": profile["avg_safety"],
                "Соц. среда": profile["avg_social"],
            }
            sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            profile["strength"] = sorted_scores[0][0]
            profile["weakness"] = sorted_scores[-1][0]

            cluster_profiles.append(profile)

        return df, cluster_profiles

    def get_recommendations(self, row: pd.Series) -> list:
        """Рекомендации по улучшению района"""
        recommendations = []

        component_recommendations = {
            "infrastructure_score": {
                "name": "Инфраструктура",
                "actions": [
                    "Построить дополнительные детские площадки",
                    "Открыть новые филиалы поликлиник",
                    "Привлечь малый бизнес (магазины, аптеки)",
                ],
            },
            "transport_score": {
                "name": "Транспорт",
                "actions": [
                    "Добавить автобусные маршруты",
                    "Оборудовать новые остановки",
                    "Создать велодорожки",
                ],
            },
            "ecology_score": {
                "name": "Экология",
                "actions": [
                    "Высадить деревья и кустарники",
                    "Создать новый сквер или парк",
                    "Усилить контроль за вредными выбросами",
                ],
            },
            "safety_score": {
                "name": "Безопасность",
                "actions": [
                    "Установить дополнительное уличное освещение",
                    "Разместить камеры видеонаблюдения",
                    "Оборудовать пешеходные переходы",
                ],
            },
            "social_score": {
                "name": "Социальная среда",
                "actions": [
                    "Организовать общественные пространства",
                    "Провести благоустройство дворов",
                    "Создать досуговые центры для жителей",
                ],
            },
        }

        # Находим самые слабые компоненты
        scores = {
            col: row.get(col, 50) for col in self.feature_cols
        }

        sorted_scores = sorted(scores.items(), key=lambda x: x[1])

        for component, score in sorted_scores[:3]:
            if score < 60:
                info = component_recommendations.get(component, {})
                recommendations.append({
                    "component": info.get("name", component),
                    "current_score": score,
                    "actions": info.get("actions", []),
                    "priority": "Высокий" if score < 35 else "Средний",
                })

        return recommendations

    def save_visualizations(self, df: pd.DataFrame, cluster_profiles: list):
        """Сохранение графиков"""
        if not HAS_MATPLOTLIB:
            print("matplotlib не установлен, пропускаем визуализации")
            return

        os.makedirs("data/visualizations", exist_ok=True)

        # 1. Барчарт индексов
        fig, ax = plt.subplots(figsize=(12, 6))
        df_sorted = df.sort_values("total_index", ascending=True)

        colors = []
        for grade in df_sorted["grade"]:
            color_map = {"A": "#27ae60", "B": "#2ecc71", "C": "#f39c12", "D": "#e67e22", "F": "#e74c3c"}
            colors.append(color_map.get(grade, "#95a5a6"))

        bars = ax.barh(df_sorted["district"], df_sorted["total_index"], color=colors)
        ax.set_xlabel("Индекс комфортности")
        ax.set_title("UrbanPulse — Рейтинг районов")
        ax.set_xlim(0, 100)

        for bar, val in zip(bars, df_sorted["total_index"]):
            ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                    f"{val:.0f}", va="center", fontsize=9)

        plt.tight_layout()
        plt.savefig("data/visualizations/rating.png", dpi=150)
        plt.close()

        # 2. Радарная диаграмма топ-5
        top5 = df.nlargest(5, "total_index")
        categories = ["Инфра", "Транспорт", "Экология", "Безопасность", "Соц.среда"]

        fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))
        angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False).tolist()
        angles += angles[:1]

        colors_radar = ["#3498db", "#2ecc71", "#e74c3c", "#f39c12", "#9b59b6"]

        for i, (_, row) in enumerate(top5.iterrows()):
            values = [
                row["infrastructure_score"],
                row["transport_score"],
                row["ecology_score"],
                row["safety_score"],
                row["social_score"],
            ]
            values += values[:1]

            ax.plot(angles, values, "o-", linewidth=2, label=row["district"], color=colors_radar[i])
            ax.fill(angles, values, alpha=0.1, color=colors_radar[i])

        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(categories)
        ax.set_ylim(0, 100)
        ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.0))
        ax.set_title("Топ-5 районов — профили", pad=20)

        plt.tight_layout()
        plt.savefig("data/visualizations/radar_top5.png", dpi=150)
        plt.close()

        # 3. PCA кластеры
        fig, ax = plt.subplots(figsize=(10, 7))
        scatter = ax.scatter(
            df["pca_1"], df["pca_2"],
            c=df["cluster"], cmap="Set2",
            s=150, alpha=0.8, edgecolors="black", linewidth=0.5
        )

        for _, row in df.iterrows():
            ax.annotate(
                row["district"],
                (row["pca_1"], row["pca_2"]),
                fontsize=8, ha="center", va="bottom",
                xytext=(0, 8), textcoords="offset points"
            )

        ax.set_xlabel("Компонента 1")
        ax.set_ylabel("Компонента 2")
        ax.set_title("Кластеры районов (PCA)")
        plt.colorbar(scatter, label="Кластер")
        plt.tight_layout()
        plt.savefig("data/visualizations/clusters_pca.png", dpi=150)
        plt.close()

        print("✅ Графики сохранены в data/visualizations/")


if __name__ == "__main__":
    df = pd.read_csv("data/processed/comfort_index.csv")

    analyzer = DistrictAnalyzer()
    df_clustered, profiles = analyzer.cluster_districts(df, n_clusters=4)

    # Сохраняем
    df_clustered.to_csv("data/processed/districts_final.csv", index=False)

    print("\n" + "=" * 60)
    print("📊 КЛАСТЕРЫ РАЙОНОВ")
    print("=" * 60)

    for p in profiles:
        print(f"\n{p['type']} (кластер {p['cluster']}):")
        print(f"  Средний индекс: {p['avg_index']}")
        print(f"  Районы: {', '.join(p['districts'])}")
        print(f"  Сильная сторона: {p['strength']}")
        print(f"  Слабая сторона: {p['weakness']}")

    # Рекомендации
    print("\n" + "=" * 60)
    print("💡 РЕКОМЕНДАЦИИ ПО УЛУЧШЕНИЮ")
    print("=" * 60)

    worst_3 = df_clustered.nsmallest(3, "total_index")
    for _, row in worst_3.iterrows():
        recs = analyzer.get_recommendations(row)
        print(f"\n🔴 {row['district']} (индекс {row['total_index']}):")
        for rec in recs:
            print(f"  [{rec['priority']}] {rec['component']} ({rec['current_score']:.0f}/100):")
            for action in rec["actions"]:
                print(f"    • {action}")

    # Визуализации
    analyzer.save_visualizations(df_clustered, profiles)