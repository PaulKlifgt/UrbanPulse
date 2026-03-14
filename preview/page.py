from __future__ import annotations

import csv
from pathlib import Path

import streamlit as st


ASSETS_DIR = Path(__file__).parent / "assets"
DATA_DIR = Path("data")

SCREEN_ASSETS = [
    {
        "title": "Городская панель",
        "description": "Главный экран собирает карту, рейтинг районов и быстрые метрики по городу в одном рабочем представлении.",
        "files": (
            "screen-dashboard.png",
            "dashboard.png",
            "dashboard.jpg",
            "dashboard.webp",
            "screen-dashboard.svg",
        ),
    },
    {
        "title": "Разбор района",
        "description": "Детальная зона показывает радар факторов, проблематику из отзывов и объясняет, что именно тянет район вверх или вниз.",
        "files": (
            "screen-district.png",
            "district.png",
            "district.jpg",
            "district.webp",
            "screen-district.svg",
        ),
    },
    {
        "title": "Недвижимость и пайплайн",
        "description": "Система связывает скоринг района с живыми объявлениями и одновременно показывает, из каких модулей складывается расчёт.",
        "files": (
            "screen-market.png",
            "market.png",
            "market.jpg",
            "market.webp",
            "screen-market.svg",
        ),
    },
]

VIDEO_FILES = ("demo.mp4", "demo.webm", "demo.mov")

PREVIEW_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;700;800&display=swap');

#MainMenu, footer { visibility: hidden; }

header {
    visibility: visible !important;
    background: transparent !important;
}

[data-testid="collapsedControl"] {
    display: flex !important;
    visibility: visible !important;
    position: fixed !important;
    top: 0.75rem !important;
    left: 0.75rem !important;
    z-index: 10000 !important;
    border-radius: 10px !important;
    background: rgba(255, 255, 255, 0.92) !important;
    border: 1px solid #d7dbe2 !important;
}

/* Hide Streamlit runtime toolbar/status ("Stop / Deploy"). */
[data-testid="stStatusWidget"],
[data-testid="stDecoration"] {
    display: none !important;
}

html, body, [class*="css"] {
    font-family: "Manrope", sans-serif !important;
    background: #ececec !important;
}

.stApp {
    background: #ececec !important;
}

[data-testid="stAppViewContainer"] > .main {
    padding-top: 0.35rem;
    background: #ececec !important;
}

[data-testid="stAppViewContainer"] > .main > div {
    max-width: 100%;
    padding: 0.25rem 0.45rem 0.9rem 0.45rem;
    background: #ececec !important;
}

.pv-shell {
    padding: 0 0 1rem 0;
}

.pv-layout {
    display: grid;
    grid-template-columns: 290px minmax(0, 1fr);
    gap: 1rem;
    align-items: start;
}

.pv-nav {
    background: #f7f7f8;
    border: 1px solid #d7dbe2;
    border-radius: 22px;
    box-shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
    padding: 1rem;
    position: sticky;
    top: 0.5rem;
}

.pv-nav-kicker {
    color: #8b94a7;
    font-size: 0.76rem;
    font-weight: 800;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

.pv-nav-title {
    margin: 0.28rem 0 0 0;
    color: #22293a;
    font-size: 2rem;
    line-height: 0.95;
    font-weight: 800;
}

.pv-nav-copy {
    margin: 0.5rem 0 0 0;
    color: #667085;
    line-height: 1.55;
}

.pv-nav-search {
    margin-top: 1rem;
    padding: 0.95rem 1rem;
    border: 1px solid #d7dbe2;
    border-radius: 14px;
    background: #ffffff;
    color: #9aa3b2;
    font-weight: 700;
}

.pv-nav-list {
    display: grid;
    gap: 0.5rem;
    margin-top: 1rem;
}

.pv-nav-item {
    padding: 0.9rem 1rem;
    border-radius: 14px;
    color: #445065;
    font-weight: 700;
}

.pv-nav-item.is-active {
    background: linear-gradient(90deg, #081735 0%, #102654 100%);
    color: #ffffff;
}

.pv-nav-footer {
    margin-top: 1.1rem;
    padding-top: 1rem;
    border-top: 1px solid #d7dbe2;
}

.pv-nav-user {
    display: flex;
    align-items: center;
    gap: 0.8rem;
}

.pv-nav-avatar {
    width: 42px;
    height: 42px;
    border-radius: 999px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: #081735;
    color: #fff;
    font-weight: 800;
}

.pv-nav-user strong {
    display: block;
    color: #22293a;
}

.pv-nav-user span {
    display: block;
    color: #667085;
    font-size: 0.88rem;
}

.pv-main {
    min-width: 0;
}

.pv-hero {
    padding: 1.15rem 1.2rem;
    border: 1px solid #d7dbe2;
    border-radius: 22px;
    background: #f7f7f8;
    box-shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
}

.pv-kicker,
.pv-section-kicker {
    display: inline-block;
    color: #8b94a7;
    font-size: 0.76rem;
    font-weight: 800;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

.pv-title,
.pv-section-title,
.pv-card-title {
    color: #22293a;
    letter-spacing: -0.04em;
}

.pv-title {
    margin: 0.3rem 0 0 0;
    font-size: 2.25rem;
    line-height: 0.98;
    font-weight: 800;
}

.pv-copy {
    max-width: 780px;
    margin: 0.45rem 0 0 0;
    color: #667085;
    line-height: 1.6;
}

.pv-actions {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem;
    margin-top: 1rem;
}

.pv-btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 44px;
    padding: 0.8rem 1rem;
    border-radius: 12px;
    text-decoration: none !important;
    font-weight: 800;
}

.pv-btn-primary {
    background: #081735;
    color: #fff !important;
}

.pv-btn-secondary {
    color: #22293a !important;
    background: #fff;
    border: 1px solid #d7dbe2;
}

.pv-grid-4,
.pv-grid-3,
.pv-roadmap {
    display: grid;
    gap: 1rem;
}

.pv-grid-4 {
    grid-template-columns: repeat(4, minmax(0, 1fr));
    margin-top: 1rem;
}

.pv-grid-3 {
    grid-template-columns: repeat(3, minmax(0, 1fr));
}

.pv-stat,
.pv-card,
.pv-video-note,
.pv-roadmap-step {
    background: #ffffff;
    border: 1px solid #d7dbe2;
    border-radius: 18px;
    box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
}

.pv-stat {
    padding: 1rem;
}

.pv-stat-label {
    color: #8b94a7;
    font-size: 0.72rem;
    font-weight: 800;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

.pv-stat-value {
    margin-top: 0.75rem;
    color: #22293a;
    font-size: 2rem;
    font-weight: 800;
}

.pv-stat-copy {
    margin-top: 0.35rem;
    color: #667085;
    font-size: 0.9rem;
    line-height: 1.55;
}

.pv-section {
    margin-top: 1rem;
}

.pv-section-head {
    margin-bottom: 0.85rem;
    padding: 0.15rem 0.2rem;
}

.pv-section-title {
    margin: 0.32rem 0 0 0;
    font-size: 1.65rem;
    line-height: 1;
    font-weight: 800;
}

.pv-section-copy {
    max-width: 760px;
    margin: 0.35rem 0 0 0;
    color: #667085;
    font-size: 0.96rem;
    line-height: 1.6;
}

.pv-card {
    overflow: hidden;
}

.pv-card-body {
    padding: 1rem 1rem 1.05rem 1rem;
}

.pv-card-title {
    margin: 0;
    font-size: 1.05rem;
    font-weight: 800;
}

.pv-card-copy {
    margin: 0.5rem 0 0 0;
    color: #667085;
    line-height: 1.6;
}

.pv-bullets {
    margin-top: 0.2rem;
    display: grid;
    gap: 0.75rem;
}

.pv-bullet {
    padding: 1rem;
    border-radius: 16px;
    background: #ffffff;
    border: 1px solid #d7dbe2;
}

.pv-bullet strong,
.pv-roadmap-step strong {
    display: block;
    margin-bottom: 0.2rem;
    color: #22293a;
}

.pv-bullet span,
.pv-roadmap-step span,
.pv-video-note span {
    color: #667085;
    line-height: 1.6;
}

.pv-video-wrap {
    padding: 0.8rem;
    border-radius: 18px;
    background: #f7f7f8;
    border: 1px solid #d7dbe2;
    box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
}

.pv-video-note {
    padding: 1rem;
    margin-top: 0.8rem;
}

.pv-roadmap {
    grid-template-columns: repeat(4, minmax(0, 1fr));
}

.pv-roadmap-step {
    padding: 1rem;
}

.pv-step-no {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 34px;
    height: 34px;
    border-radius: 50%;
    background: rgba(59, 130, 246, 0.12);
    color: #3b82f6;
    font-size: 0.9rem;
    font-weight: 800;
}

@media (max-width: 980px) {
    .pv-layout {
        grid-template-columns: 1fr;
    }

    .pv-nav {
        position: static;
    }

    .pv-title {
        font-size: 1.8rem;
    }

    .pv-grid-4,
    .pv-grid-3,
    .pv-roadmap {
        grid-template-columns: 1fr;
    }
}
</style>
"""


@st.cache_data(show_spinner=False)
def _count_rows(csv_path: str) -> int:
    try:
        with open(csv_path, "r", encoding="utf-8") as fh:
            return max(sum(1 for _ in csv.reader(fh)) - 1, 0)
    except Exception:
        return 0


@st.cache_data(show_spinner=False)
def get_project_metrics() -> dict[str, int]:
    city_csvs = sorted(DATA_DIR.glob("*/processed/districts_final.csv"))
    city_count = len(city_csvs)
    zone_count = sum(_count_rows(str(path)) for path in city_csvs)
    return {
        "cities": city_count or 32,
        "zones": zone_count,
        "components": 8,
        "categories": 21,
    }


def _find_asset(candidates: tuple[str, ...]) -> Path | None:
    for file_name in candidates:
        candidate = ASSETS_DIR / file_name
        if candidate.exists():
            return candidate
    return None


def _render_screen_card(screen: dict[str, object]) -> None:
    asset = _find_asset(screen["files"])
    with st.container():
        st.markdown('<div class="pv-card">', unsafe_allow_html=True)
        if asset is not None:
            st.image(str(asset), width="stretch")
        st.markdown(
            f"""
            <div class="pv-card-body">
                <h3 class="pv-card-title">{screen["title"]}</h3>
                <p class="pv-card-copy">{screen["description"]}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)


def render_page() -> None:
    metrics = get_project_metrics()
    video_path = _find_asset(VIDEO_FILES)

    st.markdown(PREVIEW_CSS, unsafe_allow_html=True)
    st.markdown('<div class="pv-shell">', unsafe_allow_html=True)
    st.markdown(
        f"""
        <section class="pv-hero">
            <span class="pv-kicker">Product Deck</span>
            <h1 class="pv-title">UrbanPulse Documentation</h1>
            <div class="pv-actions">
                <a class="pv-btn pv-btn-primary" href="/">Open Dashboard</a>
                <a class="pv-btn pv-btn-secondary" href="/preview">Reload Preview</a>
            </div>
            <div class="pv-grid-4">
                <div class="pv-stat">
                    <div class="pv-stat-label">Городов в базе</div>
                    <div class="pv-stat-value">{metrics["cities"]}</div>
                    <div class="pv-stat-copy">Подсчёт по локальным данным проекта.</div>
                </div>
                <div class="pv-stat">
                    <div class="pv-stat-label">Зон с оценкой</div>
                    <div class="pv-stat-value">{metrics["zones"]}</div>
                    <div class="pv-stat-copy">Суммарное число районов и зон в готовых наборах.</div>
                </div>
                <div class="pv-stat">
                    <div class="pv-stat-label">Компонентов скоринга</div>
                    <div class="pv-stat-value">{metrics["components"]}</div>
                    <div class="pv-stat-copy">{metrics["components"]} слоёв аналитики качества городской среды.</div>
                </div>
                <div class="pv-stat">
                    <div class="pv-stat-label">Категорий объектов</div>
                    <div class="pv-stat-value">{metrics["categories"]}</div>
                    <div class="pv-stat-copy">OSM-источники и городские POI для расчёта индекса.</div>
                </div>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <section class="pv-section">
            <div class="pv-section-head">
                <span class="pv-section-kicker">Screens</span>
                <h2 class="pv-section-title">Экраны и пояснения</h2>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )

    screen_cols = st.columns(3, gap="large")
    for col, screen in zip(screen_cols, SCREEN_ASSETS):
        with col:
            _render_screen_card(screen)

    st.markdown(
        """
        <section class="pv-section">
            <div class="pv-section-head">
                <span class="pv-section-kicker">Video</span>
                <h2 class="pv-section-title">Видео-демо</h2>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="pv-video-wrap">', unsafe_allow_html=True)
    if video_path is not None:
        st.video(str(video_path))
    else:
        st.info("Видео ещё не добавлено. Положите файл `preview/assets/demo.mp4`, и оно автоматически появится в этом блоке.")
    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown(
        """
        <section class="pv-section">
            <div class="pv-section-head">
                <span class="pv-section-kicker">Pipeline</span>
                <h2 class="pv-section-title">Как устроен продукт</h2>
                <p class="pv-section-copy">
                    Источники данных, расчёт индекса, NLP-модуль и витрина для финального выбора жилья.
                </p>
            </div>
            <div class="pv-roadmap">
                <div class="pv-roadmap-step">
                    <div class="pv-step-no">01</div>
                    <strong>Сбор данных</strong>
                    <span>OSM, геокодирование и городские зоны формируют пространственную основу.</span>
                </div>
                <div class="pv-roadmap-step">
                    <div class="pv-step-no">02</div>
                    <strong>Скоринговая модель</strong>
                    <span>8 компонент нормализуются и складываются в общий индекс качества района.</span>
                </div>
                <div class="pv-roadmap-step">
                    <div class="pv-step-no">03</div>
                    <strong>NLP и типология</strong>
                    <span>Отзывы жителей добавляют контекст, который не виден по одной карте объектов.</span>
                </div>
                <div class="pv-roadmap-step">
                    <div class="pv-step-no">04</div>
                    <strong>Принятие решения</strong>
                    <span>Пользователь сопоставляет рейтинг района с покупкой и арендой.</span>
                </div>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("</div>", unsafe_allow_html=True)
