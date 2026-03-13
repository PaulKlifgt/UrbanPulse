import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import plotly.graph_objects as go
import os
import json
import urllib.parse
import ast
import math
import re
from html import escape

try:
    from config import CONFIG as APP_CONFIG
except Exception:
    APP_CONFIG = {}
try:
    from styles import CSS
except Exception:
    CSS = ""

import streamlit.components.v1 as components

try:
    from preview import render_page as render_preview_page
except Exception:
    render_preview_page = None
try:
    from components import realty_cards
except Exception:
    realty_cards = None

st.set_page_config(layout="wide", page_title="UrbanPulse", page_icon="🏙️", initial_sidebar_state="expanded")
if CSS:
    st.markdown(CSS, unsafe_allow_html=True)

# === КОНФИГУРАЦИЯ ===
DATA_DIR = "data"
REALTY_LIVE_CACHE_TTL_SEC = int(APP_CONFIG.get("realty_live_cache_ttl_sec", 1800))
# Keep a larger pool so UI pagination has enough items.
REALTY_LIVE_LIMIT_PER_TYPE = int(APP_CONFIG.get("realty_live_limit_per_type", 120))
REALTY_LIVE_RADIUS_KM = float(APP_CONFIG.get("realty_live_radius_km", 2.4))

# Маппинг городов для ЦИАН (примерный, поиск по тексту работает лучше)
CITY_RU = {
    "Vladivostok": "Владивосток",
    "Astrakhan": "Астрахань",
    "Moscow": "Москва",
    "Saint_Petersburg": "Санкт-Петербург"
}


def _zone_terms(city_name):
    city_info = (APP_CONFIG.get("cities") or {}).get(city_name, {})
    zone_label = str(city_info.get("zone_label", "district") or "district")
    if zone_label == "ao":
        return {
            "many": "АО",
            "top_label": "Лучший АО",
            "map_title": "Карта АО",
            "map_meta": "Контуры и центры административных округов с индексом качества жизни",
            "analytics_meta": "Ключевые показатели по текущей выборке АО",
            "count_note": "АО после фильтрации",
            "ranking": "Топ АО",
            "detail_pick": "АО для детального анализа",
            "detail_subtitle": "Детальный разбор округа и локальная недвижимость",
            "search_term": "округ",
        }
    return {
        "many": "районов",
        "top_label": "Лучший район",
        "map_title": "Карта районов",
        "map_meta": "Контуры и центры районов с индексом качества жизни",
        "analytics_meta": "Ключевые показатели по текущей выборке районов",
        "count_note": "районов после фильтрации",
        "ranking": "Топ районов",
        "detail_pick": "Район для детального анализа",
        "detail_subtitle": "Детальный разбор района и локальная недвижимость",
        "search_term": "район",
    }

def load_cities():
    cities = []
    if not os.path.exists(DATA_DIR):
        return []
    for d in os.listdir(DATA_DIR):
        if os.path.exists(os.path.join(DATA_DIR, d, "processed", "districts_final.csv")):
            cities.append(d)
    return sorted(cities)

def _city_data_version(city):
    base = os.path.join(DATA_DIR, city)
    paths = [
        os.path.join(base, "processed", "districts_final.csv"),
        os.path.join(base, "processed", "districts_final.geojson"),
        os.path.join(base, "pipeline_stats.json"),
    ]
    version = []
    for path in paths:
        try:
            version.append(str(os.path.getmtime(path)))
        except OSError:
            version.append("missing")
    return "|".join(version)


@st.cache_data
def load_data_raw(city, _version):
    """Загружает данные."""
    base = os.path.join(DATA_DIR, city, "processed")
    df = pd.read_csv(os.path.join(base, "districts_final.csv"))

    # Hotfix: for preset zones with explicit coordinates (e.g. Moscow AO),
    # prefer config coordinates over stale centers from old pipeline runs.
    city_info = (APP_CONFIG.get("cities") or {}).get(city, {})
    preset_zones = city_info.get("preset_zones") or []
    preset_coords = {}
    for spec in preset_zones:
        if not isinstance(spec, dict):
            continue
        name = str(spec.get("name", "") or "").strip()
        lat = spec.get("lat")
        lon = spec.get("lon")
        if not name or lat is None or lon is None:
            continue
        try:
            preset_coords[name] = (float(lat), float(lon))
        except Exception:
            continue
    if preset_coords and not df.empty and "district" in df.columns:
        for district_name, (plat, plon) in preset_coords.items():
            mask = df["district"].astype(str).str.strip() == district_name
            if mask.any():
                df.loc[mask, "lat"] = plat
                df.loc[mask, "lon"] = plon
    
    stats = {}
    stats_path = os.path.join(DATA_DIR, city, "pipeline_stats.json")
    if os.path.exists(stats_path):
        with open(stats_path, "r") as f:
            stats = json.load(f)
            
    geojson_data = None
    gj_path = os.path.join(base, "districts_final.geojson")
    if os.path.exists(gj_path):
        with open(gj_path, "r", encoding="utf-8") as f:
            geojson_data = json.load(f)
            
    return df, stats, geojson_data


def _norm_text(s):
    s = str(s or "").lower().replace("ё", "е")
    s = re.sub(r"[^a-zа-я0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _district_keywords(name):
    lo = _norm_text(name)
    if not lo:
        return [], []
    stop_words = {
        "район", "округ", "микрорайон", "мкр", "жилмассив", "поселок", "пос",
        "город", "улица", "ул", "проспект", "пр", "имени", "им",
    }
    words = [w for w in lo.split() if len(w) >= 3 and w not in stop_words]
    phrases = []
    if len(words) >= 2:
        for i in range(len(words) - 1):
            phrases.append(f"{words[i]} {words[i+1]}")
    return words, phrases


def _city_keywords(city_name):
    lo = _norm_text(city_name)
    if not lo:
        return []
    stop_words = {"город", "г", "область", "край", "республика", "россия"}
    return [w for w in lo.split() if len(w) >= 3 and w not in stop_words]


def _city_link_slugs(city_name):
    city = _norm_text(city_name)
    if not city:
        return []
    # Minimal translit aliases used by cian subdomains.
    aliases = {
        "астрахань": ["astrahan", "astrakhan"],
        "екатеринбург": ["ekaterinburg"],
        "санкт петербург": ["spb", "saint-petersburg", "sankt-peterburg"],
        "нижний новгород": ["nizhniy-novgorod", "nizhny-novgorod"],
        "ростов на дону": ["rostov"],
        "набережные челны": ["naberezhnye-chelny", "chelny"],
    }
    for k, vals in aliases.items():
        if city == k or k in city:
            return vals
    return []


def _filter_realty_by_district(df_realty, district_name):
    if df_realty is None or df_realty.empty or "district" not in df_realty.columns:
        return pd.DataFrame()

    target = _norm_text(district_name)
    if not target:
        return pd.DataFrame()

    district_series = df_realty["district"].fillna("").astype(str)
    norm_series = district_series.map(_norm_text)

    exact_mask = norm_series == target
    if exact_mask.any():
        return df_realty[exact_mask].copy()

    contains_mask = norm_series.str.contains(target, regex=False)
    if contains_mask.any():
        return df_realty[contains_mask].copy()

    words, phrases = _district_keywords(district_name)
    if not words and not phrases:
        return pd.DataFrame()

    score = pd.Series(0, index=df_realty.index, dtype="int64")
    for phrase in phrases:
        score += norm_series.str.contains(phrase, regex=False).astype("int64") * 3
    for word in words:
        score += norm_series.str.contains(word, regex=False).astype("int64")

    return df_realty[score > 0].copy()


def _haversine_km(lat1, lon1, lat2, lon2):
    try:
        lat1 = float(lat1); lon1 = float(lon1)
        lat2 = float(lat2); lon2 = float(lon2)
    except Exception:
        return None
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _infer_rent_period_ui(offer_row):
    raw = str(offer_row.get("rent_period", "") or "").lower().strip()
    if raw in {"day", "сутки", "daily"}:
        return "day"
    if raw in {"month", "месяц", "monthly", "long"}:
        return "month"
    text = " ".join(
        str(offer_row.get(k, "") or "")
        for k in ("title", "address", "description", "link", "url")
    ).lower()
    if any(x in text for x in ("посуточ", "/сут", "в сутки", "за сутки", "/день", "в день")):
        return "day"
    if any(x in text for x in ("/мес", "в месяц", "за месяц", "длительно")):
        return "month"
    try:
        price = float(offer_row.get("price", 0) or 0)
    except Exception:
        price = 0
    if 0 < price <= 12000:
        # Для старых кэшей без rent_period дешевые аренды обычно посуточные.
        return "day"
    return ""


def _offer_identity_key(offer):
    link = (offer.get("link") or offer.get("url") or "").strip()
    if link:
        return (link, str(offer.get("deal_type") or ""))
    return (
        str(offer.get("deal_type") or ""),
        str(offer.get("source") or ""),
        str(offer.get("address") or "").strip().lower(),
        str(offer.get("rooms") or ""),
        str(offer.get("area") or ""),
        str(offer.get("price") or ""),
    )


def _relevance_filter_and_sort_offers(offers, district_name, city_name, center_lat, center_lon, radius_km):
    words, phrases = _district_keywords(district_name)
    city_words = _city_keywords(city_name)
    city_slugs = _city_link_slugs(city_name)
    hard_max_km = max(6.0, float(radius_km or 1.0) * 6.0)
    scored = []
    for offer in offers:
        row = dict(offer)
        if str(row.get("deal_type", "")) == "rent" and not row.get("rent_period"):
            inferred = _infer_rent_period_ui(row)
            if inferred:
                row["rent_period"] = inferred

        text = _norm_text(" ".join([
            str(row.get("address", "") or ""),
            str(row.get("title", "") or ""),
            str(row.get("link", "") or ""),
        ]))
        word_hits = sum(1 for w in words if w in text) if words else 0
        phrase_hits = sum(1 for p in phrases if p in text) if phrases else 0
        city_hits = sum(1 for w in city_words if w in text) if city_words else 0
        link_text = str(row.get("link", "") or "").lower()
        link_hits = sum(1 for s in city_slugs if s in link_text) if city_slugs else 0

        dist = None
        if row.get("lat") and row.get("lon"):
            dist = _haversine_km(center_lat, center_lon, row.get("lat"), row.get("lon"))

        district_hits = word_hits + phrase_hits

        # Жестко убираем явно далекие офферы, если район не совпадает по адресу.
        if dist is not None and dist > hard_max_km and (word_hits + phrase_hits) == 0:
            continue
        # Если нет гео, но и город не совпадает — отбрасываем как межрегиональный шум.
        if dist is None and city_words and (city_hits + link_hits) == 0 and (word_hits + phrase_hits) == 0:
            continue
        # Районный фильтр: если нет явного совпадения по району, оставляем
        # только очень близкие к центру зоны офферы (обычно в пределах ~1.2 км).
        strict_near_km = max(2.6, float(radius_km or 1.0) * 2.6)
        if district_hits == 0:
            if dist is None or dist > strict_near_km:
                continue

        score = 0.0
        score += phrase_hits * 8
        score += word_hits * 3
        score += city_hits * 1.5
        score += link_hits * 1.2
        if dist is not None:
            score += max(0.0, 4.0 - dist)  # бонус за близость к центру района
            score -= max(0.0, dist - float(radius_km or 1.0)) * 0.8
        else:
            score -= 0.5
        if row.get("address"):
            score += 0.5
        if row.get("photo") or row.get("photos"):
            score += 0.3

        row["_relevance"] = round(score, 3)
        row["_dist_km"] = round(dist, 3) if dist is not None else None
        scored.append(row)

    if not scored:
        return []

    # Если есть совпадения района в адресах — усиливаем их и отсекаем часть шума без совпадений.
    # Не применяем агрессивное урезание к коротким/средним выдачам, чтобы сохранить
    # объем объявлений для пагинации.
    has_addr_matches = any(((o.get("_relevance", 0) or 0) >= 3) for o in scored)
    if has_addr_matches and len(scored) > 24:
        filtered = []
        for o in scored:
            dist = o.get("_dist_km")
            rel = float(o.get("_relevance", 0) or 0)
            if rel >= 1.0:
                filtered.append(o)
                continue
            if dist is not None and dist <= max(2.6, float(radius_km or 1.0) * 2.2):
                filtered.append(o)
        if filtered:
            scored = filtered

    scored.sort(key=lambda x: (float(x.get("_relevance", 0) or 0), bool(x.get("address"))), reverse=True)
    return scored


@st.cache_data(ttl=REALTY_LIVE_CACHE_TTL_SEC, show_spinner=False)
def load_live_realty_offers(city_name_ru, district_name, lat, lon,
                           radius_km=1.0, refresh_nonce=0):
    """Подгружает объявления (покупка + аренда) при открытии страницы и кэширует."""
    try:
        from parsers.realty_parser import get_parser
    except Exception:
        return []

    parser = get_parser()
    all_offers = []

    for deal_type in ("sale", "rent"):
        try:
            offers = parser.search(
                lat=float(lat),
                lon=float(lon),
                city_name=city_name_ru,
                district_name=district_name,
                deal_type=deal_type,
                limit=max(40, REALTY_LIVE_LIMIT_PER_TYPE),
                radius_km=max(float(radius_km or 1.0), REALTY_LIVE_RADIUS_KM),
            )
        except Exception:
            offers = []

        for offer in offers or []:
            row = dict(offer)
            row["deal_type"] = row.get("deal_type") or deal_type
            row["district"] = district_name
            area = row.get("area") or 0
            price = row.get("price") or 0
            if area and price:
                try:
                    row["price_per_sqm"] = int(float(price) / float(area))
                except Exception:
                    row["price_per_sqm"] = 0
            all_offers.append(row)

    # Дедуп на уровне UI-кэша (на случай смешанных источников/повторов)
    seen = set()
    unique = []
    for o in all_offers:
        key = _offer_identity_key(o)
        if key in seen:
            continue
        seen.add(key)
        unique.append(o)
    return _relevance_filter_and_sort_offers(
        unique,
        district_name=district_name,
        city_name=city_name_ru,
        center_lat=lat,
        center_lon=lon,
        radius_km=radius_km,
    )


def _get_offer_images(offer_row, limit=10):
    """Возвращает список URL картинок из image_url/photo/photos."""
    images = []
    seen = set()

    def _add(url):
        if not isinstance(url, str):
            return
        url = url.strip()
        if not url.startswith("http") or url in seen:
            return
        seen.add(url)
        images.append(url)

    for key in ("image_url", "photo"):
        _add(offer_row.get(key))

    photos = offer_row.get("photos")
    parsed = None
    if isinstance(photos, list):
        parsed = photos
    elif isinstance(photos, str) and photos.strip():
        raw = photos.strip()
        if raw.startswith("http"):
            parsed = [raw]
        else:
            try:
                parsed = json.loads(raw)
            except Exception:
                try:
                    parsed = ast.literal_eval(raw)
                except Exception:
                    parsed = None

    if isinstance(parsed, list):
        for url in parsed:
            _add(url)

    return images[:limit]


def _render_offer_carousel(images, key_prefix):
    """Карусель фото с прозрачными зонами навигации по краям."""
    if not images:
        return

    safe_key = re.sub(r"[^a-zA-Z0-9_]+", "_", str(key_prefix))
    carousel_id = f"rp_carousel_{safe_key}"
    safe_images = [escape(url, quote=True) for url in images]
    total = len(safe_images)

    slides_html = "".join(
        f'<img class="rp-slide{" is-active" if idx == 0 else ""}" src="{url}" alt="Фото {idx + 1}" loading="lazy" />'
        for idx, url in enumerate(safe_images)
    )
    dots_html = "".join(
        f'<button type="button" class="rp-dot{" is-active" if idx == 0 else ""}" data-dot="{idx}" aria-label="Кадр {idx + 1}"></button>'
        for idx in range(total)
    )
    controls_html = ""
    if total > 1:
        controls_html = """
            <button type="button" class="rp-edge rp-edge-left" aria-label="Предыдущее фото"></button>
            <button type="button" class="rp-edge rp-edge-right" aria-label="Следующее фото"></button>
        """

    html = f"""
    <style>
      .rp-carousel {{
        position: relative;
        width: 100%;
        height: 304px;
        border-radius: 18px;
        overflow: hidden;
        background: linear-gradient(135deg, #eff3f8 0%, #dde6f2 100%);
        box-shadow: 0 14px 30px rgba(15, 23, 42, 0.16);
      }}
      .rp-slide {{
        position: absolute;
        inset: 0;
        width: 100%;
        height: 100%;
        object-fit: cover;
        opacity: 0;
        transform: scale(1.03);
        transition: opacity 0.26s ease, transform 0.42s ease;
      }}
      .rp-slide.is-active {{
        opacity: 1;
        transform: scale(1);
      }}
      .rp-edge {{
        position: absolute;
        top: 0;
        bottom: 0;
        width: 18%;
        border: none;
        background: transparent;
        cursor: pointer;
        z-index: 5;
      }}
      .rp-edge-left {{ left: 0; }}
      .rp-edge-right {{ right: 0; }}
      .rp-indicator {{
        position: absolute;
        left: 12px;
        bottom: 12px;
        z-index: 6;
        padding: 4px 10px;
        border-radius: 999px;
        background: rgba(17, 24, 39, 0.56);
        color: #fff;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        font-size: 12px;
        font-weight: 600;
        letter-spacing: 0.01em;
      }}
      .rp-dots {{
        position: absolute;
        right: 12px;
        bottom: 14px;
        z-index: 6;
        display: flex;
        gap: 6px;
      }}
      .rp-dot {{
        width: 8px;
        height: 8px;
        border-radius: 999px;
        border: none;
        background: rgba(255, 255, 255, 0.45);
        cursor: pointer;
        transition: transform 0.2s ease, background 0.2s ease;
      }}
      .rp-dot.is-active {{
        background: #ffffff;
        transform: scale(1.2);
      }}
    </style>
    <div class="rp-carousel" id="{carousel_id}">
      {slides_html}
      {controls_html}
      <div class="rp-indicator"><span class="rp-current">1</span> / <span class="rp-total">{total}</span></div>
      <div class="rp-dots">{dots_html}</div>
    </div>
    <script>
      (() => {{
        const root = document.getElementById("{carousel_id}");
        if (!root) return;
        const slides = Array.from(root.querySelectorAll(".rp-slide"));
        const dots = Array.from(root.querySelectorAll(".rp-dot"));
        const left = root.querySelector(".rp-edge-left");
        const right = root.querySelector(".rp-edge-right");
        const currentEl = root.querySelector(".rp-current");
        const total = slides.length;
        let idx = 0;

        const paint = (next) => {{
          idx = ((next % total) + total) % total;
          slides.forEach((slide, i) => slide.classList.toggle("is-active", i === idx));
          dots.forEach((dot, i) => dot.classList.toggle("is-active", i === idx));
          if (currentEl) currentEl.textContent = String(idx + 1);
        }};

        if (left) left.addEventListener("click", () => paint(idx - 1));
        if (right) right.addEventListener("click", () => paint(idx + 1));
        dots.forEach((dot, i) => dot.addEventListener("click", () => paint(i)));
      }})();
    </script>
    """
    components.html(html, height=316, scrolling=False)


def _inject_carousel_styles():
    """Стили карточек объявлений (идемпотентно)."""
    if st.session_state.get("_realty_offer_card_css_injected"):
        return
    st.session_state["_realty_offer_card_css_injected"] = True
    st.markdown(
        """
        <style>
        .rp-offer-card {
            margin-top: 10px;
            border-radius: 16px;
            border: 1px solid #d8e3ef;
            background: linear-gradient(140deg, #ffffff 0%, #f5f8fc 100%);
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);
            padding: 14px 16px 16px 16px;
        }
        .rp-offer-price {
            font-size: 28px;
            line-height: 1.1;
            color: #0f172a;
            font-weight: 800;
            letter-spacing: -0.02em;
        }
        .rp-offer-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            margin-top: 8px;
            margin-bottom: 10px;
        }
        .rp-pill {
            border: 1px solid #cfe0f5;
            border-radius: 999px;
            padding: 4px 10px;
            background: #edf4fd;
            color: #345a87;
            font-size: 14px;
            font-weight: 600;
            line-height: 1.15;
        }
        .rp-offer-address {
            color: #4b5563;
            font-size: 15px;
            line-height: 1.35;
            margin-bottom: 12px;
        }
        .rp-offer-link {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            text-decoration: none !important;
            border-radius: 10px;
            padding: 8px 12px;
            background: #0b3b7a;
            color: #ffffff !important;
            font-size: 14px;
            font-weight: 700;
            transition: background 0.16s ease;
        }
        .rp-offer-link:hover {
            background: #072e62;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_offer_details(offer):
    """Рендерит современный блок с метаданными объявления."""
    _inject_carousel_styles()

    deal_type = str(offer.get("deal_type", "sale") or "sale").strip().lower()
    rent_period = _infer_rent_period_ui(offer)
    if deal_type == "rent" and rent_period == "day":
        deal_badge = "Посуточно"
    elif deal_type == "rent":
        deal_badge = "Аренда"
    else:
        deal_badge = "Покупка"

    price_label = _fmt_offer_price_label(offer)
    rooms = escape(str(offer.get("rooms", "?")))
    area = escape(str(offer.get("area", "?")))
    floor = escape(str(offer.get("floor", "") or "этаж не указан"))
    address = escape(str(offer.get("address", "Нет адреса")))
    ppm = offer.get("price_per_sqm")
    ppm_badge = f"{_fmt_int(ppm)} ₽/м²" if ppm else "цена за м² не указана"
    link = str(offer.get("link", "") or "").strip()
    link_html = (
        f'<a class="rp-offer-link" target="_blank" href="{escape(link, quote=True)}">Открыть объявление</a>'
        if link.startswith("http")
        else ""
    )

    st.markdown(
        f"""
        <div class="rp-offer-card">
            <div class="rp-offer-price">{escape(price_label)}</div>
            <div class="rp-offer-meta">
                <span class="rp-pill">{deal_badge}</span>
                <span class="rp-pill">{rooms}-к</span>
                <span class="rp-pill">{area} м²</span>
                <span class="rp-pill">{floor}</span>
                <span class="rp-pill">{ppm_badge}</span>
            </div>
            <div class="rp-offer-address">{address}</div>
            {link_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _fmt_int(value):
    try:
        return f"{int(float(value)):,}".replace(",", " ")
    except Exception:
        return "0"


def _fmt_compare_cell(col_name, value):
    if value is None:
        return ""
    if col_name == "grade":
        return str(value)
    if isinstance(value, (int, float)):
        return f"{value:.1f}" if abs(float(value) - int(float(value))) > 1e-9 else str(int(float(value)))
    return str(value)


def _fmt_offer_price_label(offer):
    price = offer.get("price", 0) or 0
    deal_type = str(offer.get("deal_type", "sale"))
    if deal_type == "rent":
        rent_period = _infer_rent_period_ui(offer)
        if rent_period == "day":
            return f"{_fmt_int(price)} ₽/сутки"
        return f"{_fmt_int(price)} ₽/мес"
    return f"{int(price/1000)}к ₽" if price else "цена не указана"


def _render_realty_loading():
    st.markdown(
        """
        <style>
        .rp-skeleton {margin: 8px 0 12px 0;}
        .rp-bar {
            height: 10px; border-radius: 999px; overflow: hidden;
            background: #edf1f5; border: 1px solid #e2e8f0; margin-bottom: 10px;
        }
        .rp-shimmer {
            height: 100%;
            background: linear-gradient(90deg, #edf1f5 0%, #dbe7f3 45%, #edf1f5 100%);
            background-size: 200% 100%;
            animation: rp-slide 1.1s linear infinite;
        }
        .rp-card {
            height: 72px; border-radius: 12px; margin: 8px 0;
            background: linear-gradient(90deg, #f8fafc 0%, #eef3f8 45%, #f8fafc 100%);
            background-size: 220% 100%;
            animation: rp-slide 1.2s linear infinite;
            border: 1px solid #e8edf3;
        }
        @keyframes rp-slide {
            0% { background-position: 200% 0; }
            100% { background-position: -20% 0; }
        }
        </style>
        <div class="rp-skeleton">
          <div style="font-size:13px;color:#55606d;margin-bottom:8px;">Загрузка предложений...</div>
          <div class="rp-bar"><div class="rp-shimmer"></div></div>
          <div class="rp-card"></div>
          <div class="rp-card"></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _is_displayable_zone(row):
    """Мягкий UI-фильтр мусорных зон в рейтинге/карте."""
    name = str(row.get("district", "")).strip()
    if not name:
        return False
    lo = name.lower()
    if re.search(r"(ский|ская|ское|ские|район|округ)", lo):
        return True

    # Если в финальном CSV уже есть источник preset/качественный OSM — доверяем больше
    src = str(row.get("zone_source", "") or "")
    if src.startswith(("preset", "wikidata", "wikipedia", "osm_suburb", "osm_quarter", "osm_neighbourhood", "osm_residential")):
        return True

    bad_prefixes = ("мыс ", "остров ", "бухта ", "канал")
    bad_exact = {"минка", "мелководный", "экипажный", "подножье", "кэт", "рында", "шигино", "поспелово", "житкова"}
    if lo.startswith(bad_prefixes):
        return False
    if lo in bad_exact:
        return False

    # Очень слабые зоны обычно шум/локальные топонимы
    try:
        pop = float(row.get("population", 0) or 0)
        idx = float(row.get("total_index", 0) or 0)
        eco = float(row.get("ecology_score", 0) or 0)
        infra = float(row.get("infrastructure_score", 0) or 0)
        if pop < 8000 and idx < 60 and infra < 45:
            return False
        # Береговые топонимы часто получают завышенную эко-оценку, но слабую инфраструктуру
        if eco > 70 and infra < 40 and pop < 15000 and src.startswith(("grid_", "osm_bbox_", "osm_hub")):
            return False
    except Exception:
        pass

    return True

def get_color(grade):
    colors = {
        "A": "#2ecc71", # Green
        "B": "#3498db", # Blue
        "C": "#f1c40f", # Yellow
        "D": "#e67e22", # Orange
        "F": "#e74c3c"  # Red
    }
    return colors.get(grade, "#95a5a6")

def get_grade(x):
    if x >= 80: return "A"
    if x >= 65: return "B"
    if x >= 50: return "C"
    if x >= 35: return "D"
    return "F"


def _render_stat_card(label, value, note="", delta=None, icon_svg="", color_class="blue"):
    delta_class = "positive" if delta and str(delta).startswith("+") else "neutral"
    html = f'''
    <div class="up-stat-card">
        <div class="up-stat-top">
            <div class="up-stat-icon {color_class}">
                {icon_svg}
            </div>
            <div class="up-stat-delta {delta_class}">{escape(str(delta)) if delta else ""}</div>
        </div>
        <div class="up-stat-value">{escape(str(value))}</div>
        <div class="up-stat-label">{escape(label)}</div>
    </div>
    '''
    st.markdown(html, unsafe_allow_html=True)


def _render_rating_table(df):
    if df.empty:
        st.info("Нет районов для отображения.")
        return
    rows = []
    for _, row in df.head(12).iterrows():
        score = float(row.get("total_index", 0) or 0)
        eco = float(row.get("ecology_score", 0) or 0)
        rows.append(
            '<div class="up-rating-row">'
            f'<div class="up-rating-name">{escape(str(row.get("district", "")))}</div>'
            '<div class="up-score-cell">'
            f'<div class="up-score-track"><div class="up-score-fill" style="width:{max(0,min(score,100))}%;"></div></div>'
            f'<span class="up-score-text">{score:.1f}</span>'
            '</div>'
            f'<div class="up-grade">{escape(str(row.get("grade", "")))}</div>'
            f'<div class="up-eco">{eco:.1f}</div>'
            '</div>'
        )
    st.markdown(
        '<div class="up-rating-wrap">'
        '<div class="up-rating-head">'
        '<div>Район</div>'
        '<div>Индекс</div>'
        '<div>Cls</div>'
        '<div>Eco</div>'
        '</div>'
        + "".join(rows)
        + "</div>",
        unsafe_allow_html=True,
    )


def _load_special_offers(selected_city, district_row):
    if district_row is None:
        return pd.DataFrame()

    district_name = str(district_row.get("district", "") or "").strip()
    offers_df = pd.DataFrame()
    realty_file = os.path.join(DATA_DIR, selected_city, "processed", "realty_offers.csv")
    if os.path.exists(realty_file):
        try:
            df_realty = pd.read_csv(realty_file)
            offers_df = _filter_realty_by_district(df_realty, district_name)
        except Exception:
            offers_df = pd.DataFrame()

    if offers_df.empty:
        return offers_df

    if "_relevance" in offers_df.columns:
        offers_df = offers_df.sort_values(["_relevance", "price"], ascending=[False, True])
    elif "price" in offers_df.columns:
        offers_df = offers_df.sort_values("price", ascending=True)
    return offers_df.head(4).copy()


def _render_special_offers(offers_df, district_name):
    if offers_df is None or offers_df.empty:
        return

    html_cards = []
    for _, offer in offers_df.head(4).iterrows():
        deal_type = str(offer.get("deal_type", "sale") or "sale").strip().lower()
        if deal_type == "rent":
            badge = "Аренда"
        else:
            badge = "Покупка"
        title = f"{offer.get('rooms', '?')}-к квартира" if offer.get("rooms") else "Объявление"
        subtitle = str(offer.get("address", district_name) or district_name)
        meta_parts = []
        if offer.get("area"):
            meta_parts.append(f"{offer.get('area')} м²")
        if offer.get("floor"):
            meta_parts.append(f"этаж {offer.get('floor')}")
        elif offer.get("source"):
            meta_parts.append(str(offer.get("source")))
        meta = " • ".join(meta_parts) if meta_parts else district_name
        link = str(offer.get("link", "") or "").strip()
        link_html = (
            f'<a class="up-offer-link" target="_blank" href="{escape(link, quote=True)}">Открыть</a>'
            if link.startswith("http")
            else ""
        )
        html_cards.append(
            (
                '<div class="up-offer-card">'
                '<div class="up-offer-top">'
                f'<div class="up-offer-title">{escape(str(title))}</div>'
                f'<div class="up-offer-badge">{escape(str(badge))}</div>'
                '</div>'
                '<div class="up-offer-subtitle">'
                '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"></path><circle cx="12" cy="10" r="3"></circle></svg>'
                f'{escape(str(subtitle))}'
                '</div>'
                '<div class="up-offer-bottom">'
                f'<div class="up-offer-price">{escape(_fmt_offer_price_label(offer))}</div>'
                '<div class="up-offer-meta">'
                '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><polyline points="12 6 12 12 16 14"></polyline></svg>'
                f'{escape(str(meta))}'
                '</div>'
                '</div>'
                '<div class="up-offer-actions">'
                f'{link_html}'
                '</div>'
                '</div>'
            )
        )

    st.markdown(
        '<div class="up-offers-wrap">'
        '<div class="up-offers-head"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z"></path><line x1="7" y1="7" x2="7.01" y2="7"></line></svg> Special Offers</div>'
        '<div class="up-offers-grid">'
        + "".join(html_cards)
        + '</div></div>',
        unsafe_allow_html=True,
    )


def _make_top_chart(display_df):
    chart_df = display_df.head(6).sort_values("total_index", ascending=True)
    fig = go.Figure(
        go.Bar(
            x=chart_df["total_index"],
            y=chart_df["district"],
            orientation="h",
            marker=dict(
                color=chart_df["total_index"],
                colorscale=[[0, "#bfd4ff"], [1, "#2a61e8"]],
                line=dict(color="#1d4fd1", width=0),
            ),
            hovertemplate="%{y}: %{x:.1f}<extra></extra>",
        )
    )
    fig.update_layout(
        height=260,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=True, gridcolor="#d9e2f0", zeroline=False, title=""),
        yaxis=dict(showgrid=False, title="", tickfont=dict(size=12)),
        font=dict(color="#152033", family="Avenir Next, Segoe UI, sans-serif"),
    )
    return fig

def make_radar_chart(row, avg_row):
    """Рисует розу ветров сравнения района со средним по городу."""
    categories = ['Инфраструктура', 'Транспорт', 'Экология', 'Безопасность', 'Соц. среда']
    
    # Значения текущего района
    values = [
        row['infrastructure_score'], row['transport_score'], row['ecology_score'],
        row['safety_score'], row['social_score']
    ]
    
    # Средние значения по городу
    avg_values = [
        avg_row['infrastructure_score'], avg_row['transport_score'], avg_row['ecology_score'],
        avg_row['safety_score'], avg_row['social_score']
    ]
    
    fig = go.Figure()

    # Слой среднего
    fig.add_trace(go.Scatterpolar(
        r=avg_values,
        theta=categories,
        fill='toself',
        name='Среднее по городу',
        line_color='gray',
        opacity=0.4
    ))

    # Слой района
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name=row['district'],
        line_color='#3498db'
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )),
        showlegend=True,
        margin=dict(l=40, r=40, t=20, b=20),
        height=350
    )
    return fig


def make_compare_radar(row1, row2):
    axes = [
        ("infrastructure_score", "Инфраструктура"),
        ("transport_score", "Транспорт"),
        ("ecology_score", "Экология"),
        ("safety_score", "Безопасность"),
        ("social_score", "Соц. среда"),
    ]
    theta = [label for _, label in axes]
    r1 = [float(row1.get(col, 0) or 0) for col, _ in axes]
    r2 = [float(row2.get(col, 0) or 0) for col, _ in axes]

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=r1,
        theta=theta,
        fill="toself",
        name=f"{row1.get('district', 'Зона 1')} ({float(row1.get('total_index', 0) or 0):.0f})",
        line=dict(color="#2563eb", width=2),
        marker=dict(size=4),
    ))
    fig.add_trace(go.Scatterpolar(
        r=r2,
        theta=theta,
        fill="toself",
        name=f"{row2.get('district', 'Зона 2')} ({float(row2.get('total_index', 0) or 0):.0f})",
        line=dict(color="#ef4444", width=2),
        marker=dict(size=4),
    ))
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], gridcolor="#dbe4f0"),
            angularaxis=dict(gridcolor="#e5eaf2"),
        ),
        showlegend=True,
        margin=dict(l=20, r=20, t=20, b=20),
        height=420,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#152033", family="Avenir Next, Segoe UI, sans-serif"),
    )
    return fig

def generate_search_links(city_en, district_name, zone_search_term="район"):
    """Генерирует ссылки на поиск недвижимости."""
    city_ru = CITY_RU.get(city_en, city_en)
    
    # Очистка имени для поиска (убираем "микрорайон" и т.д. для точности)
    query = f"{city_ru} {zone_search_term} {district_name}"
    query_encoded = urllib.parse.quote(query)
    
    cian_url = f"https://www.cian.ru/find-header/?engine_version=2&deal_type=sale&offer_type=flat&text={query_encoded}"
    domclick_url = f"https://domclick.ru/search?address={query_encoded}"
    avito_url = f"https://www.avito.ru/rossiya/kvartiry/prodam-ASgBAgICAUSSA8YQ?cd=1&q={query_encoded}"
    
    return cian_url, domclick_url, avito_url

# === ИНТЕРФЕЙС ===

def render_dashboard():
    cities = load_cities()
    if not cities:
        st.error("Нет данных! Запустите run_pipeline.py")
        st.stop()

    with st.sidebar:
        st.markdown("### UrbanPulse")
        st.markdown("[Презентационная страница](/preview)")
        selected_city = st.selectbox("Город", cities)
        raw_df, stats, geojson_data = load_data_raw(selected_city, _city_data_version(selected_city))
        df = raw_df.copy()
        if not df.empty:
            before_ui = len(df)
            df = df[df.apply(_is_displayable_zone, axis=1)].copy()
            hidden_zones = before_ui - len(df)
        else:
            hidden_zones = 0

        st.markdown("**Фильтрация**")
        min_score = st.slider("Мин. индекс", 0, 100, 0)
        st.markdown("**Веса компонент**")
        w_infra = st.slider("Инфраструктура", 0.0, 1.0, 0.15, 0.05)
        w_trans = st.slider("Транспорт", 0.0, 1.0, 0.15, 0.05)
        w_eco = st.slider("Экология", 0.0, 1.0, 0.15, 0.05)
        w_safe = st.slider("Безопасность", 0.0, 1.0, 0.10, 0.05)
        w_social = st.slider("Отзывы", 0.0, 1.0, 0.20, 0.05)

    used = w_infra + w_trans + w_eco + w_safe + w_social

    if not df.empty:
        if used > 0:
            df["total_index"] = (
                df["infrastructure_score"] * w_infra +
                df["transport_score"] * w_trans +
                df["ecology_score"] * w_eco +
                df["safety_score"] * w_safe +
                df["social_score"] * w_social
            ) / used
        else:
            df["total_index"] = (
                df["infrastructure_score"] +
                df["transport_score"] +
                df["ecology_score"] +
                df["safety_score"] +
                df["social_score"]
            ) / 5.0
        df["total_index"] = df["total_index"].round(1)
        df["grade"] = df["total_index"].apply(get_grade)

    filtered_df = df[df["total_index"] >= min_score].copy() if not df.empty else df.copy()
    zone_terms = _zone_terms(selected_city)
    display_df = filtered_df[["district", "total_index", "grade", "ecology_score"]].sort_values(
        "total_index", ascending=False
    ) if not filtered_df.empty else pd.DataFrame(columns=["district", "total_index", "grade", "ecology_score"])
    top_district = display_df.iloc[0]["district"] if not display_df.empty else None
    avg_index = display_df["total_index"].mean() if not display_df.empty else 0

    top_district_row = filtered_df.sort_values("total_index", ascending=False).iloc[0] if not filtered_df.empty else None
    special_offers_df = _load_special_offers(selected_city, top_district_row)

    poly_count = 0
    if geojson_data and "features" in geojson_data and not display_df.empty:
        poly_names = {f["properties"].get("district") for f in geojson_data["features"]}
        poly_count = int(display_df["district"].isin(poly_names).sum())
    coverage = f"{poly_count}/{len(display_df)}" if len(display_df) else "0/0"

    st.markdown(
        f"""
        <style>
        .up-topbar {{
            background:#ffffff;border:1px solid #e6eaf0;border-radius:14px;padding:16px 18px;margin-bottom:12px;
            box-shadow:0 2px 10px rgba(15,23,42,0.06);
        }}
        .up-topbar-title {{font-size:26px;font-weight:700;color:#111827;margin-bottom:10px;}}
        </style>
        <div class="up-topbar">
            <div class="up-topbar-title">UrbanPulse • {escape(selected_city)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    nav_items = [
        ("map", "🗺️ Карта"),
        ("rating", "📊 Рейтинг"),
        ("analysis", "🔍 Анализ зоны"),
        ("compare", "⚖️ Сравнение"),
    ]
    if "up_main_view" not in st.session_state:
        st.session_state["up_main_view"] = "map"
    active_view = st.session_state["up_main_view"]
    nav_cols = st.columns(4)
    for i, (key, label) in enumerate(nav_items):
        with nav_cols[i]:
            clicked = st.button(
                label,
                key=f"up_main_nav_btn_{key}",
                width="stretch",
                type="primary" if active_view == key else "secondary",
            )
            if clicked and active_view != key:
                st.session_state["up_main_view"] = key
                st.rerun()

    if active_view == "map":
        col_map, col_side = st.columns([5.0, 2.5], gap="large")
        with col_map:
            st.markdown(
                f"""
                <div class="up-map-summary">
                    <div class="up-panel-head">
                        <div>
                            <h3 class="up-panel-title">{escape(zone_terms["map_title"])}</h3>
                            <p class="up-panel-meta">{escape(zone_terms["map_meta"])}</p>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if not filtered_df.empty:
                center_lat = filtered_df["lat"].mean()
                center_lon = filtered_df["lon"].mean()
                m = folium.Map(location=[center_lat, center_lon], zoom_start=10, tiles="CartoDB positron", **{"attributionControl": False})
                m.get_root().html.add_child(folium.Element("<style>.leaflet-control-attribution.leaflet-control{display:none !important;}</style>"))

                poly_map = {}
                if geojson_data and "features" in geojson_data:
                    for f in geojson_data["features"]:
                        poly_map[f["properties"].get("district")] = f

                all_lats = list(filtered_df["lat"])
                all_lons = list(filtered_df["lon"])
                for _, row in filtered_df.iterrows():
                    d_name = row["district"]
                    html = f"""
                    <div style="font-family:Avenir Next,Segoe UI,sans-serif;width:220px;">
                        <h4 style="margin:0 0 8px 0;">{escape(d_name)}</h4>
                        <p style="margin:0 0 8px 0;"><b>{row['total_index']:.0f}/100 ({row['grade']})</b></p>
                        <div style="font-size:12px;line-height:1.55;">
                            Инфраструктура: {row['infrastructure_score']:.0f}<br>
                            Транспорт: {row['transport_score']:.0f}<br>
                            Экология: {row['ecology_score']:.0f}
                        </div>
                    </div>
                    """
                    fill_color = get_color(row["grade"])
                    style = {"fillColor": fill_color, "color": fill_color, "weight": 2.4, "fillOpacity": 0.42}
                    if d_name in poly_map:
                        feat = poly_map[d_name]
                        folium.GeoJson(feat, style_function=lambda x, s=style: s, tooltip=d_name, popup=folium.Popup(html, max_width=260)).add_to(m)
                    else:
                        folium.CircleMarker(
                            location=[row["lat"], row["lon"]],
                            radius=9 + (row["total_index"] / 20),
                            popup=folium.Popup(html, max_width=260),
                            tooltip=d_name,
                            color="#ffffff",
                            fill=True,
                            fill_color=fill_color,
                            fill_opacity=1.0,
                            weight=2.5,
                        ).add_to(m)
                if all_lats and all_lons:
                    m.fit_bounds([[min(all_lats), min(all_lons)], [max(all_lats), max(all_lons)]], padding=(20, 20))
                st_folium(m, width="100%", height=620, returned_objects=[])
            else:
                st.warning(f"Нет {zone_terms['many']} для отображения.")
            _render_special_offers(special_offers_df, top_district or selected_city)

        with col_side:
            st.markdown("#### Шкала оценки")
            st.markdown("`A` 75-100  Отлично")
            st.markdown("`B` 60-74  Хорошо")
            st.markdown("`C` 45-59  Средне")
            st.markdown("`D` 30-44  Ниже среднего")
            st.markdown("`F` 0-29   Плохо")
            st.markdown("---")
            st.markdown("#### Статистика")
            st.write(f"Лучшая зона: **{top_district or '—'}**")
            st.write(f"Средний индекс: **{avg_index:.1f}**")
            st.write(f"Покрытие контуров: **{coverage}**")
            st.write(f"Показано зон: **{len(display_df)}**")
            if hidden_zones > 0:
                st.caption(f"Скрыто шумных зон: {hidden_zones}")

    elif active_view == "rating":
        st.markdown(f"### {zone_terms['ranking']}")
        if display_df.empty:
            st.info("Нет данных для рейтинга.")
        else:
            c1, c2 = st.columns([1.6, 1.0], gap="large")
            with c1:
                st.plotly_chart(_make_top_chart(display_df), width="stretch", config={"displayModeBar": False})
                st.markdown("#### Таблица районов")
                _render_rating_table(display_df)
            with c2:
                st.markdown("#### Ключевые метрики")
                st.metric(zone_terms["top_label"], top_district or "—")
                st.metric("Средний индекс", f"{avg_index:.1f}")
                st.metric("Зон в выборке", f"{len(display_df)}")

    elif active_view == "analysis":
        detail_source = filtered_df if not filtered_df.empty else df
        detail_options = detail_source["district"].sort_values().tolist() if not detail_source.empty else []
        if not detail_options:
            st.info("Нет районов для детального анализа.")
            return
        zone_select = st.selectbox(
            zone_terms["detail_pick"],
            detail_options,
            index=detail_options.index(top_district) if top_district in detail_options else 0,
        )
        z = df[df["district"] == zone_select].iloc[0]
        avg_stats = df.mean(numeric_only=True)

        st.markdown(
            f"""
            <div class="up-detail-panel">
                <div class="up-detail-head">
                    <div>
                        <h2 class="up-detail-title">{escape(str(z['district']))}</h2>
                        <p class="up-detail-subtitle">{escape(zone_terms["detail_subtitle"])}</p>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Население", f"{int(z['population']):,}")
        m2.metric("Рейтинг", z['total_index'], delta=z['grade'])
        m3.metric("Экология", z['ecology_score'], delta=round(z['ecology_score'] - avg_stats['ecology_score'], 1))
        m4.metric("Безопасность", z['safety_score'], delta=round(z['safety_score'] - avg_stats['safety_score'], 1))

        c_chart, c_real = st.columns([1, 1], gap="large")
        with c_chart:
            st.subheader("📊 Анализ факторов")
            st.plotly_chart(make_radar_chart(z, avg_stats), width="stretch")
        with c_real:
            st.markdown('<div class="up-chart-title">Недвижимость</div>', unsafe_allow_html=True)
            if realty_cards is None:
                st.info("Модуль components.realty_cards недоступен.")
            else:
                realty_cards(z, selected_city)

    elif active_view == "compare":
        compare_source = filtered_df if not filtered_df.empty else df
        compare_options = compare_source["district"].dropna().astype(str).tolist()
        if len(compare_options) < 2:
            st.info("Недостаточно районов для сравнения.")
            return
        c1, c2 = st.columns(2)
        with c1:
            d1 = st.selectbox("Зона 1", compare_options, index=0, key="cmp_page_d1")
        with c2:
            d2 = st.selectbox("Зона 2", compare_options, index=min(1, len(compare_options) - 1), key="cmp_page_d2")

        r1 = df[df["district"] == d1].iloc[0]
        r2 = df[df["district"] == d2].iloc[0]

        left, right = st.columns([1.15, 1.0], gap="large")
        with left:
            st.plotly_chart(make_compare_radar(r1, r2), width="stretch")
        with right:
            icons = {
                "total_index": "🏆",
                "infrastructure_score": "🏬",
                "education_score": "🎓",
                "healthcare_score": "🏥",
                "transport_score": "🚌",
                "ecology_score": "🌿",
                "safety_score": "🛡️",
                "leisure_score": "🎭",
                "social_score": "💬",
                "grade": "🏷️",
            }
            rows = []
            compare_cols = [
                ("total_index", "Общий индекс"),
                ("infrastructure_score", "Инфраструктура"),
                ("transport_score", "Транспорт"),
                ("ecology_score", "Экология"),
                ("safety_score", "Безопасность"),
                ("social_score", "Соц. среда"),
                ("grade", "Грейд"),
            ]
            for col, label in compare_cols:
                rows.append({
                    "Показатель": f"{icons.get(col, '')} {label}".strip(),
                    d1.upper(): _fmt_compare_cell(col, r1.get(col, "")),
                    d2.upper(): _fmt_compare_cell(col, r2.get(col, "")),
                })
            cmp_df = pd.DataFrame(rows).astype("string")
            st.dataframe(cmp_df, width="stretch", hide_index=True)

            diff = float(r1.get("total_index", 0) or 0) - float(r2.get("total_index", 0) or 0)
            if diff > 0:
                st.success(f"{d1} комфортнее на {abs(diff):.0f}")
            elif diff < 0:
                st.success(f"{d2} комфортнее на {abs(diff):.0f}")
            else:
                st.info("Зоны равны по общему индексу")


def main():
    preview_callable = render_preview_page
    if preview_callable is None:
        def preview_callable():
            st.error("Не удалось загрузить страницу `preview`.")

    current_page = st.navigation(
        [
            st.Page(render_dashboard, title="UrbanPulse", icon="🏙️", default=True),
            st.Page(preview_callable, title="Preview", icon="🎞️", url_path="preview"),
        ],
        position="sidebar",
    )
    current_page.run()


main()
