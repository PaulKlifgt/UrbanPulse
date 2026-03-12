# components.py — ПОЛНЫЙ ФАЙЛ

import streamlit as st
import streamlit.components.v1 as html_comp
from charts import grade_color, SCORE_COLS, SCORE_LABELS_MAP, SCORE_EMOJI
import math


# ── Стиль карточек (общий) ──
_CARD_STYLE = (
    'background:#fff;border:1px solid #dee2e6;'
    'border-radius:8px;padding:18px;margin-bottom:12px;'
)

_CARD_HEADER = (
    'font-size:11px;font-weight:700;color:#6c757d;'
    'text-transform:uppercase;letter-spacing:0.04em;'
    'margin-bottom:10px;'
)


def _scores_table(row, font_size="13px"):
    rows_html = ""
    for col in SCORE_COLS:
        label = SCORE_LABELS_MAP.get(col, col)
        emoji = SCORE_EMOJI.get(col, "")
        val = row.get(col, 0)
        if val >= 65:
            vc = "#198754"
            bar_c = "#198754"
        elif val >= 45:
            vc = "#212529"
            bar_c = "#fd7e14"
        else:
            vc = "#dc3545"
            bar_c = "#dc3545"
        bar_w = max(2, min(val, 100))
        rows_html += (
            f"<tr>"
            f"<td style='padding:4px 0;font-size:{font_size};'>{emoji} {label}</td>"
            f"<td style='text-align:right;font-weight:700;color:{vc};font-size:{font_size};'>"
            f"{val:.0f}</td>"
            f"<td style='padding:4px 0 4px 8px;width:60px;'>"
            f"<div style='height:4px;background:#e9ecef;border-radius:2px;overflow:hidden;'>"
            f"<div style='width:{bar_w}%;height:100%;background:{bar_c};border-radius:2px;'>"
            f"</div></div></td>"
            f"</tr>"
        )
    return (
        f"<table style='color:#212529;width:100%;border-collapse:collapse;'>"
        f"{rows_html}</table>"
    )


def _scores_balloon_table(row):
    rows_html = ""
    for col in SCORE_COLS:
        label = SCORE_LABELS_MAP.get(col, col)
        val = row.get(col, 0)
        rows_html += (
            f"<tr><td>{label}</td>"
            f"<td style=\\'text-align:right;font-weight:600;\\'>"
            f"{val:.0f}</td></tr>"
        )
    return (
        f"<table style=\\'font-size:12px;color:#212529;width:100%;\\'>"
        f"{rows_html}</table>"
    )


def _realty_balloon_link(lat, lon, deal_type="sale"):
    delta = 0.015
    bbox = f"{lon-delta:.4f}_{lat-delta:.4f}_{lon+delta:.4f}_{lat+delta:.4f}"
    return (
        f"https://domclick.ru/search?"
        f"deal_type={deal_type}&category=living"
        f"&offer_type=flat&bbox={bbox}"
    )


def yandex_map(df, city_info, api_key, micro_df=None, mode="districts", height=480):
    center = city_info["center"]
    zoom = city_info.get("zoom", 12)
    marks = ""

    for _, row in df.iterrows():
        color = grade_color(row["total_index"])
        r = max(200, row["total_index"] * 6)
        scores_html = _scores_balloon_table(row)
        buy_link = _realty_balloon_link(row["lat"], row["lon"], "sale")
        rent_link = _realty_balloon_link(row["lat"], row["lon"], "rent")

        balloon = (
            f"<div style=\\'padding:12px;font-family:-apple-system,system-ui,sans-serif;\\'>"
            f"<div style=\\'font-size:16px;font-weight:700;color:#212529;\\'>"
            f"{row['district']}</div>"
            f"<div style=\\'font-size:22px;font-weight:700;color:{color};margin:4px 0;\\'>"
            f"{row['total_index']:.0f}"
            f"<span style=\\'font-size:13px;color:#6c757d;font-weight:600;\\'>"
            f" /100 ({row['grade']})</span></div>"
            f"{scores_html}"
            f"<div style=\\'margin-top:10px;border-top:1px solid #dee2e6;"
            f"padding-top:10px;display:flex;gap:8px;\\'>"
            f"<a href=\\'{buy_link}\\' target=\\'_blank\\' "
            f"style=\\'background:#0d6efd;color:#fff;padding:6px 14px;"
            f"border-radius:6px;font-size:12px;font-weight:600;"
            f"text-decoration:none;\\'>🔑 Купить</a>"
            f"<a href=\\'{rent_link}\\' target=\\'_blank\\' "
            f"style=\\'background:#fff;color:#0d6efd;padding:6px 14px;"
            f"border-radius:6px;font-size:12px;font-weight:600;"
            f"text-decoration:none;border:1px solid #0d6efd;\\'>📋 Снять</a>"
            f"</div></div>"
        )
        balloon = balloon.replace("\n", "")
        hint = f"{row['district']}: {row['total_index']:.0f}/100"

        marks += (
            f"var c{_}=new ymaps.Circle("
            f"[[{row['lat']},{row['lon']}],{r}],"
            f"{{balloonContent:'{balloon}',hintContent:'{hint}'}},"
            f"{{fillColor:'{color}40',strokeColor:'{color}',"
            f"strokeWidth:2,fillOpacity:0.45}});"
            f"m.geoObjects.add(c{_});"
        )

    src = (
        f'<!DOCTYPE html><html><head><meta charset="utf-8">'
        f'<script src="https://api-maps.yandex.ru/2.1/?apikey={api_key}'
        f'&lang=ru_RU"></script>'
        f'<style>'
        f'body{{margin:0}}'
        f'#map{{width:100%;height:{height}px;border-radius:8px;'
        f'border:1px solid #dee2e6}}'
        f'[class*="copyrights"]{{display:none!important}}'
        f'[class*="ground-pane"]~div:last-child{{display:none!important}}'
        f'</style>'
        f'</head><body><div id="map"></div><script>'
        f'ymaps.ready(function(){{var m=new ymaps.Map("map",'
        f'{{center:[{center[0]},{center[1]}],zoom:{zoom},'
        f'controls:["zoomControl"]}},'
        f'{{suppressMapOpenBlock:true}});{marks}}});</script></body></html>'
    )
    html_comp.html(src, height=height + 10)


def legend():
    items = [
        ("#198754", "A", "75–100", "Отлично"),
        ("#0d6efd", "B", "60–74", "Хорошо"),
        ("#fd7e14", "C", "45–59", "Средне"),
        ("#dc3545", "D", "30–44", "Ниже среднего"),
        ("#6c757d", "F", "0–29", "Плохо"),
    ]
    html = (
        f'<div style="{_CARD_STYLE}">'
        f'<div style="{_CARD_HEADER}">Шкала оценки</div>'
    )
    for color, grade, range_str, label in items:
        html += (
            f'<div style="display:flex;align-items:center;gap:8px;'
            f'padding:4px 0;font-size:13px;color:#212529;">'
            f'<div style="width:10px;height:10px;border-radius:50%;'
            f'background:{color};flex-shrink:0;"></div>'
            f'<span style="font-weight:700;min-width:14px;">{grade}</span>'
            f'<span style="color:#adb5bd;font-size:11px;min-width:42px;">{range_str}</span>'
            f'<span style="flex:1;text-align:right;font-weight:500;">{label}</span>'
            f'</div>'
        )
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


def rank_list(df):
    df_s = df.sort_values("total_index", ascending=False)
    html = (
        '<div style="background:#fff;border:1px solid #dee2e6;'
        'border-radius:8px;padding:0;overflow:hidden;">'
        f'<div style="{_CARD_HEADER}padding:14px 16px 8px;">Рейтинг зон</div>'
    )
    for i, (_, row) in enumerate(df_s.iterrows(), 1):
        c = grade_color(row["total_index"])
        border = "border-bottom:1px solid #f0f0f0;" if i < len(df_s) else ""
        if i <= 3:
            medals = {1: "🥇", 2: "🥈", 3: "🥉"}
            pos = medals[i]
        else:
            pos = f'<span style="color:#adb5bd;font-weight:700;font-size:12px;">{i}</span>'
        html += (
            f'<div style="display:flex;align-items:center;'
            f'padding:8px 16px;{border}font-size:14px;">'
            f'<span style="width:26px;text-align:center;">{pos}</span>'
            f'<span style="width:8px;height:8px;border-radius:50%;'
            f'background:{c};margin:0 10px;flex-shrink:0;"></span>'
            f'<span style="flex:1;color:#212529;font-weight:500;">'
            f'{row["district"]}</span>'
            f'<span style="font-weight:700;color:#212529;font-size:15px;">'
            f'{row["total_index"]:.0f}</span></div>'
        )
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


def stat_card(best, worst):
    bc = grade_color(best["total_index"])
    wc = grade_color(worst["total_index"])
    html = (
        f'<div style="{_CARD_STYLE}">'
        f'<div style="{_CARD_HEADER}">Статистика</div>'
        f'<div style="border-left:3px solid {bc};padding-left:12px;margin-bottom:12px;">'
        f'<div style="font-size:12px;color:#6c757d;">🏆 Лучшая зона</div>'
        f'<div style="font-size:15px;font-weight:700;color:{bc};">'
        f'{best["district"]}</div>'
        f'<div style="font-size:20px;font-weight:700;color:{bc};">'
        f'{best["total_index"]:.0f}</div></div>'
        f'<div style="border-left:3px solid {wc};padding-left:12px;">'
        f'<div style="font-size:12px;color:#6c757d;">📉 Худшая зона</div>'
        f'<div style="font-size:15px;font-weight:700;color:{wc};">'
        f'{worst["district"]}</div>'
        f'<div style="font-size:20px;font-weight:700;color:{wc};">'
        f'{worst["total_index"]:.0f}</div></div>'
        f'</div>'
    )
    st.markdown(html, unsafe_allow_html=True)


def problems(row):
    """Частые жалобы жителей"""
    problem_icons = {
        "Мусор и чистота": "🗑️", "Дороги": "🛣️", "Освещение": "💡",
        "Шум": "🔊", "Парковка": "🅿️", "Озеленение": "🌳",
        "Детская инфраструктура": "👶", "Безопасность": "🚨",
        "Транспорт": "🚌", "ЖКХ": "🏚️",
    }
    tags = ""
    for c in ["top_problem_1", "top_problem_2", "top_problem_3"]:
        v = row.get(c, "")
        if v and str(v) not in ("nan", "", "0"):
            icon = problem_icons.get(v, "⚠️")
            tags += (
                f'<span style="display:inline-block;'
                f'background:#fff3cd;color:#856404;'
                f'border:1px solid #ffc107;border-radius:6px;'
                f'padding:5px 12px;font-size:12px;font-weight:600;'
                f'margin:3px 4px 3px 0;">{icon} {v}</span>'
            )
    if tags:
        html = (
            f'<div style="{_CARD_STYLE}">'
            f'<div style="{_CARD_HEADER}">💬 Жители жалуются на</div>'
            f'<div style="font-size:11px;color:#adb5bd;margin-bottom:10px;margin-top:-6px;">'
            f'По анализу отзывов</div>'
            f'<div>{tags}</div></div>'
        )
        st.markdown(html, unsafe_allow_html=True)


def recommendations(row):
    """Предупреждения для покупателя/арендатора"""
    warnings_map = {
        "infrastructure_score": (
            "🏪 Инфраструктура",
            "Мало магазинов и аптек — за покупками придётся ездить",
            "Проверьте ближайший продуктовый и аптеку",
        ),
        "education_score": (
            "🎓 Образование",
            "Мало школ и детсадов — важно для семей с детьми",
            "Уточните наличие мест в ближайших школах",
        ),
        "healthcare_score": (
            "🏥 Здравоохранение",
            "Поликлиники далеко — критично в экстренной ситуации",
            "Узнайте прикрепление по адресу",
        ),
        "transport_score": (
            "🚌 Транспорт",
            "Слабая доступность — без машины будет сложно",
            "Проверьте маршруты и расстояние до метро",
        ),
        "ecology_score": (
            "🌿 Экология",
            "Мало зелени, возможен шум или загрязнение воздуха",
            "Обратите внимание на близость промзон и трасс",
        ),
        "safety_score": (
            "🛡️ Безопасность",
            "Может быть небезопасно в тёмное время суток",
            "Прогуляйтесь вечером перед решением",
        ),
        "leisure_score": (
            "🎭 Досуг",
            "Мало мест для отдыха — «спальный» район",
            "Если важен досуг — присмотритесь к центру",
        ),
        "social_score": (
            "💬 Среда",
            "Жители часто недовольны районом",
            "Почитайте отзывы на Яндекс.Картах",
        ),
    }

    scores = {k: row.get(k, 50) for k in warnings_map}
    weak = sorted(scores.items(), key=lambda x: x[1])[:3]

    html = (
        f'<div style="{_CARD_STYLE}">'
        f'<div style="{_CARD_HEADER}">⚠️ На что обратить внимание</div>'
    )

    has_warning = False
    for key, score in weak:
        if score < 60:
            name, warning, tip = warnings_map[key]
            if score < 35:
                bg = "#f8d7da"; border_c = "#f5c2c7"; accent = "#842029"
                severity = "Критично"
            elif score < 50:
                bg = "#fff3cd"; border_c = "#ffecb5"; accent = "#664d03"
                severity = "Слабое место"
            else:
                bg = "#cff4fc"; border_c = "#b6effb"; accent = "#055160"
                severity = "Ниже среднего"

            html += (
                f'<div style="background:{bg};border:1px solid {border_c};'
                f'border-radius:6px;padding:12px 14px;margin-bottom:8px;">'
                f'<div style="display:flex;justify-content:space-between;'
                f'align-items:center;margin-bottom:4px;">'
                f'<strong style="font-size:13px;color:{accent};">{name}</strong>'
                f'<span style="font-size:10px;color:{accent};font-weight:700;'
                f'background:#fff;padding:2px 8px;border-radius:4px;'
                f'border:1px solid {border_c};">{severity} · {score:.0f}</span>'
                f'</div>'
                f'<div style="font-size:13px;color:#212529;margin-bottom:4px;">'
                f'{warning}</div>'
                f'<div style="font-size:12px;color:#6c757d;font-style:italic;">'
                f'💡 {tip}</div>'
                f'</div>'
            )
            has_warning = True

    if not has_warning:
        html += (
            '<div style="background:#d1e7dd;border:1px solid #badbcc;'
            'border-radius:6px;padding:16px;text-align:center;">'
            '<div style="font-size:20px;margin-bottom:4px;">✅</div>'
            '<div style="font-size:14px;color:#0f5132;font-weight:600;">'
            'Район комфортный для жизни</div>'
            '<div style="font-size:12px;color:#6c757d;margin-top:4px;">'
            'Все показатели в норме</div>'
            '</div>'
        )

    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


def comparison_table(d1, d2, r1, r2):
    table_rows = [
        ("🏆 Общий индекс", r1.get("total_index", 0), r2.get("total_index", 0)),
    ]
    for col in SCORE_COLS:
        label = SCORE_EMOJI.get(col, "") + " " + SCORE_LABELS_MAP.get(col, col)
        table_rows.append((label, r1.get(col, 0), r2.get(col, 0)))

    html = (
        '<div style="background:#fff;border:1px solid #dee2e6;'
        'border-radius:8px;overflow:hidden;margin-bottom:12px;">'
        '<table style="width:100%;border-collapse:collapse;">'
        f'<thead><tr style="background:#f8f9fa;">'
        f'<th style="padding:10px 14px;font-size:11px;font-weight:700;'
        f'color:#6c757d;text-align:left;text-transform:uppercase;">Показатель</th>'
        f'<th style="padding:10px 14px;font-size:11px;font-weight:700;'
        f'color:#6c757d;text-align:center;text-transform:uppercase;">{d1}</th>'
        f'<th style="padding:10px 14px;font-size:11px;font-weight:700;'
        f'color:#6c757d;text-align:center;text-transform:uppercase;">{d2}</th>'
        f'</tr></thead><tbody>'
    )
    for label, v1, v2 in table_rows:
        if v1 > v2 + 0.5:
            s1 = "color:#198754;font-weight:700;"
            s2 = "color:#dc3545;"
        elif v2 > v1 + 0.5:
            s1 = "color:#dc3545;"
            s2 = "color:#198754;font-weight:700;"
        else:
            s1 = s2 = "color:#212529;"
        html += (
            f'<tr>'
            f'<td style="padding:8px 14px;border-top:1px solid #f0f0f0;'
            f'font-size:13px;color:#212529;">{label}</td>'
            f'<td style="padding:8px 14px;border-top:1px solid #f0f0f0;'
            f'font-size:14px;text-align:center;{s1}">{v1:.0f}</td>'
            f'<td style="padding:8px 14px;border-top:1px solid #f0f0f0;'
            f'font-size:14px;text-align:center;{s2}">{v2:.0f}</td></tr>'
        )
    g1 = r1.get("grade", "?")
    g2 = r2.get("grade", "?")
    html += (
        f'<tr><td style="padding:8px 14px;border-top:1px solid #f0f0f0;'
        f'font-size:13px;color:#212529;">Грейд</td>'
        f'<td style="padding:8px 14px;border-top:1px solid #f0f0f0;'
        f'font-size:15px;text-align:center;font-weight:700;">{g1}</td>'
        f'<td style="padding:8px 14px;border-top:1px solid #f0f0f0;'
        f'font-size:15px;text-align:center;font-weight:700;">{g2}</td></tr>'
    )
    html += '</tbody></table></div>'
    st.markdown(html, unsafe_allow_html=True)


# ============================================================
# КАРТОЧКИ ЖИЛЬЯ
# ============================================================

def _format_price(price, deal_type):
    if not price or price <= 0:
        return "Цена не указана"
    if price >= 1_000_000:
        s = f"{price / 1_000_000:.1f} млн ₽"
    elif price >= 1_000:
        s = f"{price:,.0f} ₽".replace(",", " ")
    else:
        s = f"{price} ₽"
    if deal_type == "rent":
        s += "/мес"
    return s


def _build_card_html(offer, idx):
    price_str = _format_price(offer.get("price", 0), offer.get("deal_type", "sale"))
    title = offer.get("title", "Квартира")
    floor = offer.get("floor", "")
    address = offer.get("address", "")
    link = offer.get("link", "#")
    source = offer.get("source", "")
    area = offer.get("area", 0)

    photos = offer.get("photos", [])
    if not photos and offer.get("photo"):
        photos = [offer["photo"]]

    cid = f"c{idx}"

    if source == "domclick":
        badge = ('<span style="position:absolute;top:10px;left:10px;background:#d1e7dd;color:#0f5132;font-size:10px;'
                 'font-weight:700;padding:3px 8px;border-radius:4px;z-index:5;">ДомКлик</span>')
    elif source == "cian":
        badge = ('<span style="position:absolute;top:10px;left:10px;background:#fff3cd;color:#664d03;font-size:10px;'
                 'font-weight:700;padding:3px 8px;border-radius:4px;z-index:5;">ЦИАН</span>')
    else:
        badge = ""

    heart_btn = (
        '<div class="heart-btn" '
        'style="position:absolute;top:10px;right:10px;width:32px;height:32px;background:rgba(0,0,0,0.4);'
        'border-radius:50%;display:flex;align-items:center;justify-content:center;color:#fff;z-index:5;cursor:pointer;'
        'transition:background 0.2s;">'
        '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24" style="width:18px;height:18px;">'
        '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" '
        'd="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z">'
        '</path></svg></div>'
    )

    if photos and len(photos) > 1:
        slides = ""
        dots = ""
        for i, url in enumerate(photos[:5]):
            vis = "block" if i == 0 else "none"
            lazy = 'loading="lazy" ' if i > 0 else ''
            slides += (
                f'<img data-carousel="{cid}" data-idx="{i}" '
                f'src="{url}" {lazy}referrerpolicy="no-referrer" '
                f'style="position:absolute;top:0;left:0;width:100%;height:100%;'
                f'object-fit:cover;display:{vis};">'
            )
            dot_bg = "#fff" if i == 0 else "rgba(255,255,255,0.5)"
            dots += (
                f'<span data-dot="{cid}" data-idx="{i}" '
                f'style="width:6px;height:6px;border-radius:50%;background:{dot_bg};'
                f'cursor:pointer;transition:background .2s;"></span>'
            )

        photo_block = (
            f'<div class="carousel-wrap" data-cid="{cid}" '
            f'style="position:relative;height:220px;overflow:hidden;'
            f'border-radius:12px 12px 0 0;background:#e9ecef;">'
            f'{badge}'
            f'{heart_btn}'
            f'{slides}'
            f'<div class="arrow-zone arrow-left" data-dir="prev" data-cid="{cid}" '
            f'style="position:absolute;left:0;top:0;bottom:0;width:40%;z-index:3;'
            f'cursor:pointer;"></div>'
            f'<div class="arrow-zone arrow-right" data-dir="next" data-cid="{cid}" '
            f'style="position:absolute;right:0;top:0;bottom:0;width:40%;z-index:3;'
            f'cursor:pointer;"></div>'
            f'<div style="position:absolute;bottom:10px;left:50%;transform:translateX(-50%);'
            f'display:flex;gap:5px;z-index:4;">{dots}</div>'
            f'</div>'
        )
    elif photos:
        photo_block = (
            f'<div style="position:relative;height:220px;overflow:hidden;border-radius:12px 12px 0 0;'
            f'background:#e9ecef;">'
            f'{badge}'
            f'{heart_btn}'
            f'<img src="{photos[0]}" referrerpolicy="no-referrer" style="width:100%;height:100%;object-fit:cover;">'
            f'</div>'
        )
    else:
        photo_block = (
            f'<div style="position:relative;height:220px;background:#e9ecef;'
            f'display:flex;align-items:center;justify-content:center;'
            f'border-radius:12px 12px 0 0;font-size:40px;">'
            f'{badge}'
            f'{heart_btn}'
            f'🏠</div>'
        )

    # Clean up details string
    details = []
    if title: details.append(title.replace('"', '&quot;').replace("'", "&#39;"))
    if area: details.append(f"{area} м²")
    if floor and str(floor) not in ("", "?/?", "0", "/?", "/"): details.append(f"{floor} этаж")
    details_str = " · ".join(details)

    addr_html = ""
    if address:
        safe_addr = address.replace('"', '&quot;').replace("'", "&#39;")
        addr_html = (
            f'<div style="font-size:13px;color:#858585;margin-top:6px;'
            f'line-height:1.4;overflow:hidden;text-overflow:ellipsis;'
            f'display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;">'
            f'{safe_addr}</div>'
        )

    return (
        f'<div class="card">'
        f'{photo_block}'
        f'<a href="{link}" target="_blank" rel="noopener" '
        f'style="text-decoration:none;display:block;padding:16px;">'
        f'<div style="font-size:20px;font-weight:700;color:#1c1c1e;margin-bottom:6px;line-height:1.2;">'
        f'{price_str}</div>'
        f'<div style="font-size:14px;color:#1c1c1e;margin-bottom:2px;">'
        f'{details_str}</div>'
        f'{addr_html}'
        f'</a>'
        f'</div>'
    )


_CAROUSEL_JS = """
<script>
(function(){
    function go(cid, direction) {
        var slides = document.querySelectorAll('img[data-carousel="'+cid+'"]');
        var dots   = document.querySelectorAll('span[data-dot="'+cid+'"]');
        if (!slides.length) return;
        var cur = 0;
        slides.forEach(function(s, i){ if(s.style.display !== 'none') cur = i; });
        var next = direction === 'next'
            ? (cur + 1) % slides.length
            : (cur - 1 + slides.length) % slides.length;
        slides.forEach(function(s){ s.style.display = 'none'; });
        slides[next].style.display = 'block';
        dots.forEach(function(d){ d.style.background = 'rgba(255,255,255,0.5)'; });
        dots[next].style.background = '#fff';
    }
    document.addEventListener('click', function(e){
        var heart = e.target.closest('.heart-btn');
        if (heart) {
            e.preventDefault(); e.stopPropagation();
            var isFilled = heart.getAttribute('data-filled') === 'true';
            if (isFilled) {
                heart.setAttribute('data-filled', 'false');
                heart.innerHTML = '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24" style="width:18px;height:18px;"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z"></path></svg>';
                heart.style.background = 'rgba(0,0,0,0.4)';
            } else {
                heart.setAttribute('data-filled', 'true');
                heart.innerHTML = '<svg fill="currentColor" stroke="none" viewBox="0 0 24 24" style="width:18px;height:18px;color:#ff3b30;"><path d="M12 21.35l-1.45-1.32C5.4 15.36 2 12.28 2 8.5 2 5.42 4.42 3 7.5 3c1.74 0 3.41.81 4.5 2.09C13.09 3.81 14.76 3 16.5 3 19.58 3 22 5.42 22 8.5c0 3.78-3.4 6.86-8.55 11.54L12 21.35z"></path></svg>';
                heart.style.background = '#fff';
            }
            return;
        }

        var zone = e.target.closest('.arrow-zone');
        if (zone) {
            e.preventDefault(); e.stopPropagation();
            go(zone.getAttribute('data-cid'), zone.getAttribute('data-dir'));
            return;
        }
        var dot = e.target.closest('span[data-dot]');
        if (dot) {
            e.preventDefault(); e.stopPropagation();
            var cid2 = dot.getAttribute('data-dot');
            var idx  = parseInt(dot.getAttribute('data-idx'));
            var slides = document.querySelectorAll('img[data-carousel="'+cid2+'"]');
            var dots2  = document.querySelectorAll('span[data-dot="'+cid2+'"]');
            slides.forEach(function(s){ s.style.display = 'none'; });
            slides[idx].style.display = 'block';
            dots2.forEach(function(d){ d.style.background = 'rgba(255,255,255,0.5)'; });
            dots2[idx].style.background = '#fff';
        }
    }, true);
    var startX = 0, activeCid = null;
    document.addEventListener('touchstart', function(e){
        var w = e.target.closest('.carousel-wrap');
        if (w) { startX = e.touches[0].clientX; activeCid = w.getAttribute('data-cid'); }
    }, {passive:true});
    document.addEventListener('touchend', function(e){
        if (!activeCid) return;
        var diff = e.changedTouches[0].clientX - startX;
        if (Math.abs(diff) > 40) go(activeCid, diff < 0 ? 'next' : 'prev');
        activeCid = null;
    }, {passive:true});
})();
</script>
"""


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_offers(lat, lon, deal_type, limit, city_name, grade, radius_km=1.2, district_name=""):
    from parsers.realty_parser import get_parser
    import re
    parser = get_parser()
    results = parser.search(
        lat, lon, deal_type=deal_type, limit=limit,
        city_name=city_name, grade=grade, radius_km=radius_km, district_name=district_name,
    )
    if not results:
        # Second pass for sparse cities: wider radius and slightly larger limit.
        results = parser.search(
            lat, lon, deal_type=deal_type, limit=max(limit, 120),
            city_name=city_name, grade=grade, radius_km=max(radius_km * 2.0, 2.4), district_name=district_name,
        )
    for r in results:
        addr = r.get("address", "")
        if addr:
            r["address"] = re.sub(r'\s*На карте.*$', '', addr).strip()
    if results and district_name and len(results) > 2:
        district_lower = district_name.lower()
        stop_words = {"район", "округ", "микрорайон", "мкр", "имени", "им"}
        keywords = [w for w in district_lower.split() if len(w) > 2 and w not in stop_words]
        if keywords:
            def match_score(offer):
                addr = offer.get("address", "").lower()
                return sum(1 for kw in keywords if kw in addr) if addr else 0
            results.sort(key=match_score, reverse=True)
    results = _filter_offers_to_district(results, district_name, lat, lon, radius_km=radius_km)
    return results


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


def _filter_offers_to_district(offers, district_name, center_lat, center_lon, radius_km=1.2):
    if not offers:
        return []
    name = str(district_name or "").lower().replace("ё", "е").strip()
    stop_words = {"район", "округ", "микрорайон", "мкр", "имени", "им"}
    keys = [w for w in name.split() if len(w) > 2 and w not in stop_words]
    near_km = max(1.2, float(radius_km or 1.2) * 1.25)

    keep = []
    for row in offers:
        text = " ".join([
            str(row.get("address", "") or ""),
            str(row.get("title", "") or ""),
            str(row.get("link", "") or ""),
        ]).lower().replace("ё", "е")
        district_hits = sum(1 for k in keys if k in text) if keys else 0
        dist = None
        if row.get("lat") is not None and row.get("lon") is not None:
            dist = _haversine_km(center_lat, center_lon, row.get("lat"), row.get("lon"))
        if district_hits > 0:
            keep.append(row)
            continue
        if dist is not None and dist <= near_km:
            keep.append(row)
            continue

    return keep if keep else offers[:max(1, min(6, len(offers)))]


def realty_cards(row, city_name):
    from parsers.realty_parser import RealtyParser
    name = row.get("district", "")
    lat = row.get("lat", 0)
    lon = row.get("lon", 0)
    grade = row.get("grade", "C")
    radius_km = 1.2

    if not lat or not lon:
        st.caption(f"Нет координат для {name}")
        return

    tab_buy, tab_rent = st.tabs(["🔑 Купить", "📋 Снять"])
    page_size = 6

    for tab, deal_type, deal_label in [
        (tab_buy, "sale", "покупке"),
        (tab_rent, "rent", "аренде"),
    ]:
        with tab:
            with st.spinner(f"Поиск квартир по {deal_label}..."):
                offers = _fetch_offers(
                    round(lat, 4), round(lon, 4),
                    deal_type, 96, city_name, grade, radius_km, name,
                )

            if not offers:
                _show_fallback_links(lat, lon, deal_type, name, city_name)
                continue

            pages = max(1, (len(offers) + page_size - 1) // page_size)
            page_idx = int(st.number_input(
                "Страница",
                min_value=1,
                max_value=pages,
                value=1,
                step=1,
                key=f"realty_cards_page::{city_name}::{name}::{deal_type}",
            ))
            start = (page_idx - 1) * page_size
            end = start + page_size
            page_offers = offers[start:end]
            st.caption(f"Показаны {start + 1}-{min(end, len(offers))} из {len(offers)}")

            cards_html = ""
            for i, o in enumerate(page_offers):
                cards_html += _build_card_html(o, start + i)

            dc_url = RealtyParser.make_domclick_url(lat, lon, deal_type)
            ci_url = RealtyParser.make_cian_url(lat, lon, deal_type)

            page = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
    * {{ box-sizing:border-box; margin:0; padding:0; }}
    body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; padding:4px; }}
    .grid {{
        display:grid;
        grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
        gap:12px;
    }}
    .card {{
        background:#fff;
        border:1px solid #e1e1e1;
        border-radius:12px;
        overflow:hidden;
        transition: box-shadow 0.2s, transform 0.15s;
        display:flex;
        flex-direction:column;
    }}
    .card:hover {{
        box-shadow:0 8px 24px rgba(0,0,0,0.08);
        transform:translateY(-2px);
    }}
    .card img {{ display:block; }}
    .arrow-zone {{
        -webkit-tap-highlight-color: transparent;
    }}
    .footer {{
        text-align:center; margin-top:16px;
        display:flex; gap:10px; justify-content:center; flex-wrap:wrap;
        padding-bottom:8px;
    }}
    .footer a {{
        font-size:13px; font-weight:600; text-decoration:none;
        padding:8px 20px; border-radius:6px; color:#fff;
        transition: opacity 0.15s;
    }}
    .footer a:hover {{ opacity:0.85; }}
</style>
</head><body>
    <div class="grid">{cards_html}</div>
    <div class="footer">
        <a style="background:#0d6efd;" href="{dc_url}" target="_blank">Ещё на ДомКлик →</a>
        <a style="background:#fd7e14;" href="{ci_url}" target="_blank">Ещё на ЦИАН →</a>
    </div>
{_CAROUSEL_JS}
</body></html>"""

            n_rows = max(1, (len(page_offers) + 2) // 3)
            html_comp.html(page, height=n_rows * 310 + 70, scrolling=True)


def _show_fallback_links(lat, lon, deal_type, name, city_name):
    from parsers.realty_parser import RealtyParser
    dc = RealtyParser.make_domclick_url(lat, lon, deal_type)
    ci = RealtyParser.make_cian_url(lat, lon, deal_type)
    label = "покупке" if deal_type == "sale" else "аренде"

    page = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
    body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; }}
    .box {{
        background:#fff; border:1px solid #dee2e6;
        border-radius:8px; padding:28px; text-align:center;
    }}
    .btns {{
        display:flex; gap:10px; justify-content:center;
        margin-top:16px; flex-wrap:wrap;
    }}
    .btn {{
        padding:10px 20px; border-radius:6px; font-weight:600;
        font-size:14px; text-decoration:none; color:#fff;
        transition: opacity 0.15s;
    }}
    .btn:hover {{ opacity:0.85; }}
</style>
</head><body>
<div class="box">
    <div style="font-size:36px;margin-bottom:8px;">🏠</div>
    <div style="font-size:14px;color:#495057;">
        Не удалось загрузить объявления по {label}
        рядом с <b>{name}</b>
    </div>
    <div style="font-size:12px;color:#adb5bd;margin-top:6px;margin-bottom:16px;">
        Попробуйте найти на сайтах
    </div>
    <div class="btns">
        <a class="btn" style="background:#0d6efd;" href="{dc}" target="_blank">ДомКлик →</a>
        <a class="btn" style="background:#fd7e14;" href="{ci}" target="_blank">ЦИАН →</a>
    </div>
</div>
</body></html>"""
    html_comp.html(page, height=180)
