"""Парсинг карточек объявлений из DOM и HTML-источников."""

import re
import json

try:
    from selenium.webdriver.common.by import By
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False

from .realty_utils import clean_address, make_title
from .realty_offer_parser import (
    cian_api_to_offer,
    generic_item_to_offer,
    find_offers_recursive,
)


def try_json_state(driver, deal_type, limit, source):
    """Попытка извлечь офферы из JS state переменных."""
    scripts = [
        "return JSON.stringify(window.__INITIAL_STATE__ || null)",
        "return JSON.stringify(window.__DATA__ || null)",
        "return JSON.stringify(window.__NEXT_DATA__ || null)",
        "return JSON.stringify(window.__APP_STATE__ || null)",
        "return JSON.stringify(window.__PRELOADED_STATE__ || null)",
    ]

    for script in scripts:
        try:
            raw = driver.execute_script(script)
            if not raw or raw == "null":
                continue

            state = json.loads(raw)
            if not isinstance(state, dict):
                continue

            items = find_offers_recursive(state)
            if not items:
                continue

            results = []
            for item in items[:limit]:
                if source == "cian":
                    offer = cian_api_to_offer(item, deal_type)
                else:
                    offer = generic_item_to_offer(item, deal_type, source)
                if offer:
                    results.append(offer)

            if results:
                return results

        except Exception:
            continue

    return []


def parse_cards_from_dom(driver, deal_type, limit, source):
    """Парсинг карточек объявлений из DOM элементов."""
    selectors = [
        '[data-name="CardComponent"]',
        '[data-testid="offer-card"]',
        '[data-test="offer-card"]',
        'article[data-id]',
        '[class*="OfferCard"]',
        '[class*="offer-card"]',
        '[class*="_card_"]',
        '[class*="--card--"]',
        'article',
    ]

    cards = []
    for sel in selectors:
        try:
            found = driver.find_elements(By.CSS_SELECTOR, sel)
            if found and len(found) >= 1:
                real_cards = [c for c in found if len(c.text or "") > 20]
                if real_cards:
                    cards = real_cards
                    print(f"         Найдено {len(cards)} карточек по селектору: {sel}")
                    break
        except Exception:
            continue

    if not cards:
        try:
            if source == "domclick":
                cards = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/card/"]')
            else:
                cards = driver.find_elements(
                    By.CSS_SELECTOR,
                    'a[href*="/sale/flat/"], a[href*="/rent/flat/"]'
                )
            cards = [c for c in cards if len(c.text or "") > 20]
            if cards:
                print(f"         Найдено {len(cards)} ссылок-карточек")
        except Exception:
            pass

    if not cards:
        return []

    results = []
    for card in cards[:limit]:
        try:
            offer = _dom_card_to_offer(card, deal_type, source)
            if offer:
                results.append(offer)
        except Exception:
            continue

    return results


def parse_from_page_source(driver, deal_type, limit, source):
    """Парсинг офферов из HTML-источника страницы."""
    try:
        html = driver.page_source
        return parse_from_html_source(html, deal_type, limit, source)
    except Exception:
        pass

    return []


def parse_from_html_source(html, deal_type, limit, source):
    """Парсинг офферов из сырого HTML (без Selenium)."""
    if not html:
        return []

    json_candidates = []
    # Fast path: extract known JS state objects via balanced braces instead of
    # expensive global regex over large HTML blobs.
    state_markers = [
        "window.__INITIAL_STATE__",
        "window.__DATA__",
        "window.__APP_STATE__",
    ]
    for marker in state_markers:
        marker_pos = html.find(marker)
        if marker_pos < 0:
            continue
        eq_pos = html.find("=", marker_pos)
        if eq_pos < 0:
            continue
        brace_pos = html.find("{", eq_pos)
        if brace_pos < 0:
            continue
        raw = _extract_balanced_json_block(html, brace_pos, "{", "}")
        if raw:
            json_candidates.append(raw)

    # Next.js payload often contains structured offer lists.
    next_data_match = re.search(
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>\s*(\{.*?\})\s*</script>',
        html,
        re.DOTALL,
    )
    if next_data_match:
        json_candidates.append(next_data_match.group(1))

    # Lightweight array candidates near "items"/"offers" keys.
    array_markers = [
        '"items":[',
        '"offers":[',
        '"offersSerialized":[',
    ]
    for marker in array_markers:
        pos = html.find(marker)
        if pos < 0:
            continue
        lb = html.find("[", pos)
        if lb < 0:
            continue
        raw_arr = _extract_balanced_json_block(html, lb, "[", "]")
        if raw_arr:
            json_candidates.append(raw_arr)

    for raw in json_candidates:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue

        if isinstance(data, dict):
            items = find_offers_recursive(data)
        elif isinstance(data, list):
            items = data
        else:
            continue

        if not items:
            continue

        results = []
        for item in items[:limit]:
            if source == "cian":
                offer = cian_api_to_offer(item, deal_type) or generic_item_to_offer(item, deal_type, source)
            else:
                offer = generic_item_to_offer(item, deal_type, source)
            if offer:
                results.append(offer)

        if results:
            return results

    return []


def _extract_balanced_json_block(text, start_idx, open_ch, close_ch):
    """Extract balanced JSON object/array starting at start_idx."""
    if start_idx < 0 or start_idx >= len(text) or text[start_idx] != open_ch:
        return None
    depth = 0
    in_str = False
    escaped = False
    for i in range(start_idx, len(text)):
        ch = text[i]
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == open_ch:
            depth += 1
            continue
        if ch == close_ch:
            depth -= 1
            if depth == 0:
                return text[start_idx:i + 1]
    return None


def _dom_card_to_offer(element, deal_type, source):
    """Конвертация DOM-элемента карточки в оффер."""
    text = element.text
    if not text or len(text) < 10:
        return None

    # === ЦЕНА ===
    price = _extract_price_from_text(text, deal_type)
    rent_period = _extract_rent_period_from_text(text) if deal_type == "rent" else ""

    # === КОМНАТЫ ===
    rooms = "?"
    m = re.search(r'(\d)[- ]?комн', text, re.I)
    if m:
        rooms = m.group(1)
    elif re.search(r'студи', text, re.I):
        rooms = "студия"

    # === ПЛОЩАДЬ ===
    area = 0
    m = re.search(r'([\d,\.]+)\s*м[²2\s]', text)
    if m:
        try:
            area = float(m.group(1).replace(',', '.'))
        except ValueError:
            pass

    # === ЭТАЖ ===
    floor = ""
    m = re.search(r'(\d+)\s*/\s*(\d+)\s*эт', text, re.I)
    if m:
        floor = f"{m.group(1)}/{m.group(2)}"
    else:
        m = re.search(r'(\d+)\s*эт', text, re.I)
        if m:
            floor = m.group(1)

    # === АДРЕС ===
    address = _extract_address_from_text(text)

    # === ССЫЛКА ===
    link = _extract_link(element, source)

    # === ФОТО ===
    photos = _extract_photos(element)
    photo = photos[0] if photos else ""

    if not price and not area:
        return None

    result = {
        "source": source,
        "deal_type": deal_type,
        "price": price,
        "rooms": rooms,
        "area": round(area, 1) if area else 0,
        "floor": floor,
        "address": address,
        "photo": photo,
        "photos": photos,
        "link": link or (
            "https://domclick.ru" if source == "domclick"
            else "https://www.cian.ru"
        ),
        "title": make_title(rooms, area),
    }
    if rent_period:
        result["rent_period"] = rent_period
    return result


def _extract_price_from_text(text, deal_type):
    """Извлечение цены из текста."""
    if deal_type == "rent":
        # Сначала пробуем period-aware шаблоны (суточно/месяц)
        for pattern in [
            r'([\d\s\xa0]+)\s*₽\s*(?:/\s*сут(?:ки)?|в\s*сутки|за\s*сутки)',
            r'([\d\s\xa0]+)\s*₽\s*(?:/\s*день|в\s*день|за\s*день)',
            r'([\d\s\xa0]+)\s*₽\s*(?:/\s*мес|в\s*месяц|за\s*месяц)',
        ]:
            m = re.search(pattern, text, re.I)
            if m:
                try:
                    return int(m.group(1).replace(' ', '').replace('\xa0', ''))
                except ValueError:
                    pass

    price = 0

    m = re.search(r'([\d\s\xa0]{3,})\s*₽', text)
    if m:
        try:
            price = int(m.group(1).replace(' ', '').replace('\xa0', ''))
        except ValueError:
            pass

    if not price:
        m = re.search(r'([\d,\.]+)\s*млн', text)
        if m:
            try:
                price = int(float(m.group(1).replace(',', '.')) * 1_000_000)
            except ValueError:
                pass

    if not price:
        for line in text.split('\n'):
            line_clean = line.strip().replace(' ', '').replace('\xa0', '')
            line_digits = re.sub(r'[^\d]', '', line_clean)
            if line_digits and len(line_digits) >= 6:
                try:
                    val = int(line_digits)
                    if 500_000 <= val <= 500_000_000:
                        price = val
                        break
                except ValueError:
                    pass

    if not price:
        m = re.search(r'([\d\s\xa0]+)\s*₽\s*/\s*мес', text)
        if m:
            try:
                price = int(m.group(1).replace(' ', '').replace('\xa0', ''))
            except ValueError:
                pass

    if not price:
        m = re.search(r'([\d\s\xa0,.]+)\s*(?:₽|руб)', text)
        if m:
            digits = re.sub(r'[^\d]', '', m.group(1))
            if digits:
                try:
                    price = int(digits)
                except ValueError:
                    pass

    return price


def _extract_rent_period_from_text(text):
    """Определение периода аренды по тексту карточки."""
    lo = (text or "").lower()
    if any(x in lo for x in ("/сут", "в сутки", "за сутки", "/день", "в день", "посуточ")):
        return "day"
    if any(x in lo for x in ("/мес", "в месяц", "за месяц")):
        return "month"
    return ""


def _extract_address_from_text(text):
    """Извлечение адреса из текста."""
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    addr_patterns = [
        r'ул\.', r'ул\s', r'пр\.', r'пр-', r'пер\.',
        r'наб\.', r'бул\.', r'ш\.', r'просп', r'улица',
        r'проспект', r'переул', r'мкр', r'микрорайон',
        r'район', r'д\.\s*\d', r'корп', r'стр\.',
    ]
    for line in lines:
        if any(re.search(p, line, re.I) for p in addr_patterns):
            return clean_address(line)[:80]

    if lines:
        longest = max(lines, key=len)
        if len(longest) > 15:
            return clean_address(longest)[:80]

    return ""


def _extract_link(element, source):
    """Извлечение ссылки из DOM элемента."""
    link = ""
    try:
        href = element.get_attribute("href")
        if href:
            link = href
        if not link:
            a_tags = element.find_elements(By.TAG_NAME, "a")
            for a in a_tags:
                h = a.get_attribute("href") or ""
                if "/card/" in h or "/sale/flat/" in h or "/rent/flat/" in h:
                    link = h
                    break
            if not link and a_tags:
                link = a_tags[0].get_attribute("href") or ""
    except Exception:
        pass

    if link and not link.startswith("http"):
        base = (
            "https://domclick.ru" if source == "domclick"
            else "https://www.cian.ru"
        )
        link = base + link

    return link


def _extract_photos(element, limit=10):
    """Извлечение нескольких фото из DOM элемента."""
    photos = []
    seen = set()
    try:
        imgs = element.find_elements(By.TAG_NAME, "img")
        for img in imgs:
            candidates = [
                img.get_attribute("src") or "",
                img.get_attribute("data-src") or "",
                img.get_attribute("data-original") or "",
            ]
            srcset = (
                img.get_attribute("srcset") or
                img.get_attribute("data-srcset") or ""
            )
            if srcset:
                for part in srcset.split(","):
                    url = part.strip().split(" ")[0].strip()
                    if url:
                        candidates.append(url)

            for src in candidates:
                src = (src or "").strip()
                low = src.lower()
                if not src.startswith("http"):
                    continue
                if any(x in low for x in ("logo", "avatar", "icon", "static")):
                    continue
                if src in seen:
                    continue
                seen.add(src)
                photos.append(src)
                if len(photos) >= limit:
                    return photos
    except Exception:
        pass
    return photos
