"""Парсинг данных об отдельных объявлениях (из API, JSON state, страницы)."""

import re
import json

from .realty_utils import clean_address, make_title


def _norm_rent_period(value):
    s = str(value or "").lower().strip()
    if not s:
        return ""
    if any(x in s for x in ("сут", "day", "daily", "night", "ноч")):
        return "day"
    if any(x in s for x in ("мес", "month", "monthly")):
        return "month"
    return ""


def _rent_period_from_item(item):
    """Определение периода аренды по полям API/JSON."""
    if not isinstance(item, dict):
        return ""

    candidates = []
    bt = item.get("bargainTerms") or item.get("priceInfo") or item.get("price_info") or {}
    if isinstance(bt, dict):
        for k in (
            "paymentPeriod", "paymentPeriodType", "pricePeriod", "period",
            "priceType", "paymentPeriodName", "periodType",
        ):
            if k in bt:
                candidates.append(bt.get(k))

    for k in (
        "paymentPeriod", "paymentPeriodType", "pricePeriod", "period",
        "rentType", "rent_type", "dealSubtype", "description", "title", "name",
    ):
        if k in item:
            candidates.append(item.get(k))

    for v in candidates:
        p = _norm_rent_period(v)
        if p:
            return p
    return ""


def cian_api_to_offer(item, deal_type):
    """Конвертация объекта ЦИАН API в унифицированный оффер."""
    if not isinstance(item, dict):
        return None

    try:
        # Цена
        price = 0
        bt = item.get("bargainTerms", {}) or {}
        price = bt.get("price", 0) or bt.get("priceRur", 0) or 0
        if not price:
            price = item.get("price", 0) or 0
        price = int(price) if price else 0

        # Комнаты
        rooms = str(item.get("roomsCount", "?"))
        flat_type = item.get("flatType", "")
        if rooms == "0" or flat_type == "studio":
            rooms = "студия"

        # Площадь
        area = float(item.get("totalArea", 0) or item.get("area", 0) or 0)

        # Этаж
        floor_num = item.get("floorNumber", "")
        building = item.get("building", {}) or {}
        floors_total = building.get("floorsCount", "")
        if floor_num and floors_total:
            floor_str = f"{floor_num}/{floors_total}"
        elif floor_num:
            floor_str = str(floor_num)
        else:
            floor_str = ""

        # Адрес
        geo = item.get("geo", {}) or {}
        address_parts = []
        for addr_item in (geo.get("address", []) or []):
            if isinstance(addr_item, dict):
                name = addr_item.get("name", "") or addr_item.get("shortName", "")
                if name:
                    address_parts.append(name)
        address = ", ".join(address_parts)[:80] if address_parts else ""

        if not address:
            user_input = geo.get("userInput", "")
            if user_input:
                address = str(user_input)[:80]

        address = clean_address(address)

        # Координаты
        coords = geo.get("coordinates", {}) or {}
        offer_lat = coords.get("lat", 0) or 0
        offer_lon = coords.get("lng", 0) or coords.get("lon", 0) or 0

        # Фото
        photos_list = []
        photos_raw = (
            item.get("photos") or item.get("images") or
            item.get("gallery") or []
        )
        for p in photos_raw[:10]:
            if isinstance(p, dict):
                url = (
                    p.get("fullUrl") or p.get("url") or
                    p.get("thumbnailUrl") or p.get("thumbnail") or ""
                )
                if url:
                    photos_list.append(url)
            elif isinstance(p, str) and p.startswith("http"):
                photos_list.append(p)

        photo = photos_list[0] if photos_list else ""

        # Ссылка
        link = item.get("fullUrl", "") or item.get("url", "")
        if not link:
            cian_id = item.get("cianId", "") or item.get("id", "")
            if cian_id:
                dt = "sale" if deal_type == "sale" else "rent"
                link = f"https://www.cian.ru/{dt}/flat/{cian_id}/"

        rent_period = _rent_period_from_item(item) if deal_type == "rent" else ""

        if not price and not area:
            return None

        result = {
            "source": "cian",
            "deal_type": deal_type,
            "price": price,
            "rooms": rooms,
            "area": round(area, 1) if area else 0,
            "floor": floor_str,
            "address": address,
            "photo": photo,
            "photos": photos_list,
            "link": link or "https://www.cian.ru",
            "title": make_title(rooms, area),
            "lat": offer_lat,
            "lon": offer_lon,
        }
        if rent_period:
            result["rent_period"] = rent_period
        return result

    except Exception:
        return None


def generic_item_to_offer(item, deal_type, source):
    """Конвертация произвольного объекта в унифицированный оффер."""
    if not isinstance(item, dict):
        return None

    try:
        # Цена
        price = 0
        for k in ("price", "cost", "price_rub", "priceRur"):
            v = item.get(k)
            if v:
                if isinstance(v, dict):
                    v = v.get("value") or v.get("price") or v.get("rur") or 0
                try:
                    price = int(float(
                        str(v).replace(" ", "").replace("\xa0", "")
                    ))
                except (ValueError, TypeError):
                    pass
                if price:
                    break

        if not price:
            bt = (
                item.get("bargainTerms") or
                item.get("price_info") or
                item.get("priceInfo") or {}
            )
            if isinstance(bt, dict):
                for k in ("price", "value", "priceRur"):
                    if k in bt:
                        try:
                            price = int(float(str(bt[k]).replace(" ", "")))
                        except (ValueError, TypeError):
                            pass
                        if price:
                            break

        # Комнаты
        rooms = str(
            item.get("rooms") or item.get("roomsCount") or
            item.get("rooms_count") or item.get("flatType") or "?"
        )
        if rooms == "0" or rooms.lower() == "studio":
            rooms = "студия"

        # Площадь
        area = 0
        for k in ("total_area", "totalArea", "area", "area_total", "livingArea"):
            v = item.get(k)
            if v:
                try:
                    area = float(v)
                except (ValueError, TypeError):
                    pass
                if area:
                    break

        # Этаж
        floor = item.get("floor") or item.get("floorNumber") or ""
        ftotal = (
            item.get("floors_total") or item.get("floors_count") or
            item.get("floorsCount") or
            (item.get("building", {}) or {}).get("floorsCount", "") or
            (item.get("house", {}) or {}).get("floorsCount", "")
        )
        floor_str = (
            f"{floor}/{ftotal}" if floor and ftotal
            else str(floor) if floor
            else ""
        )

        # Адрес
        addr = item.get("address") or item.get("geo", {})
        if isinstance(addr, dict):
            address = str(
                addr.get("display_name") or addr.get("short") or
                addr.get("userInput") or
                ", ".join(
                    a.get("name", "") or a.get("shortName", "")
                    for a in addr.get("address", [])
                    if isinstance(a, dict)
                ) or ""
            )[:80]
        elif isinstance(addr, str):
            address = addr[:80]
        elif isinstance(addr, list):
            parts = []
            for a in addr:
                if isinstance(a, dict):
                    parts.append(a.get("name", "") or a.get("title", ""))
                elif isinstance(a, str):
                    parts.append(a)
            address = ", ".join(p for p in parts if p)[:80]
        else:
            address = str(item.get("address_string", ""))[:80]

        address = clean_address(address)

        # Фото
        photo = ""
        photos_list = []
        photos = (
            item.get("photos") or item.get("images") or
            item.get("gallery") or []
        )
        if isinstance(photos, list) and photos:
            for p in photos[:10]:
                if isinstance(p, dict):
                    url = (
                        p.get("url") or p.get("src") or
                        p.get("fullUrl") or p.get("thumbnail") or ""
                    )
                    if url:
                        photos_list.append(url)
                elif isinstance(p, str) and p.startswith("http"):
                    photos_list.append(p)
            photo = photos_list[0] if photos_list else ""

        # Ссылка
        link = (
            item.get("url") or item.get("fullUrl") or
            item.get("offerUrl") or ""
        )
        if link and not link.startswith("http"):
            base = (
                "https://domclick.ru" if source == "domclick"
                else "https://www.cian.ru"
            )
            link = base + link
        if not link:
            oid = (
                item.get("id") or item.get("offer_id") or
                item.get("cianId") or item.get("offerId") or ""
            )
            if oid:
                if source == "domclick":
                    link = f"https://domclick.ru/card/{deal_type}__flat__{oid}"
                else:
                    dt = "sale" if deal_type == "sale" else "rent"
                    link = f"https://www.cian.ru/{dt}/flat/{oid}/"

        rent_period = _rent_period_from_item(item) if deal_type == "rent" else ""

        if not price and not area:
            return None

        result = {
            "source": source,
            "deal_type": deal_type,
            "price": price,
            "rooms": rooms,
            "area": round(area, 1) if area else 0,
            "floor": floor_str,
            "address": address,
            "photo": photo,
            "photos": photos_list,
            "link": link or (
                "https://domclick.ru" if source == "domclick"
                else "https://www.cian.ru"
            ),
            "title": make_title(rooms, area),
        }
        if rent_period:
            result["rent_period"] = rent_period
        return result

    except Exception:
        return None


def find_offers_recursive(obj, depth=0):
    """Рекурсивный поиск массива офферов в произвольной структуре."""
    if depth > 8:
        return []

    if isinstance(obj, list) and len(obj) >= 1:
        if isinstance(obj[0], dict):
            keys = set(obj[0].keys())
            offer_keys = {
                "price", "cost", "rooms", "area", "totalArea",
                "roomsCount", "total_area", "rooms_count",
                "bargainTerms", "price_info", "priceInfo",
                "flatType", "floorNumber", "geo",
                "cianId", "offer_id", "offerId",
            }
            if keys & offer_keys:
                return obj

    if isinstance(obj, dict):
        for key in (
            "items", "offers", "offersSerialized",
            "results", "list", "offerList", "catalogItems",
            "offerData", "searchResults",
        ):
            if key in obj:
                found = find_offers_recursive(obj[key], depth + 1)
                if found:
                    return found

        for key in (
            "search", "result", "data", "searchResult",
            "catalog", "pageProps", "props", "state",
            "searchOffers", "offerSearch", "initialState",
        ):
            if key in obj and isinstance(obj[key], dict):
                found = find_offers_recursive(obj[key], depth + 1)
                if found:
                    return found

    return []


def find_clusters_recursive(obj, depth=0):
    """Рекурсивный поиск массива кластеров."""
    if depth > 8:
        return None

    if isinstance(obj, list) and len(obj) >= 1:
        if isinstance(obj[0], dict) and "clusterOfferIds" in obj[0]:
            return obj
        if isinstance(obj[0], dict) and "coordinates" in obj[0]:
            return obj

    if isinstance(obj, dict):
        for key, val in obj.items():
            result = find_clusters_recursive(val, depth + 1)
            if result:
                return result

    return None


def parse_cluster_json(raw):
    """Парсинг JSON строки с кластерами."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []

    if isinstance(data, dict):
        clusters = data.get("filtered", [])
        if not clusters:
            clusters = data.get("clusters", [])
        if not clusters:
            clusters = (
                data.get("data", {}).get("filtered", [])
                if isinstance(data.get("data"), dict) else []
            )
        if not clusters:
            found = find_clusters_recursive(data)
            if found:
                clusters = found
        return clusters
    elif isinstance(data, list):
        if data and isinstance(data[0], dict) and "clusterOfferIds" in data[0]:
            return data
    return []
