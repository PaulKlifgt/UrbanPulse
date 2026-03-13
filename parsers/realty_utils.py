"""Утилиты: очистка данных, валидация, геометрия, форматирование."""

import re
import math


def clean_address(addr: str) -> str:
    """Очистка адреса от мусора."""
    if not addr:
        return ''
    addr = re.split(r'На карте', addr, flags=re.IGNORECASE)[0]
    addr = re.sub(r'\s*жилой комплекс\s*', ', ЖК ', addr)
    addr = re.sub(
        r'(?:Показать|Смотреть|Открыть)\s+на\s+карте', '',
        addr, flags=re.IGNORECASE
    )
    addr = re.sub(
        r'(?:Подробнее|Ещё|Показать ещё|Свернуть).*$', '',
        addr, flags=re.IGNORECASE
    )
    addr = re.sub(r',\s*,', ',', addr)
    addr = re.sub(r'\s+', ' ', addr).strip().rstrip(',').strip()
    return addr


def is_valid_listing(item: dict) -> bool:
    """Проверка, является ли объявление валидным."""
    price = item.get('price', 0)
    if not price or price <= 0:
        return False

    photos = item.get('photos', [])
    photo = item.get('photo', '')
    address = item.get('address', '')
    area = item.get('area', 0)
    rooms = item.get('rooms', '?')

    has_photos = bool(photos) or bool(photo)
    has_real_address = bool(address) and len(address) >= 8
    has_details = bool(area) and area > 0 and rooms != '?'
    has_link = str(item.get("link", "") or "").startswith("http")

    if item.get('_from_cluster'):
        return False
    if not has_link:
        return False
    if not has_photos and not has_details and not has_real_address:
        return False

    return True


def haversine(lat1, lon1, lat2, lon2):
    """Расстояние между двумя точками в км."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2 +
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
        math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def bbox(lat, lon, radius_km=1.5):
    """Bounding box вокруг точки."""
    dlat = radius_km / 111.0
    dlon = radius_km / (111.0 * math.cos(math.radians(lat)))
    return (
        round(lat - dlat, 6), round(lon - dlon, 6),
        round(lat + dlat, 6), round(lon + dlon, 6),
    )


def make_title(rooms, area):
    """Формирование заголовка объявления."""
    if str(rooms).lower() in ("студия", "studio", "0"):
        return f"Студия, {area:.0f} м²" if area else "Студия"
    if area:
        return f"{rooms}-комн., {area:.0f} м²"
    return f"{rooms}-комн."


def detect_cian_region(city_name):
    """Определение ID региона ЦИАН по названию города."""
    from .realty_constants import CIAN_REGIONS

    if not city_name:
        return None
    city_lower = city_name.lower().strip()
    for name, region_id in CIAN_REGIONS.items():
        if name in city_lower or city_lower in name:
            return region_id
    return None


def make_domclick_url(lat, lon, deal_type="sale", radius_km=1.5):
    """URL поиска на ДомКлик."""
    return (
        f"https://domclick.ru/search?"
        f"deal_type={deal_type}&category=living&offer_type=flat"
        f"&geo_lat={lat}&geo_lng={lon}&geo_rad={int(radius_km * 1000)}"
    )


def make_cian_url(lat, lon, deal_type="sale", radius_km=1.5):
    """URL поиска на ЦИАН."""
    dlat = radius_km / 111.0
    dlon = radius_km / (111.0 * math.cos(math.radians(lat)))
    return (
        f"https://www.cian.ru/cat.php?"
        f"deal_type={deal_type}&engine_version=2&offer_type=flat"
        f"&minlat={lat - dlat:.6f}&maxlat={lat + dlat:.6f}"
        f"&minlon={lon - dlon:.6f}&maxlon={lon + dlon:.6f}"
        f"&sort=creation_date_desc&p=1"
    )
