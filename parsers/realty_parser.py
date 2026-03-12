"""
Главный модуль парсера недвижимости.
Координирует стратегии, кэш и WebDriver.
"""

import threading

from .realty_utils import clean_address, is_valid_listing, detect_cian_region, haversine
from .realty_cache import RealtyCache
from .realty_driver import DriverManager
from .realty_strategies import (
    strategy_map_clusters,
    strategy_direct_api,
    strategy_list_page,
    strategy_bbox_search,
    strategy_http_api,
    strategy_http_list_page,
    strategy_http_text_search,
)

# Реэкспорт утилит для обратной совместимости
from .realty_utils import (  # noqa: F401
    make_domclick_url,
    make_cian_url,
    haversine,
    make_title,
    bbox,
)


class RealtyParser:

    def __init__(self, cache_dir="data/_realty_cache", cache_hours=6):
        self._cache = RealtyCache(cache_dir, cache_hours)
        self._driver_mgr = DriverManager()
        self._lock = threading.Lock()

    def close(self):
        """Закрытие драйвера."""
        self._driver_mgr.close()

    # ─── Главный метод ───

    def search(self, lat, lon, deal_type="sale", limit=6,
               city_name="", grade="C", radius_km=1.5, district_name=""):
        """Поиск объявлений недвижимости."""
        if not lat or not lon:
            return []

        key = self._cache.make_key(
            lat=round(lat, 3), lon=round(lon, 3),
            deal=deal_type,
            lim=limit,
            city=str(city_name or "").strip().lower(),
            district=str(district_name or "").strip().lower(),
            radius=round(float(radius_km or 0), 2),
            v=2,
        )

        cached = self._cache.get(key)
        if cached is not None:
            print(f"      📦 Из кэша: {len(cached)} объявлений")
            return cached

        with self._lock:
            cached = self._cache.get(key)
            if cached is not None:
                print(f"      📦 Из кэша: {len(cached)} объявлений")
                return cached

            print(f"      🏠 Парсинг {deal_type} ({lat:.4f}, {lon:.4f})...")

            results = self._parse_cian(
                lat, lon, deal_type, limit, radius_km, city_name, district_name
            )

            # Фильтрация. Если детальные карточки не извлеклись, но есть
            # fallback из кластеров ЦИАН, оставляем их как запасной вариант
            # вместо полного пустого результата.
            raw_results = list(results or [])
            before = len(raw_results)
            cluster_fallbacks = [r for r in raw_results if r.get("_from_cluster")]
            results = [r for r in results if is_valid_listing(r)]
            if before > len(results):
                print(
                    f"      🧹 Отфильтровано "
                    f"{before - len(results)} битых объявлений"
                )
            if not results and cluster_fallbacks:
                results = cluster_fallbacks[:limit]
                print(
                    f"      ℹ️ Использую {len(results)} fallback-объявлений "
                    f"из кластеров ЦИАН"
                )
            if not results and raw_results:
                soft_fallbacks = []
                for row in raw_results:
                    try:
                        price = float(row.get("price", 0) or 0)
                    except Exception:
                        price = 0
                    link = str(row.get("link", "") or "")
                    address = str(row.get("address", "") or "")
                    if price <= 0:
                        continue
                    if not link.startswith("http"):
                        continue
                    if len(address.strip()) < 4:
                        continue
                    soft_fallbacks.append(row)
                if soft_fallbacks:
                    results = soft_fallbacks[:limit]
                    print(
                        f"      ℹ️ Использую {len(results)} мягкий fallback "
                        "после strict-валидации"
                    )

            # Очистка адресов
            for r in results:
                r['address'] = clean_address(r.get('address', ''))

            # Safety-filter: cut obviously wrong-city listings before caching.
            results = self._filter_by_location_hint(
                results=results,
                center_lat=lat,
                center_lon=lon,
                city_name=city_name,
            )

            self._cache.set(key, results)

            if results:
                print(f"      ✅ {len(results)} реальных объявлений")
            else:
                print(f"      ⚠️ Объявления не найдены")

            return results

    def _filter_by_location_hint(self, results, center_lat, center_lon, city_name=""):
        if not results:
            return []
        city_hint = str(city_name or "").strip().lower().replace("ё", "е")
        city_tokens = [t for t in city_hint.replace("-", " ").split() if len(t) >= 4]
        max_km = 35.0
        filtered = []
        for row in results:
            keep = False
            try:
                olat = row.get("lat")
                olon = row.get("lon")
                if olat is not None and olon is not None:
                    dist = haversine(float(center_lat), float(center_lon), float(olat), float(olon))
                    if dist <= max_km:
                        keep = True
            except Exception:
                pass

            if not keep and city_tokens:
                text = " ".join([
                    str(row.get("address", "") or ""),
                    str(row.get("link", "") or ""),
                    str(row.get("title", "") or ""),
                ]).lower().replace("ё", "е")
                if any(tok in text for tok in city_tokens):
                    keep = True

            if not city_tokens:
                keep = True

            if keep:
                filtered.append(row)

        return filtered

    def _parse_cian(self, lat, lon, deal_type, limit, radius_km, city_name="", district_name=""):
        """Запуск стратегий парсинга ЦИАН последовательно."""
        # Инициализация драйвера
        driver = self._driver_mgr.get_driver()
        region_id = detect_cian_region(city_name) or 4777

        if not driver:
            results = strategy_http_api(
                lat=lat,
                lon=lon,
                deal_type=deal_type,
                limit=limit,
                radius_km=radius_km,
                region_id=region_id,
                city_name=city_name,
            )
            if results:
                return results
            results = strategy_http_list_page(
                lat=lat,
                lon=lon,
                deal_type=deal_type,
                limit=limit,
                radius_km=radius_km,
                region_id=region_id,
                city_name=city_name,
            )
            if results:
                return results
            return strategy_http_text_search(
                deal_type=deal_type,
                limit=limit,
                region_id=region_id,
                city_name=city_name,
                district_name=district_name,
            )

        try:
            driver.current_url
        except Exception:
            driver = self._driver_mgr.restart_driver()
            if not driver:
                results = strategy_http_api(
                    lat=lat,
                    lon=lon,
                    deal_type=deal_type,
                    limit=limit,
                    radius_km=radius_km,
                    region_id=region_id,
                    city_name=city_name,
                )
                if results:
                    return results
                results = strategy_http_list_page(
                    lat=lat,
                    lon=lon,
                    deal_type=deal_type,
                    limit=limit,
                    radius_km=radius_km,
                    region_id=region_id,
                    city_name=city_name,
                )
                if results:
                    return results
                return strategy_http_text_search(
                    deal_type=deal_type,
                    limit=limit,
                    region_id=region_id,
                    city_name=city_name,
                    district_name=district_name,
                )

        # Стратегия 1: Карта с перехватом кластеров
        results = strategy_map_clusters(
            self._driver_mgr, lat, lon, deal_type,
            limit, radius_km, region_id, city_name
        )
        if results:
            return results

        # Стратегия 2: Прямой API запрос
        results = strategy_direct_api(
            self._driver_mgr, lat, lon, deal_type,
            limit, radius_km, region_id, city_name
        )
        if results:
            return results

        # Стратегия 3: Список объявлений
        results = strategy_list_page(
            self._driver_mgr, lat, lon, deal_type,
            limit, radius_km, region_id, city_name
        )
        if results:
            return results

        # Стратегия 4: Поиск по bbox
        results = strategy_bbox_search(
            self._driver_mgr, lat, lon, deal_type,
            limit, radius_km, region_id, city_name
        )
        if results:
            return results
        results = strategy_http_api(
            lat=lat,
            lon=lon,
            deal_type=deal_type,
            limit=limit,
            radius_km=radius_km,
            region_id=region_id,
            city_name=city_name,
        )
        if results:
            return results
        results = strategy_http_list_page(
            lat=lat,
            lon=lon,
            deal_type=deal_type,
            limit=limit,
            radius_km=radius_km,
            region_id=region_id,
            city_name=city_name,
        )
        if results:
            return results
        return strategy_http_text_search(
            deal_type=deal_type,
            limit=limit,
            region_id=region_id,
            city_name=city_name,
            district_name=district_name,
        )

    # Обратная совместимость — статические методы
    @staticmethod
    def make_domclick_url(lat, lon, deal_type="sale", radius_km=1.5):
        return make_domclick_url(lat, lon, deal_type, radius_km)

    @staticmethod
    def make_cian_url(lat, lon, deal_type="sale", radius_km=1.5):
        return make_cian_url(lat, lon, deal_type, radius_km)


# ─── Синглтон ───

_parser = None


def get_parser():
    global _parser
    if _parser is None:
        _parser = RealtyParser()
    return _parser
