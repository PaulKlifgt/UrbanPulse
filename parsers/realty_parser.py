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
            text_fallback_rows = [r for r in raw_results if r.get("_http_text_fallback")]
            results = [r for r in results if is_valid_listing(r)]
            if before > len(results):
                print(
                    f"      🧹 Отфильтровано "
                    f"{before - len(results)} битых объявлений"
                )
            if not results and text_fallback_rows:
                rescued = []
                for row in text_fallback_rows:
                    try:
                        price = float(row.get("price", 0) or 0)
                    except Exception:
                        price = 0
                    link = str(row.get("link", "") or "")
                    addr = str(row.get("address", "") or "")
                    if price > 0 and (link.startswith("http") or len(addr.strip()) >= 6):
                        rescued.append(row)
                if rescued:
                    results = rescued[:limit]
                    print(
                        f"      ℹ️ Rescue after strict-validation: "
                        f"{len(results)} из HTTP text fallback"
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
            location_filtered = self._filter_by_location_hint(
                results=results,
                center_lat=lat,
                center_lon=lon,
                city_name=city_name,
                district_name=district_name,
                radius_km=radius_km,
            )
            if not location_filtered and results:
                text_fallback_rows = [r for r in results if r.get("_http_text_fallback")]
                if text_fallback_rows:
                    rescued = []
                    for row in text_fallback_rows:
                        try:
                            price = float(row.get("price", 0) or 0)
                        except Exception:
                            price = 0
                        link = str(row.get("link", "") or "")
                        addr = str(row.get("address", "") or "")
                        if price > 0 and (link.startswith("http") or len(addr.strip()) >= 6):
                            rescued.append(row)
                    if rescued:
                        location_filtered = rescued[:limit]
                        print(
                            f"      ℹ️ Rescue after location-filter: "
                            f"{len(location_filtered)} из HTTP text fallback"
                        )
            results = location_filtered

            # Do not cache empty responses: transient network/captcha failures
            # would otherwise freeze this point for cache TTL hours.
            if results:
                self._cache.set(key, results)
            else:
                print("      🧊 Пустой результат не кэшируем (anti-stale miss)")

            if results:
                print(f"      ✅ {len(results)} реальных объявлений")
            else:
                print(f"      ⚠️ Объявления не найдены")

            return results

    def _filter_by_location_hint(
        self,
        results,
        center_lat,
        center_lon,
        city_name="",
        district_name="",
        radius_km=1.5,
    ):
        if not results:
            return []
        city_hint = str(city_name or "").strip().lower().replace("ё", "е")
        district_hint = str(district_name or "").strip().lower().replace("ё", "е")
        city_tokens = [t for t in city_hint.replace("-", " ").split() if len(t) >= 4]
        district_tokens = [
            t for t in district_hint.replace("-", " ").split()
            if len(t) >= 3 and t not in {"район", "округ", "ао", "микрорайон", "мкр"}
        ]
        city_aliases = {
            "астрахань": ["astrahan", "astrakhan"],
            "нижний новгород": ["nizhny", "nizhniy", "novgorod"],
            "санкт петербург": ["spb", "peterburg", "petersburg"],
            "москва": ["moscow"],
            "екатеринбург": ["ekaterinburg"],
        }
        for key, vals in city_aliases.items():
            if city_hint == key or key in city_hint:
                city_tokens.extend(vals)
                break

        max_km = max(24.0, float(radius_km or 1.5) * 8.0)
        near_km = max(2.2, float(radius_km or 1.5) * 1.8)
        filtered = []
        for row in results:
            keep = False
            city_match = False
            district_match = False
            try:
                olat = row.get("lat")
                olon = row.get("lon")
                if olat is not None and olon is not None:
                    dist = haversine(float(center_lat), float(center_lon), float(olat), float(olon))
                    if dist <= max_km:
                        keep = True
                    if dist <= near_km:
                        district_match = True
            except Exception:
                pass

            text = " ".join([
                str(row.get("address", "") or ""),
                str(row.get("link", "") or ""),
                str(row.get("title", "") or ""),
                str(row.get("_query", "") or ""),
                str(row.get("_city_hint", "") or ""),
                str(row.get("_district_hint", "") or ""),
            ]).lower().replace("ё", "е")
            if city_tokens:
                city_match = any(tok in text for tok in city_tokens)
                if not city_match:
                    city_hint_text = str(row.get("_city_hint", "") or "").lower().replace("ё", "е")
                    city_match = city_hint_text == city_hint
            else:
                city_match = True
            if district_tokens:
                district_match = district_match or any(tok in text for tok in district_tokens)
                if not district_match:
                    d_hint = str(row.get("_district_hint", "") or "").lower().replace("ё", "е")
                    district_match = any(tok in d_hint for tok in district_tokens)
            else:
                district_match = True

            # Must belong to requested city; district can be matched by text or proximity.
            if city_match and district_match:
                keep = True

            if keep:
                filtered.append(row)
        return filtered

    def _parse_cian(self, lat, lon, deal_type, limit, radius_km, city_name="", district_name=""):
        """Запуск стратегий парсинга ЦИАН последовательно."""
        region_id = detect_cian_region(city_name) or 4777

        # Инициализация драйвера
        driver = self._driver_mgr.get_driver()

        if not driver:
            print("      ⛔ Chrome-only режим: HTTP fallback отключен")
            return []

        try:
            driver.current_url
        except Exception:
            driver = self._driver_mgr.restart_driver()
            if not driver:
                print("      ⛔ Chrome-only режим: драйвер недоступен после restart")
                return []

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
        return []

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
