"""Стратегии парсинга ЦИАН: карта, API, список, bbox."""

import json
import time
import re
import requests
import os
import math

try:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False

from .realty_constants import INTERCEPT_SCRIPT
from .realty_utils import (
    bbox, clean_address, make_title,
    detect_cian_region, haversine,
)
from .realty_offer_parser import (
    cian_api_to_offer, find_offers_recursive,
    find_clusters_recursive, parse_cluster_json,
)
from .realty_dom_parser import (
    try_json_state, parse_cards_from_dom, parse_from_page_source, parse_from_html_source,
)

def _http_budget_sec():
    try:
        return max(6, int(os.environ.get("REALTY_HTTP_BUDGET_SEC", "10")))
    except Exception:
        return 10


def _cian_host_candidates(city_name):
    city = str(city_name or "").lower().replace("ё", "е").strip()
    hosts = ["www.cian.ru"]
    alias_map = {
        "астрахань": ["astrahan.cian.ru", "astrakhan.cian.ru"],
        "екатеринбург": ["ekaterinburg.cian.ru"],
        "санкт-петербург": ["spb.cian.ru"],
        "санкт петербург": ["spb.cian.ru"],
        "нижний новгород": ["nizhniy-novgorod.cian.ru", "nizhny-novgorod.cian.ru"],
        "ростов-на-дону": ["rostov.cian.ru"],
        "ростов на дону": ["rostov.cian.ru"],
        "набережные челны": ["naberezhnye-chelny.cian.ru"],
    }
    for key, aliases in alias_map.items():
        if city == key or key in city:
            hosts = aliases + hosts
            break
    dedup = []
    seen = set()
    for h in hosts:
        if h not in seen:
            seen.add(h)
            dedup.append(h)
    return dedup


def strategy_map_clusters(driver_mgr, lat, lon, deal_type, limit,
                          radius_km, region_id, city_name):
    """Стратегия 1: Карта ЦИАН + перехват fetch/XHR кластеров."""
    driver = driver_mgr.driver
    if not driver:
        return []

    print(f"         📍 Стратегия 1: карта + перехват...")

    # Сброс
    try:
        driver.execute_script("""
            window._cianClusters = null;
            window._cianNetworkLog = [];
        """)
    except Exception:
        pass

    zoom_levels = [16, 15, 14]
    if radius_km <= 0.8:
        zoom_levels = [17, 16, 15]

    for zoom in zoom_levels:
        url = (
            f"https://www.cian.ru/map/?deal_type={deal_type}"
            f"&engine_version=2&offer_type=flat"
            f"&region={region_id}"
            f"&center={lat}%2C{lon}&zoom={zoom}"
        )

        print(f"         center={lat:.4f},{lon:.4f} zoom={zoom}")

        try:
            # Перехватчик
            try:
                driver.get("about:blank")
                time.sleep(0.3)
                driver.execute_script(INTERCEPT_SCRIPT)
            except Exception:
                pass

            driver.get(url)
            time.sleep(5)

            try:
                driver.execute_script(INTERCEPT_SCRIPT)
            except Exception:
                pass

            page_src = driver.page_source
            if driver_mgr.is_captcha(page_src):
                print(f"         ❌ ЦИАН: капча на карте")
                return []

            # Ждём кластеры
            raw = driver_mgr.wait_for_clusters(max_wait=15)

            if not raw:
                # Двигаем карту
                print(f"         🔄 Двигаем карту для запроса кластеров...")
                try:
                    driver.execute_script("""
                        window.dispatchEvent(new Event('resize'));
                        var maps = document.querySelectorAll('[class*="map"]');
                        maps.forEach(function(m) {
                            m.dispatchEvent(new MouseEvent('mousedown',
                                {clientX: 500, clientY: 400}));
                            setTimeout(function() {
                                m.dispatchEvent(new MouseEvent('mousemove',
                                    {clientX: 520, clientY: 400}));
                                setTimeout(function() {
                                    m.dispatchEvent(new MouseEvent('mouseup',
                                        {clientX: 520, clientY: 400}));
                                }, 200);
                            }, 200);
                        });
                    """)
                    time.sleep(5)
                    raw = driver_mgr.wait_for_clusters(max_wait=8)
                except Exception:
                    pass

            if not raw:
                # Лог сети
                try:
                    net_log = driver.execute_script(
                        "return window._cianNetworkLog || []"
                    )
                    if net_log:
                        print(f"         📊 Перехвачено {len(net_log)} запросов:")
                        for entry in net_log[:5]:
                            print(
                                f"            {entry.get('type', 'fetch')} "
                                f"{entry.get('url', '')[:80]} "
                                f"({entry.get('size', 0)} bytes)"
                            )
                    else:
                        print(f"         📊 Ни один запрос не перехвачен")
                except Exception:
                    pass

                # Пробуем __INITIAL_STATE__
                raw = _extract_state_clusters(driver)

            if raw:
                clusters = parse_cluster_json(raw)
                if clusters:
                    return _process_clusters(
                        driver_mgr, clusters, lat, lon, deal_type,
                        limit, radius_km, city_name
                    )

            print(f"         zoom={zoom}: кластеры не найдены")

        except Exception as e:
            print(f"         zoom={zoom} ✗: {e}")
            if "DISCONNECTED" in str(e).upper():
                driver = driver_mgr.restart_driver()
                if not driver:
                    return []
            continue

    return []


def strategy_direct_api(driver_mgr, lat, lon, deal_type, limit,
                        radius_km, region_id, city_name):
    """Стратегия 2: Прямой API запрос кластеров через JS fetch."""
    driver = driver_mgr.driver
    if not driver:
        return []

    print(f"         📍 Стратегия 2: прямой API запрос...")

    lat_min, lon_min, lat_max, lon_max = bbox(lat, lon, radius_km)

    api_urls = [
        "https://api.cian.ru/search-offers-index-map/v1/get-clusters-for-map/",
        "https://api.cian.ru/search-offers/v2/search-offers-desktop/",
    ]

    api_body = json.dumps({
        "jsonQuery": {
            "_type": "flatrent" if deal_type == "rent" else "flatsale",
            "engine_version": {"type": "term", "value": 2},
            "region": {"type": "terms", "value": [region_id]},
            "bbox": {
                "type": "term",
                "value": {
                    "bottom_lat": lat_min,
                    "left_lng": lon_min,
                    "top_lat": lat_max,
                    "right_lng": lon_max,
                }
            }
        }
    })

    for api_url in api_urls:
        try:
            js_fetch = f"""
                return await (async () => {{
                    try {{
                        const resp = await fetch("{api_url}", {{
                            method: "POST",
                            headers: {{
                                "Content-Type": "application/json",
                                "Accept": "application/json",
                            }},
                            body: {json.dumps(api_body)},
                            credentials: "include"
                        }});
                        const text = await resp.text();
                        return text;
                    }} catch(e) {{
                        return "ERROR:" + e.message;
                    }}
                }})();
            """
            raw = driver.execute_script(js_fetch)

            if raw and not raw.startswith("ERROR:"):
                clusters = parse_cluster_json(raw)
                if clusters:
                    print(f"         ✅ API вернул {len(clusters)} кластеров")
                    return _process_clusters(
                        driver_mgr, clusters, lat, lon, deal_type,
                        limit, radius_km, city_name
                    )
                # Может это список офферов
                try:
                    data = json.loads(raw)
                    items = find_offers_recursive(data)
                    if items:
                        results = []
                        for item in items[:limit]:
                            offer = cian_api_to_offer(item, deal_type)
                            if offer:
                                results.append(offer)
                        if results:
                            print(f"         ✅ API вернул {len(results)} объявлений")
                            return results
                except Exception:
                    pass

        except Exception as e:
            print(f"         API {api_url[:50]}... ✗: {e}")
            continue

    return []


def strategy_list_page(driver_mgr, lat, lon, deal_type, limit,
                       radius_km, region_id, city_name):
    """Стратегия 3: Страница списка объявлений (cat.php)."""
    driver = driver_mgr.driver
    if not driver:
        return []

    print(f"         📍 Стратегия 3: список объявлений...")

    lat_min, lon_min, lat_max, lon_max = bbox(lat, lon, radius_km)

    url = (
        f"https://www.cian.ru/cat.php?"
        f"deal_type={deal_type}"
        f"&engine_version=2"
        f"&offer_type=flat"
        f"&region={region_id}"
        f"&minlat={lat_min}&maxlat={lat_max}"
        f"&minlon={lon_min}&maxlon={lon_max}"
        f"&sort=creation_date_desc&p=1"
    )

    try:
        driver.get(url)
        time.sleep(5)

        page_src = driver.page_source
        if driver_mgr.is_captcha(page_src):
            print(f"         ❌ ЦИАН: капча на списке")
            return []

        # JSON state
        results = try_json_state(driver, deal_type, limit, "cian")
        if results:
            print(f"         ✅ Список: {len(results)} из JSON state")
            return results[:limit]

        # DOM
        try:
            WebDriverWait(driver, 8).until(
                lambda d: d.find_elements(
                    By.CSS_SELECTOR,
                    '[data-name="CardComponent"], article, [class*="--card--"]'
                )
            )
        except Exception:
            time.sleep(3)

        results = parse_cards_from_dom(driver, deal_type, limit * 2, "cian")
        if results:
            print(f"         ✅ Список: {len(results)} из DOM")
            return results[:limit]

        # HTML source
        results = parse_from_page_source(driver, deal_type, limit, "cian")
        if results:
            print(f"         ✅ Список: {len(results)} из HTML")
            return results[:limit]

    except Exception as e:
        print(f"         Список ✗: {e}")

    return []


def strategy_bbox_search(driver_mgr, lat, lon, deal_type, limit,
                         radius_km, region_id, city_name):
    """Стратегия 4: Поиск по bbox без региона."""
    driver = driver_mgr.driver
    if not driver:
        return []

    print(f"         📍 Стратегия 4: bbox поиск...")

    for mult in [1.5, 3.0]:
        r = radius_km * mult
        lat_min, lon_min, lat_max, lon_max = bbox(lat, lon, r)

        url = (
            f"https://www.cian.ru/cat.php?"
            f"deal_type={deal_type}"
            f"&engine_version=2"
            f"&offer_type=flat"
            f"&minlat={lat_min}&maxlat={lat_max}"
            f"&minlon={lon_min}&maxlon={lon_max}"
            f"&sort=creation_date_desc&p=1"
        )

        try:
            driver.get(url)
            time.sleep(5)

            page_src = driver.page_source
            if driver_mgr.is_captcha(page_src):
                print(f"         ❌ ЦИАН: капча")
                return []

            results = try_json_state(driver, deal_type, limit, "cian")
            if results:
                print(f"         ✅ bbox (r={r:.1f}km): {len(results)} из JSON")
                return results[:limit]

            results = parse_cards_from_dom(driver, deal_type, limit * 2, "cian")
            if results:
                print(f"         ✅ bbox (r={r:.1f}km): {len(results)} из DOM")
                return results[:limit]

        except Exception as e:
            print(f"         bbox r={r:.1f}km ✗: {e}")
            continue

    return []


def _filter_http_results(results, lat, lon, city_name, radius_km):
    if not results:
        return []
    city = str(city_name or "").lower().replace("ё", "е").strip()
    city_tokens = [t for t in re.split(r"[\s\-]+", city) if len(t) >= 4]
    keep = []
    max_km = max(25.0, float(radius_km or 1.0) * 8.0)
    for r in results:
        ok = False
        try:
            olat = r.get("lat")
            olon = r.get("lon")
            if olat is not None and olon is not None:
                d = haversine(float(lat), float(lon), float(olat), float(olon))
                if d <= max_km:
                    ok = True
        except Exception:
            pass
        if not ok and city:
            txt = " ".join([
                str(r.get("address", "") or ""),
                str(r.get("link", "") or ""),
                str(r.get("title", "") or ""),
            ]).lower().replace("ё", "е")
            if city in txt or any(tok in txt for tok in city_tokens):
                ok = True
        if ok:
            keep.append(r)
    return keep


def strategy_http_api(lat, lon, deal_type, limit, radius_km, region_id, city_name=""):
    """HTTP API fallback без Selenium."""
    print("         📍 Стратегия HTTP API fallback...")
    lat_min, lon_min, lat_max, lon_max = bbox(lat, lon, radius_km)
    api_urls = [
        "https://api.cian.ru/search-offers/v2/search-offers-desktop/",
        "https://api.cian.ru/search-offers-index-map/v1/get-clusters-for-map/",
    ]
    payload_base = {
        "jsonQuery": {
            "_type": "flatrent" if deal_type == "rent" else "flatsale",
            "engine_version": {"type": "term", "value": 2},
            "bbox": {
                "type": "term",
                "value": {
                    "bottom_lat": lat_min,
                    "left_lng": lon_min,
                    "top_lat": lat_max,
                    "right_lng": lon_max,
                },
            },
        }
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
    }
    start_ts = time.monotonic()
    budget = _http_budget_sec()
    region_variants = [True, False]
    for api_url in api_urls:
        for with_region in region_variants:
            payload = json.loads(json.dumps(payload_base))
            if with_region and region_id:
                payload["jsonQuery"]["region"] = {"type": "terms", "value": [region_id]}
            if time.monotonic() - start_ts > budget:
                print("         ⏱️ HTTP API fallback: time budget exceeded")
                break
            try:
                resp = requests.post(api_url, headers=headers, json=payload, timeout=(6, 12))
                if resp.status_code != 200 or not resp.text:
                    continue
                data = resp.json()
                items = find_offers_recursive(data)
                if not items:
                    continue
                results = []
                for item in items[: max(limit * 2, 20)]:
                    offer = cian_api_to_offer(item, deal_type)
                    if offer:
                        results.append(offer)
                results = _filter_http_results(results, lat, lon, city_name, radius_km)
                if results:
                    mode = "region" if with_region else "no-region"
                    print(f"         ✅ HTTP API fallback ({mode}): {len(results[:limit])} объявлений")
                    return results[:limit]
            except Exception:
                continue
        if time.monotonic() - start_ts > budget:
            break
    return []


def strategy_http_list_page(lat, lon, deal_type, limit, radius_km, region_id, city_name=""):
    """HTTP fallback без Selenium: cat.php + парсинг JS state из HTML."""
    print("         📍 Стратегия HTTP fallback...")
    dt = "sale" if deal_type == "sale" else "rent"
    timeout = (6, 10)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    seen_links = set()
    collected = []
    start_ts = time.monotonic()
    budget = _http_budget_sec()
    pages_limit = min(10, max(2, int(math.ceil(max(limit, 20) / 24.0)) + 2))

    hosts = _cian_host_candidates(city_name)
    region_params = [f"&region={region_id}" if region_id else "", ""]
    for mult in (1.0, 1.8):
        if time.monotonic() - start_ts > budget:
            print("         ⏱️ HTTP fallback: time budget exceeded")
            break
        r = float(radius_km or 1.0) * mult
        lat_min, lon_min, lat_max, lon_max = bbox(lat, lon, r)
        for host in hosts:
            if time.monotonic() - start_ts > budget:
                print("         ⏱️ HTTP fallback: time budget exceeded")
                break
            for page in range(1, pages_limit + 1):
                if time.monotonic() - start_ts > budget:
                    print("         ⏱️ HTTP fallback: time budget exceeded")
                    break
                for region_arg in region_params:
                    url = (
                        f"https://{host}/cat.php?"
                        f"deal_type={dt}"
                        "&engine_version=2"
                        "&offer_type=flat"
                        f"{region_arg}"
                        f"&minlat={lat_min}&maxlat={lat_max}"
                        f"&minlon={lon_min}&maxlon={lon_max}"
                        f"&sort=creation_date_desc&p={page}"
                    )
                    try:
                        resp = requests.get(url, headers=headers, timeout=timeout)
                        if resp.status_code != 200 or not resp.text:
                            continue
                        results = parse_from_html_source(resp.text, deal_type, max(limit, 20), "cian")
                        results = _filter_http_results(results, lat, lon, city_name, radius_km)
                        if page > 1 and not results:
                            # Usually means end of list for this query.
                            break
                        for row in results:
                            row = dict(row or {})
                            row["_http_text_fallback"] = True
                            row["_query"] = q
                            row["_city_hint"] = city_name
                            row["_district_hint"] = district_name
                            link = str(row.get("link", "") or "")
                            if not link or link in seen_links:
                                continue
                            seen_links.add(link)
                            collected.append(row)
                        if len(collected) >= limit:
                            mode = "region" if region_arg else "no-region"
                            print(f"         ✅ HTTP fallback ({mode}): {len(collected[:limit])} объявлений (r={r:.1f}км, host={host})")
                            return collected[:limit]
                    except requests.exceptions.RequestException as exc:
                        msg = str(exc).lower()
                        # For network/DNS failures, don't spend extra time on bigger radius.
                        if any(x in msg for x in ("name or service not known", "failed to resolve", "nodename", "temporary failure in name resolution")):
                            break
                        continue
        if len(collected) >= limit:
            break

    if collected:
        print(f"         ✅ HTTP fallback: {len(collected[:limit])} объявлений")
        return collected[:limit]
    print("         ⚠️ HTTP fallback: офферы не найдены")
    return []


def strategy_http_text_search(deal_type, limit, region_id, city_name="", district_name=""):
    """HTTP fallback: текстовый поиск по району/городу на cat.php."""
    print("         📍 Стратегия HTTP text fallback...")
    dt = "sale" if deal_type == "sale" else "rent"
    timeout = (6, 10)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    city = str(city_name or "").strip()
    district = str(district_name or "").strip()
    district_short = re.sub(r"\bАО\b", "", district, flags=re.IGNORECASE).strip()
    queries = []
    if district and city:
        queries.append(f"{district} {city}")
        queries.append(f"район {district} {city}")
    if district_short and district_short != district and city:
        queries.append(f"{district_short} {city}")
        queries.append(f"район {district_short} {city}")
    if city:
        queries.append(city)

    hosts = _cian_host_candidates(city_name)
    seen_links = set()
    collected = []
    start_ts = time.monotonic()
    budget = _http_budget_sec()
    pages_limit = min(8, max(2, int(math.ceil(max(limit, 20) / 24.0)) + 1))
    for q in queries:
        if time.monotonic() - start_ts > budget:
            print("         ⏱️ HTTP text fallback: time budget exceeded")
            break
        for host in hosts:
            if time.monotonic() - start_ts > budget:
                print("         ⏱️ HTTP text fallback: time budget exceeded")
                break
            base_url = f"https://{host}/cat.php"
            for page in range(1, pages_limit + 1):
                if time.monotonic() - start_ts > budget:
                    print("         ⏱️ HTTP text fallback: time budget exceeded")
                    break
                for with_region in (True, False):
                    params = {
                        "deal_type": dt,
                        "engine_version": 2,
                        "offer_type": "flat",
                        "sort": "creation_date_desc",
                        "p": page,
                        "q": q,
                    }
                    if with_region and region_id:
                        params["region"] = region_id
                    try:
                        resp = requests.get(base_url, headers=headers, params=params, timeout=timeout)
                        if resp.status_code != 200 or not resp.text:
                            continue
                        results = parse_from_html_source(resp.text, deal_type, max(limit, 20), "cian")
                        results = _filter_http_results(results, 0, 0, city_name, 1000.0)
                        if page > 1 and not results:
                            break
                        for row in results:
                            link = str(row.get("link", "") or "")
                            if not link or link in seen_links:
                                continue
                            seen_links.add(link)
                            collected.append(row)
                        if len(collected) >= limit:
                            mode = "region" if with_region else "no-region"
                            print(f"         ✅ HTTP text fallback ({mode}): {len(collected[:limit])} объявлений (q={q}, host={host})")
                            return collected[:limit]
                    except requests.exceptions.RequestException:
                        continue

    if collected:
        print(f"         ✅ HTTP text fallback: {len(collected[:limit])} объявлений")
        return collected[:limit]

    print("         ⚠️ HTTP text fallback: офферы не найдены")
    return []


# ─── Вспомогательные функции стратегий ───

def _extract_state_clusters(driver):
    """Извлечение кластеров из __INITIAL_STATE__."""
    state_scripts = [
        "return JSON.stringify(window.__initialState__ || null)",
        "return JSON.stringify(window.__INITIAL_STATE__ || null)",
    ]

    for script in state_scripts:
        try:
            raw = driver.execute_script(script)
            if not raw or raw == "null":
                continue

            state = json.loads(raw)
            if not isinstance(state, dict):
                continue

            for path in [
                ["search", "map", "clusters"],
                ["map", "clusters"],
                ["results", "clusters"],
                ["searchResults", "clusters"],
                ["mapClusters"],
                ["clusters"],
            ]:
                obj = state
                for key in path:
                    if isinstance(obj, dict) and key in obj:
                        obj = obj[key]
                    else:
                        obj = None
                        break
                if obj and isinstance(obj, list) and len(obj) > 0:
                    return json.dumps({"filtered": obj})

            found = find_clusters_recursive(state)
            if found:
                return json.dumps({"filtered": found})

        except Exception:
            continue

    return None


def _process_clusters(driver_mgr, clusters, lat, lon, deal_type,
                      limit, radius_km, city_name):
    """Обработка кластеров: сортировка, загрузка деталей."""
    driver = driver_mgr.driver

    # Дистанция
    for cluster in clusters:
        coords = cluster.get("coordinates", {})
        clat = coords.get("lat", 0)
        clng = coords.get("lng", 0)
        cluster["_dist"] = haversine(lat, lon, clat, clng)

    clusters.sort(key=lambda c: c["_dist"])
    nearby = [c for c in clusters if c["_dist"] <= radius_km]
    if not nearby:
        nearby = clusters[:5]

    print(
        f"         ЦИАН: {len(nearby)} кластеров в радиусе "
        f"{radius_km}км (всего {len(clusters)})"
    )

    # Собираем ID
    offer_ids = []
    for cluster in nearby:
        ids = cluster.get("clusterOfferIds", [])
        offer_ids.extend(ids)
        if len(offer_ids) >= limit * 3:
            break

    seen = set()
    unique_ids = []
    for oid in offer_ids:
        if oid not in seen:
            seen.add(oid)
            unique_ids.append(oid)

    if not unique_ids:
        return _clusters_to_offers(nearby, deal_type, limit)

    print(f"         ЦИАН: {len(unique_ids)} уникальных ID")

    fetch_count = min(limit + 2, len(unique_ids))
    results = _quick_offers_from_clusters(
        driver_mgr, nearby, unique_ids, deal_type, fetch_count, city_name
    )

    if results:
        print(f"         ЦИАН: {len(results)} объявлений")
        return results

    return _clusters_to_offers(nearby, deal_type, limit)


def _quick_offers_from_clusters(driver_mgr, clusters, offer_ids,
                                deal_type, limit, city_name=""):
    """Быстрая загрузка офферов по ID из кластеров."""
    dt = "sale" if deal_type == "sale" else "rent"
    driver = driver_mgr.driver
    results = []
    min_valid = 8_000 if deal_type == "rent" else 500_000

    region_id = detect_cian_region(city_name)
    if driver and region_id and offer_ids:
        try:
            subdomain = (
                clusters[0].get("subdomain", "www") if clusters else "www"
            )

            # Old hard cap (4) caused chronically short output.
            # Keep bounded to avoid very slow page-by-page scraping.
            max_pages = min(limit, 18)
            for offer_id in offer_ids[:max_pages]:
                try:
                    driver.execute_script("window._cianClusters = null;")
                    url = f"https://{subdomain}.cian.ru/{dt}/flat/{offer_id}/"
                    driver.get(url)
                    time.sleep(3)

                    page_src = driver.page_source
                    if len(page_src) < 5000:
                        continue
                    if driver_mgr.is_captcha(page_src):
                        print(f"         ⚠️ Капча, останавливаем")
                        break

                    detail = _parse_single_offer_page(
                        driver, offer_id, deal_type
                    )
                    if detail and detail.get("price", 0) >= min_valid:
                        detail['address'] = clean_address(
                            detail.get('address', '')
                        )
                        results.append(detail)

                except Exception as e:
                    if "DISCONNECTED" in str(e):
                        driver = driver_mgr.restart_driver()
                        if not driver:
                            break
                    continue

        except Exception:
            pass

    if len(results) >= limit:
        return results[:limit]

    # Фоллбэк из кластеров
    needed = limit - len(results)
    if needed <= 0:
        return results[:limit]

    used_ids = set()
    for r in results:
        link = r.get("link", "")
        parts = link.rstrip("/").split("/")
        if parts:
            used_ids.add(parts[-1])

    cluster_fallbacks = []
    for cluster in clusters:
        if len(cluster_fallbacks) >= needed:
            break

        min_price = int(cluster.get("minPrice", 0) or 0)
        max_price = int(cluster.get("maxPrice", 0) or 0)
        subdomain = cluster.get("subdomain", "www")
        cluster_ids = cluster.get("clusterOfferIds", [])

        if min_price < min_valid:
            continue

        for i, cid in enumerate(cluster_ids):
            if len(cluster_fallbacks) >= needed:
                break
            if str(cid) in used_ids:
                continue
            used_ids.add(str(cid))

            link = f"https://{subdomain}.cian.ru/{dt}/flat/{cid}/"

            if len(cluster_ids) > 1 and max_price > min_price:
                step = (max_price - min_price) / len(cluster_ids)
                price = int(min_price + step * i)
            else:
                price = min_price

            if price < min_valid:
                continue

            address = city_name if city_name else ""

            cluster_fallbacks.append({
                "source": "cian",
                "deal_type": deal_type,
                "price": price,
                "rooms": "?",
                "area": 0,
                "floor": "",
                "address": address,
                "photo": "",
                "photos": [],
                "link": link,
                "title": "Квартира",
                "_from_cluster": True,
            })

    if len(results) < 2 and cluster_fallbacks:
        results.extend(cluster_fallbacks)

    return results[:limit]


def _clusters_to_offers(clusters, deal_type, limit):
    """Конвертация кластеров в офферы (фоллбэк)."""
    dt = "sale" if deal_type == "sale" else "rent"
    results = []
    min_valid = 8_000 if deal_type == "rent" else 500_000
    used_ids = set()

    for cluster in clusters:
        if len(results) >= limit:
            break

        min_price = int(cluster.get("minPrice", 0) or 0)
        max_price = int(cluster.get("maxPrice", 0) or 0)
        subdomain = cluster.get("subdomain", "www")
        cluster_ids = cluster.get("clusterOfferIds", [])

        if min_price < min_valid:
            continue

        for i, cid in enumerate(cluster_ids):
            if len(results) >= limit:
                break
            if str(cid) in used_ids:
                continue
            used_ids.add(str(cid))

            link = f"https://{subdomain}.cian.ru/{dt}/flat/{cid}/"

            if len(cluster_ids) > 1 and max_price > min_price:
                step = (max_price - min_price) / len(cluster_ids)
                price = int(min_price + step * i)
            else:
                price = min_price

            if price < min_valid:
                continue

            results.append({
                "source": "cian",
                "deal_type": deal_type,
                "price": price,
                "rooms": "?",
                "area": 0,
                "floor": "",
                "address": "Смотреть на ЦИАН →",
                "photo": "",
                "photos": [],
                "link": link,
                "title": "Квартира",
                "_from_cluster": True,
            })

    return results[:limit]


def _parse_single_offer_page(driver, offer_id, deal_type):
    """Парсинг одной страницы объявления ЦИАН."""
    try:
        # Способ 1: JSON state
        for var in ["window.__INITIAL_STATE__", "window.__DATA__"]:
            try:
                raw = driver.execute_script(
                    f"return JSON.stringify({var} || null)"
                )
                if raw and raw != "null":
                    state = json.loads(raw)
                    if isinstance(state, dict):
                        offer_data = (
                            state.get("offerData", {}).get("offer", {}) or
                            state.get("offer", {}) or
                            {}
                        )
                        if offer_data:
                            result = cian_api_to_offer(offer_data, deal_type)
                            if result:
                                return result
            except Exception:
                continue

        # Способ 2: парсим текст страницы
        text = driver.find_element(By.TAG_NAME, "body").text
        rent_period = _extract_page_rent_period(text) if deal_type == "rent" else ""

        # Цена
        price = _extract_page_price(driver, text, deal_type, rent_period=rent_period)

        # Комнаты
        rooms = "?"
        m = re.search(r'(\d)[- ]?комн', text, re.I)
        if m:
            rooms = m.group(1)
        elif 'студи' in text.lower():
            rooms = "студия"

        # Площадь
        area = 0
        m = re.search(r'([\d,\.]+)\s*м[²2]', text)
        if m:
            try:
                area = float(m.group(1).replace(',', '.'))
            except ValueError:
                pass

        # Этаж
        floor = ""
        m = re.search(r'(\d+)\s*/\s*(\d+)\s*эт', text, re.I)
        if m:
            floor = f"{m.group(1)}/{m.group(2)}"

        # Адрес
        address = _extract_page_address(driver, text)

        # Фото
        photos = _extract_page_photos(driver)

        if not price and not area:
            return None

        dt = "sale" if deal_type == "sale" else "rent"
        result = {
            "source": "cian",
            "deal_type": deal_type,
            "price": price,
            "rooms": rooms,
            "area": round(area, 1) if area else 0,
            "floor": floor,
            "address": address,
            "photo": photos[0] if photos else "",
            "photos": photos,
            "link": f"https://www.cian.ru/{dt}/flat/{offer_id}/",
            "title": make_title(rooms, area),
        }
        if rent_period:
            result["rent_period"] = rent_period
        return result

    except Exception:
        return None


def _extract_page_price(driver, text, deal_type, rent_period=""):
    """Извлечение цены со страницы объявления."""
    price = 0
    if deal_type == "rent" and rent_period == "day":
        min_valid = 1_000
    else:
        min_valid = 8_000 if deal_type == "rent" else 500_000
    max_valid = 1_000_000 if deal_type == "rent" else 500_000_000

    try:
        price_script = """
            try {
                var s = window.__INITIAL_STATE__ || window.__DATA__ || {};
                var o = s.offerData && s.offerData.offer
                    ? s.offerData.offer : (s.offer || {});
                var bt = o.bargainTerms || o.priceInfo || {};
                return bt.price || bt.priceRur || o.price || 0;
            } catch(e) { return 0; }
        """
        js_price = driver.execute_script(price_script)
        if js_price and int(js_price) >= min_valid:
            price = int(js_price)
    except Exception:
        pass

    if not price and deal_type == "rent":
        # Посуточная аренда
        daily_matches = re.findall(
            r'([\d\s\xa0]+)\s*₽\s*(?:/\s*сут(?:ки)?|в\s*сутки|за\s*сутки|/\s*день|в\s*день)',
            text, re.I
        )
        for match in daily_matches:
            try:
                val = int(match.replace(' ', '').replace('\xa0', ''))
                if 1_000 <= val <= max_valid:
                    price = val
                    break
            except ValueError:
                pass

    if not price and deal_type == "rent":
        rent_matches = re.findall(
            r'([\d\s\xa0]+)\s*₽\s*(?:/\s*мес|в\s*месяц)', text
        )
        for match in rent_matches:
            try:
                val = int(match.replace(' ', '').replace('\xa0', ''))
                if min_valid <= val <= max_valid:
                    price = val
                    break
            except ValueError:
                pass

    if not price:
        price_matches = re.findall(r'([\d\s\xa0]+)\s*₽', text)
        for match in price_matches:
            try:
                val = int(match.replace(' ', '').replace('\xa0', ''))
                if min_valid <= val <= max_valid:
                    price = val
                    break
            except ValueError:
                pass

    if not price and deal_type == "sale":
        m = re.search(r'([\d,\.]+)\s*млн', text)
        if m:
            try:
                price = int(float(m.group(1).replace(',', '.')) * 1_000_000)
            except ValueError:
                pass

    if not price and deal_type == "sale":
        for line in text.split('\n'):
            clean = line.strip().replace(' ', '').replace('\xa0', '')
            digits = re.sub(r'[^\d]', '', clean)
            if digits and len(digits) >= 7:
                try:
                    val = int(digits)
                    if 500_000 <= val <= 500_000_000:
                        price = val
                        break
                except ValueError:
                    pass

    return price


def _extract_page_rent_period(text):
    """Определение периода аренды по тексту страницы."""
    lo = (text or "").lower()
    if any(x in lo for x in ("посуточ", "/сут", "в сутки", "за сутки", "/день", "в день")):
        return "day"
    if any(x in lo for x in ("/мес", "в месяц", "за месяц")):
        return "month"
    return ""


def _extract_page_address(driver, text):
    """Извлечение адреса со страницы объявления."""
    address = ""

    try:
        addr_els = driver.find_elements(
            By.CSS_SELECTOR,
            '[data-name="Geo"] span, '
            '[data-name="AddressItem"], '
            '[data-name="OfferTitle"] + div span, '
            'address, '
            '[class*="address" i], '
            '[class*="geo" i] span'
        )
        for el in addr_els:
            t = el.text.strip()
            if (t and len(t) > 10
                    and 'Продажа' not in t
                    and 'Недвижимость' not in t):
                address = t[:80]
                break
    except Exception:
        pass

    if not address:
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        addr_markers = [
            r'ул\.', r'улица\s', r'пр\.', r'пр-кт', r'пер\.',
            r'наб\.', r'бул\.', r'ш\.', r'просп', r'переул',
            r'мкр\.', r'мкр\s', r'микрорайон', r'корп\.',
            r'д\.\s*\d', r'стр\.\s*\d',
        ]
        skip_words = [
            'Продажа', 'Недвижимость', 'Аренда', 'Купить',
            'Снять', 'квартир', 'комнат', 'Показать',
            'Объявлен', 'Подпис', 'Избран',
        ]
        for line in lines:
            if any(sw in line for sw in skip_words):
                continue
            if any(re.search(p, line, re.I) for p in addr_markers):
                clean = clean_address(line)
                if len(clean) > 10:
                    address = clean[:80]
                    break

    if not address:
        try:
            title_text = driver.title
            m = re.search(
                r'(?:м²|м2)[,\s]+(.+?)(?:\s*[-–|]|\s*$)', title_text
            )
            if m:
                addr_part = m.group(1).strip()
                if (addr_part
                        and 'Циан' not in addr_part
                        and 'ЦИАН' not in addr_part):
                    address = addr_part[:80]
        except Exception:
            pass

    return clean_address(address)


def _extract_page_photos(driver):
    """Извлечение фото со страницы объявления."""
    photos = []
    try:
        imgs = driver.find_elements(
            By.CSS_SELECTOR,
            'img[src*="cdn-p.cian"], img[src*="images.cdn-cian"]'
        )
        seen_src = set()
        for img in imgs:
            src = img.get_attribute("src") or ""
            if (src and src.startswith("http")
                    and "logo" not in src.lower()
                    and "avatar" not in src.lower()
                    and "icon" not in src.lower()
                    and "static" not in src.lower()
                    and src not in seen_src):
                seen_src.add(src)
                photos.append(src)
            if len(photos) >= 10:
                break
    except Exception:
        pass
    return photos
