"""Управление Chrome WebDriver."""

import os
import shutil
import subprocess
import time
import socket
import glob
import re
import threading

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        HAS_WDM = True
    except Exception:
        HAS_WDM = False
    HAS_SELENIUM = True
except ImportError as e:
    HAS_SELENIUM = False
    HAS_WDM = False
    print(f"⚠️ Selenium не доступен: {e}")

from .realty_constants import INTERCEPT_SCRIPT


class DriverManager:
    """Управление жизненным циклом Chrome WebDriver."""

    def __init__(self):
        self._driver = None
        self._driver_failed = False
        self._last_failure_ts = 0.0
        self._startup_timeout_sec = int(os.environ.get("REALTY_DRIVER_STARTUP_TIMEOUT_SEC", "12"))
        self._retry_cooldown_sec = int(os.environ.get("REALTY_DRIVER_RETRY_COOLDOWN_SEC", "45"))
        self._max_candidates = int(os.environ.get("REALTY_DRIVER_MAX_CANDIDATES", "2"))

    @property
    def driver(self):
        return self._driver

    def _detect_chrome_binary(self):
        candidates = [
            os.environ.get("CHROME_BINARY"),
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
            "/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
        ]
        for path in candidates:
            if path and os.path.exists(path):
                return path
        return None

    def _discover_local_chromedrivers(self):
        candidates = []
        chrome_major = self._chrome_major_version()

        env_driver = os.environ.get("CHROMEDRIVER")
        if env_driver:
            candidates.append(("env", env_driver))

        path_driver = shutil.which("chromedriver")
        if path_driver:
            candidates.append(("PATH", path_driver))

        # Common local install locations on macOS
        for p in (
            "/opt/homebrew/bin/chromedriver",
            "/usr/local/bin/chromedriver",
        ):
            if os.path.exists(p):
                candidates.append(("system", p))

        # Auto mode: if explicit flag not set, include wdm cache when local PATH/system
        # driver major doesn't match installed Chrome major.
        use_wdm_cache_raw = str(os.environ.get("REALTY_USE_WDM_CACHE", "auto")).strip().lower()
        if use_wdm_cache_raw in {"1", "true", "yes"}:
            use_wdm_cache = True
        elif use_wdm_cache_raw in {"0", "false", "no"}:
            use_wdm_cache = False
        else:
            # auto
            local_major_match = False
            for _, path in candidates:
                dm = self._driver_major_from_path(path)
                if chrome_major is not None and dm is not None and dm == chrome_major:
                    local_major_match = True
                    break
            use_wdm_cache = not local_major_match
        if use_wdm_cache:
            # webdriver_manager cache may contain multiple versions; optional because
            # stale binaries from this folder often slow down startup.
            cache_globs = [
                os.path.expanduser("~/.wdm/drivers/chromedriver/**/chromedriver"),
                os.path.expanduser("~/.wdm/drivers/chromedriver/**/chromedriver.exe"),
            ]
            cached_paths = []
            for pattern in cache_globs:
                cached_paths.extend(glob.glob(pattern, recursive=True))
            cached_paths = [p for p in cached_paths if os.path.exists(p)]
            def _score(path):
                base = os.path.getmtime(path)
                drv_major = self._driver_major_from_path(path)
                major_bonus = 0
                if chrome_major is not None and drv_major is not None and chrome_major == drv_major:
                    major_bonus = 1_000_000_000
                return major_bonus + base

            cached_paths.sort(key=_score, reverse=True)
            for p in cached_paths[:5]:
                candidates.append(("wdm-cache", p))

        dedup = []
        seen = set()
        for label, path in candidates:
            if not path or path in seen:
                continue
            seen.add(path)
            dedup.append((label, path))

        # Prefer driver matching current Chrome major version regardless of source.
        def _all_score(item):
            _, path = item
            drv_major = self._driver_major_from_path(path)
            major_bonus = 0
            if chrome_major is not None and drv_major is not None and drv_major == chrome_major:
                major_bonus = 1_000_000_000
            try:
                mtime = int(os.path.getmtime(path))
            except Exception:
                mtime = 0
            return major_bonus + mtime

        dedup.sort(key=_all_score, reverse=True)
        return dedup

    def _chrome_major_version(self):
        chrome = self._detect_chrome_binary()
        if not chrome:
            return None
        try:
            proc = subprocess.run([chrome, "--version"], capture_output=True, text=True, timeout=5)
            txt = (proc.stdout + proc.stderr).strip()
            m = re.search(r"(\d+)\.", txt)
            if m:
                return int(m.group(1))
        except Exception:
            return None
        return None

    def _driver_major_from_path(self, path):
        try:
            m = re.search(r"/(\d+)\.\d+\.\d+\.\d+/", path.replace("\\", "/"))
            if m:
                return int(m.group(1))
        except Exception:
            return None
        return None

    def _driver_binary_healthy(self, path):
        if not path or not os.path.exists(path):
            return False
        if not os.access(path, os.X_OK):
            return False
        # Important: avoid running `chromedriver --version` here. On some macOS
        # setups this call can block indefinitely (codesign/quarantine prompt),
        # freezing the whole parsing flow. Real startup health is checked in
        # _start_chrome() with a strict watchdog timeout.
        try:
            st = os.stat(path)
            return st.st_size > 1_000_000
        except Exception:
            return False

    def _find_free_port(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.bind(("127.0.0.1", 0))
                return int(sock.getsockname()[1])
        except Exception:
            return None

    def _service_ports(self):
        dynamic = self._find_free_port()
        if dynamic:
            return [dynamic]
        return [9515]

    def _start_chrome(self, options, driver_path=None, label="selenium-manager"):
        last_exc = None
        for port in self._service_ports():
            try:
                if driver_path:
                    service = Service(executable_path=driver_path, port=port)
                else:
                    service = Service(port=port)
                box = {"driver": None, "error": None}

                def _boot():
                    try:
                        box["driver"] = webdriver.Chrome(service=service, options=options)
                    except Exception as exc:
                        box["error"] = exc

                t = threading.Thread(target=_boot, daemon=True)
                t.start()
                t.join(timeout=max(3, self._startup_timeout_sec))
                if t.is_alive():
                    last_exc = TimeoutError(
                        f"webdriver startup timeout ({self._startup_timeout_sec}s)"
                    )
                    continue
                if box["error"] is not None:
                    last_exc = box["error"]
                    continue
                driver = box["driver"]
                if not driver:
                    last_exc = RuntimeError("webdriver returned empty driver")
                    continue

                driver.set_page_load_timeout(60)
                print(f"      ✅ Chrome запущен ({label}, port={port})")
                return driver
            except Exception as exc:
                last_exc = exc
                continue
        print(f"      ⚠️ Не удалось запустить Chrome через {label}: {last_exc}")
        return None

    def get_driver(self):
        """Получить (или создать) экземпляр WebDriver."""
        if self._driver is not None:
            try:
                self._driver.current_url
                return self._driver
            except Exception:
                self._driver = None

        if not HAS_SELENIUM:
            return None

        # If Chrome startup failed recently, don't block every request with retries.
        if self._driver_failed and self._last_failure_ts > 0:
            elapsed = time.time() - self._last_failure_ts
            if elapsed < self._retry_cooldown_sec:
                left = int(self._retry_cooldown_sec - elapsed)
                print(
                    "      ⏭️ Пропускаю запуск Chrome: cooldown после ошибки "
                    f"({left}s)"
                )
                return None

        try:
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-first-run")
            options.add_argument("--no-default-browser-check")
            options.add_argument("--disable-background-networking")
            options.add_argument("--disable-background-timer-throttling")
            options.add_argument("--disable-renderer-backgrounding")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument(
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            )
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)
            options.page_load_strategy = 'eager'
            chrome_binary = self._detect_chrome_binary()
            if chrome_binary:
                options.binary_location = chrome_binary

            # By default we avoid network-dependent driver resolution because it can
            # hang in restricted environments. Enable explicitly if needed:
            # REALTY_ALLOW_REMOTE_DRIVER=1
            allow_remote = str(os.environ.get("REALTY_ALLOW_REMOTE_DRIVER", "")).strip() in {"1", "true", "yes"}

            driver_candidates = self._discover_local_chromedrivers()
            if allow_remote and HAS_WDM:
                try:
                    cached_driver = ChromeDriverManager().install()
                    driver_candidates.append(("webdriver-manager", cached_driver))
                except Exception as exc:
                    print(f"      ⚠️ webdriver_manager не подготовил драйвер: {exc}")

            tried_paths = set()
            for label, path in driver_candidates[:max(1, self._max_candidates)]:
                if not path or path in tried_paths:
                    continue
                tried_paths.add(path)
                print(f"      🔎 Проверяю chromedriver ({label}): {path}")
                if not self._driver_binary_healthy(path):
                    print(f"      ⚠️ Пропускаю неисправный chromedriver ({label}): {path}")
                    continue
                self._driver = self._start_chrome(
                    options=options,
                    driver_path=path,
                    label=f"{label}: {path}",
                )
                if self._driver:
                    break

            if self._driver is None and allow_remote:
                self._driver = self._start_chrome(
                    options=options,
                    driver_path=None,
                    label="selenium-manager",
                )
            if self._driver is None:
                raise RuntimeError("ChromeDriver service could not be started")

            # Антидетект
            self._driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {"source": """
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    window.chrome = { runtime: {} };
                """}
            )

            # Перехватчик для всех последующих страниц
            self._driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {"source": INTERCEPT_SCRIPT}
            )

            self._driver_failed = False
            self._last_failure_ts = 0.0
            return self._driver

        except Exception as e:
            print(f"      ❌ Chrome не запустился: {e}")
            self._driver_failed = True
            self._last_failure_ts = time.time()
            return None

    def restart_driver(self):
        """Перезапуск Chrome."""
        if self._driver_failed and self._last_failure_ts > 0:
            elapsed = time.time() - self._last_failure_ts
            if elapsed < self._retry_cooldown_sec:
                return None
        print("      🔄 Перезапуск Chrome...")
        try:
            if self._driver:
                self._driver.quit()
        except Exception:
            pass
        self._driver = None
        return self.get_driver()

    def close(self):
        """Закрытие драйвера."""
        if self._driver:
            try:
                self._driver.quit()
            except Exception:
                pass
            self._driver = None

    def is_alive(self):
        """Проверка, жив ли драйвер."""
        if self._driver is None:
            return False
        try:
            self._driver.current_url
            return True
        except Exception:
            return False

    def is_captcha(self, page_src):
        """Проверка на капчу."""
        if not page_src:
            return False
        lower = page_src.lower()
        if len(page_src) < 30000:
            if "captcha" in lower or "я не робот" in lower or "recaptcha" in lower:
                return True
        return False

    def wait_for_clusters(self, max_wait=15):
        """Ожидание перехвата данных кластеров."""
        driver = self._driver
        if not driver:
            return None

        start = time.time()
        while time.time() - start < max_wait:
            try:
                raw = driver.execute_script("return window._cianClusters")
                if raw and len(raw) > 50:
                    return raw
            except Exception:
                break
            time.sleep(1)
        return None
