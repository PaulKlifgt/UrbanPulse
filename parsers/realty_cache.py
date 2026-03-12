
"""Кэширование результатов парсинга."""

import json
import os
import hashlib
from datetime import datetime, timedelta


class RealtyCache:

    def __init__(self, cache_dir="data/_realty_cache", cache_hours=6):
        self.cache_dir = cache_dir
        self.cache_hours = cache_hours
        os.makedirs(cache_dir, exist_ok=True)

    def make_key(self, **kw) -> str:
        """Генерация ключа кэша."""
        return hashlib.md5(
            json.dumps(kw, sort_keys=True).encode()
        ).hexdigest()

    def get(self, key):
        """Получение данных из кэша. Возвращает None если нет или устарели."""
        path = os.path.join(self.cache_dir, f"{key}.json")
        if not os.path.exists(path):
            return None
        try:
            age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(path))
            if age > timedelta(hours=self.cache_hours):
                return None
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list) and len(data) > 0:
                return data
        except Exception:
            pass
        return None

    def set(self, key, data):
        """Сохранение данных в кэш."""
        try:
            path = os.path.join(self.cache_dir, f"{key}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data if data else [], f, ensure_ascii=False, indent=2)
        except Exception:
            pass