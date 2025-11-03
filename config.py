"""Простая конфигурация приложения без внешних зависимостей."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from typing import Optional


@dataclass(frozen=True)
class Settings:
    """Настройки приложения, полученные из переменных окружения."""

    port: int
    bonus_data_file: Optional[str]
    default_expiry_days: int


@lru_cache
def get_settings() -> Settings:
    """Вернуть кэшированный экземпляр настроек."""

    port_str = os.getenv("PORT", "8000")
    try:
        port = int(port_str)
    except ValueError:
        port = 8000

    file_path = os.getenv("BONUS_DATA_FILE")

    expiry_days_str = os.getenv("DEFAULT_EXPIRY_DAYS", "365")
    try:
        expiry_days = int(expiry_days_str)
    except ValueError:
        expiry_days = 365

    return Settings(port=port, bonus_data_file=file_path, default_expiry_days=expiry_days)
