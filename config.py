"""Application configuration management."""

from functools import lru_cache
from typing import Optional

from pydantic import Field, HttpUrl, PostgresDsn, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    telegram_bot_token: str = Field(alias="TELEGRAM_BOT_TOKEN")
    database_url: PostgresDsn = Field(alias="DATABASE_URL")
    webhook_url: Optional[HttpUrl] = Field(default=None, alias="WEBHOOK_URL")
    port: int = Field(default=8000, alias="PORT", ge=1, le=65535)
    pool_min_size: int = Field(default=1, alias="POOL_MIN_SIZE", ge=1)
    pool_max_size: int = Field(default=10, alias="POOL_MAX_SIZE", ge=1)

    @model_validator(mode="after")
    def validate_pool_limits(self) -> "Settings":
        if self.pool_min_size > self.pool_max_size:
            msg = "POOL_MIN_SIZE cannot be greater than POOL_MAX_SIZE"
            raise ValueError(msg)
        return self


@lru_cache
def get_settings() -> Settings:
    """Return a cached instance of application settings."""

    return Settings()

