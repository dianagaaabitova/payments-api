from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    database_url: str
    rabbitmq_url: str
    api_key: SecretStr = Field(min_length=16)
    webhook_signing_secret: SecretStr = Field(min_length=32)
    webhook_allowed_hosts: frozenset[str] = frozenset()
    webhook_timeout: float = Field(default=5, gt=0, le=30)
    retry_base_delay: float = Field(default=2, gt=0, le=60)


@lru_cache
def get_settings() -> Settings:
    return Settings()
