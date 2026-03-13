"""Shared configuration schema and environment variable loading."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    vietmap_api_key: str = ""
    dry_run: bool = False
    routes_file: str = "routes.json"

    model_config = {"env_file": ".env", "case_sensitive": False, "extra": "ignore"}


settings = Settings()
