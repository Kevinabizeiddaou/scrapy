"""Typed, environment-driven configuration.

Every value here is overridable via a ``WRC_``-prefixed environment variable so the
same code runs against local MinIO and against AWS S3 without edits.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

PartitionSize = Literal["daily", "weekly", "monthly", "yearly"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="WRC_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- target site -----------------------------------------------------------
    source: str = "workplacerelations.ie"
    base_url: str = "https://www.workplacerelations.ie"
    search_path: str = "/en/search/"
    advanced_search_path: str = "/en/search/?advance=true"
    results_per_page: int = 10

    # --- partitioning ----------------------------------------------------------
    partition_size: PartitionSize = "monthly"

    # --- MongoDB ---------------------------------------------------------------
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_database: str = "wrc_landing"
    mongo_landing_collection: str = "landing_documents"
    mongo_state_collection: str = "landing_state"

    # --- S3 / MinIO ------------------------------------------------------------
    s3_endpoint_url: str | None = "http://localhost:9000"
    s3_region: str = "us-east-1"
    # Unset by default so boto3 falls back to the standard AWS credential chain
    # (env vars, shared config, instance/IRSA roles). Local MinIO values live in .env.
    s3_access_key_id: SecretStr | None = None
    s3_secret_access_key: SecretStr | None = None
    landing_bucket: str = "wrc-landing"
    s3_create_bucket: bool = True

    # --- Scrapy tuning ---------------------------------------------------------
    concurrent_requests: int = 8
    concurrent_requests_per_domain: int = 4
    download_delay: float = 0.25
    request_timeout: int = 60
    retry_times: int = 4
    autothrottle_enabled: bool = True
    autothrottle_target_concurrency: float = 2.0
    autothrottle_max_delay: float = 30.0
    robotstxt_obey: bool = True
    user_agent: str = (
        "wrc-pipeline/0.1 (+https://github.com/Kevinabizeiddaou/scrapy; research/archival)"
    )

    # --- logging ---------------------------------------------------------------
    log_level: str = "INFO"

    @property
    def search_url(self) -> str:
        return f"{self.base_url.rstrip('/')}{self.search_path}"

    @property
    def advanced_search_url(self) -> str:
        return f"{self.base_url.rstrip('/')}{self.advanced_search_path}"

    def boto3_client_kwargs(self) -> dict[str, object]:
        kwargs: dict[str, object] = {"region_name": self.s3_region}
        if self.s3_access_key_id and self.s3_secret_access_key:
            kwargs["aws_access_key_id"] = self.s3_access_key_id.get_secret_value()
            kwargs["aws_secret_access_key"] = self.s3_secret_access_key.get_secret_value()
        # Unset for real AWS S3; set for MinIO and other S3-compatible endpoints.
        if self.s3_endpoint_url:
            kwargs["endpoint_url"] = self.s3_endpoint_url
        return kwargs


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


# Explicit re-export so ruff does not flag the alias as unused.
__all__ = ["PartitionSize", "Settings", "get_settings"]
