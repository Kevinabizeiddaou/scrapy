"""Typed, environment-driven configuration.

Every value here is overridable via a ``WRC_``-prefixed environment variable so the
same code runs against local MinIO and against AWS S3 without edits.

Numeric and name fields carry bounds because a bad value would otherwise surface far from
its cause -- ``WRC_CONCURRENT_REQUESTS=0`` stalls Scrapy, an empty bucket name fails deep
inside boto3. Startup is the right place to reject them.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PartitionSize = Literal["daily", "weekly", "monthly", "yearly"]
LOG_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")

Name = Annotated[str, Field(min_length=1)]
Positive = Annotated[int, Field(ge=1)]
NonNegative = Annotated[float, Field(ge=0)]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="WRC_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- target site -----------------------------------------------------------
    source: Name = "workplacerelations.ie"
    base_url: Name = "https://www.workplacerelations.ie"
    search_path: Name = "/en/search/"
    advanced_search_path: Name = "/en/search/?advance=true"
    # Fallback only: the real page size is read from each partition's first results page.
    results_per_page: Positive = 10

    # --- partitioning ----------------------------------------------------------
    partition_size: PartitionSize = "monthly"

    # --- MongoDB ---------------------------------------------------------------
    mongo_uri: Name = "mongodb://localhost:27017"
    mongo_database: Name = "wrc_landing"
    mongo_landing_collection: Name = "landing_documents"
    mongo_state_collection: Name = "landing_state"
    mongo_transformed_collection: Name = "transformed_documents"

    # --- S3 / MinIO ------------------------------------------------------------
    s3_endpoint_url: str | None = "http://localhost:9000"
    s3_region: Name = "us-east-1"
    # Unset by default so boto3 falls back to the standard AWS credential chain
    # (env vars, shared config, instance/IRSA roles). Local MinIO values live in .env.
    s3_access_key_id: SecretStr | None = None
    s3_secret_access_key: SecretStr | None = None
    landing_bucket: Name = "wrc-landing"
    s3_transformed_bucket: Name = "wrc-transformed"
    s3_create_bucket: bool = True

    # --- Scrapy tuning ---------------------------------------------------------
    concurrent_requests: Positive = 8
    concurrent_requests_per_domain: Positive = 4
    download_delay: NonNegative = 0.25
    request_timeout: Positive = 60
    retry_times: Annotated[int, Field(ge=0)] = 4  # 0 disables retries
    autothrottle_enabled: bool = True
    autothrottle_target_concurrency: Annotated[float, Field(gt=0)] = 2.0
    autothrottle_max_delay: NonNegative = 30.0
    robotstxt_obey: bool = True
    user_agent: Name = (
        "wrc-pipeline/0.1 (+https://github.com/Kevinabizeiddaou/scrapy; research/archival)"
    )

    # --- logging ---------------------------------------------------------------
    log_level: Name = "INFO"

    @field_validator("log_level")
    @classmethod
    def _known_log_level(cls, value: str) -> str:
        level = value.strip().upper()
        if level not in LOG_LEVELS:
            raise ValueError(f"log_level must be one of {LOG_LEVELS}, got {value!r}")
        return level

    @model_validator(mode="after")
    def _separate_zones(self) -> Settings:
        """The transformed zone must never share the immutable landing zone's storage."""
        if self.s3_transformed_bucket == self.landing_bucket:
            raise ValueError(
                "s3_transformed_bucket must differ from landing_bucket; the landing zone "
                "is immutable and must not receive transformed objects"
            )
        if self.mongo_transformed_collection in (
            self.mongo_landing_collection,
            self.mongo_state_collection,
        ):
            raise ValueError(
                "mongo_transformed_collection must differ from the landing and state collections"
            )
        return self

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


__all__ = ["LOG_LEVELS", "PartitionSize", "Settings", "get_settings"]
