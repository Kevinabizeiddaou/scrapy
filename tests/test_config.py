"""Configuration validation and the Scrapy settings derived from it.

A bad configuration value must be rejected at startup, not surface later as a stalled
crawl or an obscure boto3 error.
"""

from __future__ import annotations

from importlib import reload

import pytest
from pydantic import ValidationError

import wrc_pipeline.config as config_module
from wrc_pipeline.config import LOG_LEVELS, Settings


def settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, **overrides)  # type: ignore[arg-type]


# --- rejected values ------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        {"concurrent_requests": 0},
        {"concurrent_requests": -5},
        {"concurrent_requests_per_domain": 0},
        {"request_timeout": 0},
        {"results_per_page": 0},
        {"results_per_page": -10},
        {"retry_times": -1},
        {"download_delay": -0.5},
        {"autothrottle_target_concurrency": 0.0},
        {"autothrottle_max_delay": -1.0},
    ],
)
def test_nonsense_tuning_values_are_rejected(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        settings(**overrides)


@pytest.mark.parametrize(
    "field",
    [
        "source",
        "base_url",
        "mongo_uri",
        "mongo_database",
        "mongo_landing_collection",
        "mongo_state_collection",
        "mongo_transformed_collection",
        "landing_bucket",
        "s3_transformed_bucket",
        "s3_region",
        "user_agent",
    ],
)
def test_empty_names_are_rejected(field: str) -> None:
    with pytest.raises(ValidationError):
        settings(**{field: ""})


@pytest.mark.parametrize("bad", ["LOUD", "verbose", "", "warn"])
def test_an_unknown_log_level_is_rejected(bad: str) -> None:
    with pytest.raises(ValidationError):
        settings(log_level=bad)


def test_an_unknown_partition_size_is_rejected() -> None:
    with pytest.raises(ValidationError):
        settings(partition_size="fortnightly")


def test_the_transformed_zone_cannot_share_the_landing_zone() -> None:
    """Transformed output in the immutable landing bucket would break its guarantee."""
    with pytest.raises(ValidationError, match="must differ from landing_bucket"):
        settings(s3_transformed_bucket="wrc-landing", landing_bucket="wrc-landing")

    with pytest.raises(ValidationError, match="must differ from the landing"):
        settings(mongo_transformed_collection="landing_documents")

    with pytest.raises(ValidationError, match="must differ from the landing"):
        settings(mongo_transformed_collection="landing_state")


# --- accepted values -----------------------------------------------------------


@pytest.mark.parametrize("overrides", [{"retry_times": 0}, {"download_delay": 0.0}])
def test_legitimate_edge_values_are_accepted(overrides: dict[str, object]) -> None:
    assert settings(**overrides)


@pytest.mark.parametrize("level", [*LOG_LEVELS, "debug", " info "])
def test_log_levels_are_normalised(level: str) -> None:
    assert settings(log_level=level).log_level == level.strip().upper()


def test_defaults_are_self_consistent() -> None:
    config = settings()
    assert config.landing_bucket != config.s3_transformed_bucket
    assert config.search_url == "https://www.workplacerelations.ie/en/search/"
    assert config.advanced_search_url.endswith("?advance=true")


def test_a_trailing_slash_on_base_url_does_not_double_up() -> None:
    config = settings(base_url="https://www.workplacerelations.ie/")
    assert config.search_url == "https://www.workplacerelations.ie/en/search/"


# --- secrets --------------------------------------------------------------------


def test_credentials_are_omitted_when_unset_so_boto3_uses_the_aws_chain() -> None:
    kwargs = settings().boto3_client_kwargs()
    assert "aws_access_key_id" not in kwargs
    assert "aws_secret_access_key" not in kwargs
    assert kwargs["endpoint_url"] == "http://localhost:9000"


def test_credentials_are_passed_when_set_and_never_leak_in_repr() -> None:
    config = settings(s3_access_key_id="AKIAEXAMPLE", s3_secret_access_key="s3cret")
    kwargs = config.boto3_client_kwargs()
    assert kwargs["aws_access_key_id"] == "AKIAEXAMPLE"
    assert kwargs["aws_secret_access_key"] == "s3cret"
    assert "s3cret" not in repr(config)
    assert "s3cret" not in str(config.model_dump())


def test_no_endpoint_url_is_sent_for_real_aws_s3() -> None:
    assert "endpoint_url" not in settings(s3_endpoint_url=None).boto3_client_kwargs()


# --- Scrapy settings derived from config ---------------------------------------


def scrapy_settings(monkeypatch: pytest.MonkeyPatch, **env: str) -> dict[str, object]:
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    config_module.get_settings.cache_clear()
    import wrc_pipeline.ingestion.settings as scrapy_module

    reload(scrapy_module)
    try:
        return scrapy_module.as_dict()
    finally:
        config_module.get_settings.cache_clear()
        reload(scrapy_module)


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504, 522, 524])
def test_every_transient_status_is_retried(monkeypatch: pytest.MonkeyPatch, status: int) -> None:
    """The assessment names 429/500/502/503/504; 408/522/524 are the same class of fault."""
    assert status in scrapy_settings(monkeypatch)["RETRY_HTTP_CODES"]


def test_a_permanent_status_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    codes = scrapy_settings(monkeypatch)["RETRY_HTTP_CODES"]
    for status in (400, 401, 403, 404, 410):
        assert status not in codes


def test_politeness_and_pipeline_wiring(monkeypatch: pytest.MonkeyPatch) -> None:
    values = scrapy_settings(monkeypatch)
    assert values["ROBOTSTXT_OBEY"] is True, "respecting robots.txt is the default"
    assert values["AUTOTHROTTLE_ENABLED"] is True
    assert values["RETRY_ENABLED"] is True
    assert values["COOKIES_ENABLED"] is False
    assert values["ITEM_PIPELINES"] == {"wrc_pipeline.ingestion.pipelines.LandingZonePipeline": 300}


def test_tuning_flows_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    values = scrapy_settings(
        monkeypatch,
        WRC_CONCURRENT_REQUESTS="3",
        WRC_DOWNLOAD_DELAY="2.5",
        WRC_RETRY_TIMES="9",
        WRC_REQUEST_TIMEOUT="45",
        WRC_ROBOTSTXT_OBEY="false",
        WRC_LOG_LEVEL="debug",
    )
    assert values["CONCURRENT_REQUESTS"] == 3
    assert values["DOWNLOAD_DELAY"] == 2.5
    assert values["RETRY_TIMES"] == 9
    assert values["DOWNLOAD_TIMEOUT"] == 45
    assert values["ROBOTSTXT_OBEY"] is False
    assert values["LOG_LEVEL"] == "DEBUG"


def test_autothrottle_start_delay_never_undercuts_the_configured_delay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = scrapy_settings(monkeypatch, WRC_DOWNLOAD_DELAY="3.0")
    assert values["AUTOTHROTTLE_START_DELAY"] >= 3.0
