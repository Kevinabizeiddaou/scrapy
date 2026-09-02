"""Scrapy settings, derived from the typed environment configuration.

Nothing environment-specific is written here: every knob comes from :mod:`wrc_pipeline.config`
so the same module serves local runs and deployments.
"""

from __future__ import annotations

from typing import Any

from wrc_pipeline.config import get_settings

_config = get_settings()

BOT_NAME = "wrc_pipeline"
SPIDER_MODULES = ["wrc_pipeline.ingestion"]
NEWSPIDER_MODULE = "wrc_pipeline.ingestion"

USER_AGENT = _config.user_agent
ROBOTSTXT_OBEY = _config.robotstxt_obey

# --- concurrency and politeness ------------------------------------------------
CONCURRENT_REQUESTS = _config.concurrent_requests
CONCURRENT_REQUESTS_PER_DOMAIN = _config.concurrent_requests_per_domain
DOWNLOAD_DELAY = _config.download_delay
RANDOMIZE_DOWNLOAD_DELAY = True

AUTOTHROTTLE_ENABLED = _config.autothrottle_enabled
AUTOTHROTTLE_START_DELAY = max(_config.download_delay, 0.5)
AUTOTHROTTLE_MAX_DELAY = _config.autothrottle_max_delay
AUTOTHROTTLE_TARGET_CONCURRENCY = _config.autothrottle_target_concurrency
AUTOTHROTTLE_DEBUG = False

# --- timeouts and retries ------------------------------------------------------
DOWNLOAD_TIMEOUT = _config.request_timeout
DNS_TIMEOUT = 20
RETRY_ENABLED = True
RETRY_TIMES = _config.retry_times
RETRY_HTTP_CODES = [408, 429, 500, 502, 503, 504, 522, 524]
RETRY_PRIORITY_ADJUST = -1

# --- pipeline ------------------------------------------------------------------
ITEM_PIPELINES: dict[str, int] = {
    "wrc_pipeline.ingestion.pipelines.LandingZonePipeline": 300,
}

# The GET search endpoint is stateless; skipping cookies avoids serialising every
# request behind one ASP.NET session.
COOKIES_ENABLED = False

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IE,en;q=0.9",
}

LOG_LEVEL = _config.log_level.upper()
TELNETCONSOLE_ENABLED = False

# Scrapy >= 2.13 defaults; pinned so behaviour does not drift with the library.
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
FEED_EXPORT_ENCODING = "utf-8"


def as_dict() -> dict[str, Any]:
    """This module's settings, for programmatic runs via ``CrawlerProcess``."""
    return {
        name: value
        for name, value in globals().items()
        if name.isupper() and not name.startswith("_")
    }


__all__ = ["as_dict"]
