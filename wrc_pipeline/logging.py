"""JSON structured logging.

Scrapy logs through the stdlib ``logging`` module, so replacing the root formatter is
enough to make *every* line -- ours and Scrapy's own -- valid JSON on one line.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import TextIO

# Attributes present on every LogRecord; anything else was attached by us and belongs
# in the JSON payload.
_RESERVED = frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "message",
    "asctime",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": dt.datetime.fromtimestamp(record.created, dt.UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(
            {
                k: v
                for k, v in record.__dict__.items()
                if k not in _RESERVED and not k.startswith("_")
            }
        )
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(level: str = "INFO", stream: TextIO | None = None) -> None:
    """Force JSON output on the root logger, replacing any handlers Scrapy installed."""
    target = stream or sys.stderr
    # Scraped text carries accents and the odd malformed byte from the source pages, and a
    # redirected stream would otherwise inherit the platform locale (cp1252 on Windows),
    # emitting logs that are not valid UTF-8.
    if hasattr(target, "reconfigure"):
        target.reconfigure(encoding="utf-8", errors="replace")

    handler = logging.StreamHandler(target)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level.upper())


class EventLogger:
    """Emits named events that always carry ``run_id``."""

    def __init__(self, run_id: str, logger_name: str = "wrc.ingestion") -> None:
        self.run_id = run_id
        self._log = logging.getLogger(logger_name)

    def event(self, event: str, level: int = logging.INFO, **fields: Any) -> None:
        self._log.log(level, event, extra={"event": event, "run_id": self.run_id, **fields})

    def warning(self, event: str, **fields: Any) -> None:
        self.event(event, level=logging.WARNING, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self.event(event, level=logging.ERROR, **fields)


__all__ = ["EventLogger", "JsonFormatter", "configure_logging"]
