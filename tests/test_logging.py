"""JSON log output: one valid JSON object per line, UTF-8, always carrying run_id."""

from __future__ import annotations

import io
import json
import logging

import pytest

from wrc_pipeline.logging import EventLogger, JsonFormatter, configure_logging


@pytest.fixture
def captured() -> tuple[EventLogger, io.TextIOWrapper, io.BytesIO]:
    raw = io.BytesIO()
    stream = io.TextIOWrapper(raw, encoding="ascii", write_through=True)  # hostile default
    configure_logging("INFO", stream=stream)
    return EventLogger("run-xyz"), stream, raw


def lines(raw: io.BytesIO) -> list[dict]:
    text = raw.getvalue().decode("utf-8")  # must decode as UTF-8, not the stream's default
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def test_every_event_is_one_json_object_with_the_run_id(captured) -> None:
    events, _, raw = captured
    events.event("document_persisted", identifier="LCR22912", file_size=25046)
    events.error("document_download_failed", http_status=503, identifier="X")

    payloads = lines(raw)
    assert [p["event"] for p in payloads] == ["document_persisted", "document_download_failed"]
    assert all(p["run_id"] == "run-xyz" for p in payloads)
    assert payloads[0]["identifier"] == "LCR22912"
    assert payloads[0]["level"] == "INFO"
    assert payloads[1]["level"] == "ERROR"
    assert payloads[1]["http_status"] == 503


def test_non_ascii_scraped_text_is_written_as_utf8(captured) -> None:
    """Source pages carry accents and the odd malformed byte; logs must stay UTF-8."""
    events, _, raw = captured
    events.event("document_persisted", title="Bredá Slevin \ufffd v Iarnród Éireann")

    (payload,) = lines(raw)
    assert payload["title"] == "Bredá Slevin \ufffd v Iarnród Éireann"


def test_scrapy_own_log_lines_are_json_too(captured) -> None:
    _, _, raw = captured
    logging.getLogger("scrapy.core.engine").info("Spider opened")

    (payload,) = lines(raw)
    assert payload["logger"] == "scrapy.core.engine"
    assert payload["message"] == "Spider opened"


def test_timestamps_are_utc_iso8601(captured) -> None:
    events, _, raw = captured
    events.event("run_started")
    (payload,) = lines(raw)
    assert payload["timestamp"].endswith("+00:00")


def test_unserialisable_values_do_not_break_a_log_line() -> None:
    record = logging.LogRecord("t", logging.INFO, "f", 1, "msg", None, None)
    record.thing = object()
    payload = json.loads(JsonFormatter().format(record))
    assert payload["message"] == "msg"
    assert payload["thing"].startswith("<object object")


def test_exceptions_are_included(caplog) -> None:
    record = logging.LogRecord("t", logging.ERROR, "f", 1, "boom", None, None)
    try:
        raise ValueError("nope")
    except ValueError:
        import sys

        record.exc_info = sys.exc_info()
    payload = json.loads(JsonFormatter().format(record))
    assert "ValueError: nope" in payload["exception"]
