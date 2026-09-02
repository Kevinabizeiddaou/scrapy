"""Spider request/callback wiring, driven by fixtures with no network access."""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
from scrapy.http import HtmlResponse, Request, Response

from tests.conftest import load_fixture
from wrc_pipeline.ingestion.items import LandingDocument
from wrc_pipeline.ingestion.parsing import SearchResult
from wrc_pipeline.ingestion.partitions import DatePartition
from wrc_pipeline.ingestion.spider import Context, WrcDecisionsSpider

PARTITION = DatePartition(dt.date(2024, 1, 1), dt.date(2024, 1, 31))
CTX = Context(body="Labour Court", body_id="3", partition=PARTITION, page=1)
DETAIL_URL = "https://www.workplacerelations.ie/en/cases/2024/february/lcr22912.html"


class StateStoreStub:
    """Stands in for MongoLandingStore's read side."""

    def __init__(self, state: dict[str, Any] | None = None) -> None:
        self.state = state
        self.touched: list[str] = []

    def get_state(self, source: str, body: str, identifier: str) -> dict[str, Any] | None:
        return self.state

    def touch_state(self, *, source: str, body: str, identifier: str, run_id: str) -> None:
        self.touched.append(identifier)

    def close(self) -> None:
        pass


def make_spider(
    state: dict[str, Any] | None = None, caplog: Any = None, **kwargs: Any
) -> WrcDecisionsSpider:
    spider = WrcDecisionsSpider(
        start_date="2024-01-01",
        end_date="2024-01-31",
        run_id="run-test",
        state_store=StateStoreStub(state),  # type: ignore[arg-type]
        **kwargs,
    )
    if caplog is not None:
        # The spider installs the project's JSON handler on the root logger, which
        # detaches pytest's capturing handler; put it back so assertions can see records.
        logging.getLogger().addHandler(caplog.handler)
    spider.accounting.start_partition(CTX.body, CTX.body_id, PARTITION)
    return spider


def html_response(fixture: str, url: str, ctx: Context, **kwargs: Any) -> HtmlResponse:
    return HtmlResponse(
        url=url,
        body=load_fixture(fixture).encode("utf-8"),
        encoding="utf-8",
        request=Request(url, meta={"wrc": ctx}),
        **kwargs,
    )


# --- body discovery -> partition fan-out ---------------------------------------


def test_body_discovery_fans_out_over_bodies_and_partitions() -> None:
    spider = WrcDecisionsSpider(
        start_date="2024-01-15",
        end_date="2024-03-10",
        run_id="run-test",
        state_store=StateStoreStub(),  # type: ignore[arg-type]
    )
    url = "https://www.workplacerelations.ie/en/search/?advance=true"
    response = HtmlResponse(
        url=url, body=load_fixture("advanced_search.html").encode("utf-8"), encoding="utf-8"
    )

    requests = list(spider.parse_bodies(response))
    assert len(requests) == 4 * 3  # 4 bodies x 3 monthly partitions
    assert len(spider.accounting.partitions) == 12

    params = parse_qs(urlparse(requests[0].url).query)
    assert params["decisions"] == ["1"]
    assert params["from"] == ["15/01/2024"]
    assert params["to"] == ["31/01/2024"]
    assert params["pageNumber"] == ["1"]
    assert len(params["body"]) == 1, "one body per request; the site ignores repeated values"


def test_body_filter_restricts_the_crawl() -> None:
    spider = WrcDecisionsSpider(
        start_date="2024-01-01",
        end_date="2024-01-31",
        bodies="labour court, 15376",
        run_id="run-test",
        state_store=StateStoreStub(),  # type: ignore[arg-type]
    )
    url = "https://www.workplacerelations.ie/en/search/?advance=true"
    response = HtmlResponse(
        url=url, body=load_fixture("advanced_search.html").encode("utf-8"), encoding="utf-8"
    )
    requests = list(spider.parse_bodies(response))
    body_ids = {parse_qs(urlparse(r.url).query)["body"][0] for r in requests}
    assert body_ids == {"3", "15376"}


# --- search pagination ---------------------------------------------------------


def test_page_one_schedules_every_remaining_page_from_the_reported_total() -> None:
    spider = make_spider()
    response = html_response("search_results_page1.html", "https://x/search?p=1", CTX)

    yielded = list(spider.parse_search(response))
    search_requests = [r for r in yielded if r.callback == spider.parse_search]
    detail_requests = [r for r in yielded if r.callback == spider.parse_detail]

    pages = sorted(int(parse_qs(urlparse(r.url).query)["pageNumber"][0]) for r in search_requests)
    assert pages == [2, 3, 4, 5], "45 results / 10 per page = 5 pages"
    assert len(detail_requests) == 10

    counters = spider.accounting.partitions[CTX.key]
    assert counters.found == 45
    assert counters.pages_expected == 5
    assert counters.pages_parsed == 1


def test_pagination_covers_pages_beyond_the_rendered_pager_window() -> None:
    """234 WRC results is 24 pages, although the site renders only 10 pager links."""
    spider = make_spider()
    ctx = Context(body="Workplace Relations Commission", body_id="15376", partition=PARTITION)
    spider.accounting.start_partition(ctx.body, ctx.body_id, PARTITION)
    response = html_response("search_results_wrc_page1.html", "https://x/search?p=1", ctx)

    yielded = list(spider.parse_search(response))
    pages = sorted(
        int(parse_qs(urlparse(r.url).query)["pageNumber"][0])
        for r in yielded
        if r.callback == spider.parse_search
    )
    assert pages == list(range(2, 25))


def test_later_pages_do_not_reschedule_pagination() -> None:
    spider = make_spider()
    spider.accounting.record_result_count(CTX.key, 45, 5)
    ctx = Context(body=CTX.body, body_id=CTX.body_id, partition=PARTITION, page=5)
    response = html_response("search_results_last_page.html", "https://x/search?p=5", ctx)

    yielded = list(spider.parse_search(response))
    assert all(r.callback == spider.parse_detail for r in yielded)
    assert len(yielded) == 5


def test_empty_partition_completes_with_zero_records() -> None:
    spider = make_spider()
    response = html_response("search_results_empty.html", "https://x/search?p=1", CTX)

    assert list(spider.parse_search(response)) == []
    counters = spider.accounting.partitions[CTX.key]
    assert counters.completed
    assert (counters.found, counters.successful, counters.unchanged, counters.failed) == (
        0,
        0,
        0,
        0,
    )


def test_unreadable_search_page_fails_the_partition_loudly() -> None:
    spider = make_spider()
    response = HtmlResponse(
        url="https://x/search?p=1",
        body=b"<html><body>maintenance</body></html>",
        encoding="utf-8",
        request=Request("https://x/search?p=1", meta={"wrc": CTX}),
    )

    assert list(spider.parse_search(response)) == []
    counters = spider.accounting.partitions[CTX.key]
    assert counters.status == "failed"
    assert counters.error == "result_count_unreadable"


def test_a_duplicate_result_row_is_not_requested_twice() -> None:
    spider = make_spider()
    spider.accounting.record_result_count(CTX.key, 45, 5)
    page = html_response("search_results_page1.html", "https://x/search?p=1", CTX)

    first = [r for r in spider.parse_search(page) if r.callback == spider.parse_detail]
    ctx2 = Context(body=CTX.body, body_id=CTX.body_id, partition=PARTITION, page=2)
    repeat = html_response("search_results_page1.html", "https://x/search?p=2", ctx2)
    second = [r for r in spider.parse_search(repeat) if r.callback == spider.parse_detail]

    assert len(first) == 10
    assert second == []
    assert spider.accounting.partitions[CTX.key].duplicate_rows == 10


# --- detail pages --------------------------------------------------------------


def row(identifier: str = "LCR22912") -> SearchResult:
    return SearchResult(
        identifier=identifier,
        title=identifier,
        description="SONOMA VALLEY AND A WORKER",
        published_date=dt.date(2024, 1, 30),
        detail_url=DETAIL_URL,
    )


def test_inline_decision_yields_the_detail_html_as_the_document() -> None:
    spider = make_spider()
    ctx = Context(CTX.body, CTX.body_id, PARTITION, result=row())
    response = html_response(
        "detail_inline_html.html",
        DETAIL_URL,
        ctx,
        headers={"Content-Type": "text/html; charset=utf-8"},
    )

    (item,) = list(spider.parse_detail(response))
    assert isinstance(item, LandingDocument)
    assert item.document_type == "html"
    assert item.document_url == DETAIL_URL
    assert item.content == response.body, "the original HTML is stored untransformed"
    assert item.content_type == "text/html; charset=utf-8"
    assert item.identifier == "LCR22912"
    assert item.published_date == dt.date(2024, 1, 30)
    assert item.partition_date == PARTITION.start
    assert item.partition_start == PARTITION.start
    assert item.partition_end == PARTITION.end
    assert item.run_id == "run-test"
    assert item.body == "Labour Court"


def test_attached_pdf_becomes_a_follow_up_request() -> None:
    spider = make_spider()
    ctx = Context(CTX.body, CTX.body_id, PARTITION, result=row("UD893/2008"))
    response = html_response("detail_with_pdf.html", DETAIL_URL, ctx)

    (request,) = list(spider.parse_detail(response))
    assert request.callback == spider.parse_document
    assert request.url.endswith(".pdf")
    assert request.meta["handle_httpstatus_list"] == [304]


def test_stored_validators_become_conditional_headers() -> None:
    spider = make_spider(state={"etag": "635084656496170000", "last_modified": "Wed, 03 Jul 2013"})
    ctx = Context(CTX.body, CTX.body_id, PARTITION, result=row("UD893/2008"))
    response = html_response("detail_with_pdf.html", DETAIL_URL, ctx)

    (request,) = list(spider.parse_detail(response))
    assert request.headers[b"If-None-Match"] == b"635084656496170000"
    assert request.headers[b"If-Modified-Since"] == b"Wed, 03 Jul 2013"


def test_no_conditional_headers_without_stored_state() -> None:
    spider = make_spider(state=None)
    ctx = Context(CTX.body, CTX.body_id, PARTITION, result=row("UD893/2008"))
    response = html_response("detail_with_pdf.html", DETAIL_URL, ctx)

    (request,) = list(spider.parse_detail(response))
    assert b"If-None-Match" not in request.headers


# --- documents -----------------------------------------------------------------


def test_304_is_counted_unchanged_and_downloads_nothing() -> None:
    spider = make_spider()
    spider.accounting.record_result_count(CTX.key, 1, 1)
    spider.accounting.record_page_parsed(CTX.key)
    ctx = Context(CTX.body, CTX.body_id, PARTITION, result=row(), document_url="https://x/a.pdf")
    spider.accounting.register_row(CTX.key, DETAIL_URL)
    response = Response(
        url="https://x/a.pdf", status=304, request=Request("https://x/a.pdf", meta={"wrc": ctx})
    )

    assert list(spider.parse_document(response)) == []
    counters = spider.accounting.partitions[CTX.key]
    assert (counters.unchanged, counters.successful) == (1, 0)
    assert counters.completed
    assert spider.state_store.touched == ["LCR22912"]  # type: ignore[attr-defined]


def test_pdf_bytes_and_validators_reach_the_item() -> None:
    spider = make_spider()
    ctx = Context(CTX.body, CTX.body_id, PARTITION, result=row())
    payload = b"%PDF-1.4 exact bytes"
    response = Response(
        url="https://x/a.pdf",
        status=200,
        body=payload,
        headers={
            "Content-Type": "application/pdf",
            "ETag": "635084656496170000",
            "Last-Modified": "Wed, 03 Jul 2013 04:27:29",
        },
        request=Request("https://x/a.pdf", meta={"wrc": ctx}),
    )

    (item,) = list(spider.parse_document(response))
    assert item.content == payload
    assert item.document_type == "pdf"
    assert item.content_type == "application/pdf"
    assert item.etag == "635084656496170000"
    assert item.last_modified == "Wed, 03 Jul 2013 04:27:29"


# --- failures ------------------------------------------------------------------


def test_a_failed_detail_download_is_logged_and_counted() -> None:
    from scrapy.spidermiddlewares.httperror import HttpError
    from twisted.python.failure import Failure

    spider = make_spider()
    spider.accounting.record_result_count(CTX.key, 1, 1)
    spider.accounting.record_page_parsed(CTX.key)
    spider.accounting.register_row(CTX.key, DETAIL_URL)

    ctx = Context(CTX.body, CTX.body_id, PARTITION, result=row())
    request = Request(DETAIL_URL, meta={"wrc": ctx, "retry_times": 4})
    response = Response(url=DETAIL_URL, status=503, request=request)
    failure = Failure(HttpError(response))
    failure.request = request  # type: ignore[attr-defined]

    spider.on_detail_error(failure)
    counters = spider.accounting.partitions[CTX.key]
    assert counters.failed == 1
    assert counters.completed
    assert counters.balanced


@pytest.mark.parametrize("bad_range", [("2024-04-10", "2024-01-15")])
def test_a_reversed_date_range_is_rejected_before_any_request(bad_range: tuple[str, str]) -> None:
    start, end = bad_range
    with pytest.raises(ValueError, match="is after end_date"):
        WrcDecisionsSpider(
            start_date=start,
            end_date=end,
            state_store=StateStoreStub(),  # type: ignore[arg-type]
        )


# --- requirement 11: the failure-record log contract ---------------------------


def failure_for(status: int | None, *, retry_times: int, error: Exception | None = None):
    from scrapy.spidermiddlewares.httperror import HttpError
    from twisted.python.failure import Failure

    ctx = Context(CTX.body, CTX.body_id, PARTITION, result=row(), document_url="https://x/a.pdf")
    request = Request(DETAIL_URL, meta={"wrc": ctx, "retry_times": retry_times})
    if error is None:
        response = Response(url=DETAIL_URL, status=status or 500, request=request)
        failure = Failure(HttpError(response))
    else:
        failure = Failure(error)
    failure.request = request  # type: ignore[attr-defined]
    return failure


def test_a_failed_record_logs_every_field_the_spec_requires(caplog) -> None:
    spider = make_spider(caplog=caplog)
    spider.accounting.record_result_count(CTX.key, 1, 1)
    spider.accounting.record_page_parsed(CTX.key)
    spider.accounting.register_row(CTX.key, DETAIL_URL)

    with caplog.at_level(logging.ERROR):
        spider.on_document_error(failure_for(503, retry_times=4))

    record = next(r for r in caplog.records if r.msg == "document_download_failed")
    assert record.run_id == "run-test"
    assert record.identifier == "LCR22912"
    assert record.url == DETAIL_URL
    assert record.document_url == "https://x/a.pdf"
    assert record.body == "Labour Court"
    assert record.partition_date == "2024-01-01"
    assert record.partition_start == "2024-01-01"
    assert record.partition_end == "2024-01-31"
    assert record.http_status == 503
    assert record.error_type == "HttpError"
    assert record.error
    assert record.retry_times == 4
    assert record.retry_exhausted is True
    assert record.stage == "document"
    assert record.levelname == "ERROR"


def test_a_non_http_failure_logs_no_status_but_still_names_the_error(caplog) -> None:
    spider = make_spider(caplog=caplog)
    with caplog.at_level(logging.ERROR):
        spider.on_detail_error(failure_for(None, retry_times=0, error=TimeoutError("timed out")))

    record = next(r for r in caplog.records if r.msg == "document_download_failed")
    assert record.http_status is None
    assert record.error_type == "TimeoutError"
    assert record.retry_exhausted is False


def test_a_per_request_retry_override_is_what_exhaustion_is_measured_against(caplog) -> None:
    spider = make_spider(caplog=caplog)
    failure = failure_for(503, retry_times=2)
    failure.request.meta["max_retry_times"] = 2

    with caplog.at_level(logging.ERROR):
        spider.on_document_error(failure)

    record = next(r for r in caplog.records if r.msg == "document_download_failed")
    assert record.max_retry_times == 2
    assert record.retry_exhausted is True


# --- page size is taken from the site, not from config -------------------------


def test_page_count_uses_the_page_size_the_site_actually_rendered() -> None:
    """If the site switched to 5 results per page, pagination must still cover everything."""
    spider = make_spider()
    html = (
        """
    <div class="searchhead">Shows 1 to 5 of 23 results</div>
    <div class="item-list search-list"><ul>
    """
        + "".join(
            f'<li class="each-item"><h2 class="title"><a href="/en/cases/x{i}.html" '
            f'title="X{i}">X{i}</a></h2><span class="refNO">X{i}</span></li>'
            for i in range(5)
        )
        + "</ul></div>"
    )
    response = HtmlResponse(
        url="https://x/search?p=1",
        body=html.encode("utf-8"),
        encoding="utf-8",
        request=Request("https://x/search?p=1", meta={"wrc": CTX}),
    )

    yielded = list(spider.parse_search(response))
    pages = sorted(
        int(parse_qs(urlparse(r.url).query)["pageNumber"][0])
        for r in yielded
        if r.callback == spider.parse_search
    )
    assert pages == [2, 3, 4, 5], "ceil(23/5) = 5 pages, not ceil(23/10) = 3"


# --- a Mongo outage must not lose a row ----------------------------------------


def test_a_state_lookup_failure_degrades_to_an_unconditional_download(caplog) -> None:
    from pymongo.errors import ConnectionFailure

    spider = make_spider(state={"etag": "abc"}, caplog=caplog)

    def explode(*args: object, **kwargs: object) -> None:
        raise ConnectionFailure("no primary available")

    spider._state_store.get_state = explode  # type: ignore[attr-defined,union-attr]
    ctx = Context(CTX.body, CTX.body_id, PARTITION, result=row("UD893/2008"))
    response = html_response("detail_with_pdf.html", DETAIL_URL, ctx)

    with caplog.at_level(logging.WARNING):
        (request,) = list(spider.parse_detail(response))

    assert b"If-None-Match" not in request.headers, "no validator, so download unconditionally"
    assert any(r.msg == "document_state_lookup_failed" for r in caplog.records)


def test_an_unexpected_callback_error_resolves_the_row_with_its_identity(caplog) -> None:
    """Nothing may leave a registered row counted-but-unresolved and anonymous."""
    spider = make_spider(caplog=caplog)
    spider.accounting.record_result_count(CTX.key, 1, 1)
    spider.accounting.record_page_parsed(CTX.key)
    spider.accounting.register_row(CTX.key, DETAIL_URL)

    ctx = Context(CTX.body, CTX.body_id, PARTITION, result=row())
    response = html_response("detail_inline_html.html", DETAIL_URL, ctx)

    def explode(*args: object, **kwargs: object) -> None:
        raise RuntimeError("selector blew up")

    spider._build_item = explode  # type: ignore[method-assign]

    with caplog.at_level(logging.ERROR):
        assert list(spider.parse_detail(response)) == []

    record = next(r for r in caplog.records if r.msg == "document_download_failed")
    assert record.identifier == "LCR22912"
    assert record.error_type == "RuntimeError"
    assert record.body == "Labour Court"
    assert record.partition_date == "2024-01-01"

    counters = spider.accounting.partitions[CTX.key]
    assert counters.failed == 1
    assert counters.completed
    assert counters.balanced


def test_a_state_touch_failure_on_304_still_counts_the_record(caplog) -> None:
    from pymongo.errors import ConnectionFailure

    spider = make_spider(caplog=caplog)
    spider.accounting.record_result_count(CTX.key, 1, 1)
    spider.accounting.record_page_parsed(CTX.key)
    spider.accounting.register_row(CTX.key, DETAIL_URL)

    def explode(**kwargs: object) -> None:
        raise ConnectionFailure("no primary available")

    spider._state_store.touch_state = explode  # type: ignore[attr-defined,union-attr]
    ctx = Context(CTX.body, CTX.body_id, PARTITION, result=row(), document_url="https://x/a.pdf")
    response = Response(
        url="https://x/a.pdf", status=304, request=Request("https://x/a.pdf", meta={"wrc": ctx})
    )

    with caplog.at_level(logging.WARNING):
        assert list(spider.parse_document(response)) == []

    assert any(r.msg == "document_state_touch_failed" for r in caplog.records)
    counters = spider.accounting.partitions[CTX.key]
    assert (counters.unchanged, counters.failed) == (1, 0)
    assert counters.completed
