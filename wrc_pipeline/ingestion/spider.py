"""Scrapy spider for the workplacerelations.ie decision search.

Discovered request contract (see README):

* Body filter values come from ``span#CB2`` on ``/en/search/?advance=true``.
* Search is a plain GET:
  ``/en/search/?decisions=1&from=DD/MM/YYYY&to=DD/MM/YYYY&body={id}&pageNumber={n}``
  -- 10 results per page, one body per request (repeated ``body`` params are ignored by
  the site and silently widen the search to every body).
* ``div.searchhead`` reports the authoritative total ("Shows 1 to 10 of 45 results").
  The rendered pager only ever shows a 10-page window, so pages are derived from that
  total rather than by following pager links.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode
from uuid import uuid4

import scrapy
from pymongo.errors import PyMongoError
from scrapy.exceptions import CloseSpider
from scrapy.spidermiddlewares.httperror import HttpError

from wrc_pipeline.config import get_settings
from wrc_pipeline.ingestion.accounting import PartitionKey, RunAccounting
from wrc_pipeline.ingestion.items import LandingDocument
from wrc_pipeline.ingestion.parsing import (
    CONTENT_TYPE_BY_DOCUMENT_TYPE,
    BodyDiscoveryError,
    SearchParseError,
    SearchResult,
    document_type_from_url,
    find_document_url,
    parse_bodies,
    parse_result_rows,
    parse_total_results,
    total_pages,
)
from wrc_pipeline.ingestion.partitions import DatePartition, build_partitions, parse_date
from wrc_pipeline.logging import EventLogger, configure_logging
from wrc_pipeline.storage.mongo import MongoLandingStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from scrapy.http import Response
    from twisted.python.failure import Failure


@dataclass(frozen=True, slots=True)
class Context:
    """Travels on ``Request.meta`` so callbacks *and* errbacks share the same context."""

    body: str
    body_id: str
    partition: DatePartition
    page: int = 1
    result: SearchResult | None = None
    document_url: str | None = None

    @property
    def key(self) -> PartitionKey:
        return (self.body_id, self.partition.key)

    @property
    def identifier(self) -> str | None:
        return self.result.identifier if self.result else None

    def log_fields(self) -> dict[str, Any]:
        return {
            "body": self.body,
            "body_id": self.body_id,
            "partition_date": self.partition.start.isoformat(),
            "partition_start": self.partition.start.isoformat(),
            "partition_end": self.partition.end.isoformat(),
            "identifier": self.identifier,
        }


class WrcDecisionsSpider(scrapy.Spider):
    name = "wrc_decisions"

    def __init__(
        self,
        start_date: str,
        end_date: str,
        partition_size: str | None = None,
        bodies: str | None = None,
        run_id: str | None = None,
        state_store: MongoLandingStore | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.config = get_settings()
        configure_logging(self.config.log_level)

        self.run_id = run_id or uuid4().hex
        self.events = EventLogger(self.run_id)
        self.partition_size = partition_size or self.config.partition_size
        self.partitions = build_partitions(
            parse_date(start_date), parse_date(end_date), self.partition_size
        )
        self.body_filter = (
            {name.strip().casefold() for name in bodies.split(",") if name.strip()}
            if bodies
            else None
        )
        self.accounting = RunAccounting(self.events)
        self._state_store = state_store
        self._owns_state_store = state_store is None

        # Emitted here rather than in start(), which Scrapy calls after opening pipelines.
        self.events.event(
            "run_started",
            source=self.config.source,
            partition_size=self.partition_size,
            partitions=len(self.partitions),
            partition_first=self.partitions[0].start.isoformat(),
            partition_last=self.partitions[-1].end.isoformat(),
            body_filter=sorted(self.body_filter) if self.body_filter else None,
        )

    # -- lifecycle ---------------------------------------------------------------

    @property
    def state_store(self) -> MongoLandingStore:
        """Read-only view of operational state, used to build conditional requests."""
        if self._state_store is None:
            self._state_store = MongoLandingStore(self.config)
        return self._state_store

    async def start(self) -> AsyncIterator[Any]:
        yield scrapy.Request(
            self.config.advanced_search_url,
            callback=self.parse_bodies,
            errback=self.on_bodies_error,
            dont_filter=True,
        )

    def closed(self, reason: str) -> None:
        totals = self.accounting.finalise()
        self.events.event("run_completed", reason=reason, **totals)
        if self._owns_state_store and self._state_store is not None:
            self._state_store.close()

    # -- body discovery ----------------------------------------------------------

    def parse_bodies(self, response: Response) -> Iterator[scrapy.Request]:
        try:
            discovered = parse_bodies(response.text)
        except BodyDiscoveryError as exc:
            self.events.error("body_discovery_failed", url=response.url, error=str(exc))
            raise CloseSpider("body_discovery_failed") from exc

        selected = [
            body
            for body in discovered
            if self.body_filter is None
            or body.name.casefold() in self.body_filter
            or body.body_id in self.body_filter
        ]
        self.events.event(
            "bodies_discovered",
            url=response.url,
            discovered=[{"body_id": b.body_id, "name": b.name} for b in discovered],
            selected=[b.name for b in selected],
        )
        if not selected:
            self.events.error(
                "body_selection_empty",
                requested=sorted(self.body_filter or ()),
                available=[b.name for b in discovered],
            )
            raise CloseSpider("body_selection_empty")

        for body in selected:
            for partition in self.partitions:
                self.accounting.start_partition(body.name, body.body_id, partition)
                yield self._search_request(Context(body.name, body.body_id, partition, page=1))

    def on_bodies_error(self, failure: Failure) -> None:
        self.events.error(
            "body_discovery_failed",
            url=self.config.advanced_search_url,
            **_failure_fields(failure),
        )
        raise CloseSpider("body_discovery_failed")

    # -- search pagination -------------------------------------------------------

    def _search_request(self, ctx: Context) -> scrapy.Request:
        date_from, date_to = ctx.partition.as_query_params()
        query = urlencode(
            {
                "decisions": 1,
                "from": date_from,
                "to": date_to,
                "legislationsub": "",
                "body": ctx.body_id,
                "pageNumber": ctx.page,
            }
        )
        return scrapy.Request(
            f"{self.config.search_url}?{query}",
            callback=self.parse_search,
            errback=self.on_search_error,
            meta={"wrc": ctx},
            dont_filter=True,
        )

    def parse_search(self, response: Response) -> Iterator[Any]:
        """Guarded entry point: an unreadable search page must fail its partition.

        Scrapy delivers a callback exception to ``spider_error``, not to the request's
        errback, and keeps the engine running. Without this guard a page-1 response the
        parser cannot read -- a non-text Content-Type from a WAF, say -- would leave the
        partition's result count unestablished and it would close as an empty, balanced,
        successful partition while holding real decisions.
        """
        ctx: Context = response.meta["wrc"]
        try:
            yield from self._parse_search(response)
        except Exception as exc:
            self.events.error(
                "partition_failed",
                reason="search_page_unreadable",
                url=response.url,
                page=ctx.page,
                http_status=response.status,
                error=str(exc),
                error_type=type(exc).__name__,
                **ctx.log_fields(),
            )
            if ctx.page == 1:
                self.accounting.fail_partition(
                    ctx.key, f"search_page_unreadable:{type(exc).__name__}"
                )
            else:
                self.accounting.partitions[ctx.key].pages_parsed += 1
                self.accounting.complete_partition_if_done(ctx.key)

    def _parse_search(self, response: Response) -> Iterator[Any]:
        ctx: Context = response.meta["wrc"]
        key = ctx.key
        rows = parse_result_rows(response.text, response.url)

        if ctx.page == 1:
            try:
                found = parse_total_results(response.text)
            except SearchParseError as exc:
                self.events.error(
                    "partition_failed",
                    reason="result_count_unreadable",
                    url=response.url,
                    error=str(exc),
                    **ctx.log_fields(),
                )
                self.accounting.fail_partition(key, "result_count_unreadable")
                return
            # Take the page size from the page the site just rendered rather than trusting
            # the configured value: under-counting pages would silently drop records, and
            # over-counting only costs one empty request.
            per_page = len(rows) or self.config.results_per_page
            pages = max(total_pages(found, per_page), 1)
            self.accounting.record_result_count(key, found, pages)
            self.events.event(
                "partition_result_count",
                records_found=found,
                pages_expected=pages,
                results_per_page=per_page,
                url=response.url,
                **ctx.log_fields(),
            )
            for page in range(2, pages + 1):
                yield self._search_request(replace(ctx, page=page))

        self.accounting.record_page_parsed(key)
        for row in rows:
            row_ctx = replace(ctx, result=row)
            if not self.accounting.register_row(key, row.detail_url):
                self.events.event(
                    "document_unchanged",
                    reason="duplicate_result_row",
                    detail_url=row.detail_url,
                    **row_ctx.log_fields(),
                )
                continue
            yield scrapy.Request(
                row.detail_url,
                callback=self.parse_detail,
                errback=self.on_detail_error,
                meta={"wrc": row_ctx},
                dont_filter=True,
            )
        self.accounting.complete_partition_if_done(key)

    def on_search_error(self, failure: Failure) -> None:
        ctx: Context = failure.request.meta["wrc"]
        fields = _failure_fields(failure)
        self.events.error(
            "search_page_failed",
            url=failure.request.url,
            page=ctx.page,
            **fields,
            **ctx.log_fields(),
        )
        if ctx.page == 1:
            self.accounting.fail_partition(ctx.key, f"search_page_1_failed:{fields['error']}")
            return
        # A lost page N>1 means its rows never arrive; the shortfall against the site's
        # reported total is booked as failures when the partition closes.
        self.accounting.partitions[ctx.key].pages_parsed += 1
        self.accounting.complete_partition_if_done(ctx.key)

    # -- detail pages ------------------------------------------------------------

    def parse_detail(self, response: Response) -> Iterator[Any]:
        """Guarded entry point -- see :meth:`_guarded`."""
        yield from self._guarded(self._parse_detail(response), response, stage="detail_page")

    def _parse_detail(self, response: Response) -> Iterator[Any]:
        ctx: Context = response.meta["wrc"]
        document_url = find_document_url(response.text, response.url)

        if document_url is None:
            # No attachment: the detail page itself is the decision document, stored as
            # the original bytes exactly as served.
            yield self._build_item(ctx, response, document_url=response.url, document_type="html")
            return

        doc_ctx = replace(ctx, document_url=document_url)
        yield scrapy.Request(
            document_url,
            callback=self.parse_document,
            errback=self.on_document_error,
            headers=self._conditional_headers(doc_ctx),
            meta={"wrc": doc_ctx, "handle_httpstatus_list": [304]},
            dont_filter=True,
        )

    def on_detail_error(self, failure: Failure) -> None:
        self._record_download_failure(failure, stage="detail_page")

    # -- documents ---------------------------------------------------------------

    def _conditional_headers(self, ctx: Context) -> dict[str, str]:
        """Reuse stored HTTP validators so unchanged documents can answer 304.

        A state lookup is an optimisation, never a precondition. If Mongo is unreachable
        the exception must not escape into the callback -- Scrapy routes callback errors to
        ``spider_error`` rather than the request's errback, so the row would never resolve
        and would vanish from the partition tally.
        """
        if ctx.identifier is None:
            return {}
        try:
            state = self.state_store.get_state(self.config.source, ctx.body, ctx.identifier)
        except PyMongoError as exc:
            self.events.warning(
                "document_state_lookup_failed",
                reason="falling_back_to_unconditional_download",
                error=str(exc),
                error_type=type(exc).__name__,
                **ctx.log_fields(),
            )
            return {}
        if not state:
            return {}
        headers: dict[str, str] = {}
        if etag := state.get("etag"):
            headers["If-None-Match"] = etag
        if last_modified := state.get("last_modified"):
            headers["If-Modified-Since"] = last_modified
        return headers

    def parse_document(self, response: Response) -> Iterator[Any]:
        """Guarded entry point -- see :meth:`_guarded`."""
        yield from self._guarded(self._parse_document(response), response, stage="document")

    def _parse_document(self, response: Response) -> Iterator[Any]:
        ctx: Context = response.meta["wrc"]
        if response.status == 304:
            self.events.event(
                "document_unchanged",
                reason="http_304_not_modified",
                document_url=response.url,
                **ctx.log_fields(),
            )
            self.accounting.record_unchanged(ctx.key)
            try:
                self.state_store.touch_state(
                    source=self.config.source,
                    body=ctx.body,
                    identifier=ctx.identifier or "",
                    run_id=self.run_id,
                )
            except PyMongoError as exc:
                # Bookkeeping only: the document is genuinely unchanged either way.
                self.events.warning(
                    "document_state_touch_failed",
                    error=str(exc),
                    error_type=type(exc).__name__,
                    **ctx.log_fields(),
                )
            self.accounting.complete_partition_if_done(ctx.key)
            return

        yield self._build_item(
            ctx,
            response,
            document_url=response.url,
            document_type=document_type_from_url(response.url),
        )

    def on_document_error(self, failure: Failure) -> None:
        self._record_download_failure(failure, stage="document")

    # -- helpers -----------------------------------------------------------------

    def _build_item(
        self, ctx: Context, response: Response, *, document_url: str, document_type: str
    ) -> LandingDocument:
        row = ctx.result
        if row is None:  # pragma: no cover - guards a programming error, not input
            raise RuntimeError("detail/document responses must carry a search result")
        return LandingDocument(
            source=self.config.source,
            body=ctx.body,
            body_id=ctx.body_id,
            identifier=row.identifier,
            title=row.title,
            description=row.description,
            reference_no=row.reference_no,
            published_date=row.published_date,
            detail_url=row.detail_url,
            document_url=document_url,
            document_type=document_type,
            partition_date=ctx.partition.partition_date,
            partition_start=ctx.partition.start,
            partition_end=ctx.partition.end,
            scraped_at=dt.datetime.now(dt.UTC),
            run_id=self.run_id,
            content=response.body,
            content_type=_content_type(response, document_type),
            etag=_header(response, "ETag"),
            last_modified=_header(response, "Last-Modified"),
        )

    def _guarded(self, results: Iterator[Any], response: Response, *, stage: str) -> Iterator[Any]:
        """Deliberate boundary around a callback so a registered row always resolves.

        Scrapy routes callback exceptions to ``spider_error``, not to the request's
        errback, so an unexpected error here would leave the row counted in ``rows_seen``
        and never resolved -- surfacing only at shutdown, anonymously. Anything that
        escapes is booked against this record with its full identity instead.
        """
        ctx: Context = response.meta["wrc"]
        try:
            yield from results
        except Exception as exc:
            self.events.error(
                "document_download_failed",
                stage=stage,
                url=response.url,
                http_status=response.status,
                detail_url=ctx.result.detail_url if ctx.result else None,
                document_url=ctx.document_url,
                error=str(exc),
                error_type=type(exc).__name__,
                retry_times=response.request.meta.get("retry_times", 0) if response.request else 0,
                retry_exhausted=False,
                **ctx.log_fields(),
            )
            self.accounting.record_failed(ctx.key)
            self.accounting.complete_partition_if_done(ctx.key)

    def _record_download_failure(self, failure: Failure, *, stage: str) -> None:
        ctx: Context = failure.request.meta["wrc"]
        self.events.error(
            "document_download_failed",
            stage=stage,
            url=failure.request.url,
            detail_url=ctx.result.detail_url if ctx.result else None,
            document_url=ctx.document_url,
            **_failure_fields(failure),
            **ctx.log_fields(),
        )
        self.accounting.record_failed(ctx.key)
        self.accounting.complete_partition_if_done(ctx.key)


def _failure_fields(failure: Failure) -> dict[str, Any]:
    """Status, retry state and reason for a Twisted failure, for the failure log."""
    meta = failure.request.meta if getattr(failure, "request", None) is not None else {}
    retry_times = meta.get("retry_times", 0) or 0
    # RetryMiddleware honours a per-request max_retry_times override before the setting.
    max_retries = meta.get("max_retry_times", get_settings().retry_times)
    fields: dict[str, Any] = {
        "error": failure.getErrorMessage() or failure.type.__name__,
        "error_type": failure.type.__name__,
        "http_status": None,
        "retry_times": retry_times,
        "max_retry_times": max_retries,
        "retry_exhausted": retry_times >= max_retries,
    }
    if failure.check(HttpError):
        fields["http_status"] = failure.value.response.status
    return fields


def _header(response: Response, name: str) -> str | None:
    raw = response.headers.get(name)
    return raw.decode("latin-1") if raw else None


def _content_type(response: Response, document_type: str) -> str:
    if header := _header(response, "Content-Type"):
        return header
    return CONTENT_TYPE_BY_DOCUMENT_TYPE.get(document_type, "application/octet-stream")


__all__ = ["Context", "WrcDecisionsSpider"]
