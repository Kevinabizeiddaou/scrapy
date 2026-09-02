"""Per-(body, partition) record accounting.

The invariant this enforces is::

    records_found == records_successful + records_unchanged + records_failed

``records_found`` is whatever the site itself reported ("Shows 1 to 10 of 45 results").
Every result row must therefore resolve exactly once. Rows the site promised but never
rendered are booked as failures with an explicit reason, so nothing is lost silently.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from wrc_pipeline.ingestion.partitions import DatePartition
    from wrc_pipeline.logging import EventLogger

PartitionKey = tuple[str, str]


@dataclass(slots=True)
class PartitionCounters:
    body: str
    body_id: str
    partition_start: dt.date
    partition_end: dt.date
    found: int = 0
    site_reported_total: int = 0
    rows_seen: int = 0
    duplicate_rows: int = 0
    successful: int = 0
    unchanged: int = 0
    failed: int = 0
    pages_expected: int = 0
    pages_parsed: int = 0
    count_known: bool = False
    completed: bool = False
    status: str = "ok"
    error: str | None = None

    @property
    def resolved(self) -> int:
        return self.successful + self.unchanged + self.failed

    @property
    def balanced(self) -> bool:
        return self.found == self.resolved

    def summary(self) -> dict[str, Any]:
        return {
            "body": self.body,
            "body_id": self.body_id,
            "partition_date": self.partition_start.isoformat(),
            "partition_start": self.partition_start.isoformat(),
            "partition_end": self.partition_end.isoformat(),
            "records_found": self.found,
            "site_reported_total": self.site_reported_total,
            "records_successful": self.successful,
            "records_unchanged": self.unchanged,
            "records_failed": self.failed,
            "records_duplicate_rows": self.duplicate_rows,
            "rows_seen": self.rows_seen,
            "pages_expected": self.pages_expected,
            "pages_parsed": self.pages_parsed,
            "accounting_balanced": self.balanced,
            "partition_status": self.status,
            "partition_error": self.error,
        }


@dataclass
class RunAccounting:
    """Tracks every partition of a run and emits the partition/run lifecycle events."""

    events: EventLogger
    partitions: dict[PartitionKey, PartitionCounters] = field(default_factory=dict)
    _seen_targets: set[tuple[str, str, str]] = field(default_factory=set)

    @staticmethod
    def key(body_id: str, partition: DatePartition) -> PartitionKey:
        return (body_id, partition.key)

    def start_partition(self, body: str, body_id: str, partition: DatePartition) -> PartitionKey:
        key = self.key(body_id, partition)
        self.partitions[key] = PartitionCounters(
            body=body,
            body_id=body_id,
            partition_start=partition.start,
            partition_end=partition.end,
        )
        self.events.event(
            "partition_started",
            body=body,
            body_id=body_id,
            partition_date=partition.start.isoformat(),
            partition_start=partition.start.isoformat(),
            partition_end=partition.end.isoformat(),
        )
        return key

    def record_result_count(self, key: PartitionKey, found: int, pages_expected: int) -> None:
        counters = self.partitions[key]
        counters.found = found
        counters.site_reported_total = found
        counters.pages_expected = pages_expected
        counters.count_known = True

    def record_page_parsed(self, key: PartitionKey) -> None:
        self.partitions[key].pages_parsed += 1

    def register_row(self, key: PartitionKey, target: str) -> bool:
        """Book a result row. False when this partition already saw ``target``."""
        counters = self.partitions[key]
        counters.rows_seen += 1
        marker = (key[0], key[1], target)
        if marker in self._seen_targets:
            counters.duplicate_rows += 1
            counters.unchanged += 1  # nothing new to land for a repeated row
            return False
        self._seen_targets.add(marker)
        return True

    def fail_partition(self, key: PartitionKey, reason: str) -> None:
        """Abandon a partition whose result count could not be established.

        Emitted as a failed partition rather than an empty one, so a broken search page
        can never be mistaken for a genuinely empty date range.
        """
        counters = self.partitions.get(key)
        if counters is None or counters.completed:
            return
        counters.status = "failed"
        counters.error = reason
        counters.count_known = True
        counters.pages_expected = counters.pages_parsed
        self._close(counters)

    def record_successful(self, key: PartitionKey) -> None:
        self.partitions[key].successful += 1

    def record_unchanged(self, key: PartitionKey) -> None:
        self.partitions[key].unchanged += 1

    def record_failed(self, key: PartitionKey, count: int = 1) -> None:
        self.partitions[key].failed += count

    def is_partition_done(self, key: PartitionKey) -> bool:
        counters = self.partitions[key]
        return (
            counters.count_known
            and counters.pages_parsed >= counters.pages_expected
            and counters.resolved >= counters.rows_seen
        )

    def complete_partition_if_done(self, key: PartitionKey) -> None:
        counters = self.partitions.get(key)
        if counters is None or counters.completed or not self.is_partition_done(key):
            return
        self._close(counters)

    def finalise(self) -> dict[str, Any]:
        """Close any partition still open (e.g. after an early shutdown) and total up."""
        for counters in self.partitions.values():
            if not counters.completed:
                self._close(counters, incomplete=True)

        totals = {
            "records_found": sum(c.found for c in self.partitions.values()),
            "records_successful": sum(c.successful for c in self.partitions.values()),
            "records_unchanged": sum(c.unchanged for c in self.partitions.values()),
            "records_failed": sum(c.failed for c in self.partitions.values()),
            "records_duplicate_rows": sum(c.duplicate_rows for c in self.partitions.values()),
            "partitions": len(self.partitions),
            "partitions_failed": sum(1 for c in self.partitions.values() if c.status == "failed"),
        }
        totals["accounting_balanced"] = totals["records_found"] == (
            totals["records_successful"] + totals["records_unchanged"] + totals["records_failed"]
        )
        return totals

    def _close(self, counters: PartitionCounters, *, incomplete: bool = False) -> None:
        """Reconcile a partition's tally, then emit ``partition_completed``."""
        if not counters.count_known:
            # We never learned how many records the site had for this partition, so an
            # empty tally proves nothing. Reporting "0 found, balanced, ok" here would
            # record a populated date range as genuinely empty.
            counters.status = "failed"
            counters.error = counters.error or "result_count_never_established"

        # Reconcile against whichever is larger: the total the site reported, or the rows
        # this run actually registered. Using ``found`` alone would let rows the site
        # rendered beyond its own count go unresolved yet still look balanced.
        expected = max(counters.found, counters.rows_seen)
        shortfall = expected - counters.resolved
        if shortfall > 0:
            counters.failed += shortfall
            self.events.error(
                "partition_records_unaccounted",
                reason="incomplete_shutdown" if incomplete else "missing_result_rows",
                records_missing=shortfall,
                **counters.summary(),
            )
        if counters.found < counters.resolved:
            # The site rendered more rows than it counted. Raise records_found to what was
            # actually accounted for -- reporting an unbalanced tally would be worse than
            # admitting the site's own total was low, which site_reported_total preserves.
            extra = counters.resolved - counters.found
            counters.found = counters.resolved
            self.events.warning(
                "partition_records_overcount",
                reason="site_reported_fewer_results_than_it_rendered",
                records_extra=extra,
                **counters.summary(),
            )

        if counters.duplicate_rows:
            # The site's result ordering is not a stable total order, so a row can appear
            # on two pages of the same partition -- which means another row it counted was
            # never rendered. The tally still balances, so say so explicitly.
            self.events.warning(
                "partition_pagination_unstable",
                reason="site_returned_the_same_result_row_on_more_than_one_page",
                records_possibly_missed=counters.duplicate_rows,
                **counters.summary(),
            )

        counters.completed = True
        self.events.event("partition_completed", **counters.summary())


__all__ = ["PartitionCounters", "PartitionKey", "RunAccounting"]
