"""The records_found == successful + unchanged + failed invariant."""

from __future__ import annotations

import datetime as dt
import json
import logging

import pytest

from wrc_pipeline.ingestion.accounting import RunAccounting
from wrc_pipeline.ingestion.partitions import DatePartition
from wrc_pipeline.logging import EventLogger, JsonFormatter

BODY, BODY_ID = "Labour Court", "3"
PARTITION = DatePartition(dt.date(2024, 1, 1), dt.date(2024, 1, 31))


@pytest.fixture
def accounting() -> RunAccounting:
    return RunAccounting(EventLogger("run-1"))


def start(accounting: RunAccounting) -> tuple[str, str]:
    return accounting.start_partition(BODY, BODY_ID, PARTITION)


def test_a_fully_resolved_partition_balances(accounting: RunAccounting) -> None:
    key = start(accounting)
    accounting.record_result_count(key, found=3, pages_expected=1)
    accounting.record_page_parsed(key)
    for target in ("/a", "/b", "/c"):
        assert accounting.register_row(key, target)
    accounting.record_successful(key)
    accounting.record_unchanged(key)
    accounting.record_failed(key)

    accounting.complete_partition_if_done(key)
    counters = accounting.partitions[key]
    assert counters.completed
    assert counters.balanced
    assert (counters.successful, counters.unchanged, counters.failed) == (1, 1, 1)


def test_partition_stays_open_until_every_row_resolves(accounting: RunAccounting) -> None:
    key = start(accounting)
    accounting.record_result_count(key, found=2, pages_expected=1)
    accounting.record_page_parsed(key)
    accounting.register_row(key, "/a")
    accounting.register_row(key, "/b")

    accounting.record_successful(key)
    accounting.complete_partition_if_done(key)
    assert not accounting.partitions[key].completed

    accounting.record_successful(key)
    accounting.complete_partition_if_done(key)
    assert accounting.partitions[key].completed


def test_partition_stays_open_until_every_page_is_parsed(accounting: RunAccounting) -> None:
    key = start(accounting)
    accounting.record_result_count(key, found=20, pages_expected=2)
    accounting.record_page_parsed(key)
    for target in (f"/{n}" for n in range(10)):
        accounting.register_row(key, target)
        accounting.record_successful(key)

    accounting.complete_partition_if_done(key)
    assert not accounting.partitions[key].completed


def test_rows_the_site_never_rendered_are_booked_as_failures(accounting: RunAccounting) -> None:
    key = start(accounting)
    accounting.record_result_count(key, found=10, pages_expected=1)
    accounting.record_page_parsed(key)
    accounting.register_row(key, "/a")
    accounting.record_successful(key)

    accounting.complete_partition_if_done(key)
    counters = accounting.partitions[key]
    assert counters.failed == 9, "no silent loss: the 9 missing rows must show up as failures"
    assert counters.balanced


def test_duplicate_rows_within_a_partition_count_once(accounting: RunAccounting) -> None:
    key = start(accounting)
    accounting.record_result_count(key, found=2, pages_expected=1)
    accounting.record_page_parsed(key)
    assert accounting.register_row(key, "/same") is True
    assert accounting.register_row(key, "/same") is False
    accounting.record_successful(key)

    accounting.complete_partition_if_done(key)
    counters = accounting.partitions[key]
    assert counters.duplicate_rows == 1
    assert (counters.successful, counters.unchanged, counters.failed) == (1, 1, 0)
    assert counters.balanced


def test_duplicate_rows_raise_a_pagination_warning(accounting: RunAccounting, caplog) -> None:
    """A repeated row means the site never rendered one it counted -- that must be loud."""
    key = start(accounting)
    accounting.record_result_count(key, found=2, pages_expected=1)
    accounting.record_page_parsed(key)
    accounting.register_row(key, "/same")
    accounting.register_row(key, "/same")
    accounting.record_successful(key)

    with caplog.at_level(logging.WARNING):
        accounting.complete_partition_if_done(key)

    warning = next(r for r in caplog.records if r.msg == "partition_pagination_unstable")
    assert warning.records_possibly_missed == 1


def test_no_pagination_warning_without_duplicates(accounting: RunAccounting, caplog) -> None:
    key = start(accounting)
    accounting.record_result_count(key, found=1, pages_expected=1)
    accounting.record_page_parsed(key)
    accounting.register_row(key, "/a")
    accounting.record_successful(key)

    with caplog.at_level(logging.WARNING):
        accounting.complete_partition_if_done(key)
    assert not [r for r in caplog.records if r.msg == "partition_pagination_unstable"]


def test_an_empty_partition_completes_immediately(accounting: RunAccounting) -> None:
    key = start(accounting)
    accounting.record_result_count(key, found=0, pages_expected=1)
    accounting.record_page_parsed(key)

    accounting.complete_partition_if_done(key)
    counters = accounting.partitions[key]
    assert counters.completed
    assert counters.balanced
    assert counters.status == "ok"


def test_a_site_overcount_is_reported_not_hidden(accounting: RunAccounting, caplog) -> None:
    key = start(accounting)
    accounting.record_result_count(key, found=1, pages_expected=1)
    accounting.record_page_parsed(key)
    accounting.register_row(key, "/a")
    accounting.register_row(key, "/b")
    accounting.record_successful(key)
    accounting.record_successful(key)

    with caplog.at_level(logging.WARNING):
        accounting.complete_partition_if_done(key)
    assert any(record.msg == "partition_records_overcount" for record in caplog.records)

    # records_found is reconciled upward so the invariant still holds, and the site's own
    # (lower) total is preserved for comparison.
    counters = accounting.partitions[key]
    assert counters.balanced
    assert counters.found == 2
    assert counters.site_reported_total == 1


def test_a_partition_with_an_unreadable_count_is_marked_failed(accounting: RunAccounting) -> None:
    key = start(accounting)
    accounting.fail_partition(key, "result_count_unreadable")

    counters = accounting.partitions[key]
    assert counters.completed
    assert counters.status == "failed"
    assert counters.error == "result_count_unreadable"


def test_fail_partition_is_a_no_op_once_completed(accounting: RunAccounting) -> None:
    key = start(accounting)
    accounting.record_result_count(key, found=0, pages_expected=1)
    accounting.record_page_parsed(key)
    accounting.complete_partition_if_done(key)

    accounting.fail_partition(key, "too late")
    assert accounting.partitions[key].status == "ok"


def test_run_totals_balance_across_partitions() -> None:
    accounting = RunAccounting(EventLogger("run-1"))
    for month in (1, 2):
        partition = DatePartition(dt.date(2024, month, 1), dt.date(2024, month, 28))
        key = accounting.start_partition(BODY, BODY_ID, partition)
        accounting.record_result_count(key, found=2, pages_expected=1)
        accounting.record_page_parsed(key)
        accounting.register_row(key, f"/{month}-a")
        accounting.register_row(key, f"/{month}-b")
        accounting.record_successful(key)
        accounting.record_unchanged(key)
        accounting.complete_partition_if_done(key)

    totals = accounting.finalise()
    assert totals["records_found"] == 4
    assert totals["records_successful"] == 2
    assert totals["records_unchanged"] == 2
    assert totals["records_failed"] == 0
    assert totals["partitions"] == 2
    assert totals["accounting_balanced"] is True


def test_finalise_closes_a_partition_abandoned_by_an_early_shutdown() -> None:
    accounting = RunAccounting(EventLogger("run-1"))
    key = accounting.start_partition(BODY, BODY_ID, PARTITION)
    accounting.record_result_count(key, found=5, pages_expected=1)

    totals = accounting.finalise()
    assert accounting.partitions[key].completed
    assert totals["records_failed"] == 5
    assert totals["accounting_balanced"] is True


def test_partition_summary_is_json_serialisable(accounting: RunAccounting) -> None:
    key = start(accounting)
    accounting.record_result_count(key, found=0, pages_expected=1)
    summary = accounting.partitions[key].summary()
    assert json.loads(json.dumps(summary))["partition_date"] == "2024-01-01"


def test_events_are_emitted_as_json_with_the_run_id(caplog) -> None:
    accounting = RunAccounting(EventLogger("run-abc"))
    with caplog.at_level(logging.INFO):
        accounting.start_partition(BODY, BODY_ID, PARTITION)

    record = next(r for r in caplog.records if r.msg == "partition_started")
    payload = json.loads(JsonFormatter().format(record))
    assert payload["event"] == "partition_started"
    assert payload["run_id"] == "run-abc"
    assert payload["body"] == BODY
    assert payload["partition_date"] == "2024-01-01"


def test_run_totals_balance_even_when_the_site_undercounted() -> None:
    accounting = RunAccounting(EventLogger("run-1"))
    key = accounting.start_partition(BODY, BODY_ID, PARTITION)
    accounting.record_result_count(key, found=1, pages_expected=1)
    accounting.record_page_parsed(key)
    accounting.register_row(key, "/a")
    accounting.register_row(key, "/b")
    accounting.record_successful(key)
    accounting.record_successful(key)
    accounting.complete_partition_if_done(key)

    totals = accounting.finalise()
    assert totals["records_found"] == 2
    assert totals["records_successful"] == 2
    assert totals["accounting_balanced"] is True
