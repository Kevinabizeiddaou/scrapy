from __future__ import annotations

import datetime as dt

import pytest

from wrc_pipeline.ingestion.partitions import (
    DatePartition,
    build_partitions,
    parse_date,
)

D = dt.date


def test_monthly_partitions_match_the_assessment_example() -> None:
    assert build_partitions(D(2024, 1, 15), D(2024, 4, 10), "monthly") == [
        DatePartition(D(2024, 1, 15), D(2024, 1, 31)),
        DatePartition(D(2024, 2, 1), D(2024, 2, 29)),
        DatePartition(D(2024, 3, 1), D(2024, 3, 31)),
        DatePartition(D(2024, 4, 1), D(2024, 4, 10)),
    ]


def test_partition_date_is_always_the_start_date() -> None:
    for partition in build_partitions(D(2024, 1, 15), D(2024, 4, 10)):
        assert partition.partition_date == partition.start
        assert partition.key == partition.start.isoformat()


def test_monthly_default_is_monthly() -> None:
    assert build_partitions(D(2023, 5, 3), D(2023, 7, 9)) == build_partitions(
        D(2023, 5, 3), D(2023, 7, 9), "monthly"
    )


def test_partial_first_and_last_month_only() -> None:
    partitions = build_partitions(D(2024, 3, 20), D(2024, 5, 4), "monthly")
    assert partitions[0] == DatePartition(D(2024, 3, 20), D(2024, 3, 31))
    assert partitions[-1] == DatePartition(D(2024, 5, 1), D(2024, 5, 4))


def test_single_day_range_yields_one_partition() -> None:
    assert build_partitions(D(2024, 2, 29), D(2024, 2, 29)) == [
        DatePartition(D(2024, 2, 29), D(2024, 2, 29))
    ]


def test_range_inside_one_month_is_not_expanded() -> None:
    assert build_partitions(D(2024, 6, 5), D(2024, 6, 20)) == [
        DatePartition(D(2024, 6, 5), D(2024, 6, 20))
    ]


@pytest.mark.parametrize(
    ("year", "last_day"),
    [(2024, 29), (2023, 28), (2000, 29), (1900, 28), (2100, 28)],
)
def test_leap_year_february_end(year: int, last_day: int) -> None:
    partitions = build_partitions(D(year, 2, 1), D(year, 3, 15), "monthly")
    assert partitions[0].end == D(year, 2, last_day)
    assert partitions[1].start == D(year, 3, 1)


def test_february_start_inside_leap_year() -> None:
    assert build_partitions(D(2024, 2, 10), D(2024, 2, 29)) == [
        DatePartition(D(2024, 2, 10), D(2024, 2, 29))
    ]


@pytest.mark.parametrize("size", ["daily", "weekly", "monthly", "yearly"])
def test_partitions_are_contiguous_and_non_overlapping(size: str) -> None:
    start, end = D(2023, 11, 17), D(2025, 2, 3)
    partitions = build_partitions(start, end, size)

    assert partitions[0].start == start
    assert partitions[-1].end == end
    for partition in partitions:
        assert partition.start <= partition.end
    for earlier, later in zip(partitions, partitions[1:], strict=False):
        assert later.start == earlier.end + dt.timedelta(days=1)

    covered = sum((p.end - p.start).days + 1 for p in partitions)
    assert covered == (end - start).days + 1


def test_weekly_partitions_snap_to_iso_weeks() -> None:
    # 2024-01-03 is a Wednesday; the first partition must stop on Sunday the 7th.
    partitions = build_partitions(D(2024, 1, 3), D(2024, 1, 20), "weekly")
    assert partitions[0] == DatePartition(D(2024, 1, 3), D(2024, 1, 7))
    assert partitions[1] == DatePartition(D(2024, 1, 8), D(2024, 1, 14))
    assert all(p.start.weekday() == 0 for p in partitions[1:])


def test_yearly_partitions_snap_to_calendar_years() -> None:
    assert build_partitions(D(2022, 6, 1), D(2024, 2, 2), "yearly") == [
        DatePartition(D(2022, 6, 1), D(2022, 12, 31)),
        DatePartition(D(2023, 1, 1), D(2023, 12, 31)),
        DatePartition(D(2024, 1, 1), D(2024, 2, 2)),
    ]


def test_daily_partitions_across_a_month_boundary() -> None:
    partitions = build_partitions(D(2024, 2, 28), D(2024, 3, 1), "daily")
    assert [p.start for p in partitions] == [D(2024, 2, 28), D(2024, 2, 29), D(2024, 3, 1)]


def test_reversed_range_is_rejected() -> None:
    with pytest.raises(ValueError, match="is after end_date"):
        build_partitions(D(2024, 4, 10), D(2024, 1, 15))


def test_unknown_partition_size_is_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported partition size"):
        build_partitions(D(2024, 1, 1), D(2024, 1, 2), "fortnightly")


def test_query_params_use_the_sites_date_format() -> None:
    partition = DatePartition(D(2024, 1, 15), D(2024, 1, 31))
    assert partition.as_query_params() == ("15/01/2024", "31/01/2024")


def test_parse_date_accepts_iso_and_dates() -> None:
    assert parse_date("2024-01-15") == D(2024, 1, 15)
    assert parse_date(D(2024, 1, 15)) == D(2024, 1, 15)
    assert parse_date(dt.datetime(2024, 1, 15, 9, 30)) == D(2024, 1, 15)


@pytest.mark.parametrize("bad", ["15/01/2024", "2024-13-01", "", "yesterday"])
def test_parse_date_rejects_non_iso_input(bad: str) -> None:
    with pytest.raises(ValueError, match="expected an ISO date"):
        parse_date(bad)
