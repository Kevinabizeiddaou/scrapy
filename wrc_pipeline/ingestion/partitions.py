"""Contiguous, non-overlapping date partitions over a closed [start, end] interval."""

from __future__ import annotations

import calendar
import datetime as dt
from collections.abc import Iterator
from dataclasses import dataclass
from typing import get_args

from wrc_pipeline.config import PartitionSize

PARTITION_SIZES: tuple[str, ...] = get_args(PartitionSize)

# The WRC search form emits and accepts dd/mm/yyyy.
WRC_DATE_FORMAT = "%d/%m/%Y"


@dataclass(frozen=True, slots=True)
class DatePartition:
    """A closed date interval. ``partition_date`` is always the start date."""

    start: dt.date
    end: dt.date

    @property
    def partition_date(self) -> dt.date:
        return self.start

    @property
    def key(self) -> str:
        return self.start.isoformat()

    def as_query_params(self) -> tuple[str, str]:
        return self.start.strftime(WRC_DATE_FORMAT), self.end.strftime(WRC_DATE_FORMAT)

    def __str__(self) -> str:
        return f"{self.start.isoformat()}..{self.end.isoformat()}"


def _period_end(day: dt.date, size: str) -> dt.date:
    """Last day of the calendar period of ``size`` that contains ``day``."""
    match size:
        case "daily":
            return day
        case "weekly":  # ISO weeks: Monday..Sunday
            return day + dt.timedelta(days=6 - day.weekday())
        case "monthly":
            return day.replace(day=calendar.monthrange(day.year, day.month)[1])
        case "yearly":
            return day.replace(month=12, day=31)
    raise ValueError(f"unsupported partition size {size!r}; expected one of {PARTITION_SIZES}")


def iter_partitions(start: dt.date, end: dt.date, size: str = "monthly") -> Iterator[DatePartition]:
    """Yield partitions covering [start, end] exactly once each.

    Boundaries snap to calendar periods, so the first and last partitions are truncated
    to the requested interval while the ones in between are whole periods.
    """
    if size not in PARTITION_SIZES:
        raise ValueError(f"unsupported partition size {size!r}; expected one of {PARTITION_SIZES}")
    if start > end:
        raise ValueError(f"start_date {start.isoformat()} is after end_date {end.isoformat()}")

    cursor = start
    while cursor <= end:
        period_end = min(_period_end(cursor, size), end)
        yield DatePartition(cursor, period_end)
        cursor = period_end + dt.timedelta(days=1)


def build_partitions(start: dt.date, end: dt.date, size: str = "monthly") -> list[DatePartition]:
    return list(iter_partitions(start, end, size))


def parse_date(value: str | dt.date) -> dt.date:
    """Accept an ISO date string (or a date) and fail loudly on anything else."""
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(value.strip())
    except (AttributeError, ValueError) as exc:
        raise ValueError(f"expected an ISO date (YYYY-MM-DD), got {value!r}") from exc


__all__ = [
    "PARTITION_SIZES",
    "WRC_DATE_FORMAT",
    "DatePartition",
    "build_partitions",
    "iter_partitions",
    "parse_date",
]
