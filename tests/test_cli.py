"""CLI argument handling. --dry-run keeps these tests off the network."""

from __future__ import annotations

import pytest

from wrc_pipeline.ingestion.run import exit_code, main


def test_dry_run_prints_the_assessment_example(capsys) -> None:
    assert main(["--start-date", "2024-01-15", "--end-date", "2024-04-10", "--dry-run"]) == 0
    assert capsys.readouterr().out.splitlines() == [
        "2024-01-15  2024-01-15..2024-01-31",
        "2024-02-01  2024-02-01..2024-02-29",
        "2024-03-01  2024-03-01..2024-03-31",
        "2024-04-01  2024-04-01..2024-04-10",
    ]


def test_partition_size_flag_is_honoured(capsys) -> None:
    main(
        [
            "--start-date",
            "2024-02-28",
            "--end-date",
            "2024-03-01",
            "--partition-size",
            "daily",
            "--dry-run",
        ]
    )
    assert len(capsys.readouterr().out.splitlines()) == 3


@pytest.mark.parametrize(
    "argv",
    [
        ["--start-date", "2024-04-10", "--end-date", "2024-01-15", "--dry-run"],
        ["--start-date", "10/04/2024", "--end-date", "2024-01-15", "--dry-run"],
    ],
)
def test_bad_dates_exit_with_a_usage_error_not_a_traceback(argv: list[str], capsys) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(argv)
    assert exit_info.value.code == 2
    assert "wrc-ingest" in capsys.readouterr().err


def test_unknown_partition_size_is_rejected_by_argparse() -> None:
    with pytest.raises(SystemExit):
        main(
            [
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-02",
                "--partition-size",
                "fortnightly",
                "--dry-run",
            ]
        )


def test_missing_required_dates_are_rejected() -> None:
    with pytest.raises(SystemExit):
        main(["--start-date", "2024-01-01"])


class StatsStub:
    def __init__(self, reason: str) -> None:
        self._reason = reason

    def get_value(self, key: str, default: object = None) -> object:
        return self._reason if key == "finish_reason" else default


class SpiderStub:
    def __init__(self, totals: dict) -> None:
        self.accounting = type("A", (), {"finalise": lambda _self: totals})()


class CrawlerStub:
    def __init__(self, reason: str, totals: dict | None, spider: bool = True) -> None:
        self.stats = StatsStub(reason)
        self.spider = SpiderStub(totals or {}) if spider else None


BALANCED = {"partitions_failed": 0, "accounting_balanced": True, "records_found": 3}


def test_a_clean_run_exits_zero() -> None:
    assert exit_code(CrawlerStub("finished", BALANCED)) == 0


@pytest.mark.parametrize(
    ("reason", "totals", "spider"),
    [
        ("body_discovery_failed", BALANCED, True),
        ("body_selection_empty", BALANCED, True),
        ("shutdown", BALANCED, True),
        ("finished", {"partitions_failed": 2, "accounting_balanced": True}, True),
        ("finished", {"partitions_failed": 0, "accounting_balanced": False}, True),
        ("finished", None, False),
    ],
)
def test_an_unsuccessful_run_exits_nonzero(reason: str, totals: dict | None, spider: bool) -> None:
    assert exit_code(CrawlerStub(reason, totals, spider)) == 1
