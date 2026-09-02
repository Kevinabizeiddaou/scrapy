"""CLI entrypoint: ``wrc-ingest --start-date 2024-01-15 --end-date 2024-04-10``."""

from __future__ import annotations

import argparse
import logging
import sys
from typing import TYPE_CHECKING
from uuid import uuid4

from scrapy.crawler import CrawlerProcess

from wrc_pipeline.config import get_settings
from wrc_pipeline.ingestion import settings as scrapy_settings
from wrc_pipeline.ingestion.partitions import PARTITION_SIZES, build_partitions, parse_date
from wrc_pipeline.ingestion.spider import WrcDecisionsSpider
from wrc_pipeline.logging import configure_logging

if TYPE_CHECKING:
    from scrapy.crawler import Crawler


def build_parser() -> argparse.ArgumentParser:
    config = get_settings()
    parser = argparse.ArgumentParser(
        prog="wrc-ingest",
        description="Ingest Workplace Relations decisions into the MongoDB/S3 landing zone.",
    )
    parser.add_argument("--start-date", required=True, help="inclusive start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="inclusive end date (YYYY-MM-DD)")
    parser.add_argument(
        "--partition-size",
        choices=PARTITION_SIZES,
        default=config.partition_size,
        help=f"time partition granularity (default: {config.partition_size})",
    )
    parser.add_argument(
        "--bodies",
        default=None,
        help="comma-separated body names or ids to restrict the crawl (default: all discovered)",
    )
    parser.add_argument("--run-id", default=None, help="reuse a run id instead of generating one")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the partitions that would be crawled and exit",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = get_settings()
    configure_logging(config.log_level)

    try:
        start, end = parse_date(args.start_date), parse_date(args.end_date)
        partitions = build_partitions(start, end, args.partition_size)
    except ValueError as exc:
        # A bad date range is user error: report it like one, not as a traceback.
        parser.error(str(exc))

    if args.dry_run:
        for partition in partitions:
            print(f"{partition.partition_date.isoformat()}  {partition}")  # noqa: T201
        return 0

    process = CrawlerProcess(settings=scrapy_settings.as_dict(), install_root_handler=False)
    crawler = process.create_crawler(WrcDecisionsSpider)
    process.crawl(
        crawler,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        partition_size=args.partition_size,
        bodies=args.bodies,
        run_id=args.run_id or uuid4().hex,
    )
    process.start()
    return exit_code(crawler)


def exit_code(crawler: Crawler) -> int:
    """0 only when the run completed and every partition balanced.

    Individual record failures are reported in the logs and the run totals but do not fail
    the process; a run that aborted, lost a whole partition, or could not account for the
    records the site reported does.
    """
    reason = crawler.stats.get_value("finish_reason") if crawler.stats else None
    spider = crawler.spider
    totals = spider.accounting.finalise() if spider is not None else {}
    failed = (
        reason != "finished"
        or not spider
        or totals.get("partitions_failed", 1) > 0
        or not totals.get("accounting_balanced", False)
    )
    if failed:
        logging.getLogger("wrc.ingestion").error(
            "run_unsuccessful",
            extra={"event": "run_unsuccessful", "finish_reason": reason, **totals},
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
