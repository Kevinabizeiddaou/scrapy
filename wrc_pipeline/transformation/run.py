"""CLI entrypoint: ``python -m wrc_pipeline.transformation.run --start-date … --end-date …``.

Runs independently of ingestion; it only reads the Landing Zone.
"""

from __future__ import annotations

import argparse
import sys
from uuid import uuid4

from wrc_pipeline.config import get_settings
from wrc_pipeline.ingestion.partitions import parse_date
from wrc_pipeline.logging import configure_logging
from wrc_pipeline.transformation import TRANSFORMATION_VERSION
from wrc_pipeline.transformation.transformer import TransformationRun, TransformationTally


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wrc-transform",
        description=(
            "Transform Landing Zone documents into the cleaned transformed zone "
            f"(transformation version {TRANSFORMATION_VERSION})."
        ),
    )
    parser.add_argument("--start-date", required=True, help="inclusive start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="inclusive end date (YYYY-MM-DD)")
    parser.add_argument(
        "--bodies",
        default=None,
        help="comma-separated body names to restrict the run (default: every body)",
    )
    parser.add_argument("--run-id", default=None, help="reuse a run id instead of generating one")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="list the landing versions that would be transformed and exit",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = get_settings()
    configure_logging(config.log_level)

    try:
        start, end = parse_date(args.start_date), parse_date(args.end_date)
        if start > end:
            raise ValueError(f"start_date {start.isoformat()} is after end_date {end.isoformat()}")
    except ValueError as exc:
        parser.error(str(exc))

    bodies = [b.strip() for b in args.bodies.split(",") if b.strip()] if args.bodies else None

    with TransformationRun(args.run_id or uuid4().hex, config) as run:
        if args.dry_run:
            for landing in run.mongo.select_landing_versions(start, end, bodies):
                print(  # noqa: T201
                    f"{landing['published_date'].date().isoformat()}  "
                    f"{landing['body']}  {landing['identifier']}  "
                    f"{landing['document_type']}  {landing['storage_key']}"
                )
            return 0
        tally = run.execute(start, end, bodies)

    return exit_code(tally)


def exit_code(tally: TransformationTally) -> int:
    """0 when the run completed and the tally balanced.

    Individual document failures are logged and counted but do not fail the process, which
    matches the ingestion stage's contract.
    """
    return 0 if tally.balanced else 1


if __name__ == "__main__":
    sys.exit(main())
