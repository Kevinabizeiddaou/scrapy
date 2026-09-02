"""Dagster job: ingest the Landing Zone, then transform it.

    dagster dev -m wrc_pipeline.orchestration.definitions

The date range is configured once, on the ingestion op, and reaches transformation through
the op dependency -- so the two stages cannot drift onto different ranges.

Both ops shell out to the existing CLIs rather than importing them:

* Ingestion *must* be isolated. Scrapy starts a Twisted reactor via
  ``CrawlerProcess.start()``, and a reactor cannot be restarted inside a process that has
  already run one, so a second Dagster run in the same process would die with
  ``ReactorNotRestartable``. A process boundary is the simplest robust fix.
* Transformation is isolated for a different reason: run in-process, its structured JSON
  events are swallowed by Dagster's own logging configuration and the per-document audit
  trail is lost. In its own process the CLI installs the project's JSON handler and every
  event survives.

Each op echoes its child's output so it stays observable, reads the run summary back off
that JSON stream for Dagster metadata, and fails on a non-zero exit code.
"""

# No ``from __future__ import annotations`` here: Dagster resolves the pythonic config
# class from the op's real annotation, and a stringified one cannot be resolved.
import json
import subprocess
import sys
from typing import Any, TextIO
from uuid import uuid4

from dagster import (
    Config,
    Definitions,
    Failure,
    MetadataValue,
    OpExecutionContext,
    Out,
    job,
    op,
)
from pydantic import Field

from wrc_pipeline.ingestion.partitions import PARTITION_SIZES, parse_date
from wrc_pipeline.transformation import TRANSFORMATION_VERSION

INGESTION_MODULE = "wrc_pipeline.ingestion.run"
TRANSFORMATION_MODULE = "wrc_pipeline.transformation.run"


class IngestionConfig(Config):
    """Configured once per run; the transformation op inherits the same range."""

    start_date: str = Field(description="inclusive start date, YYYY-MM-DD")
    end_date: str = Field(description="inclusive end date, YYYY-MM-DD")
    bodies: str | None = Field(
        default=None,
        description="comma-separated body names to restrict the run (default: all)",
    )
    partition_size: str = Field(
        default="monthly", description=f"one of {', '.join(PARTITION_SIZES)}"
    )


@op(out=Out(dict, description="date range plus the ingestion run's record tally"))
def ingest_landing_zone(context: OpExecutionContext, config: IngestionConfig) -> dict[str, Any]:
    """Run the ingestion CLI in its own process and fail the op if it aborted."""
    start, end = parse_date(config.start_date), parse_date(config.end_date)
    if start > end:
        raise Failure(f"start_date {start.isoformat()} is after end_date {end.isoformat()}")

    run_id = f"dagster-ingest-{uuid4().hex[:12]}"
    command = [
        sys.executable,
        "-m",
        INGESTION_MODULE,
        "--start-date",
        start.isoformat(),
        "--end-date",
        end.isoformat(),
        "--partition-size",
        config.partition_size,
        "--run-id",
        run_id,
    ]
    if config.bodies:
        command += ["--bodies", config.bodies]

    context.log.info(f"running: {' '.join(command)}")
    returncode, summary = _run_and_capture(command, "run_completed")
    metadata = _ingestion_metadata(run_id, summary)

    if returncode != 0:
        # Non-zero means the run itself aborted (e.g. body discovery failed) or its tally
        # did not balance. Failing here stops the downstream transformation op.
        raise Failure(
            description=f"ingestion exited {returncode}; transformation will not run",
            metadata=metadata | {"exit_code": returncode},
        )

    context.add_output_metadata(metadata)
    return {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "bodies": config.bodies,
        "ingestion_run_id": run_id,
        "ingestion": summary,
    }


@op(out=Out(dict, description="the transformation run's record tally"))
def transform_landing_zone(
    context: OpExecutionContext, ingestion: dict[str, Any]
) -> dict[str, Any]:
    """Transform whatever landing versions exist for the range ingestion just crawled."""
    run_id = f"dagster-transform-{uuid4().hex[:12]}"
    command = [
        sys.executable,
        "-m",
        TRANSFORMATION_MODULE,
        "--start-date",
        ingestion["start_date"],
        "--end-date",
        ingestion["end_date"],
        "--run-id",
        run_id,
    ]
    if ingestion.get("bodies"):
        command += ["--bodies", ingestion["bodies"]]

    context.log.info(f"running: {' '.join(command)}")
    returncode, summary = _run_and_capture(command, "transformation_run_completed")
    metadata = _transformation_metadata(run_id, summary)

    if returncode != 0:
        raise Failure(
            description=f"transformation exited {returncode}",
            metadata=metadata | {"exit_code": returncode},
        )

    context.add_output_metadata(metadata)
    return {"transformation_run_id": run_id, "transformation": summary}


@job(
    description=(
        "Ingest Workplace Relations decisions into the immutable Landing Zone, then "
        "transform that range into the transformed zone."
    )
)
def wrc_landing_and_transformation() -> None:
    transform_landing_zone(ingest_landing_zone())


def _run_and_capture(command: list[str], summary_event: str) -> tuple[int, dict[str, Any]]:
    """Run ``command``, echo its output so it stays observable, return its run summary.

    Both stages already log single-line JSON, so the summary is read back off the stream
    rather than reimplemented here.
    """
    summary: dict[str, Any] = {}
    echo = _utf8_stdout()
    with subprocess.Popen(  # noqa: S603 - fixed argv, no shell
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    ) as process:
        assert process.stdout is not None
        try:
            for line in process.stdout:
                echo.write(line)
                if event := _summary_event(line, summary_event):
                    summary = event
        except BaseException:
            # Dagster signals cancellation with a BaseException; without this the child
            # crawl would keep running and Popen.__exit__ would block waiting for it.
            process.kill()
            raise
        finally:
            echo.flush()
    return process.returncode, summary


def _utf8_stdout() -> TextIO:
    """Echo target that cannot fail on scraped text.

    The children emit UTF-8 JSON containing accented party names and the occasional
    malformed byte from the source pages. A redirected stdout inherits the platform locale
    (cp1252 on Windows), so writing those lines raises UnicodeEncodeError and would fail an
    op whose child actually succeeded.
    """
    stream = sys.stdout
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")
    return stream


def _summary_event(line: str, wanted: str) -> dict[str, Any] | None:
    """The child's own summary line, or None for any other output.

    Anything can appear on a child's stream -- Scrapy's own logs, a truncated line, a
    partial write -- so a line that is not JSON, or is JSON of another shape, is simply
    not a summary.
    """
    if wanted not in line:
        return None
    try:
        payload = json.loads(line)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload if payload.get("event") == wanted else None


def _ingestion_metadata(run_id: str, summary: dict[str, Any]) -> dict[str, Any]:
    metadata = _counts(
        summary,
        "records_found",
        "records_successful",
        "records_unchanged",
        "records_failed",
        "partitions",
        "partitions_failed",
    )
    metadata["ingestion_run_id"] = run_id
    if "reason" in summary:
        metadata["finish_reason"] = summary["reason"]
    return metadata


def _transformation_metadata(run_id: str, summary: dict[str, Any]) -> dict[str, Any]:
    metadata = _counts(
        summary,
        "records_selected",
        "records_transformed",
        "records_unchanged",
        "records_failed",
    )
    metadata["transformation_run_id"] = run_id
    metadata["transformation_version"] = TRANSFORMATION_VERSION
    return metadata


def _counts(summary: dict[str, Any], *keys: str) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        key: MetadataValue.int(summary[key]) for key in keys if isinstance(summary.get(key), int)
    }
    if "accounting_balanced" in summary:
        metadata["accounting_balanced"] = summary["accounting_balanced"]
    return metadata


defs = Definitions(jobs=[wrc_landing_and_transformation])

__all__ = [
    "IngestionConfig",
    "defs",
    "ingest_landing_zone",
    "transform_landing_zone",
    "wrc_landing_and_transformation",
]
