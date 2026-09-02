"""Dagster wiring: transformation runs only after a successful ingestion.

No real crawl and no real Mongo/S3 -- the subprocess boundary is substituted, so what is
under test is the orchestration contract.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from wrc_pipeline.orchestration import definitions as orch

JOB = orch.wrc_landing_and_transformation
RANGE = {"start_date": "2024-01-01", "end_date": "2024-02-10"}
INGESTION_MODULE = "wrc_pipeline.ingestion.run"
TRANSFORMATION_MODULE = "wrc_pipeline.transformation.run"

INGESTION_OK = {
    "event": "run_completed",
    "reason": "finished",
    "records_found": 53,
    "records_successful": 53,
    "records_unchanged": 0,
    "records_failed": 0,
    "partitions": 2,
    "partitions_failed": 0,
    "accounting_balanced": True,
}
INGESTION_ABORTED = {
    "event": "run_completed",
    "reason": "body_discovery_failed",
    "records_found": 0,
    "records_successful": 0,
    "records_unchanged": 0,
    "records_failed": 0,
    "partitions": 0,
    "partitions_failed": 0,
    "accounting_balanced": True,
}
TRANSFORMATION_OK = {
    "event": "transformation_run_completed",
    "records_selected": 53,
    "records_transformed": 53,
    "records_unchanged": 0,
    "records_failed": 0,
    "accounting_balanced": True,
}


def run_job(
    ingest_returncode: int = 0,
    ingest_summary: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    transform_returncode: int = 0,
    transform_summary: dict[str, Any] | None = None,
) -> tuple[Any, list[dict[str, Any]]]:
    """Execute the job with both child processes replaced by canned results."""
    outcomes = {
        "run_completed": (
            ingest_returncode,
            INGESTION_OK if ingest_summary is None else ingest_summary,
        ),
        "transformation_run_completed": (
            transform_returncode,
            TRANSFORMATION_OK if transform_summary is None else transform_summary,
        ),
    }
    spawned: list[dict[str, Any]] = []

    def fake_run(command: list[str], summary_event: str) -> tuple[int, dict[str, Any]]:
        spawned.append({"command": command, "summary_event": summary_event})
        return outcomes[summary_event]

    with patch.object(orch, "_run_and_capture", side_effect=fake_run):
        result = JOB.execute_in_process(
            run_config={"ops": {"ingest_landing_zone": {"config": config or dict(RANGE)}}},
            raise_on_error=False,
        )
    return result, spawned


def command_for(spawned: list[dict[str, Any]], module: str) -> list[str]:
    for call in spawned:
        if module in call["command"]:
            return call["command"]
    raise AssertionError(f"{module} was never invoked; got {spawned}")


def was_spawned(spawned: list[dict[str, Any]], module: str) -> bool:
    return any(module in call["command"] for call in spawned)


def step_succeeded(result: Any, op_name: str) -> bool:
    return any(event.is_step_success and event.step_key == op_name for event in result.all_events)


def step_ran(result: Any, op_name: str) -> bool:
    return any(getattr(event, "step_key", None) == op_name for event in result.all_events)


# --- dependency -----------------------------------------------------------------


def test_the_job_declares_transformation_after_ingestion() -> None:
    assert [node.name for node in JOB.graph.node_defs] == [
        "ingest_landing_zone",
        "transform_landing_zone",
    ]

    dependencies = {
        invocation.name: {key: dep.node for key, dep in deps.items()}
        for invocation, deps in JOB.graph.dependencies.items()
    }
    assert dependencies["transform_landing_zone"] == {"ingestion": "ingest_landing_zone"}
    assert dependencies["ingest_landing_zone"] == {}


# --- successful ingestion permits transformation --------------------------------


def test_a_successful_ingestion_lets_transformation_run() -> None:
    result, spawned = run_job()

    assert result.success
    assert step_succeeded(result, "ingest_landing_zone")
    assert step_succeeded(result, "transform_landing_zone")
    assert was_spawned(spawned, TRANSFORMATION_MODULE)


def test_transformation_receives_the_same_date_range_as_ingestion() -> None:
    result, spawned = run_job()
    assert result.success

    for module in (INGESTION_MODULE, TRANSFORMATION_MODULE):
        command = command_for(spawned, module)
        assert command[command.index("--start-date") + 1] == "2024-01-01"
        assert command[command.index("--end-date") + 1] == "2024-02-10"


def test_the_body_filter_reaches_both_stages() -> None:
    result, spawned = run_job(config={**RANGE, "bodies": "Labour Court, 15376"})
    assert result.success

    for module in (INGESTION_MODULE, TRANSFORMATION_MODULE):
        command = command_for(spawned, module)
        assert command[command.index("--bodies") + 1] == "Labour Court, 15376"


def test_the_partition_size_reaches_ingestion_only() -> None:
    result, spawned = run_job(config={**RANGE, "partition_size": "weekly"})
    assert result.success

    ingest = command_for(spawned, INGESTION_MODULE)
    assert ingest[ingest.index("--partition-size") + 1] == "weekly"
    assert "--partition-size" not in command_for(spawned, TRANSFORMATION_MODULE)


def test_each_stage_gets_its_own_run_id() -> None:
    result, spawned = run_job()
    assert result.success

    ingest = command_for(spawned, INGESTION_MODULE)
    transform = command_for(spawned, TRANSFORMATION_MODULE)
    ingest_id = ingest[ingest.index("--run-id") + 1]
    transform_id = transform[transform.index("--run-id") + 1]
    assert ingest_id.startswith("dagster-ingest-")
    assert transform_id.startswith("dagster-transform-")
    assert ingest_id != transform_id


def test_counts_from_both_stages_surface_as_dagster_metadata() -> None:
    result, _ = run_job()

    ingested = result.output_for_node("ingest_landing_zone")
    assert ingested["ingestion"]["records_successful"] == 53
    assert ingested["start_date"] == "2024-01-01"

    transformed = result.output_for_node("transform_landing_zone")
    assert transformed["transformation"]["records_selected"] == 53
    assert transformed["transformation"]["accounting_balanced"] is True
    assert transformed["transformation_run_id"].startswith("dagster-transform-")


# --- failed ingestion blocks transformation -------------------------------------


def test_a_nonzero_ingestion_exit_fails_the_job_and_skips_transformation() -> None:
    result, spawned = run_job(ingest_returncode=1, ingest_summary=INGESTION_ABORTED)

    assert not result.success
    assert not step_succeeded(result, "ingest_landing_zone")
    assert not step_ran(result, "transform_landing_zone")
    assert not was_spawned(spawned, TRANSFORMATION_MODULE), "transformation must not run at all"


def test_the_ingestion_failure_reports_the_exit_code() -> None:
    result, _ = run_job(ingest_returncode=2, ingest_summary=INGESTION_ABORTED)
    assert not result.success

    failures = [event for event in result.all_events if event.is_step_failure]
    assert failures
    assert "ingestion exited 2" in str(failures[0].event_specific_data.error)


def test_record_level_ingestion_failures_still_allow_transformation() -> None:
    """Ingestion exits 0 when its tally balances, even with some failed records."""
    partial = {**INGESTION_OK, "records_successful": 50, "records_failed": 3}
    result, spawned = run_job(ingest_summary=partial)

    assert result.success
    assert step_succeeded(result, "transform_landing_zone")
    assert was_spawned(spawned, TRANSFORMATION_MODULE)


def test_a_nonzero_transformation_exit_fails_the_job() -> None:
    result, _ = run_job(transform_returncode=1, transform_summary={})

    assert not result.success
    assert step_succeeded(result, "ingest_landing_zone")
    failures = [event for event in result.all_events if event.is_step_failure]
    assert "transformation exited 1" in str(failures[0].event_specific_data.error)


# --- config validation ----------------------------------------------------------


def test_a_reversed_range_fails_before_any_subprocess_is_started() -> None:
    reversed_range = {"start_date": "2024-02-10", "end_date": "2024-01-01"}
    with patch.object(orch, "_run_and_capture") as spawn:
        result = JOB.execute_in_process(
            run_config={"ops": {"ingest_landing_zone": {"config": reversed_range}}},
            raise_on_error=False,
        )

    assert not result.success
    spawn.assert_not_called()


# --- the reactor boundary -------------------------------------------------------


def test_both_stages_are_invoked_as_subprocesses_running_the_existing_clis() -> None:
    """Scrapy's Twisted reactor cannot be restarted, so it must not run in-process."""
    result, spawned = run_job()
    assert result.success

    assert command_for(spawned, INGESTION_MODULE)[1:3] == ["-m", INGESTION_MODULE]
    assert command_for(spawned, TRANSFORMATION_MODULE)[1:3] == ["-m", TRANSFORMATION_MODULE]
    for call in spawned:
        assert call["command"][0].endswith(("python", "python.exe")), call["command"][0]


def test_the_orchestration_module_never_runs_scrapy_in_process() -> None:
    """Instantiating a CrawlerProcess here would reintroduce the reactor problem."""
    import ast
    from pathlib import Path

    tree = ast.parse(Path(orch.__file__).read_text(encoding="utf-8"))

    imported = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert not any(name.startswith("scrapy") for name in imported), imported

    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "CrawlerProcess" not in called
    assert "WrcDecisionsSpider" not in called


# --- child summary parsing ------------------------------------------------------


def test_the_summary_event_is_read_from_the_childs_json_stream() -> None:
    line = (
        '{"timestamp": "2026-01-01T00:00:00+00:00", "level": "INFO", "event": '
        '"run_completed", "records_found": 9}'
    )
    assert orch._summary_event(line, "run_completed")["records_found"] == 9


@pytest.mark.parametrize(
    "line",
    [
        "",
        "plain scrapy log line",
        '{"event": "run_completed"',  # truncated JSON
        '{"event": "partition_completed", "records_found": 9}',  # a different event
        '{"event": "transformation_run_completed"}',  # the other stage's summary
        "not json at all: run_completed",
        '["run_completed"]',  # valid JSON, wrong shape
    ],
)
def test_a_line_that_is_not_the_wanted_summary_is_ignored(line: str) -> None:
    assert orch._summary_event(line, "run_completed") is None


def test_the_transformation_summary_is_matched_separately() -> None:
    """'transformation_run_completed' contains 'run_completed' as a substring."""
    line = '{"event": "transformation_run_completed", "records_selected": 4}'
    assert orch._summary_event(line, "run_completed") is None
    assert orch._summary_event(line, "transformation_run_completed")["records_selected"] == 4


def test_a_missing_child_summary_still_succeeds_but_reports_no_counts() -> None:
    """A child can exit 0 without its summary line being parsed; that is not a failure."""
    result, _ = run_job(ingest_summary={}, transform_summary={})

    assert result.success
    ingested = result.output_for_node("ingest_landing_zone")
    assert ingested["ingestion"] == {}
    assert ingested["start_date"] == "2024-01-01", "the date range is still propagated"
    assert result.output_for_node("transform_landing_zone")["transformation"] == {}


def test_metadata_omits_counts_that_the_child_never_reported() -> None:
    metadata = orch._ingestion_metadata("run-1", {})
    assert metadata == {"ingestion_run_id": "run-1"}

    partial = orch._ingestion_metadata("run-1", {"records_found": 3, "reason": "finished"})
    assert partial["records_found"].value == 3
    assert partial["finish_reason"] == "finished"


def test_metadata_ignores_non_integer_counts() -> None:
    """A malformed child line must not crash metadata assembly."""
    metadata = orch._ingestion_metadata("run-1", {"records_found": "many", "partitions": None})
    assert "records_found" not in metadata
    assert "partitions" not in metadata


def test_a_signal_killed_child_is_treated_as_failure() -> None:
    """subprocess reports a negative returncode when the child dies by signal."""
    result, spawned = run_job(ingest_returncode=-9, ingest_summary={})

    assert not result.success
    assert not was_spawned(spawned, TRANSFORMATION_MODULE)
