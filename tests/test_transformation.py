"""Transformation behaviour and idempotency, against mongomock and moto S3."""

from __future__ import annotations

import datetime as dt
from typing import Any
from unittest.mock import patch

import mongomock
import pytest
from moto import mock_aws

from tests.conftest import load_fixture
from wrc_pipeline.config import Settings
from wrc_pipeline.storage.mongo import MongoLandingStore, MongoTransformedStore
from wrc_pipeline.storage.object_store import ObjectStore, sha256_hex
from wrc_pipeline.transformation import TRANSFORMATION_VERSION
from wrc_pipeline.transformation.transformer import TransformationRun

SOURCE = "workplacerelations.ie"
LANDING_BUCKET = "wrc-landing-test"
TRANSFORMED_BUCKET = "wrc-transformed-test"

PDF_BYTES = b"%PDF-1.4\nnot parsed, not converted\n%%EOF"
DOCX_BYTES = b"PK\x03\x04binary docx payload\x00\xff"
DOC_BYTES = b"\xd0\xcf\x11\xe0legacy word payload"
HTML_PAGE = load_fixture("detail_inline_html.html").encode("utf-8")


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,
        s3_endpoint_url=None,
        s3_access_key_id="testing",
        s3_secret_access_key="testing",
        landing_bucket=LANDING_BUCKET,
        s3_transformed_bucket=TRANSFORMED_BUCKET,
        mongo_database="wrc_test",
    )


@pytest.fixture
def zone(settings: Settings):
    """A landing zone plus a wired transformation run sharing one mongomock client."""
    with mock_aws():
        client = mongomock.MongoClient(tz_aware=True)  # matches production
        landing_mongo = MongoLandingStore(settings, client=client)
        landing_mongo.ensure_indexes()
        landing_store = ObjectStore(settings, bucket=LANDING_BUCKET)
        transformed_store = ObjectStore(settings, bucket=TRANSFORMED_BUCKET)

        def make_run(run_id: str = "t-run-1") -> TransformationRun:
            run = TransformationRun(
                run_id,
                settings,
                mongo=MongoTransformedStore(settings, client=client),
                landing_store=landing_store,
                transformed_store=transformed_store,
            )
            run.mongo.ensure_indexes()  # execute() does this; tests call transform_one directly
            return run

        yield landing_mongo, landing_store, transformed_store, make_run


def land(
    landing_mongo: MongoLandingStore,
    landing_store: ObjectStore,
    *,
    identifier: str,
    body: str = "Labour Court",
    document_type: str = "pdf",
    content: bytes = PDF_BYTES,
    version_hash: str | None = None,
    published: dt.date = dt.date(2024, 1, 30),
    content_type: str = "application/pdf",
    store_object: bool = True,
) -> dict[str, Any]:
    """Create one immutable landing version, exactly as ingestion would."""
    file_hash = sha256_hex(content)
    version = version_hash or file_hash
    key = f"landing/{body.lower().replace(' ', '-')}/{identifier}/{version}.{document_type}"
    if store_object:
        landing_store.put_if_absent(key, content, content_type=content_type)
    record = {
        "source": SOURCE,
        "body": body,
        "identifier": identifier,
        "title": identifier,
        "description": None,
        "reference_no": identifier,
        "published_date": dt.datetime.combine(published, dt.time.min, tzinfo=dt.UTC),
        "detail_url": f"https://www.workplacerelations.ie/en/cases/2024/february/{identifier}.html",
        "document_url": f"https://www.workplacerelations.ie/en/x/{identifier}.{document_type}",
        "document_type": document_type,
        "partition_date": dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
        "partition_start": dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
        "partition_end": dt.datetime(2024, 1, 31, tzinfo=dt.UTC),
        "scraped_at": dt.datetime(2024, 5, 1, tzinfo=dt.UTC),
        "run_id": "ingest-1",
        "file_hash": file_hash,
        "version_hash": version,
        "file_size": len(content),
        "content_type": content_type,
        "storage_bucket": LANDING_BUCKET,
        "storage_key": key,
    }
    record["_id"] = landing_mongo.landing.insert_one(dict(record)).inserted_id
    return record


def keys(store: ObjectStore) -> list[str]:
    response = store._client.list_objects_v2(Bucket=store.bucket)
    return sorted(o["Key"] for o in response.get("Contents", []))


# --- passthrough ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("document_type", "content", "content_type"),
    [
        ("pdf", PDF_BYTES, "application/pdf"),
        (
            "docx",
            DOCX_BYTES,
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ),
        ("doc", DOC_BYTES, "application/msword"),
        ("rtf", b"{\\rtf1 payload}", "application/rtf"),
    ],
)
def test_binary_documents_pass_through_byte_for_byte(
    zone, document_type: str, content: bytes, content_type: str
) -> None:
    landing_mongo, landing_store, transformed_store, make_run = zone
    landing = land(
        landing_mongo,
        landing_store,
        identifier="ADJ-00057058",
        document_type=document_type,
        content=content,
        content_type=content_type,
    )
    run = make_run()
    assert run.transform_one(landing) == "transformed"

    record = run.mongo.transformed.find_one({})
    assert record is not None
    stored = transformed_store.get(record["transformed_storage_path"])
    assert stored == content, "bytes must be identical, not re-encoded"
    assert record["transformed_file_hash"] == landing["file_hash"]
    assert record["transformed_file_size"] == len(content)
    assert record["content_type"] == content_type
    assert record["transformed_filename"] == f"ADJ-00057058.{document_type}"


def test_passthrough_does_not_parse_the_document(zone) -> None:
    """A corrupt PDF is still copied verbatim; nothing tries to read it."""
    landing_mongo, landing_store, transformed_store, make_run = zone
    junk = b"%PDF-1.4 truncated and invalid \x00\x01\x02"
    landing = land(landing_mongo, landing_store, identifier="ADJ-1", content=junk)
    run = make_run()
    assert run.transform_one(landing) == "transformed"
    record = run.mongo.transformed.find_one({})
    assert transformed_store.get(record["transformed_storage_path"]) == junk


def test_a_slash_identifier_keeps_its_filename_and_one_path_segment(zone) -> None:
    landing_mongo, landing_store, _, make_run = zone
    landing = land(
        landing_mongo, landing_store, identifier="UD570/2009", body="Employment Appeals Tribunal"
    )
    run = make_run()
    assert run.transform_one(landing) == "transformed"

    record = run.mongo.transformed.find_one({})
    assert record["transformed_filename"] == "UD570%2F2009.pdf"
    assert record["identifier"] == "UD570/2009", "the original identifier is never mutated"
    assert record["transformed_storage_path"].endswith("/UD570%2F2009.pdf")
    assert len(record["transformed_storage_path"].split("/")) == 5


# --- html -----------------------------------------------------------------------


def test_html_is_cleaned_stored_and_hashed(zone) -> None:
    landing_mongo, landing_store, transformed_store, make_run = zone
    landing = land(
        landing_mongo,
        landing_store,
        identifier="LCR22912",
        document_type="html",
        content=HTML_PAGE,
        content_type="text/html; charset=utf-8",
    )
    run = make_run()
    assert run.transform_one(landing) == "transformed"

    record = run.mongo.transformed.find_one({})
    stored = transformed_store.get(record["transformed_storage_path"])
    assert record["transformed_filename"] == "LCR22912.html"
    assert record["content_type"] == "text/html; charset=utf-8"
    assert record["transformed_file_hash"] == sha256_hex(stored)
    assert record["transformed_file_size"] == len(stored)
    assert record["transformed_file_hash"] != landing["file_hash"], "HTML is transformed"

    text = stored.decode("utf-8")
    assert "Return to Search" not in text
    assert "<script" not in text
    assert "SONOMA VALLEY" in text
    assert "<table>" in text


def test_html_that_is_not_a_decision_page_fails_and_stores_nothing(zone) -> None:
    landing_mongo, landing_store, transformed_store, make_run = zone
    landing = land(
        landing_mongo,
        landing_store,
        identifier="BROKEN-1",
        document_type="html",
        content=b"<html><body><header>nav</header><p>Page not found</p></body></html>",
        content_type="text/html",
    )
    run = make_run()
    assert run.transform_one(landing) == "failed"
    assert run.mongo.transformed.count_documents({}) == 0
    assert keys(transformed_store) == [], "the whole website page must never be stored"
    assert run.tally.failed == 1


# --- idempotency ----------------------------------------------------------------


def test_rerunning_the_same_landing_version_changes_nothing(zone) -> None:
    landing_mongo, landing_store, transformed_store, make_run = zone
    landing = land(landing_mongo, landing_store, identifier="ADJ-1")

    first = make_run("t-run-1")
    assert first.transform_one(landing) == "transformed"
    second = make_run("t-run-2")
    assert second.transform_one(landing) == "unchanged"

    assert second.mongo.transformed.count_documents({}) == 1
    assert len(keys(transformed_store)) == 1
    assert second.mongo.transformed.find_one({})["run_id"] == "t-run-1"
    assert (first.tally.transformed, second.tally.transformed) == (1, 0)
    assert second.tally.unchanged == 1


def test_the_unchanged_path_does_not_read_the_landing_object(zone) -> None:
    """The short-circuit is by metadata alone, so a re-run costs no object reads."""
    landing_mongo, landing_store, _, make_run = zone
    landing = land(landing_mongo, landing_store, identifier="ADJ-1")
    make_run("t-run-1").transform_one(landing)

    run = make_run("t-run-2")
    with patch.object(run.landing_store, "get", side_effect=AssertionError("must not read")):
        assert run.transform_one(landing) == "unchanged"


def test_a_new_transformation_version_produces_a_new_transformed_version(zone) -> None:
    landing_mongo, landing_store, transformed_store, make_run = zone
    landing = land(landing_mongo, landing_store, identifier="ADJ-1")
    make_run("t-run-1").transform_one(landing)

    with patch("wrc_pipeline.transformation.transformer.TRANSFORMATION_VERSION", 2):
        run = make_run("t-run-2")
        assert run.transform_one(landing) == "transformed"

    versions = sorted(d["transformation_version"] for d in run.mongo.transformed.find({}))
    assert versions == [TRANSFORMATION_VERSION, 2]
    assert run.mongo.transformed.count_documents({}) == 2
    assert len(keys(transformed_store)) == 1, "same landing bytes, same content-addressed key"


def test_a_changed_landing_version_produces_a_new_transformed_version(zone) -> None:
    landing_mongo, landing_store, transformed_store, make_run = zone
    first = land(landing_mongo, landing_store, identifier="ADJ-1", content=PDF_BYTES)
    amended = b"%PDF-1.4\namended decision\n%%EOF"
    second = land(landing_mongo, landing_store, identifier="ADJ-1", content=amended)

    run = make_run()
    assert run.transform_one(first) == "transformed"
    assert run.transform_one(second) == "transformed"

    assert run.mongo.transformed.count_documents({}) == 2
    assert len(keys(transformed_store)) == 2
    hashes = {d["transformed_file_hash"] for d in run.mongo.transformed.find({})}
    assert hashes == {sha256_hex(PDF_BYTES), sha256_hex(amended)}
    # both versions keep the identifier.ext filename, separated by the version directory
    assert all(
        d["transformed_storage_path"].endswith("/ADJ-1.pdf") for d in run.mongo.transformed.find({})
    )


def test_the_unique_index_rejects_a_duplicate_transformed_version(zone) -> None:
    landing_mongo, landing_store, _, make_run = zone
    landing = land(landing_mongo, landing_store, identifier="ADJ-1")
    run = make_run()
    run.transform_one(landing)

    duplicate = dict(run.mongo.transformed.find_one({}))
    duplicate.pop("_id")
    assert run.mongo.insert_transformed(duplicate) is None


# --- the landing zone stays untouched -------------------------------------------


def test_transformation_never_writes_to_the_landing_zone(zone) -> None:
    landing_mongo, landing_store, _, make_run = zone
    landing = land(landing_mongo, landing_store, identifier="ADJ-1")
    before = dict(landing_mongo.landing.find_one({"_id": landing["_id"]}))
    before_object = landing_store.get(landing["storage_key"])
    before_keys = keys(landing_store)

    run = make_run()
    run.transform_one(landing)

    assert dict(landing_mongo.landing.find_one({"_id": landing["_id"]})) == before
    assert landing_store.get(landing["storage_key"]) == before_object
    assert keys(landing_store) == before_keys
    assert landing_mongo.landing.count_documents({}) == 1


def test_transformed_records_go_to_their_own_collection(zone) -> None:
    landing_mongo, landing_store, _, make_run = zone
    landing = land(landing_mongo, landing_store, identifier="ADJ-1")
    run = make_run()
    run.transform_one(landing)

    assert run.mongo.transformed.name != landing_mongo.landing.name
    assert landing_mongo.landing.count_documents({"transformation_version": {"$exists": True}}) == 0


# --- lineage --------------------------------------------------------------------


def test_the_transformed_record_carries_full_lineage(zone) -> None:
    landing_mongo, landing_store, _, make_run = zone
    landing = land(landing_mongo, landing_store, identifier="ADJ-1")
    run = make_run("t-run-9")
    run.transform_one(landing)

    record = run.mongo.transformed.find_one({})
    assert record["source"] == SOURCE
    assert record["body"] == landing["body"]
    assert record["identifier"] == landing["identifier"]
    assert record["published_date"] == landing["published_date"]
    assert record["landing_metadata_id"] == landing["_id"]
    assert record["landing_storage_path"] == landing["storage_key"]
    assert record["landing_storage_bucket"] == LANDING_BUCKET
    assert record["landing_file_hash"] == landing["file_hash"]
    assert record["landing_version_hash"] == landing["version_hash"]
    assert record["transformation_version"] == TRANSFORMATION_VERSION
    assert record["transformed_storage_bucket"] == TRANSFORMED_BUCKET
    assert record["run_id"] == "t-run-9"
    assert record["transformed_at"].tzinfo is not None


# --- failures -------------------------------------------------------------------


def test_a_missing_landing_object_fails_cleanly(zone, caplog) -> None:
    import logging

    landing_mongo, landing_store, transformed_store, make_run = zone
    landing = land(landing_mongo, landing_store, identifier="GONE-1", store_object=False)

    run = make_run()
    with caplog.at_level(logging.ERROR):
        assert run.transform_one(landing) == "failed"

    record = next(r for r in caplog.records if r.msg == "transformation_document_failed")
    assert record.identifier == "GONE-1"
    assert record.landing_storage_path == landing["storage_key"]
    assert record.body == "Labour Court"
    assert record.error
    assert run.mongo.transformed.count_documents({}) == 0
    assert keys(transformed_store) == []


def test_a_corrupt_landing_object_is_refused(zone) -> None:
    """The recorded file_hash is checked before anything is transformed."""
    landing_mongo, landing_store, transformed_store, make_run = zone
    landing = land(landing_mongo, landing_store, identifier="ADJ-1")
    landing_store._client.put_object(
        Bucket=LANDING_BUCKET, Key=landing["storage_key"], Body=b"tampered"
    )

    run = make_run()
    assert run.transform_one(landing) == "failed"
    assert keys(transformed_store) == []


def test_one_failure_does_not_stop_the_run(zone) -> None:
    landing_mongo, landing_store, _, make_run = zone
    good_a = land(landing_mongo, landing_store, identifier="OK-1", content=b"%PDF a")
    broken = land(landing_mongo, landing_store, identifier="BAD-1", store_object=False)
    good_b = land(landing_mongo, landing_store, identifier="OK-2", content=b"%PDF b")

    run = make_run()
    tally = run.process([good_a, broken, good_b])

    assert (tally.selected, tally.transformed, tally.unchanged, tally.failed) == (3, 2, 0, 1)
    assert tally.balanced


# --- accounting -----------------------------------------------------------------


def test_the_accounting_invariant_holds_across_mixed_outcomes(zone) -> None:
    landing_mongo, landing_store, _, make_run = zone
    already = land(landing_mongo, landing_store, identifier="OLD-1", content=b"%PDF old")
    make_run("t-run-0").transform_one(already)

    fresh = land(landing_mongo, landing_store, identifier="NEW-1", content=b"%PDF new")
    broken = land(landing_mongo, landing_store, identifier="BAD-1", store_object=False)

    run = make_run("t-run-1")
    tally = run.process([already, fresh, broken])

    assert tally.summary() == {
        "records_selected": 3,
        "records_transformed": 1,
        "records_unchanged": 1,
        "records_failed": 1,
        "accounting_balanced": True,
    }


def test_run_start_and_completion_are_logged_with_the_run_id(zone, caplog) -> None:
    import logging

    landing_mongo, landing_store, _, make_run = zone
    land(landing_mongo, landing_store, identifier="ADJ-1")

    run = make_run("t-run-5")
    with caplog.at_level(logging.INFO):
        run.execute(dt.date(2024, 1, 1), dt.date(2024, 1, 31))

    events = {r.msg: r for r in caplog.records}
    assert events["transformation_run_started"].run_id == "t-run-5"
    assert events["transformation_run_started"].date_field == "published_date"
    assert events["transformation_run_completed"].records_selected == 1
    assert events["transformation_run_completed"].accounting_balanced is True


# --- selection ------------------------------------------------------------------


def test_selection_is_inclusive_of_both_endpoints(zone) -> None:
    landing_mongo, landing_store, _, make_run = zone
    for day, ident in ((1, "A"), (15, "B"), (31, "C")):
        land(landing_mongo, landing_store, identifier=ident, published=dt.date(2024, 1, day))

    run = make_run()
    selected = list(run.mongo.select_landing_versions(dt.date(2024, 1, 1), dt.date(2024, 1, 31)))
    assert {d["identifier"] for d in selected} == {"A", "B", "C"}

    narrow = list(run.mongo.select_landing_versions(dt.date(2024, 1, 15), dt.date(2024, 1, 15)))
    assert [d["identifier"] for d in narrow] == ["B"]


def test_selection_can_be_filtered_by_body(zone) -> None:
    landing_mongo, landing_store, _, make_run = zone
    land(landing_mongo, landing_store, identifier="LC-1", body="Labour Court")
    land(landing_mongo, landing_store, identifier="EAT-1", body="Employment Appeals Tribunal")

    run = make_run()
    selected = list(
        run.mongo.select_landing_versions(
            dt.date(2024, 1, 1), dt.date(2024, 1, 31), ["Labour Court"]
        )
    )
    assert [d["identifier"] for d in selected] == ["LC-1"]


def test_a_reversed_date_range_is_rejected(zone) -> None:
    _, _, _, make_run = zone
    with pytest.raises(ValueError, match="is after end_date"):
        make_run().execute(dt.date(2024, 2, 1), dt.date(2024, 1, 1))
