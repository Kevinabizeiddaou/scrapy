"""Landing-zone immutability and idempotency, against mongomock and moto S3."""

from __future__ import annotations

import datetime as dt
from typing import Any

import mongomock
import pytest
from moto import mock_aws
from scrapy.exceptions import DropItem

from wrc_pipeline.config import Settings
from wrc_pipeline.ingestion.accounting import RunAccounting
from wrc_pipeline.ingestion.items import LandingDocument
from wrc_pipeline.ingestion.partitions import DatePartition
from wrc_pipeline.ingestion.pipelines import LandingZonePipeline
from wrc_pipeline.logging import EventLogger
from wrc_pipeline.storage.mongo import MongoLandingStore
from wrc_pipeline.storage.object_store import ObjectStore, StoredObject, sha256_hex

BODY = "Labour Court"
BODY_ID = "3"
IDENTIFIER = "LCR22912"
SOURCE = "workplacerelations.ie"
PARTITION = DatePartition(dt.date(2024, 1, 15), dt.date(2024, 1, 31))
PDF_V1 = b"%PDF-1.4\nversion one\n%%EOF"
PDF_V2 = b"%PDF-1.4\nversion two, amended\n%%EOF"


class SpiderStub:
    """The narrow slice of the spider that the pipeline actually touches."""

    def __init__(self, run_id: str = "run-1") -> None:
        self.run_id = run_id
        self.events = EventLogger(run_id)
        self.accounting = RunAccounting(self.events)
        self.accounting.start_partition(BODY, BODY_ID, PARTITION)

    @property
    def counters(self) -> Any:
        return self.accounting.partitions[(BODY_ID, PARTITION.key)]


def make_item(
    content: bytes = PDF_V1,
    *,
    identifier: str = IDENTIFIER,
    run_id: str = "run-1",
    document_type: str = "pdf",
    etag: str | None = '"etag-v1"',
) -> LandingDocument:
    return LandingDocument(
        source=SOURCE,
        body=BODY,
        body_id=BODY_ID,
        identifier=identifier,
        title=identifier,
        description="SONOMA VALLEY AND A WORKER",
        reference_no=identifier,
        published_date=dt.date(2024, 1, 30),
        detail_url=f"https://www.workplacerelations.ie/en/cases/2024/february/{identifier}.html",
        document_url=f"https://www.workplacerelations.ie/en/eat_import/{identifier}.pdf",
        document_type=document_type,
        partition_date=PARTITION.partition_date,
        partition_start=PARTITION.start,
        partition_end=PARTITION.end,
        scraped_at=dt.datetime(2024, 5, 1, 12, 0, tzinfo=dt.UTC),
        run_id=run_id,
        content=content,
        content_type="application/pdf",
        etag=etag,
        last_modified="Wed, 03 Jul 2013 04:27:29",
    )


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,
        s3_endpoint_url=None,  # let moto intercept
        s3_access_key_id="testing",
        s3_secret_access_key="testing",
        landing_bucket="wrc-landing-test",
        mongo_database="wrc_landing_test",
    )


@pytest.fixture
def landing(settings: Settings):
    """A wired-up pipeline plus direct handles on the two stores."""
    with mock_aws():
        mongo = MongoLandingStore(settings, client=mongomock.MongoClient())
        mongo.ensure_indexes()
        store = ObjectStore(settings)
        pipeline = LandingZonePipeline(settings=settings, mongo=mongo, object_store=store)
        yield pipeline, mongo, store


def stored_stub(data: bytes, key: str = "k") -> StoredObject:
    return StoredObject(key=key, uploaded=True, file_hash=sha256_hex(data), file_size=len(data))


def object_keys(store: ObjectStore) -> list[str]:
    response = store._client.list_objects_v2(Bucket=store.bucket)
    return sorted(obj["Key"] for obj in response.get("Contents", []))


# --- re-running the same crawl --------------------------------------------------


def test_identical_crawl_twice_lands_exactly_one_version(landing) -> None:
    pipeline, mongo, store = landing

    first, second = SpiderStub("run-1"), SpiderStub("run-2")
    pipeline.process_item(make_item(PDF_V1), first)
    pipeline.process_item(make_item(PDF_V1, run_id="run-2"), second)

    assert mongo.landing.count_documents({}) == 1
    assert len(object_keys(store)) == 1
    assert (first.counters.successful, first.counters.unchanged) == (1, 0)
    assert (second.counters.successful, second.counters.unchanged) == (0, 1)


def test_second_run_does_not_reupload_the_object(landing) -> None:
    pipeline, _, store = landing
    pipeline.process_item(make_item(PDF_V1), SpiderStub("run-1"))
    key = object_keys(store)[0]
    before = store._client.head_object(Bucket=store.bucket, Key=key)["LastModified"]

    pipeline.process_item(make_item(PDF_V1, run_id="run-2"), SpiderStub("run-2"))
    after = store._client.head_object(Bucket=store.bucket, Key=key)["LastModified"]
    assert before == after


def test_first_run_records_the_original_run_id(landing) -> None:
    pipeline, mongo, _ = landing
    pipeline.process_item(make_item(PDF_V1), SpiderStub("run-1"))
    pipeline.process_item(make_item(PDF_V1, run_id="run-2"), SpiderStub("run-2"))

    record = mongo.landing.find_one({})
    assert record is not None
    assert record["run_id"] == "run-1", "landing metadata must never be rewritten"


# --- changed content ------------------------------------------------------------


def test_changed_content_creates_a_new_immutable_version(landing) -> None:
    pipeline, mongo, store = landing
    pipeline.process_item(make_item(PDF_V1), SpiderStub("run-1"))
    spider = SpiderStub("run-2")
    pipeline.process_item(make_item(PDF_V2, run_id="run-2", etag='"etag-v2"'), spider)

    assert mongo.landing.count_documents({}) == 2
    assert len(object_keys(store)) == 2
    assert spider.counters.successful == 1

    hashes = {doc["file_hash"] for doc in mongo.landing.find({})}
    assert hashes == {sha256_hex(PDF_V1), sha256_hex(PDF_V2)}


def test_old_version_bytes_are_preserved_after_a_change(landing) -> None:
    pipeline, mongo, store = landing
    pipeline.process_item(make_item(PDF_V1), SpiderStub("run-1"))
    pipeline.process_item(make_item(PDF_V2, run_id="run-2"), SpiderStub("run-2"))

    old = mongo.landing.find_one({"file_hash": sha256_hex(PDF_V1)})
    assert old is not None
    assert store.get(old["storage_key"]) == PDF_V1


def test_state_tracks_only_the_latest_hash(landing) -> None:
    pipeline, mongo, _ = landing
    pipeline.process_item(make_item(PDF_V1), SpiderStub("run-1"))
    pipeline.process_item(make_item(PDF_V2, run_id="run-2", etag='"etag-v2"'), SpiderStub("run-2"))

    state = mongo.get_state(SOURCE, BODY, IDENTIFIER)
    assert state is not None
    assert state["latest_hash"] == sha256_hex(PDF_V2)
    assert state["etag"] == '"etag-v2"'
    assert mongo.state.count_documents({}) == 1


def test_reverted_content_matches_the_earlier_version_and_lands_nothing_new(landing) -> None:
    pipeline, mongo, store = landing
    pipeline.process_item(make_item(PDF_V1), SpiderStub("run-1"))
    pipeline.process_item(make_item(PDF_V2, run_id="run-2"), SpiderStub("run-2"))

    spider = SpiderStub("run-3")
    pipeline.process_item(make_item(PDF_V1, run_id="run-3"), spider)

    assert mongo.landing.count_documents({}) == 2, "the reverted version already existed"
    assert len(object_keys(store)) == 2
    assert (spider.counters.successful, spider.counters.unchanged) == (0, 1)


# --- the unique index itself ----------------------------------------------------


def test_unique_index_rejects_a_duplicate_version(settings: Settings) -> None:
    mongo = MongoLandingStore(settings, client=mongomock.MongoClient())
    mongo.ensure_indexes()
    metadata = make_item(PDF_V1).to_metadata(stored=stored_stub(PDF_V1), bucket="b")

    assert mongo.insert_version(metadata) is not None
    assert mongo.insert_version(metadata) is None, "second insert must be rejected, not raise"
    assert mongo.landing.count_documents({}) == 1


def test_unique_index_is_scoped_per_body_and_identifier(settings: Settings) -> None:
    mongo = MongoLandingStore(settings, client=mongomock.MongoClient())
    mongo.ensure_indexes()
    for identifier in ("LCR22912", "LCR22913"):
        metadata = make_item(PDF_V1, identifier=identifier).to_metadata(
            stored=stored_stub(PDF_V1), bucket="b"
        )
        assert mongo.insert_version(metadata) is not None
    assert mongo.landing.count_documents({}) == 2


# --- bytes and metadata fidelity ------------------------------------------------


def test_document_bytes_are_stored_exactly(landing) -> None:
    pipeline, mongo, store = landing
    payload = b"\x00\x01\x02%PDF-1.7 binary \xff\xfe payload"
    pipeline.process_item(make_item(payload), SpiderStub())

    record = mongo.landing.find_one({})
    assert record is not None
    assert store.get(record["storage_key"]) == payload
    assert record["file_size"] == len(payload)
    assert record["file_hash"] == sha256_hex(payload)
    assert record["content_type"] == "application/pdf"


def test_metadata_carries_every_required_field(landing) -> None:
    pipeline, mongo, _ = landing
    pipeline.process_item(make_item(PDF_V1), SpiderStub())
    record = mongo.landing.find_one({})
    assert record is not None

    required = {
        "source",
        "body",
        "identifier",
        "reference_no",
        "title",
        "description",
        "published_date",
        "detail_url",
        "document_url",
        "document_type",
        "partition_date",
        "partition_start",
        "partition_end",
        "scraped_at",
        "run_id",
        "file_hash",
        "file_size",
        "content_type",
        "storage_bucket",
        "storage_key",
    }
    assert required <= record.keys()
    assert record["partition_date"].date() == PARTITION.start
    assert "content" not in record, "raw bytes must never be written to Mongo"


def test_html_documents_land_as_html(landing) -> None:
    pipeline, mongo, _ = landing
    html = b"<html><body>original, untransformed</body></html>"
    item = make_item(html, document_type="html")
    item.content_type = "text/html; charset=utf-8"
    pipeline.process_item(item, SpiderStub())

    record = mongo.landing.find_one({})
    assert record is not None
    assert record["document_type"] == "html"
    assert record["storage_key"].endswith(".html")


# --- failure accounting ---------------------------------------------------------


def test_storage_failure_is_logged_dropped_and_counted(landing) -> None:
    pipeline, mongo, store = landing
    spider = SpiderStub()

    def explode(*args: object, **kwargs: object) -> bool:
        raise OSError("MinIO unreachable")

    store.put_if_absent = explode  # type: ignore[method-assign]

    with pytest.raises(DropItem):
        pipeline.process_item(make_item(PDF_V1), spider)

    assert mongo.landing.count_documents({}) == 0
    assert spider.counters.failed == 1
    assert spider.counters.successful == 0


# --- volatile HTML: version identity vs stored-byte integrity -------------------

HTML_V1 = b"<html><body>decision text</body></html>\n<!-- Elapsed time: 0.1562556 -->"
HTML_V1_REFETCHED = b"<html><body>decision text</body></html>\n<!-- Elapsed time: 0.0312295 -->"
HTML_V2 = b"<html><body>decision text, amended</body></html>\n<!-- Elapsed time: 0.1562556 -->"
# Served from the CMS cache: an extra marker appears before the timing comment.
HTML_V1_FROM_CACHE = (
    b"<html><body>decision text</body></html>\n"
    b"<!-- cached or not being index.aspx page --><!-- Elapsed time: 0 -->"
)


def html_item(content: bytes, run_id: str = "run-1") -> LandingDocument:
    item = make_item(content, run_id=run_id, document_type="html", etag=None)
    item.content_type = "text/html; charset=utf-8"
    item.last_modified = None
    return item


def test_render_timing_comment_is_excluded_from_the_version_identity() -> None:
    first, refetched = html_item(HTML_V1), html_item(HTML_V1_REFETCHED)
    assert first.file_hash != refetched.file_hash, "raw bytes genuinely differ"
    assert first.version_hash == refetched.version_hash


def test_a_real_html_change_still_changes_the_version_identity() -> None:
    assert html_item(HTML_V1).version_hash != html_item(HTML_V2).version_hash


def test_attachment_version_identity_is_just_its_byte_hash() -> None:
    item = make_item(PDF_V1)
    assert item.version_hash == item.file_hash == sha256_hex(PDF_V1)


def test_refetched_html_lands_no_second_version(landing) -> None:
    pipeline, mongo, store = landing
    pipeline.process_item(html_item(HTML_V1), SpiderStub("run-1"))
    spider = SpiderStub("run-2")
    pipeline.process_item(html_item(HTML_V1_REFETCHED, run_id="run-2"), spider)

    assert mongo.landing.count_documents({}) == 1
    assert len(object_keys(store)) == 1
    assert (spider.counters.successful, spider.counters.unchanged) == (0, 1)


def test_changed_html_lands_a_new_version(landing) -> None:
    pipeline, mongo, store = landing
    pipeline.process_item(html_item(HTML_V1), SpiderStub("run-1"))
    spider = SpiderStub("run-2")
    pipeline.process_item(html_item(HTML_V2, run_id="run-2"), spider)

    assert mongo.landing.count_documents({}) == 2
    assert len(object_keys(store)) == 2
    assert spider.counters.successful == 1


def test_stored_html_is_the_untransformed_original(landing) -> None:
    pipeline, mongo, store = landing
    pipeline.process_item(html_item(HTML_V1), SpiderStub("run-1"))

    record = mongo.landing.find_one({})
    assert record is not None
    stored = store.get(record["storage_key"])
    assert stored == HTML_V1
    assert b"<!-- Elapsed time: 0.1562556 -->" in stored, "nothing is stripped from the original"
    assert record["file_hash"] == sha256_hex(HTML_V1)
    assert record["version_hash"] != record["file_hash"]


def test_object_key_is_named_by_the_version_hash(landing) -> None:
    pipeline, mongo, _ = landing
    pipeline.process_item(html_item(HTML_V1), SpiderStub("run-1"))

    record = mongo.landing.find_one({})
    assert record is not None
    assert record["storage_key"].endswith(f"{record['version_hash']}.html")


# --- crash between the object write and the metadata write ----------------------


def test_metadata_file_hash_always_describes_the_stored_object(landing) -> None:
    """Regression: object uploaded, Mongo insert failed, then the page is refetched.

    The refetch has different raw bytes (new render-timing comment) but the same
    version_hash, so it maps to the occupied key and is not re-uploaded. The metadata must
    describe the bytes that are actually there, not the ones this run happened to download.
    """
    pipeline, mongo, store = landing

    def boom(_metadata: dict) -> None:
        raise RuntimeError("mongo write timeout")

    real_insert = mongo.insert_version
    mongo.insert_version = boom  # type: ignore[method-assign]
    with pytest.raises(DropItem):
        pipeline.process_item(html_item(HTML_V1), SpiderStub("run-1"))
    mongo.insert_version = real_insert  # type: ignore[method-assign]

    assert len(object_keys(store)) == 1, "the object landed before the metadata failed"
    assert mongo.landing.count_documents({}) == 0

    pipeline.process_item(html_item(HTML_V1_REFETCHED, run_id="run-2"), SpiderStub("run-2"))

    record = mongo.landing.find_one({})
    assert record is not None
    stored_bytes = store.get(record["storage_key"])
    assert record["file_hash"] == sha256_hex(stored_bytes)
    assert record["file_size"] == len(stored_bytes)
    assert stored_bytes == HTML_V1, "the first run's bytes are the ones that were kept"


def test_every_landed_record_matches_its_object(landing) -> None:
    pipeline, mongo, store = landing
    for run, content in (("run-1", HTML_V1), ("run-2", HTML_V2), ("run-3", PDF_V1)):
        item = html_item(content, run_id=run) if content is not PDF_V1 else make_item(PDF_V1)
        pipeline.process_item(item, SpiderStub(run))

    for record in mongo.landing.find({}):
        raw = store.get(record["storage_key"])
        assert record["file_hash"] == sha256_hex(raw)
        assert record["file_size"] == len(raw)


def test_a_lost_state_row_converges_on_the_next_run(landing) -> None:
    """State is rebuildable: an already-landed version must repoint state, not loop."""
    pipeline, mongo, store = landing
    pipeline.process_item(make_item(PDF_V1), SpiderStub("run-1"))
    landed_id = mongo.landing.find_one({})["_id"]
    mongo.state.delete_many({})  # simulate a lost or never-written state row

    spider = SpiderStub("run-2")
    pipeline.process_item(make_item(PDF_V1, run_id="run-2"), spider)

    state = mongo.get_state(SOURCE, BODY, IDENTIFIER)
    assert state is not None, "state must be reinstated, not left missing forever"
    assert state["latest_hash"] == sha256_hex(PDF_V1)
    assert state["latest_metadata_id"] == landed_id
    assert mongo.landing.count_documents({}) == 1
    assert (spider.counters.successful, spider.counters.unchanged) == (0, 1)

    # and the run after that takes the cheap path
    third = SpiderStub("run-3")
    pipeline.process_item(make_item(PDF_V1, run_id="run-3"), third)
    assert third.counters.unchanged == 1
    assert mongo.landing.count_documents({}) == 1
    assert len(object_keys(store)) == 1


def test_a_cache_hit_marker_does_not_change_the_version_identity() -> None:
    """The CMS prepends '<!-- cached or not being index.aspx page -->' on a cache hit."""
    direct, cached = html_item(HTML_V1), html_item(HTML_V1_FROM_CACHE)
    assert direct.file_hash != cached.file_hash, "raw bytes genuinely differ"
    assert direct.version_hash == cached.version_hash


def test_a_cached_refetch_lands_no_second_version(landing) -> None:
    pipeline, mongo, store = landing
    pipeline.process_item(html_item(HTML_V1), SpiderStub("run-1"))
    spider = SpiderStub("run-2")
    pipeline.process_item(html_item(HTML_V1_FROM_CACHE, run_id="run-2"), spider)

    assert mongo.landing.count_documents({}) == 1
    assert len(object_keys(store)) == 1
    assert (spider.counters.successful, spider.counters.unchanged) == (0, 1)


def test_cached_and_uncached_bytes_are_both_preserved_verbatim_when_content_changes(
    landing,
) -> None:
    """Whatever landed first keeps its exact bytes, cache marker and all."""
    pipeline, mongo, store = landing
    pipeline.process_item(html_item(HTML_V1_FROM_CACHE), SpiderStub("run-1"))
    pipeline.process_item(html_item(HTML_V2, run_id="run-2"), SpiderStub("run-2"))

    first = mongo.landing.find_one({"run_id": "run-1"})
    assert store.get(first["storage_key"]) == HTML_V1_FROM_CACHE
    assert b"cached or not being index.aspx page" in store.get(first["storage_key"])
    assert mongo.landing.count_documents({}) == 2


# --- the normalisation must not be able to hide a legal edit --------------------

LEGAL = b"<html><body><p>The Court awards the complainant EUR 17,000.</p></body></html>"


@pytest.mark.parametrize(
    ("label", "other"),
    [
        ("render time differs", LEGAL + b"\n<!-- Elapsed time: 9.9 -->"),
        (
            "cache marker appears",
            LEGAL + b"\n<!-- cached or not being index.aspx page --><!-- Elapsed time: 0 -->",
        ),
    ],
)
def test_only_the_two_known_server_markers_are_ignored(label: str, other: bytes) -> None:
    baseline = html_item(LEGAL + b"\n<!-- Elapsed time: 0.1 -->")
    assert baseline.version_hash == html_item(other).version_hash, label


@pytest.mark.parametrize(
    ("label", "edited"),
    [
        ("award amount changed", LEGAL.replace(b"17,000", b"18,000")),
        ("one letter changed", LEGAL.replace(b"awards", b"awardz")),
        ("a word removed", LEGAL.replace(b"the complainant ", b"")),
        ("whitespace changed", LEGAL.replace(b"<p>The", b"<p>  The")),
        ("an unrelated comment differs", LEGAL + b"<!-- reviewed by AB -->"),
        ("a tag changed", LEGAL.replace(b"<p>", b"<h2>").replace(b"</p>", b"</h2>")),
    ],
)
def test_any_real_content_change_changes_the_version_identity(label: str, edited: bytes) -> None:
    baseline = html_item(LEGAL + b"\n<!-- Elapsed time: 0.1 -->")
    assert (
        baseline.version_hash != html_item(edited + b"\n<!-- Elapsed time: 0.1 -->").version_hash
    ), label


def test_an_edited_decision_lands_a_new_version_rather_than_reporting_unchanged(landing) -> None:
    pipeline, mongo, store = landing
    original = LEGAL + b"\n<!-- Elapsed time: 0.1 -->"
    amended = LEGAL.replace(b"17,000", b"18,000") + b"\n<!-- Elapsed time: 0.4 -->"

    pipeline.process_item(html_item(original), SpiderStub("run-1"))
    spider = SpiderStub("run-2")
    pipeline.process_item(html_item(amended, run_id="run-2"), spider)

    assert mongo.landing.count_documents({}) == 2, "an edited decision is a new version"
    assert len(object_keys(store)) == 2
    assert spider.counters.successful == 1


def test_the_stored_bytes_are_never_normalised(landing) -> None:
    """Normalisation exists only to compute the identity; storage stays byte-exact."""
    pipeline, mongo, store = landing
    raw = LEGAL + b"\n<!-- cached or not being index.aspx page --><!-- Elapsed time: 0.25 -->"
    pipeline.process_item(html_item(raw), SpiderStub("run-1"))

    record = mongo.landing.find_one({})
    stored = store.get(record["storage_key"])
    assert stored == raw
    assert record["file_hash"] == sha256_hex(raw)
    assert record["file_hash"] != record["version_hash"]
