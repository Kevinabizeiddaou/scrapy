"""The landing-zone item pipeline: hash, store bytes once, append metadata once.

Landing is append-only. Nothing here updates or deletes an existing landing record or
landing object; a changed document simply hashes differently and lands beside its
predecessor. The only mutable write is to the operational ``landing_state`` collection.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from scrapy.exceptions import DropItem

from wrc_pipeline.config import Settings, get_settings
from wrc_pipeline.ingestion.items import LandingDocument
from wrc_pipeline.storage.mongo import MongoLandingStore
from wrc_pipeline.storage.object_store import ObjectStore, build_object_key, slugify

if TYPE_CHECKING:
    from wrc_pipeline.ingestion.spider import WrcDecisionsSpider


class LandingZonePipeline:
    """Persists each :class:`LandingDocument` into MinIO/S3 and MongoDB."""

    def __init__(
        self,
        settings: Settings | None = None,
        mongo: MongoLandingStore | None = None,
        object_store: ObjectStore | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._mongo = mongo
        self._object_store = object_store
        self._owns_resources = mongo is None and object_store is None

    @classmethod
    def from_crawler(cls, crawler: Any) -> LandingZonePipeline:  # noqa: ARG003
        return cls()

    def open_spider(self, spider: WrcDecisionsSpider) -> None:
        if self._mongo is None:
            self._mongo = MongoLandingStore(self.settings)
        if self._object_store is None:
            self._object_store = ObjectStore(self.settings)
        self._mongo.ensure_indexes()
        spider.events.event(
            "landing_zone_ready",
            mongo_database=self.settings.mongo_database,
            landing_collection=self.settings.mongo_landing_collection,
            state_collection=self.settings.mongo_state_collection,
            landing_bucket=self.settings.landing_bucket,
            s3_endpoint_url=self.settings.s3_endpoint_url,
        )

    # `spider` is part of Scrapy's pipeline hook signature.
    def close_spider(self, spider: WrcDecisionsSpider) -> None:  # noqa: ARG002
        if not self._owns_resources:
            return
        if self._mongo is not None:
            self._mongo.close()
        if self._object_store is not None:
            self._object_store.close()

    def process_item(self, item: LandingDocument, spider: WrcDecisionsSpider) -> LandingDocument:
        assert self._mongo is not None and self._object_store is not None
        key = (item.body_id, item.partition_start.isoformat())
        # The version identity, not the raw-byte hash: see LandingDocument.version_hash.
        version_hash = item.version_hash
        object_key = build_object_key(
            body=item.body,
            partition_date=item.partition_date,
            identifier=item.identifier,
            version_hash=version_hash,
            document_type=item.document_type,
        )

        fields = self._log_fields(item, version_hash=version_hash, object_key=object_key)

        try:
            outcome = self._land(item, version_hash=version_hash, object_key=object_key)
        except Exception as exc:  # deliberate boundary: one bad record must not kill the run
            spider.events.error(
                "document_persist_failed",
                error=str(exc),
                error_type=type(exc).__name__,
                **fields,
            )
            spider.accounting.record_failed(key)
            spider.accounting.complete_partition_if_done(key)
            raise DropItem(f"persist failed for {item.identifier}") from exc

        if stored := outcome.get("stored"):
            fields["file_hash"] = stored.file_hash
            fields["file_size"] = stored.file_size

        if outcome["persisted"]:
            spider.events.event("document_persisted", object_uploaded=outcome["uploaded"], **fields)
            spider.accounting.record_successful(key)
        else:
            spider.events.event("document_unchanged", reason=outcome["reason"], **fields)
            spider.accounting.record_unchanged(key)

        spider.accounting.complete_partition_if_done(key)
        return item

    def _log_fields(
        self, item: LandingDocument, *, version_hash: str, object_key: str
    ) -> dict[str, Any]:
        return {
            "identifier": item.identifier,
            "body": item.body,
            "body_id": item.body_id,
            "partition_date": item.partition_date.isoformat(),
            "partition_start": item.partition_start.isoformat(),
            "partition_end": item.partition_end.isoformat(),
            "document_url": item.document_url,
            "document_type": item.document_type,
            "file_hash": item.file_hash,
            "version_hash": version_hash,
            "file_size": item.file_size,
            "content_type": item.content_type,
            "storage_bucket": self.settings.landing_bucket,
            "storage_key": object_key,
        }

    def _land(self, item: LandingDocument, *, version_hash: str, object_key: str) -> dict[str, Any]:
        """Write the object and metadata if this version is new.

        Returns ``persisted`` (a new immutable version was created), whether bytes were
        actually uploaded, and -- when nothing was persisted -- why.
        """
        assert self._mongo is not None and self._object_store is not None
        state = self._mongo.get_state(item.source, item.body, item.identifier)

        if state and state.get("latest_hash") == version_hash:
            # Same content as the version we already landed: refresh only the validators
            # so the next run can be answered with a 304.
            self._mongo.upsert_state(
                source=item.source,
                body=item.body,
                identifier=item.identifier,
                latest_hash=version_hash,
                latest_metadata_id=state.get("latest_metadata_id"),
                etag=item.etag or state.get("etag"),
                last_modified=item.last_modified or state.get("last_modified"),
                run_id=item.run_id,
            )
            return {
                "persisted": False,
                "uploaded": False,
                "stored": None,
                "reason": "hash_unchanged",
            }

        stored = self._object_store.put_if_absent(
            object_key,
            item.content,
            content_type=item.content_type,
            # S3 user metadata must be US-ASCII, so the identifier goes in slug form;
            # the verbatim identifier lives in the Mongo record.
            metadata={"identifier-slug": slugify(item.identifier), "run-id": item.run_id},
        )
        metadata_id = self._mongo.insert_version(
            item.to_metadata(stored=stored, bucket=self.settings.landing_bucket)
        )
        if metadata_id is None:
            # The unique index rejected it: this exact version is already in the landing
            # zone (e.g. content reverted to an earlier hash, or a re-run after a crash).
            # Point state at it so a lost or stale state row converges instead of making
            # every future run re-download and re-attempt the same document.
            existing = self._mongo.landing.find_one(
                {
                    "source": item.source,
                    "body": item.body,
                    "identifier": item.identifier,
                    "version_hash": version_hash,
                },
                {"_id": 1},
            )
            self._mongo.upsert_state(
                source=item.source,
                body=item.body,
                identifier=item.identifier,
                latest_hash=version_hash,
                latest_metadata_id=existing["_id"] if existing else None,
                etag=item.etag,
                last_modified=item.last_modified,
                run_id=item.run_id,
            )
            return {
                "persisted": False,
                "uploaded": stored.uploaded,
                "stored": stored,
                "reason": "version_already_landed",
            }

        self._mongo.upsert_state(
            source=item.source,
            body=item.body,
            identifier=item.identifier,
            latest_hash=version_hash,
            latest_metadata_id=metadata_id,
            etag=item.etag,
            last_modified=item.last_modified,
            run_id=item.run_id,
        )
        return {"persisted": True, "uploaded": stored.uploaded, "stored": stored, "reason": None}


__all__ = ["LandingZonePipeline"]
