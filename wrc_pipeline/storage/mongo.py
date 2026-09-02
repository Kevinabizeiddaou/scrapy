"""MongoDB access for the immutable landing zone plus its mutable operational state.

Two collections with deliberately different rules:

``landing_documents``
    Append-only. One record per *document version*, uniquely keyed by
    ``(source, body, identifier, version_hash)``. Never updated, never deleted.
    ``version_hash`` equals the stored bytes' ``file_hash`` for attachments; for HTML it
    excludes the server's render-timing comment (see :mod:`wrc_pipeline.ingestion.items`).

``landing_state``
    Mutable bookkeeping, one record per ``(source, body, identifier)``, holding the
    latest hash and the HTTP validators needed for conditional requests. Not part of
    the landing zone -- it can be rebuilt from ``landing_documents``.
"""

from __future__ import annotations

import datetime as dt
from types import TracebackType
from typing import TYPE_CHECKING, Any

from pymongo import ASCENDING, MongoClient
from pymongo.errors import DuplicateKeyError

if TYPE_CHECKING:
    from wrc_pipeline.config import Settings

LANDING_VERSION_KEY = ("source", "body", "identifier", "version_hash")
STATE_KEY = ("source", "body", "identifier")


class MongoLandingStore:
    def __init__(self, settings: Settings, client: MongoClient | None = None) -> None:
        self._owns_client = client is None
        self._client = client or MongoClient(settings.mongo_uri, tz_aware=True)
        database = self._client[settings.mongo_database]
        self.landing = database[settings.mongo_landing_collection]
        self.state = database[settings.mongo_state_collection]

    def ensure_indexes(self) -> None:
        self.landing.create_index(
            [(field, ASCENDING) for field in LANDING_VERSION_KEY],
            name="uniq_landing_version",
            unique=True,
        )
        self.landing.create_index([("run_id", ASCENDING)], name="by_run")
        self.landing.create_index(
            [("body", ASCENDING), ("partition_date", ASCENDING)], name="by_body_partition"
        )
        self.state.create_index(
            [(field, ASCENDING) for field in STATE_KEY], name="uniq_state_identity", unique=True
        )

    def insert_version(self, metadata: dict[str, Any]) -> Any | None:
        """Insert an immutable version. Returns ``None`` if this version already exists."""
        try:
            return self.landing.insert_one(dict(metadata)).inserted_id
        except DuplicateKeyError:
            return None

    def get_state(self, source: str, body: str, identifier: str) -> dict[str, Any] | None:
        return self.state.find_one(dict(zip(STATE_KEY, (source, body, identifier), strict=True)))

    def upsert_state(
        self,
        *,
        source: str,
        body: str,
        identifier: str,
        latest_hash: str,
        latest_metadata_id: Any,
        etag: str | None = None,
        last_modified: str | None = None,
        run_id: str | None = None,
    ) -> None:
        self.state.update_one(
            {"source": source, "body": body, "identifier": identifier},
            {
                "$set": {
                    "latest_hash": latest_hash,
                    "latest_metadata_id": latest_metadata_id,
                    "etag": etag,
                    "last_modified": last_modified,
                    "last_seen_run_id": run_id,
                    "updated_at": dt.datetime.now(dt.UTC),
                },
                "$setOnInsert": {"first_seen_at": dt.datetime.now(dt.UTC)},
            },
            upsert=True,
        )

    def touch_state(self, *, source: str, body: str, identifier: str, run_id: str) -> None:
        """Record that an unchanged identifier was re-checked, without altering validators."""
        self.state.update_one(
            {"source": source, "body": body, "identifier": identifier},
            {"$set": {"last_seen_run_id": run_id, "updated_at": dt.datetime.now(dt.UTC)}},
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> MongoLandingStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


__all__ = ["LANDING_VERSION_KEY", "STATE_KEY", "MongoLandingStore"]
