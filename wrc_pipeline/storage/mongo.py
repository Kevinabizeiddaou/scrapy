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

``transformed_documents``
    Append-only output of the transformation stage, one record per
    ``(landing version, transformation_version)``. Kept in its own collection: the
    landing zone is never written to by transformation.
"""

from __future__ import annotations

import datetime as dt
from types import TracebackType
from typing import TYPE_CHECKING, Any

from pymongo import ASCENDING, MongoClient
from pymongo.cursor import Cursor
from pymongo.errors import DuplicateKeyError

if TYPE_CHECKING:
    from wrc_pipeline.config import Settings

LANDING_VERSION_KEY = ("source", "body", "identifier", "version_hash")
STATE_KEY = ("source", "body", "identifier")
TRANSFORMED_VERSION_KEY = (
    "source",
    "body",
    "identifier",
    "landing_version_hash",
    "transformation_version",
)


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
        # Selection index for the transformation stage, which queries by publication date.
        # Created here so transformation only ever reads from the landing collection.
        self.landing.create_index([("published_date", ASCENDING)], name="by_published_date")
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


class MongoTransformedStore:
    """Append-only store for transformation output.

    Reads the landing collection (never writes to it) and writes ``transformed_documents``.
    """

    def __init__(self, settings: Settings, client: MongoClient | None = None) -> None:
        self._owns_client = client is None
        self._client = client or MongoClient(settings.mongo_uri, tz_aware=True)
        database = self._client[settings.mongo_database]
        self.landing = database[settings.mongo_landing_collection]
        self.transformed = database[settings.mongo_transformed_collection]

    def ensure_indexes(self) -> None:
        """Only ``transformed_documents`` is indexed here; landing is read-only to us."""
        self.transformed.create_index(
            [(field, ASCENDING) for field in TRANSFORMED_VERSION_KEY],
            name="uniq_transformed_version",
            unique=True,
        )
        self.transformed.create_index([("run_id", ASCENDING)], name="by_run")
        self.transformed.create_index(
            [("landing_metadata_id", ASCENDING)], name="by_landing_metadata"
        )

    def select_landing_versions(
        self, start: dt.date, end: dt.date, bodies: list[str] | None = None
    ) -> Cursor:
        """Every immutable landing version published within the inclusive date range.

        ``published_date`` is the decision's own publication date, stored by ingestion as a
        UTC-midnight BSON datetime -- the one normalised, queryable date in the schema.
        """
        query: dict[str, Any] = {
            "published_date": {"$gte": _utc_midnight(start), "$lte": _utc_midnight(end)}
        }
        if bodies:
            query["body"] = {"$in": bodies}
        return self.landing.find(query).sort([("published_date", ASCENDING), ("_id", ASCENDING)])

    def count_undated_landing_records(self) -> int:
        """Landing records a date query can never select, so they are never lost silently."""
        return self.landing.count_documents({"published_date": None})

    def insert_transformed(self, metadata: dict[str, Any]) -> Any | None:
        """Insert one transformed version. ``None`` if that version already exists."""
        try:
            return self.transformed.insert_one(dict(metadata)).inserted_id
        except DuplicateKeyError:
            return None

    def find_transformed(
        self, *, source: str, body: str, identifier: str, landing_version_hash: str, version: int
    ) -> dict[str, Any] | None:
        return self.transformed.find_one(
            dict(
                zip(
                    TRANSFORMED_VERSION_KEY,
                    (source, body, identifier, landing_version_hash, version),
                    strict=True,
                )
            )
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> MongoTransformedStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


def _utc_midnight(day: dt.date) -> dt.datetime:
    return dt.datetime.combine(day, dt.time.min, tzinfo=dt.UTC)


__all__ = [
    "LANDING_VERSION_KEY",
    "STATE_KEY",
    "TRANSFORMED_VERSION_KEY",
    "MongoLandingStore",
    "MongoTransformedStore",
]
