"""Transform Landing Zone versions into cleaned, identifier-named transformed documents.

One landing version plus one ``TRANSFORMATION_VERSION`` yields at most one transformed
version. The Landing Zone is read-only here: nothing in ``landing_documents`` or the
landing bucket is written, updated or deleted.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from wrc_pipeline.config import Settings, get_settings
from wrc_pipeline.logging import EventLogger
from wrc_pipeline.storage.mongo import MongoTransformedStore
from wrc_pipeline.storage.object_store import ObjectStore, sha256_hex, slugify
from wrc_pipeline.transformation import TRANSFORMATION_VERSION
from wrc_pipeline.transformation.html_cleaner import extract_decision
from wrc_pipeline.transformation.naming import transformed_filename, transformed_object_key

if TYPE_CHECKING:
    from collections.abc import Iterable

# Types whose bytes are stored verbatim: no parsing, no conversion, so the transformed
# hash is by definition the landing hash.
PASSTHROUGH_TYPES = frozenset({"pdf", "doc", "docx", "rtf"})


class TransformationError(RuntimeError):
    """A single document could not be transformed. Logged, counted, and skipped."""


@dataclass(slots=True)
class TransformationTally:
    """Run-level accounting. ``selected == transformed + unchanged + failed`` always."""

    selected: int = 0
    transformed: int = 0
    unchanged: int = 0
    failed: int = 0

    @property
    def resolved(self) -> int:
        return self.transformed + self.unchanged + self.failed

    @property
    def balanced(self) -> bool:
        return self.selected == self.resolved

    def summary(self) -> dict[str, Any]:
        return {
            "records_selected": self.selected,
            "records_transformed": self.transformed,
            "records_unchanged": self.unchanged,
            "records_failed": self.failed,
            "accounting_balanced": self.balanced,
        }


class TransformationRun:
    """Owns the stores for one transformation run."""

    def __init__(
        self,
        run_id: str,
        settings: Settings | None = None,
        *,
        mongo: MongoTransformedStore | None = None,
        landing_store: ObjectStore | None = None,
        transformed_store: ObjectStore | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.run_id = run_id
        self.events = EventLogger(run_id, logger_name="wrc.transformation")
        self.tally = TransformationTally()
        self._owns = mongo is None and landing_store is None and transformed_store is None
        self.mongo = mongo or MongoTransformedStore(self.settings)
        self.landing_store = landing_store or ObjectStore(
            self.settings, bucket=self.settings.landing_bucket
        )
        self.transformed_store = transformed_store or ObjectStore(
            self.settings, bucket=self.settings.s3_transformed_bucket
        )

    # -- lifecycle ---------------------------------------------------------------

    def close(self) -> None:
        if not self._owns:
            return
        self.mongo.close()
        self.landing_store.close()
        self.transformed_store.close()

    def __enter__(self) -> TransformationRun:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # -- run ---------------------------------------------------------------------

    def execute(
        self, start: dt.date, end: dt.date, bodies: list[str] | None = None
    ) -> TransformationTally:
        if start > end:
            raise ValueError(f"start_date {start.isoformat()} is after end_date {end.isoformat()}")

        self.mongo.ensure_indexes()
        self.events.event(
            "transformation_run_started",
            transformation_version=TRANSFORMATION_VERSION,
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            bodies=bodies,
            date_field="published_date",
            landing_bucket=self.settings.landing_bucket,
            transformed_bucket=self.settings.s3_transformed_bucket,
            transformed_collection=self.settings.mongo_transformed_collection,
        )

        if undated := self.mongo.count_undated_landing_records():
            # A date query cannot reach these; say so rather than skipping them silently.
            self.events.warning(
                "transformation_undated_landing_records",
                reason="landing_records_without_published_date_cannot_be_selected_by_date",
                records=undated,
            )

        self.process(self.mongo.select_landing_versions(start, end, bodies))
        self.events.event("transformation_run_completed", **self.tally.summary())
        return self.tally

    def process(self, landing_versions: Iterable[dict[str, Any]]) -> TransformationTally:
        for landing in landing_versions:
            self.tally.selected += 1
            self.transform_one(landing)
        return self.tally

    def transform_one(self, landing: dict[str, Any]) -> str:
        """Transform one landing version. Returns its outcome and never raises."""
        fields = _lineage_fields(landing)
        self.events.event("transformation_document_started", **fields)
        try:
            outcome = self._transform(landing)
        except Exception as exc:
            # Deliberate boundary: one bad document must not abandon the rest of the run,
            # and every document must resolve to exactly one outcome. Listing expected
            # types here instead would let an OSError from MinIO or a PyMongoError escape
            # and leave the remaining selection unprocessed and the tally unbalanced.
            # KeyboardInterrupt and SystemExit derive from BaseException, so Ctrl-C still
            # aborts the run rather than being counted as a document failure.
            self.events.error(
                "transformation_document_failed",
                error=str(exc),
                error_type=type(exc).__name__,
                **fields,
            )
            self.tally.failed += 1
            return "failed"

        if outcome["status"] == "transformed":
            self.tally.transformed += 1
            self.events.event("transformation_document_completed", **fields, **outcome["fields"])
        else:
            self.tally.unchanged += 1
            self.events.event(
                "transformation_document_unchanged",
                reason=outcome["reason"],
                **fields,
                **outcome["fields"],
            )
        return outcome["status"]

    # -- one document ------------------------------------------------------------

    def _transform(self, landing: dict[str, Any]) -> dict[str, Any]:
        identifier = landing["identifier"]
        document_type = landing["document_type"]
        version_hash = landing["version_hash"]

        existing = self.mongo.find_transformed(
            source=landing["source"],
            body=landing["body"],
            identifier=identifier,
            landing_version_hash=version_hash,
            version=TRANSFORMATION_VERSION,
        )
        if existing is not None:
            # Cheap short-circuit: the landing object is not even read.
            return {
                "status": "unchanged",
                "reason": "version_already_transformed",
                "fields": {
                    "transformed_storage_path": existing["transformed_storage_path"],
                    "transformed_file_hash": existing["transformed_file_hash"],
                },
            }

        landing_bytes = self._read_landing_object(landing)
        content, content_type = self._render(landing, landing_bytes)
        transformed_hash = sha256_hex(content)

        if document_type in PASSTHROUGH_TYPES and transformed_hash != landing["file_hash"]:
            raise TransformationError(
                "passthrough must preserve bytes exactly, but the transformed hash "
                f"{transformed_hash} differs from the landing hash {landing['file_hash']}"
            )

        filename = transformed_filename(identifier, document_type)
        key = transformed_object_key(
            body=landing["body"],
            identifier=identifier,
            landing_version_hash=version_hash,
            transformation_version=TRANSFORMATION_VERSION,
            document_type=document_type,
        )

        # Object first, metadata second: a transformed record never claims an object that
        # is not there. The key is deterministic, so a racing worker writes the same bytes
        # to the same key and the loser's insert is rejected by the unique index.
        stored = self.transformed_store.put_if_absent(
            key,
            content,
            content_type=content_type,
            metadata={
                "identifier-slug": slugify(identifier),
                "run-id": self.run_id,
                "sha256": transformed_hash,
                "landing-version-hash": version_hash,
            },
        )
        record = self._metadata(
            landing,
            filename=filename,
            key=stored.key,
            file_hash=stored.file_hash,
            file_size=stored.file_size,
            content_type=content_type,
        )
        result_fields = {
            "transformed_filename": filename,
            "transformed_storage_path": stored.key,
            "transformed_file_hash": stored.file_hash,
            "transformed_file_size": stored.file_size,
            "content_type": content_type,
            "object_uploaded": stored.uploaded,
        }

        if self.mongo.insert_transformed(record) is None:
            return {
                "status": "unchanged",
                "reason": "version_already_transformed",
                "fields": result_fields,
            }
        return {"status": "transformed", "reason": None, "fields": result_fields}

    def _read_landing_object(self, landing: dict[str, Any]) -> bytes:
        try:
            data = self.landing_store.get(landing["storage_key"])
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            raise TransformationError(
                f"landing object {landing['storage_key']!r} is unreadable ({code})"
            ) from exc
        if sha256_hex(data) != landing["file_hash"]:
            raise TransformationError(
                f"landing object {landing['storage_key']!r} does not match its recorded "
                "file_hash; refusing to transform corrupt input"
            )
        return data

    def _render(self, landing: dict[str, Any], landing_bytes: bytes) -> tuple[bytes, str]:
        document_type = landing["document_type"]
        if document_type in PASSTHROUGH_TYPES:
            return landing_bytes, landing.get("content_type") or "application/octet-stream"
        if document_type != "html":
            raise TransformationError(f"unsupported document type {document_type!r}")
        cleaned = extract_decision(
            landing_bytes,
            base_url=landing.get("detail_url") or self.settings.base_url,
            title=landing["identifier"],
        )
        return cleaned, "text/html; charset=utf-8"

    def _metadata(
        self,
        landing: dict[str, Any],
        *,
        filename: str,
        key: str,
        file_hash: str,
        file_size: int,
        content_type: str,
    ) -> dict[str, Any]:
        """Lineage-first record: which landing version produced this, and by what."""
        return {
            "source": landing["source"],
            "body": landing["body"],
            "identifier": landing["identifier"],
            "published_date": landing.get("published_date"),
            "landing_metadata_id": landing["_id"],
            "landing_storage_bucket": landing.get("storage_bucket"),
            "landing_storage_path": landing["storage_key"],
            "landing_file_hash": landing["file_hash"],
            "landing_version_hash": landing["version_hash"],
            "landing_document_type": landing["document_type"],
            "transformation_version": TRANSFORMATION_VERSION,
            "transformed_filename": filename,
            "transformed_storage_bucket": self.transformed_store.bucket,
            "transformed_storage_path": key,
            "transformed_file_hash": file_hash,
            "transformed_file_size": file_size,
            "content_type": content_type,
            "transformed_at": dt.datetime.now(dt.UTC),
            "run_id": self.run_id,
        }


def _lineage_fields(landing: dict[str, Any]) -> dict[str, Any]:
    """Identity every log line for this document carries, failures included."""
    return {
        "identifier": landing.get("identifier"),
        "body": landing.get("body"),
        "document_type": landing.get("document_type"),
        "landing_storage_path": landing.get("storage_key"),
        "landing_version_hash": landing.get("version_hash"),
        "landing_metadata_id": str(landing.get("_id")),
        "transformation_version": TRANSFORMATION_VERSION,
    }


__all__ = [
    "PASSTHROUGH_TYPES",
    "TransformationError",
    "TransformationRun",
    "TransformationTally",
]
