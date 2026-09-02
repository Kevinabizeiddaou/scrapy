"""The single unit of work that flows from the spider to the landing pipeline."""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass, field
from typing import Any

from wrc_pipeline.storage.object_store import StoredObject, sha256_hex

# Every HTML page the site renders ends with server instrumentation comments appended
# after the closing markup:
#
#     <!-- Elapsed time: 0.1562556 -->                          (always; changes per fetch)
#     <!-- cached or not being index.aspx page -->              (only on a cache hit)
#
# Raw bytes are therefore never stable for HTML and cannot serve as the version identity.
# Enumerated from the trailing 400 bytes of every stored HTML object -- these two are the
# only variants that occur, and both sit outside the decision content. Attachment PDFs are
# byte-stable. The original bytes are still stored untouched: these patterns are excluded
# from the identity hash only.
_VOLATILE_HTML = re.compile(
    rb"<!--\s*Elapsed time:[^>]*-->|<!--\s*cached or not being index\.aspx page\s*-->"
)


@dataclass(slots=True)
class LandingDocument:
    """A discovered decision plus the exact bytes of its primary document.

    ``content`` never reaches MongoDB -- it goes to object storage and is summarised in
    the metadata by hash, size and key.
    """

    source: str
    body: str
    body_id: str
    identifier: str
    title: str
    description: str | None
    reference_no: str | None
    published_date: dt.date | None
    detail_url: str
    document_url: str
    document_type: str
    partition_date: dt.date
    partition_start: dt.date
    partition_end: dt.date
    scraped_at: dt.datetime
    run_id: str
    content_type: str
    content: bytes = field(default=b"", repr=False)
    etag: str | None = None
    last_modified: str | None = None

    @property
    def file_size(self) -> int:
        return len(self.content)

    @property
    def file_hash(self) -> str:
        """SHA-256 of the exact bytes written to object storage."""
        return sha256_hex(self.content)

    @property
    def version_hash(self) -> str:
        """SHA-256 that identifies this *version* of the document.

        Identical to ``file_hash`` for attachments. For HTML it ignores the server's
        render-timing comment, so an unchanged page is recognised as unchanged instead of
        landing a new version on every run.
        """
        if self.document_type != "html":
            return self.file_hash
        return sha256_hex(_VOLATILE_HTML.sub(b"", self.content))

    def to_metadata(self, *, stored: StoredObject, bucket: str) -> dict[str, Any]:
        """Immutable landing-zone metadata record.

        Byte-level fields come from ``stored`` rather than from ``self.content``, so
        ``file_hash`` and ``file_size`` always describe the object at ``storage_key``.
        """
        return {
            "source": self.source,
            "body": self.body,
            "body_id": self.body_id,
            "identifier": self.identifier,
            "reference_no": self.reference_no,
            "title": self.title,
            "description": self.description,
            "published_date": _as_datetime(self.published_date),
            "detail_url": self.detail_url,
            "document_url": self.document_url,
            "document_type": self.document_type,
            "partition_date": _as_datetime(self.partition_date),
            "partition_start": _as_datetime(self.partition_start),
            "partition_end": _as_datetime(self.partition_end),
            "scraped_at": self.scraped_at,
            "run_id": self.run_id,
            "file_hash": stored.file_hash,
            "version_hash": self.version_hash,
            "file_size": stored.file_size,
            "content_type": self.content_type,
            "storage_bucket": bucket,
            "storage_key": stored.key,
            "http_etag": self.etag,
            "http_last_modified": self.last_modified,
        }


def _as_datetime(value: dt.date | None) -> dt.datetime | None:
    """BSON has no date type; store dates as UTC midnight datetimes."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    return dt.datetime.combine(value, dt.time.min, tzinfo=dt.UTC)


__all__ = ["LandingDocument"]
