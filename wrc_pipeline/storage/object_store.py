"""S3-compatible landing-zone object storage (MinIO locally, AWS S3 unchanged).

Object keys are deterministic and content-addressed:

    landing/{body_slug}/{partition_date}/{identifier_slug}/{sha256}.{ext}

Because the filename is the content hash, re-running a crawl produces the exact same
key for unchanged bytes (so nothing is written twice) while changed bytes land beside
the old version instead of overwriting it.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
import unicodedata
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from wrc_pipeline.config import Settings

_UNSAFE = re.compile(r"[^a-z0-9]+")
_SLUG_MAX_LENGTH = 80


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def slugify(value: str, *, max_length: int = _SLUG_MAX_LENGTH) -> str:
    """Reduce an external string to a safe, stable object-key segment.

    Anything outside ``[a-z0-9-]`` is collapsed to a single hyphen. Over-long values are
    truncated with a short digest of the original appended, so two different long
    identifiers cannot collapse onto the same prefix.
    """
    normalised = unicodedata.normalize("NFKD", value)
    ascii_only = normalised.encode("ascii", "ignore").decode("ascii").lower()
    slug = _UNSAFE.sub("-", ascii_only).strip("-")

    if not slug:
        return f"unnamed-{hashlib.sha256(value.encode()).hexdigest()[:12]}"
    if len(slug) > max_length:
        digest = hashlib.sha256(value.encode()).hexdigest()[:12]
        keep = max_length - len(digest) - 1
        slug = f"{slug[:keep].rstrip('-')}-{digest}"
    return slug


def build_object_key(
    *,
    body: str,
    partition_date: dt.date,
    identifier: str,
    version_hash: str,
    document_type: str,
    prefix: str = "landing",
) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", version_hash):
        raise ValueError(f"version_hash must be a hex sha256 digest, got {version_hash!r}")
    extension = slugify(document_type, max_length=8) or "bin"
    return "/".join(
        (
            prefix,
            slugify(body),
            partition_date.isoformat(),
            slugify(identifier),
            f"{version_hash}.{extension}",
        )
    )


@dataclass(frozen=True, slots=True)
class StoredObject:
    """What actually sits at a landing key after a write attempt.

    ``file_hash`` and ``file_size`` always describe the *stored* bytes, so the metadata
    record can never claim a hash for content that was never written.
    """

    key: str
    uploaded: bool
    file_hash: str
    file_size: int


class ObjectStore:
    """Thin boto3 wrapper. Objects are written once and never overwritten.

    ``bucket`` defaults to the landing bucket; the transformation stage passes the
    transformed bucket so both stages share one client implementation.
    """

    def __init__(self, settings: Settings, bucket: str | None = None) -> None:
        self.bucket = bucket or settings.landing_bucket
        self._client = boto3.client(
            "s3",
            config=Config(
                retries={"max_attempts": 5, "mode": "standard"},
                s3={"addressing_style": "path"},  # MinIO does not do virtual-host buckets
            ),
            **settings.boto3_client_kwargs(),
        )
        if settings.s3_create_bucket:
            self.ensure_bucket()

    def ensure_bucket(self) -> None:
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {"404", "NoSuchBucket"}:
                raise
            self._client.create_bucket(Bucket=self.bucket)

    def put_if_absent(
        self, key: str, data: bytes, *, content_type: str, metadata: dict[str, str] | None = None
    ) -> StoredObject:
        """Upload ``data`` unless the key is already occupied.

        Always describes whatever now sits at ``key`` -- which is not necessarily ``data``.
        Two fetches of the same HTML decision differ in the render-timing comment while
        sharing a version hash, so if an earlier run uploaded the object but then failed
        before writing its metadata, the bytes already there are the ones that count.
        """
        if (existing := self._describe(key)) is not None:
            return existing

        file_hash = sha256_hex(data)
        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType=content_type or "application/octet-stream",
            # sha256 is recorded on the object so a later run can describe it without
            # downloading it again.
            Metadata={**(metadata or {}), "sha256": file_hash},
        )
        return StoredObject(key=key, uploaded=True, file_hash=file_hash, file_size=len(data))

    def _describe(self, key: str) -> StoredObject | None:
        """The object at ``key``, or ``None`` if the key is free."""
        try:
            head = self._client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
                return None
            raise
        file_hash = head.get("Metadata", {}).get("sha256") or sha256_hex(self.get(key))
        return StoredObject(
            key=key, uploaded=False, file_hash=file_hash, file_size=head["ContentLength"]
        )

    def get(self, key: str) -> bytes:
        return self._client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> ObjectStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


__all__ = ["ObjectStore", "StoredObject", "build_object_key", "sha256_hex", "slugify"]
