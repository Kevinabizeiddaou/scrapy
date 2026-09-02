"""Transformed filenames and object keys.

The assessment asks for transformed files named ``identifier.ext`` -- ``ADJ-00054658.html``,
not a slug. Some real identifiers cannot be one path segment as-is (``UD570/2009``), so
exactly the characters that would break a segment are percent-encoded, and nothing else.
The encoding is reversible, so the original identifier is always recoverable from the
filename.
"""

from __future__ import annotations

from urllib.parse import quote, unquote

from wrc_pipeline.storage.object_store import slugify

# Characters that cannot appear in a single filesystem/object-key segment. ``%`` is
# included so the encoding round-trips: an identifier that already contains a percent sign
# must not be confused with an escape this function produced.
RESERVED_CHARACTERS = '%/\\:*?"<>|'

_SAFE = "".join(chr(code) for code in range(0x20, 0x7F) if chr(code) not in RESERVED_CHARACTERS)

EXTENSIONS: dict[str, str] = {
    "html": "html",
    "pdf": "pdf",
    "doc": "doc",
    "docx": "docx",
    "rtf": "rtf",
}


def transformed_filename(identifier: str, document_type: str) -> str:
    """``identifier.ext``, with only unusable characters percent-encoded.

    >>> transformed_filename("ADJ-00054658", "html")
    'ADJ-00054658.html'
    >>> transformed_filename("UD570/2009", "pdf")
    'UD570%2F2009.pdf'
    """
    if not identifier or not identifier.strip():
        raise ValueError("identifier must not be blank")
    extension = EXTENSIONS.get(document_type.lower())
    if extension is None:
        raise ValueError(f"unsupported document type {document_type!r}")
    return f"{encode_identifier(identifier)}.{extension}"


def encode_identifier(identifier: str) -> str:
    """Percent-encode only ``RESERVED_CHARACTERS`` and anything outside printable ASCII."""
    return quote(identifier, safe=_SAFE, encoding="utf-8")


def decode_identifier(encoded: str) -> str:
    """Inverse of :func:`encode_identifier`."""
    return unquote(encoded, encoding="utf-8")


def identifier_from_filename(filename: str) -> str:
    """Recover the original identifier from a transformed filename."""
    stem, _, extension = filename.rpartition(".")
    if not stem or extension.lower() not in EXTENSIONS:
        raise ValueError(f"{filename!r} is not a transformed filename")
    return decode_identifier(stem)


def transformed_object_key(
    *,
    body: str,
    identifier: str,
    landing_version_hash: str,
    document_type: str,
    prefix: str = "transformed",
) -> str:
    """``transformed/{body}/{identifier}/{landing_version_hash}/{identifier.ext}``.

    The directories are slugified and carry the landing version hash, which keeps keys
    collision-safe and stops one transformed version from overwriting another, while the
    final segment stays the ``identifier.ext`` filename the assessment asks for.
    """
    return "/".join(
        (
            prefix,
            slugify(body),
            slugify(identifier),
            landing_version_hash,
            transformed_filename(identifier, document_type),
        )
    )


__all__ = [
    "EXTENSIONS",
    "RESERVED_CHARACTERS",
    "decode_identifier",
    "encode_identifier",
    "identifier_from_filename",
    "transformed_filename",
    "transformed_object_key",
]
