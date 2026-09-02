"""Object-key sanitisation and content hashing."""

from __future__ import annotations

import datetime as dt
import hashlib
import re

import pytest

from wrc_pipeline.storage.object_store import build_object_key, sha256_hex, slugify

PARTITION = dt.date(2024, 1, 15)
HASH_A = "a" * 64


# --- hashing --------------------------------------------------------------------


def test_sha256_matches_hashlib() -> None:
    payload = b"%PDF-1.4 decision bytes"
    assert sha256_hex(payload) == hashlib.sha256(payload).hexdigest()


def test_sha256_of_empty_bytes() -> None:
    assert sha256_hex(b"") == ("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")


def test_sha256_is_stable_and_distinguishes_a_single_bit() -> None:
    assert sha256_hex(b"abc") == sha256_hex(b"abc")
    assert sha256_hex(b"abc") != sha256_hex(b"abd")


def test_sha256_is_byte_exact_not_text_normalised() -> None:
    assert sha256_hex(b"a\r\nb") != sha256_hex(b"a\nb")


# --- slug sanitisation ----------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Labour Court", "labour-court"),
        ("Workplace Relations Commission", "workplace-relations-commission"),
        ("Employment Appeals Tribunal", "employment-appeals-tribunal"),
        ("ADJ-00039132", "adj-00039132"),
        ("IR - SC - 00001761", "ir-sc-00001761"),
        ("UD893/2008, RP767/2008", "ud893-2008-rp767-2008"),
        ("DEC-S2010-007 - Full Case Report", "dec-s2010-007-full-case-report"),
        ("  padded  ", "padded"),
        ("Bredá Slevin", "breda-slevin"),
        ("EE22-1998", "ee22-1998"),
    ],
)
def test_slugify_normalises_external_strings(raw: str, expected: str) -> None:
    assert slugify(raw) == expected


@pytest.mark.parametrize(
    "hostile",
    [
        "../../etc/passwd",
        "/absolute/path",
        "a/b/c",
        "..",
        "with\\backslash",
        "null\x00byte",
        "new\nline",
        "semi;colon&amp",
        "<script>alert(1)</script>",
        "%2e%2e%2f",
    ],
)
def test_slugify_never_emits_path_or_control_characters(hostile: str) -> None:
    slug = slugify(hostile)
    assert re.fullmatch(r"[a-z0-9-]+", slug), slug
    assert "/" not in slug
    assert ".." not in slug


def test_slugify_falls_back_for_input_with_nothing_usable() -> None:
    slug = slugify("///")
    assert slug.startswith("unnamed-")
    assert re.fullmatch(r"unnamed-[0-9a-f]{12}", slug)


def test_slugify_truncates_but_keeps_long_values_distinct() -> None:
    a, b = "x" * 300 + "-one", "x" * 300 + "-two"
    assert len(slugify(a)) <= 80
    assert slugify(a) != slugify(b)


def test_slugify_is_deterministic() -> None:
    assert slugify("IR - SC - 00001761") == slugify("IR - SC - 00001761")


# --- object keys ----------------------------------------------------------------


def test_object_key_shape() -> None:
    key = build_object_key(
        body="Labour Court",
        partition_date=PARTITION,
        identifier="LCR22912",
        version_hash=HASH_A,
        document_type="html",
    )
    assert key == f"landing/labour-court/2024-01-15/lcr22912/{HASH_A}.html"


def test_object_key_uses_the_partition_start_date() -> None:
    key = build_object_key(
        body="Labour Court",
        partition_date=dt.date(2024, 2, 1),
        identifier="X",
        version_hash=HASH_A,
        document_type="pdf",
    )
    assert "/2024-02-01/" in key


def test_changed_content_lands_beside_the_old_version() -> None:
    common = {
        "body": "Labour Court",
        "partition_date": PARTITION,
        "identifier": "LCR22912",
        "document_type": "pdf",
    }
    old = build_object_key(version_hash=HASH_A, **common)
    new = build_object_key(version_hash="b" * 64, **common)
    assert old != new
    assert old.rsplit("/", 1)[0] == new.rsplit("/", 1)[0]


def test_object_key_is_safe_for_hostile_identifiers() -> None:
    key = build_object_key(
        body="../../Labour Court",
        partition_date=PARTITION,
        identifier="../../../etc/passwd",
        version_hash=HASH_A,
        document_type="pdf",
    )
    assert ".." not in key
    assert key.startswith("landing/")
    assert len(key.split("/")) == 5


def test_object_key_rejects_a_non_sha256_hash() -> None:
    for bad in ("", "deadbeef", "z" * 64, HASH_A.upper()):
        with pytest.raises(ValueError, match="hex sha256 digest"):
            build_object_key(
                body="Labour Court",
                partition_date=PARTITION,
                identifier="X",
                version_hash=bad,
                document_type="pdf",
            )
