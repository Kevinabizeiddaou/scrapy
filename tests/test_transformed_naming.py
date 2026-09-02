"""Transformed filenames must be ``identifier.ext``, reversibly encoded, never slugified."""

from __future__ import annotations

import pytest

from wrc_pipeline.transformation.naming import (
    decode_identifier,
    encode_identifier,
    identifier_from_filename,
    transformed_filename,
    transformed_object_key,
)

HASH = "a" * 64


@pytest.mark.parametrize(
    ("identifier", "document_type", "expected"),
    [
        ("ADJ-00054658", "html", "ADJ-00054658.html"),
        ("ADJ-00057058", "pdf", "ADJ-00057058.pdf"),
        ("LCR22912", "html", "LCR22912.html"),
        ("EDA2356", "html", "EDA2356.html"),
        ("EE22-1998", "pdf", "EE22-1998.pdf"),
        ("DEC-S2010-007 - Full Case Report", "html", "DEC-S2010-007 - Full Case Report.html"),
        ("IR - SC - 00001761", "html", "IR - SC - 00001761.html"),
        ("ADJ-00039132", "docx", "ADJ-00039132.docx"),
        ("X1", "doc", "X1.doc"),
        ("X1", "rtf", "X1.rtf"),
    ],
)
def test_ordinary_identifiers_are_used_verbatim(
    identifier: str, document_type: str, expected: str
) -> None:
    assert transformed_filename(identifier, document_type) == expected


@pytest.mark.parametrize(
    ("identifier", "expected"),
    [
        ("UD570/2009", "UD570%2F2009.pdf"),
        ("UD893/2008, RP767/2008", "UD893%2F2008, RP767%2F2008.pdf"),
        ("RP403/2009, MN408/2009, WT177/2009", "RP403%2F2009, MN408%2F2009, WT177%2F2009.pdf"),
        (r"A\B", "A%5CB.pdf"),
        ("A:B", "A%3AB.pdf"),
        ('A"B', "A%22B.pdf"),
        ("A<B>C", "A%3CB%3EC.pdf"),
        ("A|B", "A%7CB.pdf"),
        ("A?B*C", "A%3FB%2AC.pdf"),
    ],
)
def test_reserved_characters_are_percent_encoded(identifier: str, expected: str) -> None:
    assert transformed_filename(identifier, "pdf") == expected


def test_a_slash_identifier_is_one_path_segment() -> None:
    filename = transformed_filename("UD570/2009", "pdf")
    assert "/" not in filename
    assert filename == "UD570%2F2009.pdf"


@pytest.mark.parametrize(
    "identifier",
    [
        "ADJ-00054658",
        "UD570/2009",
        "UD893/2008, RP767/2008",
        "A%2FB",  # a literal percent sign in the source identifier
        "100%",
        r"A\B/C:D",
        "IR - SC - 00001761",
        "DEC-S2010-007 - Full Case Report",
    ],
)
def test_encoding_round_trips(identifier: str) -> None:
    assert decode_identifier(encode_identifier(identifier)) == identifier
    assert identifier_from_filename(transformed_filename(identifier, "pdf")) == identifier


def test_a_literal_percent_is_not_confused_with_an_escape() -> None:
    """``A%2FB`` and ``A/B`` are different identifiers and must not collide."""
    assert transformed_filename("A%2FB", "pdf") == "A%252FB.pdf"
    assert transformed_filename("A/B", "pdf") == "A%2FB.pdf"
    assert transformed_filename("A%2FB", "pdf") != transformed_filename("A/B", "pdf")


def test_identifiers_are_not_slugified() -> None:
    """Regression guard: the landing zone slugifies keys, transformed filenames must not."""
    assert transformed_filename("ADJ-00054658", "html") == "ADJ-00054658.html"
    assert transformed_filename("LCR22912", "html") != "lcr22912.html"


@pytest.mark.parametrize("bad", ["", "   "])
def test_a_blank_identifier_is_rejected(bad: str) -> None:
    with pytest.raises(ValueError, match="must not be blank"):
        transformed_filename(bad, "html")


def test_an_unknown_document_type_is_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported document type"):
        transformed_filename("ADJ-1", "xlsx")


def test_identifier_from_filename_rejects_non_transformed_names() -> None:
    for bad in ("noextension", "thing.xlsx", ".html"):
        with pytest.raises(ValueError, match="not a transformed filename"):
            identifier_from_filename(bad)


# --- object keys ---------------------------------------------------------------


def test_object_key_shape_ends_with_the_identifier_filename() -> None:
    key = transformed_object_key(
        body="Labour Court",
        identifier="LCR22912",
        landing_version_hash=HASH,
        document_type="html",
    )
    assert key == f"transformed/labour-court/lcr22912/{HASH}/LCR22912.html"


def test_object_key_is_safe_and_single_segment_for_a_slash_identifier() -> None:
    key = transformed_object_key(
        body="Employment Appeals Tribunal",
        identifier="UD570/2009",
        landing_version_hash=HASH,
        document_type="pdf",
    )
    assert key == f"transformed/employment-appeals-tribunal/ud570-2009/{HASH}/UD570%2F2009.pdf"
    assert len(key.split("/")) == 5


def test_object_key_is_deterministic() -> None:
    args = {
        "body": "Labour Court",
        "identifier": "LCR22912",
        "landing_version_hash": HASH,
        "document_type": "html",
    }
    assert transformed_object_key(**args) == transformed_object_key(**args)


def test_a_new_landing_version_gets_its_own_key() -> None:
    args = {"body": "Labour Court", "identifier": "LCR22912", "document_type": "html"}
    first = transformed_object_key(landing_version_hash=HASH, **args)
    second = transformed_object_key(landing_version_hash="b" * 64, **args)
    assert first != second
    assert first.rsplit("/", 2)[0] == second.rsplit("/", 2)[0]
    assert first.rsplit("/", 1)[-1] == second.rsplit("/", 1)[-1] == "LCR22912.html"


def test_object_key_is_safe_for_hostile_identifiers() -> None:
    key = transformed_object_key(
        body="../../Labour Court",
        identifier="../../../etc/passwd",
        landing_version_hash=HASH,
        document_type="pdf",
    )
    assert key.startswith("transformed/")
    assert len(key.split("/")) == 5
    assert "/etc/passwd" not in key
