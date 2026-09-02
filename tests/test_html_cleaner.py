"""HTML extraction, driven by the real landing pages saved in tests/fixtures."""

from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from tests.conftest import load_fixture
from wrc_pipeline.transformation.html_cleaner import ContentNotFoundError, extract_decision

DETAIL_URL = "https://www.workplacerelations.ie/en/cases/2024/february/lcr22912.html"


def clean(fixture: str = "detail_inline_html.html", identifier: str = "LCR22912") -> str:
    return extract_decision(load_fixture(fixture), base_url=DETAIL_URL, title=identifier).decode(
        "utf-8"
    )


# --- chrome removal -------------------------------------------------------------


@pytest.mark.parametrize(
    "chrome",
    [
        "Return to Search",
        "Skip to main content",
        "acceptCookie",
        "googleTranslateElement",
        "navbar",
        "<script",
        "<footer",
        "<header",
        "<nav",
        "searchbanner",
        "Cookie Management",
        "Gaeilge",
        "labour-court-decisions-logo",
        "Elapsed time",
        "numBinder",
    ],
)
def test_site_chrome_is_removed(chrome: str) -> None:
    """Each of these is present in the landing page and must not survive."""
    assert chrome in load_fixture("detail_inline_html.html"), f"{chrome} not in the source page"
    assert chrome not in clean()


def test_output_is_a_small_fraction_of_the_landing_page() -> None:
    source = load_fixture("detail_inline_html.html")
    assert len(clean()) < len(source) / 2


# --- structure preservation -----------------------------------------------------


def test_the_decision_heading_is_preserved() -> None:
    soup = BeautifulSoup(clean(), "lxml")
    assert soup.h1 is not None
    assert soup.h1.get_text(strip=True) == "LCR22912"
    assert soup.title.get_text() == "LCR22912"


def test_paragraphs_tables_and_emphasis_are_preserved() -> None:
    soup = BeautifulSoup(clean(), "lxml")
    assert len(soup.find_all("p")) > 5
    assert len(soup.find_all("table")) >= 2
    assert soup.find_all("tr")
    assert soup.find_all("td")
    assert soup.find_all("strong")


def test_the_legal_text_survives_verbatim() -> None:
    text = BeautifulSoup(clean(), "lxml").get_text(" ", strip=True)
    for phrase in (
        "INDUSTRIAL RELATIONS ACTS 1946 TO 2015",
        "SONOMA VALLEY",
        "A WORKER",
        "Appeal of Adjudication Officer Decision",
        "Chairman:",
        "Mr Foley",
    ):
        assert phrase in text, phrase


def test_table_structure_is_not_flattened_to_text() -> None:
    soup = BeautifulSoup(clean(), "lxml")
    # innermost table holding the DIVISION rows, not an ancestor that merely contains it
    division = [
        t for t in soup.find_all("table") if "Chairman:" in t.get_text() and not t.find("table")
    ]
    assert division, "the DIVISION table should still be a table"
    rows = division[0].find_all("tr")
    assert [c.get_text(strip=True) for c in rows[0].find_all("td")] == ["Chairman:", "Mr Foley"]
    assert [c.get_text(strip=True) for c in rows[1].find_all("td")] == [
        "Employer Member:",
        "Ms Doyle",
    ]


def test_output_is_a_standalone_html_document() -> None:
    output = clean()
    assert output.startswith("<!DOCTYPE html>")
    assert '<meta charset="utf-8"/>' in output
    assert "<article>" in output


# --- attribute cleaning ---------------------------------------------------------


def test_dead_word_import_classes_and_presentational_attributes_are_dropped() -> None:
    source = load_fixture("detail_inline_html.html")
    for present in ('class="c3"', "cellspacing", "valign", "width="):
        assert present in source, present
    output = clean()
    for gone in ('class="c', "cellspacing", "cellpadding", "valign", "width="):
        assert gone not in output, gone


def test_inline_layout_styles_are_dropped() -> None:
    """Only layout properties appear inline in the corpus; none carry legal meaning."""
    page = (
        '<div class="content"><p style="padding-left:60px;background-color:#ff0">indented</p></div>'
    )
    output = extract_decision(page, base_url=DETAIL_URL, title="X").decode("utf-8")
    assert "padding-left" not in output
    assert "background-color" not in output
    assert "indented" in output


def test_spacer_images_are_dropped() -> None:
    source = load_fixture("detail_inline_html.html")
    assert "ecblank.gif" in source
    assert "ecblank.gif" not in clean()


def test_a_real_document_image_is_kept() -> None:
    """The Labour Court signature seal is part of the decision, unlike the spacer gif."""
    page = (
        '<div class="content"><p>text</p>'
        '<img src="/images_upload/wrc/en/labour_court_import/signature_logo.png" alt="seal"/>'
        '<img src="/icons/ecblank.gif" alt=""/></div>'
    )
    soup = BeautifulSoup(extract_decision(page, base_url=DETAIL_URL, title="X"), "lxml")
    images = soup.find_all("img")
    assert len(images) == 1
    assert images[0]["src"].endswith("/signature_logo.png")
    assert images[0]["alt"] == "seal"


def test_relative_links_are_made_absolute() -> None:
    """The Equality Tribunal decision links its own PDF from inside the content."""
    output = extract_decision(
        load_fixture("detail_with_pdf_equality.html"),
        base_url="https://www.workplacerelations.ie/en/cases/1998/december/ee22-1998.html",
        title="EE22-1998",
    ).decode("utf-8")
    soup = BeautifulSoup(output, "lxml")
    (link,) = soup.find_all("a")
    assert link["href"] == (
        "https://www.workplacerelations.ie/en/Equality_Tribunal_Import/"
        "Database-of-Decisions/1998/EE-1998-22.pdf"
    )


# --- determinism ----------------------------------------------------------------


@pytest.mark.parametrize(
    "fixture", ["detail_inline_html.html", "detail_with_pdf.html", "detail_with_pdf_equality.html"]
)
def test_extraction_is_byte_deterministic(fixture: str) -> None:
    first = extract_decision(load_fixture(fixture), base_url=DETAIL_URL, title="X")
    second = extract_decision(load_fixture(fixture), base_url=DETAIL_URL, title="X")
    assert first == second


def test_every_body_type_extracts() -> None:
    for fixture, identifier in (
        ("detail_inline_html.html", "LCR22912"),
        ("detail_with_pdf.html", "UD893/2008"),
        ("detail_with_pdf_equality.html", "EE22-1998"),
    ):
        output = clean(fixture, identifier)
        assert BeautifulSoup(output, "lxml").get_text(strip=True)


# --- failure rather than fallback -----------------------------------------------


def test_a_page_without_the_container_fails_instead_of_storing_the_whole_page() -> None:
    """A redesigned or error page must never be stored as a "cleaned" decision."""
    page = """
    <html><body>
      <header><nav>Workplace Relations</nav></header>
      <div class="searchbanner"><p>This website contains decisions…</p></div>
      <div class="container"><h1>Page not found</h1><p>Try the search.</p></div>
      <footer>Cookie Management</footer>
    </body></html>
    """
    with pytest.raises(ContentNotFoundError, match="refusing to store the whole page"):
        extract_decision(page, base_url=DETAIL_URL, title="X")


@pytest.mark.parametrize(
    "empty",
    [
        '<div class="content"></div>',
        '<div class="content">   </div>',
        '<div class="content"><script>var x=1;</script></div>',
        '<div class="content"><p> </p><span>  </span></div>',
    ],
)
def test_an_empty_container_fails(empty: str) -> None:
    with pytest.raises(ContentNotFoundError, match="empty after cleaning"):
        extract_decision(f"<html><body>{empty}</body></html>", base_url=DETAIL_URL, title="X")


def test_malformed_html_without_a_container_fails_cleanly() -> None:
    with pytest.raises(ContentNotFoundError):
        extract_decision(b"\x00\x01 not html at all", base_url=DETAIL_URL, title="X")


# --- minimal synthetic shape ----------------------------------------------------


def test_unknown_tags_are_unwrapped_rather_than_dropped() -> None:
    """A Word artefact tag (o3a_p appears once in the corpus) must not eat its text."""
    page = '<div class="content"><o3a_p>kept text</o3a_p><p>and this</p></div>'
    output = extract_decision(page, base_url=DETAIL_URL, title="X").decode("utf-8")
    assert "kept text" in output
    assert "o3a_p" not in output
    assert "and this" in output


def test_structural_table_attributes_are_kept() -> None:
    page = (
        '<div class="content"><table><tr>'
        '<td colspan="2" rowspan="3" width="80" class="c3">cell</td>'
        "</tr></table></div>"
    )
    soup = BeautifulSoup(extract_decision(page, base_url=DETAIL_URL, title="X"), "lxml")
    cell = soup.find("td")
    assert cell["colspan"] == "2"
    assert cell["rowspan"] == "3"
    assert "width" not in cell.attrs
    assert "class" not in cell.attrs


def test_ordered_list_numbering_is_kept() -> None:
    page = '<div class="content"><ol start="4"><li>four</li></ol></div>'
    soup = BeautifulSoup(extract_decision(page, base_url=DETAIL_URL, title="X"), "lxml")
    assert soup.find("ol")["start"] == "4"
