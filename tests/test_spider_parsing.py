"""Parsing tests driven entirely by saved fixtures - never by the live WRC site."""

from __future__ import annotations

import datetime as dt

import pytest

from tests.conftest import load_fixture
from wrc_pipeline.ingestion.parsing import (
    Body,
    BodyDiscoveryError,
    SearchParseError,
    document_type_from_url,
    find_document_url,
    parse_bodies,
    parse_result_rows,
    parse_total_results,
    parse_wrc_date,
    total_pages,
)

SEARCH_URL = "https://www.workplacerelations.ie/en/search/?decisions=1&body=3&pageNumber=1"


# --- body discovery -------------------------------------------------------------


def test_bodies_are_discovered_from_the_live_filter_markup() -> None:
    assert parse_bodies(load_fixture("advanced_search.html")) == [
        Body(body_id="2", name="Employment Appeals Tribunal"),
        Body(body_id="1", name="Equality Tribunal"),
        Body(body_id="3", name="Labour Court"),
        Body(body_id="15376", name="Workplace Relations Commission"),
    ]


def test_body_discovery_fails_loudly_when_the_filter_is_gone() -> None:
    with pytest.raises(BodyDiscoveryError, match="span#CB2"):
        parse_bodies("<html><body><p>redesigned site</p></body></html>")


def test_body_discovery_ignores_checkboxes_without_a_label() -> None:
    html = '<span id="CB2"><input id="CB2_0" type="checkbox" value="9" /></span>'
    with pytest.raises(BodyDiscoveryError):
        parse_bodies(html)


# --- result counts --------------------------------------------------------------


def test_total_results_read_from_searchhead() -> None:
    assert parse_total_results(load_fixture("search_results_page1.html")) == 45


def test_total_results_on_the_last_page() -> None:
    assert parse_total_results(load_fixture("search_results_last_page.html")) == 45


def test_total_results_for_a_large_result_set() -> None:
    assert parse_total_results(load_fixture("search_results_wrc_page1.html")) == 234


def test_empty_result_set_reports_zero() -> None:
    assert parse_total_results(load_fixture("search_results_empty.html")) == 0


def test_thousands_separator_is_handled() -> None:
    html = '<div class="searchhead">Shows 1 to 10 of 21,330 results</div>'
    assert parse_total_results(html) == 21330


def test_missing_searchhead_raises_rather_than_assuming_zero() -> None:
    with pytest.raises(SearchParseError, match="searchhead missing"):
        parse_total_results("<html><body>nothing here</body></html>")


def test_unreadable_searchhead_raises() -> None:
    with pytest.raises(SearchParseError, match="could not read a result count"):
        parse_total_results('<div class="searchhead">Something unexpected</div>')


# --- pagination -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("total", "expected"),
    [(0, 0), (1, 1), (10, 1), (11, 2), (45, 5), (234, 24), (21330, 2133)],
)
def test_page_count_derived_from_the_reported_total(total: int, expected: int) -> None:
    assert total_pages(total, 10) == expected


def test_page_count_rejects_a_nonsense_page_size() -> None:
    with pytest.raises(ValueError, match="per_page must be positive"):
        total_pages(10, 0)


def test_pager_only_renders_a_window_so_totals_must_drive_pagination() -> None:
    """234 results is 24 pages, but the site only ever renders 10 pager links."""
    html = load_fixture("search_results_wrc_page1.html")
    rendered_pages = set(_pager_numbers(html))
    assert len(rendered_pages) < total_pages(parse_total_results(html), 10)


def _pager_numbers(html: str) -> list[int]:
    import re

    return [int(n) for n in re.findall(r"pageNumber=(\d+)", html)]


# --- result rows ----------------------------------------------------------------


def test_result_rows_are_fully_extracted() -> None:
    rows = parse_result_rows(load_fixture("search_results_page1.html"), SEARCH_URL)
    assert len(rows) == 10

    first = rows[0]
    assert first.identifier == "LCR22912"
    assert first.title == "LCR22912"
    assert first.published_date == dt.date(2024, 1, 30)
    assert (
        first.detail_url == "https://www.workplacerelations.ie/en/cases/2024/february/lcr22912.html"
    )
    assert first.description is not None
    assert "SONOMA VALLEY" in first.description


def test_last_page_has_the_remainder_of_the_result_set() -> None:
    rows = parse_result_rows(load_fixture("search_results_last_page.html"), SEARCH_URL)
    assert len(rows) == 45 % 10 == 5


def test_empty_result_page_yields_no_rows() -> None:
    assert parse_result_rows(load_fixture("search_results_empty.html"), SEARCH_URL) == []


def test_every_row_has_an_identifier_and_an_absolute_detail_url() -> None:
    for name in ("search_results_page1.html", "search_results_wrc_page1.html"):
        for row in parse_result_rows(load_fixture(name), SEARCH_URL):
            assert row.identifier
            assert row.detail_url.startswith("https://www.workplacerelations.ie/en/cases/")


def test_identifiers_with_spaces_survive_parsing() -> None:
    rows = parse_result_rows(load_fixture("search_results_wrc_page1.html"), SEARCH_URL)
    identifiers = {row.identifier for row in rows}
    assert any(" " in identifier for identifier in identifiers), identifiers


def test_row_without_a_link_is_skipped() -> None:
    html = """
    <div class="item-list search-list"><ul>
      <li class="each-item"><h2 class="title">no link</h2></li>
      <li class="each-item"><h2 class="title"><a href="/en/cases/x.html">X1</a></h2>
        <span class="refNO">X1</span></li>
    </ul></div>
    """
    rows = parse_result_rows(html, SEARCH_URL)
    assert [row.identifier for row in rows] == ["X1"]


# --- dates ----------------------------------------------------------------------


def test_wrc_dates_are_day_first() -> None:
    assert parse_wrc_date("30/01/2024") == dt.date(2024, 1, 30)
    assert parse_wrc_date("  01/12/1998 ") == dt.date(1998, 12, 1)


@pytest.mark.parametrize("bad", [None, "", "2024-01-30", "31/02/2024", "not a date"])
def test_unparseable_dates_become_none(bad: str | None) -> None:
    assert parse_wrc_date(bad) is None


# --- detail pages ---------------------------------------------------------------


def test_detail_page_with_inline_decision_has_no_attachment() -> None:
    html = load_fixture("detail_inline_html.html")
    detail_url = "https://www.workplacerelations.ie/en/cases/2024/february/lcr22912.html"
    assert find_document_url(html, detail_url) is None
    assert 'class="page-title">LCR22912<' in html, "fixture really is the LCR22912 page"


def test_detail_page_attachment_is_resolved_to_an_absolute_url() -> None:
    html = load_fixture("detail_with_pdf.html")
    detail_url = (
        "https://www.workplacerelations.ie/en/cases/2010/january/"
        "ud893_2008_rp767_2008_mn822_2008_wt369_2008.html"
    )
    assert find_document_url(html, detail_url) == (
        "https://www.workplacerelations.ie/en/eat_import/2010/01/"
        "0db08dbf-90f7-4780-8b6c-70ce1af103d7.pdf"
    )


def test_pdf_preview_image_is_not_mistaken_for_the_document() -> None:
    """The download block renders an <img> at the same PDF plus a resizing query string."""
    html = load_fixture("detail_with_pdf.html")
    assert "type=pdfPreview" in html
    document_url = find_document_url(html, "https://www.workplacerelations.ie/en/cases/x.html")
    assert document_url is not None
    assert "pdfPreview" not in document_url


def test_equality_tribunal_attachment_is_found() -> None:
    html = load_fixture("detail_with_pdf_equality.html")
    detail_url = "https://www.workplacerelations.ie/en/cases/1998/december/ee22-1998.html"
    assert find_document_url(html, detail_url) == (
        "https://www.workplacerelations.ie/en/Equality_Tribunal_Import/"
        "Database-of-Decisions/1998/EE-1998-22.pdf"
    )


def test_site_chrome_pdfs_outside_the_content_block_are_ignored() -> None:
    """The page footer links a cookie-policy PDF; it must never be mistaken for a decision."""
    html = load_fixture("detail_inline_html.html")
    assert "cookie_policy.pdf" in html
    assert find_document_url(html, "https://www.workplacerelations.ie/en/cases/x.html") is None


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://x.ie/a/b.pdf", "pdf"),
        ("https://x.ie/a/b.PDF", "pdf"),
        ("https://x.ie/a/b.docx", "docx"),
        ("https://x.ie/a/b.doc", "doc"),
        ("https://x.ie/a/b.rtf", "rtf"),
        ("https://x.ie/a/b.html", "html"),
        ("https://x.ie/a/b.pdf?v=2", "pdf"),
        ("https://x.ie/a/b", "html"),
    ],
)
def test_document_type_from_url(url: str, expected: str) -> None:
    assert document_type_from_url(url) == expected


# --- identifier vs the site's own "Ref no" --------------------------------------


def test_identifier_is_the_decision_reference_not_the_cms_ref() -> None:
    """For EAT rows span.refNO is an opaque CMS id; the reference lives in the heading."""
    html = """
    <div class="item-list search-list"><ul>
      <li class="each-item">
        <h2 class="title" title="UD570/2009"><a href="/en/cases/2010/january/ud570_2009.html"
           title="UD570/2009">UD570/2009</a></h2>
        <span class="date">12/01/2010</span>
        <div class="ref"><span>Ref no: </span><span class="refNO">35575</span></div>
      </li>
    </ul></div>
    """
    (row,) = parse_result_rows(html, SEARCH_URL)
    assert row.identifier == "UD570/2009"
    assert row.title == "UD570/2009"
    assert row.reference_no == "35575"


def test_labour_court_reference_and_ref_no_coincide() -> None:
    rows = parse_result_rows(load_fixture("search_results_page1.html"), SEARCH_URL)
    assert rows[0].identifier == rows[0].reference_no == "LCR22912"


def test_trailing_whitespace_is_stripped_from_identifiers() -> None:
    html = """
    <div class="item-list search-list"><ul>
      <li class="each-item">
        <h2 class="title"><a href="/en/cases/x.html" title="ADJ-00049619 ">ADJ-00049619 </a></h2>
        <span class="refNO">ADJ-00049619</span>
      </li>
    </ul></div>
    """
    (row,) = parse_result_rows(html, SEARCH_URL)
    assert row.identifier == "ADJ-00049619"


def test_ref_no_is_used_when_the_heading_is_empty() -> None:
    html = """
    <div class="item-list search-list"><ul>
      <li class="each-item">
        <h2 class="title"><a href="/en/cases/x.html"></a></h2>
        <span class="refNO">35575</span>
      </li>
    </ul></div>
    """
    (row,) = parse_result_rows(html, SEARCH_URL)
    assert row.identifier == "35575"


# --- attachment selection precedence -------------------------------------------


def test_download_widget_wins_over_a_document_cited_in_the_decision_body() -> None:
    """A PDF cited inside the decision text must not displace the real attachment."""
    html = """
    <div class="content">
      <p>See <a href="/en/some/citation/other-case.pdf">an earlier decision</a>.</p>
    </div>
    <div class="related-items related-file">
      <a class="download" href="/en/eat_import/2010/01/real-document.pdf">Download</a>
    </div>
    """
    assert find_document_url(html, "https://www.workplacerelations.ie/en/cases/x.html") == (
        "https://www.workplacerelations.ie/en/eat_import/2010/01/real-document.pdf"
    )


def test_an_import_path_wins_over_an_earlier_body_citation() -> None:
    html = """
    <div class="content">
      <p>See <a href="/en/publications/guide.pdf">the guide</a>.</p>
      <ul><li><a href="/en/Equality_Tribunal_Import/Database-of-Decisions/1998/EE-1998-22.pdf">
        EE-1998-22.pdf</a></li></ul>
    </div>
    """
    assert find_document_url(html, "https://www.workplacerelations.ie/en/cases/x.html").endswith(
        "/en/Equality_Tribunal_Import/Database-of-Decisions/1998/EE-1998-22.pdf"
    )


def test_a_lone_body_document_is_still_used_when_no_import_path_matches() -> None:
    html = '<div class="content"><ul><li><a href="/en/other/decision.pdf">d</a></li></ul></div>'
    assert find_document_url(html, "https://www.workplacerelations.ie/en/cases/x.html").endswith(
        "/en/other/decision.pdf"
    )


def test_footnote_anchors_are_not_documents() -> None:
    """Real Equality Tribunal decisions carry '#_ftn1' style footnote links."""
    html = '<div class="content"><p><a href="#_ftn1">[1]</a><a href="#_ftnref1">back</a></p></div>'
    assert find_document_url(html, "https://www.workplacerelations.ie/en/cases/x.html") is None


# --- body-label robustness ------------------------------------------------------


def test_body_label_text_is_read_from_descendants_too() -> None:
    html = """
    <span id="CB2">
      <input id="CB2_0" type="checkbox" value="3" />
      <label for="CB2_0"><span>Labour Court</span></label>
    </span>
    """
    assert parse_bodies(html) == [Body(body_id="3", name="Labour Court")]


def test_an_unreadable_label_fails_instead_of_dropping_a_body() -> None:
    html = """
    <span id="CB2">
      <input id="CB2_0" type="checkbox" value="3" /><label for="CB2_0">Labour Court</label>
      <input id="CB2_1" type="checkbox" value="1" /><label for="CB2_1"></label>
    </span>
    """
    with pytest.raises(BodyDiscoveryError, match="no readable label"):
        parse_bodies(html)
