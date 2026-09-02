"""Pure HTML parsing for the WRC decision search.

Kept free of Scrapy request/response plumbing so it can be unit tested against saved
fixtures.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

from parsel import Selector

# "Shows 1 to 10 of 45 results" -- whitespace inside is generous in the real markup.
_TOTAL_RE = re.compile(r"of\s+([\d,]+)\s+results", re.IGNORECASE)
_NO_RESULTS_RE = re.compile(r"no search results", re.IGNORECASE)

DOCUMENT_EXTENSIONS: dict[str, str] = {
    ".pdf": "pdf",
    ".doc": "doc",
    ".docx": "docx",
    ".rtf": "rtf",
}

CONTENT_TYPE_BY_DOCUMENT_TYPE: dict[str, str] = {
    "pdf": "application/pdf",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "rtf": "application/rtf",
    "html": "text/html",
}


class BodyDiscoveryError(RuntimeError):
    """The Body filter could not be read from the advanced-search page."""


class SearchParseError(RuntimeError):
    """A search results page did not contain the expected result-count header."""


@dataclass(frozen=True, slots=True)
class Body:
    """A legal body offered by the search form's Body filter."""

    body_id: str
    name: str


@dataclass(frozen=True, slots=True)
class SearchResult:
    """One search result row.

    ``identifier`` is the decision reference (``LCR22912``, ``UD570/2009``,
    ``ADJ-00039132``) taken from the row heading. ``reference_no`` is the site's own
    "Ref no" field, which repeats the reference for WRC and Labour Court records but is an
    opaque CMS id for Employment Appeals Tribunal ones -- kept for traceability, not used
    as the identity.
    """

    identifier: str
    title: str
    description: str | None
    published_date: dt.date | None
    detail_url: str
    reference_no: str | None = None


def parse_bodies(html: str) -> list[Body]:
    """Read the Body checkbox list from the advanced-search page.

    Raises ``BodyDiscoveryError`` rather than returning an empty list, so a silently
    changed page shape stops the run instead of scraping nothing.
    """
    sel = Selector(text=html)
    checkboxes = sel.css("span#CB2 input[type=checkbox]")
    bodies: list[Body] = []
    for checkbox in checkboxes:
        value = (checkbox.attrib.get("value") or "").strip()
        checkbox_id = checkbox.attrib.get("id")
        if not value or not checkbox_id:
            continue
        # Descendant text, not just direct children: a label wrapping its text in a span
        # must not make a whole body silently disappear from the crawl.
        name = _clean(" ".join(sel.css(f'span#CB2 label[for="{checkbox_id}"] ::text').getall()))
        if not name:
            raise BodyDiscoveryError(
                f"Body checkbox {checkbox_id!r} (value {value!r}) has no readable label; "
                "refusing to crawl an incomplete set of bodies"
            )
        bodies.append(Body(body_id=value, name=name))

    if not bodies:
        raise BodyDiscoveryError(
            "no Body filter values found in span#CB2 on the advanced search page; "
            "the site markup has probably changed"
        )
    return bodies


def parse_total_results(html: str) -> int:
    """Total result count the site reports for a search, or 0 for an empty result set."""
    head = Selector(text=html).css("div.searchhead").get()
    if head is None:
        raise SearchParseError("div.searchhead missing from search response")
    text = " ".join(Selector(text=head).css("::text").getall())
    if match := _TOTAL_RE.search(text):
        return int(match.group(1).replace(",", ""))
    if _NO_RESULTS_RE.search(text):
        return 0
    raise SearchParseError(f"could not read a result count from searchhead: {text.strip()!r}")


def parse_result_rows(html: str, base_url: str) -> list[SearchResult]:
    """Extract every result row on one search results page, in page order."""
    sel = Selector(text=html)
    rows: list[SearchResult] = []
    for item in sel.css("div.search-list li.each-item"):
        href = item.css("h2.title a::attr(href)").get() or item.css("div.link a::attr(href)").get()
        if not href:
            continue
        reference_no = _clean(item.css("span.refNO::text").get())
        title = (
            _clean(item.css("h2.title a::attr(title)").get())
            or _clean(" ".join(item.css("h2.title a::text").getall()))
            or reference_no
        )
        if not title:
            continue
        rows.append(
            SearchResult(
                identifier=title,
                title=title,
                description=_clean(item.css("p.description::attr(title)").get())
                or _clean(" ".join(item.css("p.description::text").getall())),
                published_date=parse_wrc_date(item.css("span.date::text").get()),
                detail_url=urljoin(base_url, href.strip()),
                reference_no=reference_no,
            )
        )
    return rows


# The CMS keeps imported originals under these roots (they are also the paths robots.txt
# names). A link under one of them is an attachment; anything else inside the decision body
# is far more likely to be a citation.
IMPORT_ROOTS = ("/en/eat_import/", "/en/equality_tribunal_import/", "/en/labour_court_import/")

# The explicit download widget, when the page has one (EAT imports).
_DOWNLOAD_SELECTOR = "div.related-items a::attr(href)"
# Inline links, which include the attachment on Equality Tribunal imports -- and footnote
# anchors and citations on everything else.
_BODY_SELECTOR = "div.content a::attr(href)"


def find_document_url(html: str, detail_url: str) -> str | None:
    """The attached PDF/DOC for this decision, if the detail page links one.

    Older Employment Appeals Tribunal and Equality Tribunal records attach the original
    document; newer WRC and Labour Court decisions carry the full text inline instead.

    Candidates are considered most-trustworthy first -- the download widget, then a body
    link under a known import root, then any other non-HTML body link -- so a document
    cited inside the decision text cannot displace the real attachment. Only ``<a>`` hrefs
    are considered: the download widget also renders a first-page preview ``<img>``
    pointing at the same PDF with a resizing query string.
    """
    sel = Selector(text=html)
    download = _first_document(sel.css(_DOWNLOAD_SELECTOR).getall(), detail_url)
    if download:
        return download

    body_links = _documents(sel.css(_BODY_SELECTOR).getall(), detail_url)
    imported = [url for url in body_links if _is_import_path(url)]
    return next(iter(imported or body_links), None)


def _documents(hrefs: list[str], detail_url: str) -> list[str]:
    urls = (urljoin(detail_url, href.strip()) for href in hrefs)
    return [url for url in urls if document_type_from_url(url) != "html"]


def _first_document(hrefs: list[str], detail_url: str) -> str | None:
    return next(iter(_documents(hrefs, detail_url)), None)


def _is_import_path(url: str) -> bool:
    return urlparse(url).path.lower().startswith(IMPORT_ROOTS)


def document_type_from_url(url: str) -> str:
    """Map a URL's extension to a document type, defaulting to ``html``."""
    path = urlparse(url).path.lower()
    for extension, doc_type in DOCUMENT_EXTENSIONS.items():
        if path.endswith(extension):
            return doc_type
    return "html"


def parse_wrc_date(value: str | None) -> dt.date | None:
    """Parse the dd/mm/yyyy dates the site renders; ``None`` when absent or malformed."""
    text = _clean(value)
    if not text:
        return None
    try:
        return dt.datetime.strptime(text, "%d/%m/%Y").date()
    except ValueError:
        return None


def total_pages(total_results: int, per_page: int) -> int:
    if per_page <= 0:
        raise ValueError("per_page must be positive")
    return -(-total_results // per_page)  # ceil division


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    collapsed = " ".join(value.split())
    return collapsed or None


__all__ = [
    "CONTENT_TYPE_BY_DOCUMENT_TYPE",
    "DOCUMENT_EXTENSIONS",
    "IMPORT_ROOTS",
    "Body",
    "BodyDiscoveryError",
    "SearchParseError",
    "SearchResult",
    "document_type_from_url",
    "find_document_url",
    "parse_bodies",
    "parse_result_rows",
    "parse_total_results",
    "parse_wrc_date",
    "total_pages",
]
