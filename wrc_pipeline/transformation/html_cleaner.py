"""Extract the legal decision from a Landing Zone HTML page.

Selector evidence, measured over every stored HTML landing object (53/53) and every saved
fixture, across WRC, Labour Court, Employment Appeals Tribunal and Equality Tribunal pages:

* ``div.content`` holds the decision and nothing else. Its ancestor chain is always
  ``div.container > div.row > div.col-sm-9``. It contains no ``script``, ``nav``,
  ``header``, ``footer``, ``form`` or button markup -- only document tags (``p``, ``table``,
  ``tr``, ``td``, ``strong``, ``b``, ``i``, ``em``, ``ul``, ``ol``, ``li``, ``span``,
  ``br``, ``img``, ``div``).
* ``h1.page-title`` immediately before it carries the decision reference (53/53).
* The *parent* ``div.col-sm-9`` additionally holds a ``<script>`` and, on attachment pages,
  the ``div.related-items`` download widget; its sibling ``div.col-sm-3`` holds the
  "Return to Search" control. So ``div.content`` is the smallest reliable container, and
  taking the parent instead would drag chrome in.

Attribute evidence, same corpus:

* ``class`` values inside the content are Word-import leftovers (``c1``--``c5``, ``BCX8``,
  ``SCXW…``, ``TextRun``, ``EOP``). No stylesheet the page loads defines any of them, so
  they are inert even on the live site.
* ``style`` declarations are only ``padding-left``, ``background-color``, ``font-size``,
  ``height``, ``margin`` and ``vertical-align`` -- layout, never ``font-weight`` or
  ``font-style``. Emphasis lives in real tags (``strong`` 1085x, ``b`` 433x, ``i`` 458x).

So presentational attributes are dropped and structural ones kept, and no formatting that
carries legal meaning is lost.
"""

from __future__ import annotations

from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Comment, NavigableString, Tag

CONTENT_SELECTOR = "div.content"
TITLE_SELECTOR = "h1.page-title"

# Schemes allowed to survive into a transformed document. The output is a stored artefact
# a reviewer may open in a browser, so a "javascript:" or "data:" href from upstream markup
# must not be carried over -- the link is dropped and its text kept.
_SAFE_SCHEMES = frozenset({"http", "https", "mailto", ""})

# Floor for "the container was found but yielded nothing usable". The shortest real
# decision in the captured corpus extracts 2,107 characters and the median is 7,566, so
# this rejects degenerate output with a wide margin and cannot fail a genuinely short one.
_MIN_TEXT_LENGTH = 200

# Dropped with their subtrees.
_DISCARD_TAGS = frozenset({"script", "style", "noscript", "iframe", "object", "embed", "form"})

# A 1x1 layout spacer the CMS emits between table cells (159 of the 160 images in the
# corpus); it is not part of any decision.
_SPACER_IMAGE_SUFFIXES = ("/icons/ecblank.gif",)

# Tags replaced by their children: inert carriers for the dead classes above.
_UNWRAP_TAGS = frozenset({"span", "font"})

# Attributes kept, per tag. Everything else is presentational or a Word artefact.
_KEPT_ATTRIBUTES: dict[str, tuple[str, ...]] = {
    "a": ("href", "title"),
    "img": ("src", "alt"),
    "td": ("colspan", "rowspan", "headers", "scope"),
    "th": ("colspan", "rowspan", "headers", "scope"),
    "ol": ("start", "type"),
}

# Structural, formatting and table tags a legal decision legitimately uses. Anything else
# inside the content (e.g. the ``o3a_p`` Word artefact) is unwrapped rather than dropped,
# so text is never lost.
_KNOWN_TAGS = frozenset(
    {
        "a",
        "abbr",
        "b",
        "blockquote",
        "br",
        "caption",
        "cite",
        "col",
        "colgroup",
        "dd",
        "div",
        "dl",
        "dt",
        "em",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "hr",
        "i",
        "img",
        "li",
        "ol",
        "p",
        "pre",
        "q",
        "s",
        "small",
        "strong",
        "sub",
        "sup",
        "table",
        "tbody",
        "td",
        "tfoot",
        "th",
        "thead",
        "tr",
        "u",
        "ul",
    }
)


class ContentNotFoundError(RuntimeError):
    """The page has no recognisable decision container, so it must not be transformed.

    Raised instead of falling back to the whole page: storing site chrome as a "cleaned"
    legal document would be worse than failing loudly.
    """


def extract_decision(html: bytes | str, *, base_url: str, title: str) -> bytes:
    """Return a minimal, deterministic HTML document holding only the decision.

    ``base_url`` resolves relative links so the output stands alone; ``title`` is the
    landing metadata identifier, used for ``<title>``.

    Raises :class:`ContentNotFoundError` if the decision container is absent or empty.
    """
    soup = BeautifulSoup(html, "lxml")
    content = soup.select_one(CONTENT_SELECTOR)
    if content is None:
        raise ContentNotFoundError(
            f"no {CONTENT_SELECTOR!r} container in the landing HTML; refusing to store the "
            "whole page as a decision"
        )

    _strip(content, base_url=base_url)
    text = content.get_text(" ", strip=True)
    if len(text) < _MIN_TEXT_LENGTH and not content.find("img"):
        raise ContentNotFoundError(
            f"{CONTENT_SELECTOR!r} container yielded only {len(text)} characters of text "
            f"(minimum {_MIN_TEXT_LENGTH}); refusing to store a truncated decision"
        )

    heading = soup.select_one(TITLE_SELECTOR)
    heading_text = heading.get_text(" ", strip=True) if heading else ""
    return _serialise(content, title=title, heading=heading_text)


def _strip(content: Tag, *, base_url: str) -> None:
    """Remove non-document nodes and presentational attributes, in place."""
    for comment in content.find_all(string=lambda s: isinstance(s, Comment)):
        comment.extract()

    for tag in content.find_all(list(_DISCARD_TAGS)):
        tag.decompose()

    for image in content.find_all("img"):
        source = image.get("src") or ""
        if any(source.endswith(suffix) for suffix in _SPACER_IMAGE_SUFFIXES):
            image.decompose()

    # Deepest-first so unwrapping a parent cannot skip its children.
    for tag in reversed(content.find_all(True)):
        if tag.name in _UNWRAP_TAGS or tag.name not in _KNOWN_TAGS:
            tag.unwrap()
            continue
        _clean_attributes(tag, base_url=base_url)

    _clean_attributes(content, base_url=base_url)


def _clean_attributes(tag: Tag, *, base_url: str) -> None:
    kept = _KEPT_ATTRIBUTES.get(tag.name, ())
    # Sorted so serialisation is byte-identical for identical input.
    resolved = (
        (name, _resolve(name, value, base_url))
        for name, value in sorted(tag.attrs.items())
        if name in kept
    )
    tag.attrs = {name: value for name, value in resolved if value is not None}


def _resolve(name: str, value: object, base_url: str) -> object | None:
    """Make ``href``/``src`` absolute, dropping any unsafe scheme.

    ``None`` means the attribute is removed.
    """
    if name not in ("href", "src") or not isinstance(value, str):
        return value
    absolute = urljoin(base_url, value.strip())
    scheme = urlparse(absolute).scheme.lower()
    return absolute if scheme in _SAFE_SCHEMES else None


def _serialise(content: Tag, *, title: str, heading: str) -> bytes:
    """Wrap the cleaned content in a minimal standalone HTML5 document."""
    document = BeautifulSoup(
        "<!DOCTYPE html>"
        '<html lang="en"><head><meta charset="utf-8"><title></title></head>'
        "<body><article></article></body></html>",
        "lxml",
    )
    document.title.string = title
    article = document.article

    if heading:
        h1 = document.new_tag("h1")
        h1.string = heading
        article.append(h1)

    for child in list(content.children):
        if isinstance(child, NavigableString) and not child.strip():
            continue
        article.append(child.extract())

    return document.decode(formatter="minimal").encode("utf-8")


__all__ = ["CONTENT_SELECTOR", "TITLE_SELECTOR", "ContentNotFoundError", "extract_decision"]
