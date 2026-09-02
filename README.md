# WRC decisions — ingestion / landing zone

Scrapy ingestion for Ireland's [Workplace Relations](https://www.workplacerelations.ie)
legal decisions. Raw bytes land in S3-compatible object storage (MinIO locally, AWS S3
unchanged); metadata lands in MongoDB. The landing zone is **immutable**: nothing is ever
updated or deleted, and a changed document becomes a new version beside the old one.

This is the ingestion stage only. Transformation and Dagster orchestration are out of scope.

---

## 1. Discovered request contract

Established by probing the live site, not from documentation.

### Body filter discovery

`GET /en/search/?advance=true` renders the advanced search form. The Body filter is a
checkbox list in `span#CB2`:

```html
<span id="CB2">
  <input id="CB2_0" type="checkbox" name="ctl00$ContentPlaceHolder_Main$CB2$CB2_0" value="2" />
  <label for="CB2_0">Employment Appeals Tribunal</label>
  ...
</span>
```

| Body                           | `body` value |
| ------------------------------ | ------------ |
| Employment Appeals Tribunal    | `2`          |
| Equality Tribunal              | `1`          |
| Labour Court                   | `3`          |
| Workplace Relations Commission | `15376`      |

Values are read from the page on every run rather than hardcoded. If `span#CB2` yields no
options the run aborts with `body_discovery_failed` instead of scraping nothing.

### Search

The form itself is an ASP.NET WebForms POST with a ~370 KB `__VIEWSTATE`, **but its own
pager emits plain GET links**, so the whole search is addressable without VIEWSTATE or a
session:

```text
GET /en/search/?decisions=1&from=DD/MM/YYYY&to=DD/MM/YYYY&legislationsub=&body={id}&pageNumber={n}
```

| Parameter    | Notes                                                                     |
| ------------ | ------------------------------------------------------------------------- |
| `decisions`  | `1` — selects the decisions dataset. Required.                            |
| `from`, `to` | Inclusive, `dd/mm/yyyy` (the format the site's own datepicker emits).      |
| `body`       | **One body per request.** Repeated or comma-joined values are *ignored* and silently widen the search to every body. |
| `pageNumber` | 1-based; 10 results per page.                                             |

Verified: Jan 2024 gives Labour Court 45 + WRC 234 + EAT 0 + Equality 0 = 279, which is
exactly the unfiltered total for the same range.

### Result count and pagination

`div.searchhead` carries the authoritative total:

```html
<div class="searchhead">Shows 1 to 10 of 45 results</div>
```

An empty result set renders `There are no search results fitting your keywords`.

The rendered pager (`ul.pager`) only ever shows a **10-page window**, so a 234-result
partition would lose pages 11–24 if the crawler followed pager links. Pages are therefore
derived as `ceil(total / page_size)` from the reported total, which is also what makes the
exhaustiveness accounting possible. `page_size` is taken from the number of rows page 1
actually rendered rather than from config, so a change to the site's page size cannot
silently truncate pagination (`WRC_RESULTS_PER_PAGE` is only a fallback for an empty
first page).

### Result rows

```html
<li class="each-item clearfix">
  <h2 class="title" title="LCR22912"><a href="/en/cases/2024/february/lcr22912.html">LCR22912</a></h2>
  <span class="date">30/01/2024</span>
  <p class="description" title="SONOMA VALLEY ...">…</p>
  <span class="refNO">LCR22912</span>
</li>
```

Two gotchas:

* The decision date (30/01/2024) does not match the month in the URL path
  (`/2024/february/`) — dates come from `span.date`, never from the URL.
* `span.refNO` repeats the decision reference for WRC and Labour Court records, but for
  Employment Appeals Tribunal records it is an opaque CMS id (`35575` for `UD570/2009`).
  The `identifier` is therefore taken from the row heading — the real decision reference —
  and `refNO` is kept as a separate `reference_no` field.

**Result ordering is not a stable total order.** Across the five page requests of one
partition the site can return the same row twice, which means another row it counted was
never rendered. Observed on a real re-run of Labour Court January 2024. It cannot be
prevented client-side, so it is detected and reported as
`partition_pagination_unstable` with `records_possibly_missed`.

### Detail pages and documents

`h1.page-title` repeats the decision reference. The document is one of two shapes:

* **Inline HTML** — WRC and Labour Court decisions carry the full text in `div.content`.
  There is no attachment, so the detail page *is* the document and its original bytes are
  stored untransformed.
* **Attachment** — older imports link the original file. Two markup variants:
  * `div.related-items.related-file a.download` (EAT imports)
  * `div.content ul li a[href$=".pdf"]` (Equality Tribunal imports)

  Candidates are taken most-trustworthy-first: the download widget, then a body link under
  a known import root (`/en/eat_import/`, `/en/equality_tribunal_import/`,
  `/en/labour_court_import/`), then any other non-HTML body link. Decision bodies contain
  their own `<a>` links — footnote anchors, and potentially cited documents — which must
  not displace the real attachment. Only `<a href>` values are considered: the download
  widget also renders a preview `<img src="….pdf?type=pdfPreview&width=200">`.

### HTTP validators

| Resource                        | `ETag` | `Last-Modified` | Conditional GET               |
| ------------------------------- | ------ | --------------- | ----------------------------- |
| Attachment PDFs (IIS static)    | yes    | yes             | `If-None-Match` → **304** ✅   |
| Detail HTML pages (ASP.NET)     | no     | no              | none (`Cache-Control: no-cache`) |

`If-Modified-Since` is *not* honoured by the server (its `Last-Modified` values lack a
timezone), so `If-None-Match` is the validator that works. Where no validator exists the
document is downloaded, hashed, and simply not persisted again if the hash is unchanged.

### HTML pages are not byte-stable

Every page the CMS renders ends with server instrumentation appended after the closing
markup:

```html
<!-- Elapsed time: 0.1562556 -->                 <!-- always; changes on every fetch -->
<!-- cached or not being index.aspx page -->     <!-- only when served from cache -->
```

The timing value changes on **every single fetch**, and the cache marker appears and
disappears depending on whether the CMS served the page from cache — so the raw bytes of an
HTML document are never reliably identical twice and cannot serve as the version identity.
A naive implementation lands a new "version" of every HTML decision on every run (and the
cache marker makes it look like a real content change hours later, which is how it was
caught here). Both variants were enumerated from the trailing bytes of every stored HTML
object; attachment PDFs are byte-stable.

The resolution keeps both properties the assessment asks for:

| Field          | Meaning                                                              |
| -------------- | -------------------------------------------------------------------- |
| `file_hash`    | SHA-256 of the **exact bytes stored** — integrity of the landed object |
| `version_hash` | SHA-256 that identifies the **version** — the unique index and object key use this |

For attachments the two are identical. For HTML, `version_hash` excludes the render-timing
comment. **The stored bytes are still the untransformed original**, comment included —
nothing is cleaned or rewritten, only the identity hash ignores a known server artefact.

### robots.txt — read this before running a large crawl

`https://www.workplacerelations.ie/robots.txt` contains, among others:

```text
Disallow: /Cases/
Disallow: /en/Cases/
Disallow: /en/EAT_Import/
Disallow: /en/Equality_Tribunal_Import/
Disallow: /en/Labour_Court_Import/
```

Those entries are **mixed case**, and robots.txt paths are case-sensitive (RFC 9309), which
is how Scrapy's parser (Protego) treats them. The URLs the site actually serves and links
from its own search results are lowercase, so with `WRC_ROBOTSTXT_OBEY=true`:

| URL                                                          | Verdict |
| ------------------------------------------------------------ | ------- |
| `/en/search/?...`                                            | allowed |
| `/en/cases/2024/february/lcr22912.html`                      | allowed |
| `/en/eat_import/2010/01/….pdf`                               | allowed |
| `/en/Equality_Tribunal_Import/Database-of-Decisions/….pdf`   | **denied** |

Equality Tribunal attachments are linked with capitalised paths, so they are blocked. The
crawler does **not** rewrite URL casing to get around that — a blocked request surfaces as
a `document_download_failed` event with `error_type: IgnoreRequest` and is counted as a
failure, so the shortfall is visible rather than silent.

If you have permission to crawl those sections, set `WRC_ROBOTSTXT_OBEY=false`. That is a
deliberate, logged configuration choice, not a default.

---

## 2. Layout

```text
wrc_pipeline/
    config.py                 typed settings (pydantic-settings, WRC_ env prefix)
    logging.py                JSON formatter + run_id-bound EventLogger
    ingestion/
        partitions.py         contiguous, non-overlapping date partitions
        parsing.py            pure HTML parsing (no Scrapy types) — unit tested on fixtures
        accounting.py         per-(body, partition) record tally and its invariant
        items.py              LandingDocument: metadata + the exact document bytes
        spider.py             request flow: bodies → partitions → pages → detail → document
        pipelines.py          hash → object storage → immutable metadata → mutable state
        settings.py           Scrapy settings derived from config.py
        run.py                CLI entrypoint
    storage/
        object_store.py       boto3 S3/MinIO, key building, slug sanitisation, SHA-256
        mongo.py              landing_documents (append-only) + landing_state (mutable)
tests/                        145 tests, fixtures captured from the live site
docker-compose.yml            MongoDB + MinIO only
```

### Request flow

```text
/en/search/?advance=true ──► parse_bodies ──► for each body × partition
                                                  │
                             page 1 ──► parse_search ──► schedules pages 2..ceil(N/10)
                                                  │
                                          each result row
                                                  │
                                        detail page ──► parse_detail
                                            │                │
                                    inline HTML          attachment
                                            │                │
                                            │        conditional GET ──► 304 → unchanged
                                            └────────────────┴──► LandingZonePipeline
```

---

## 3. Immutability and idempotency

**Object keys** are deterministic and content-addressed:

```text
landing/{body_slug}/{partition_date}/{identifier_slug}/{version_hash}.{ext}
landing/labour-court/2024-01-01/lcr22912/00618c…8a.html
```

Every external string is slugified to `[a-z0-9-]` before it reaches a key, so path
traversal, control characters and non-ASCII cannot leak in. Over-long values are truncated
with a digest of the original appended, so two long identifiers cannot collide.

Because the filename is the content hash, unchanged bytes produce the identical key
(nothing is rewritten) and changed bytes land in the *same folder* beside their predecessor.

**MongoDB `landing_documents`** — append-only, with a unique index on
`(source, body, identifier, version_hash)`. A duplicate insert is caught and reported as
`document_unchanged`, never as an error and never as an update.

`ObjectStore.put_if_absent` returns a `StoredObject` describing whatever now occupies the
key, and `file_hash`/`file_size` in the metadata come from that — never from the bytes the
current run happened to download. Without this, a crash between the object write and the
metadata write let a later refetch (same `version_hash`, different render-timing comment)
record a `file_hash` for bytes that were never stored. Covered by
`test_metadata_file_hash_always_describes_the_stored_object`.

**MongoDB `landing_state`** — the only mutable collection, one document per
`(source, body, identifier)`: `latest_hash`, `etag`, `last_modified`,
`latest_metadata_id`, `last_seen_run_id`. It exists purely to build conditional requests
and short-circuit unchanged content, and can be rebuilt from `landing_documents`.

| Scenario                        | Outcome                                                   |
| ------------------------------- | --------------------------------------------------------- |
| Same crawl run twice            | 1 Mongo record, 1 object; second run reports `unchanged`   |
| Attachment unchanged, ETag held | `304 Not Modified`, no body transferred                    |
| Content changed                 | New hash → new object + new Mongo version; old preserved   |
| Content reverted to an old hash | Unique index rejects it → `version_already_landed`         |
| HTML refetched (new render time)| Same `version_hash` → `unchanged`; stored bytes untouched   |
| Crash between object and metadata write | Next run reuses the object already there and records **its** hash/size |

---

## 4. Accounting — no silent loss

Every `(body, partition)` tracks `records_found` (whatever the site itself reported) and
resolves every result row exactly once, so this always holds:

```text
records_found == records_successful + records_unchanged + records_failed
```

* `records_successful` — a new immutable version was landed.
* `records_unchanged` — nothing new to land: hash match, `304`, an already-landed version,
  or a duplicate row within the same partition (also reported separately as
  `records_duplicate_rows`).
* `records_failed` — an error, **plus** any shortfall between the site's reported total and
  the rows it actually rendered, logged as `partition_records_unaccounted`.

A duplicate row implies the site skipped one it counted, so `records_duplicate_rows > 0`
also raises `partition_pagination_unstable` with `records_possibly_missed`: the tally
balances, but the gap is stated rather than absorbed.

A partition whose result count cannot be read is closed with `partition_status: "failed"`
rather than as an empty partition, so a broken search page can never look like an empty
date range. Each `partition_completed` carries `accounting_balanced`, and so does
`run_completed`.

### Log events

All logs are single-line JSON and carry `run_id`.

`run_started` · `landing_zone_ready` · `bodies_discovered` · `partition_started` ·
`partition_result_count` · `document_persisted` · `document_unchanged` ·
`document_download_failed` · `document_persist_failed` · `search_page_failed` ·
`partition_records_unaccounted` · `partition_records_overcount` ·
`partition_pagination_unstable` · `partition_failed` · `partition_completed` ·
`document_state_lookup_failed` · `document_state_touch_failed` · `run_completed` ·
`run_unsuccessful`

Scrapy routes a *callback* exception to `spider_error`, not to the request's errback, so
every detail/document callback runs inside a boundary that books an unexpected error
against that record — with identifier, body and partition — instead of letting the row
sit counted-but-unresolved until shutdown.

### Exit code

`0` only when the run finished, no partition failed, and the tally balanced. A run that
aborted (e.g. `body_discovery_failed`) or lost a whole partition exits `1` and logs
`run_unsuccessful`. Individual record failures are reported in the logs and totals but do
not fail the process.

Every failure logs the identifier (when known), URL, body, partition, HTTP status when
available, error type/reason, `retry_times` and `retry_exhausted`.

---

## 5. Quickstart

```bash
docker compose up -d                     # MongoDB + MinIO
cp .env.example .env                     # required: supplies the local MinIO credentials

uv venv --python 3.12 .venv
uv pip install -e ".[dev]"

# smallest useful real crawl: one body, one month
.venv/Scripts/python -m wrc_pipeline.ingestion.run \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --bodies "Labour Court" --partition-size monthly
```

The assessment's partitioning example, without touching the network:

```bash
.venv/Scripts/python -m wrc_pipeline.ingestion.run \
  --start-date 2024-01-15 --end-date 2024-04-10 --dry-run
# 2024-01-15  2024-01-15..2024-01-31
# 2024-02-01  2024-02-01..2024-02-29
# 2024-03-01  2024-03-01..2024-03-31
# 2024-04-01  2024-04-01..2024-04-10
```

`scrapy crawl wrc_decisions -a start_date=… -a end_date=…` also works via `scrapy.cfg`.

MinIO console: <http://localhost:9001> (`minioadmin` / `minioadmin`).

---

## 6. Configuration

Everything is environment-driven with the `WRC_` prefix — see `.env.example`. No
environment-specific values are baked into code.

| Group        | Variables                                                                                   |
| ------------ | ------------------------------------------------------------------------------------------- |
| Mongo        | `WRC_MONGO_URI`, `WRC_MONGO_DATABASE`, `WRC_MONGO_LANDING_COLLECTION`, `WRC_MONGO_STATE_COLLECTION` |
| Object store | `WRC_S3_ENDPOINT_URL` (empty for AWS S3), `WRC_S3_REGION`, `WRC_S3_ACCESS_KEY_ID`, `WRC_S3_SECRET_ACCESS_KEY`, `WRC_LANDING_BUCKET`, `WRC_S3_CREATE_BUCKET` |

S3 credentials are **unset in code**. When they are absent boto3 falls back to the standard
AWS credential chain (environment, shared config, instance/IRSA role), so deploying to AWS
needs no code change; the local MinIO values live in `.env.example`. This is why
`cp .env.example .env` is a required setup step and not a convenience.
| Partitioning | `WRC_PARTITION_SIZE` (`daily`/`weekly`/`monthly`/`yearly`, default `monthly`)                |
| Scrapy       | `WRC_CONCURRENT_REQUESTS`, `WRC_CONCURRENT_REQUESTS_PER_DOMAIN`, `WRC_DOWNLOAD_DELAY`, `WRC_REQUEST_TIMEOUT`, `WRC_RETRY_TIMES`, `WRC_AUTOTHROTTLE_*`, `WRC_ROBOTSTXT_OBEY`, `WRC_USER_AGENT` |
| Logging      | `WRC_LOG_LEVEL`                                                                             |

Politeness defaults are conservative: 8 concurrent requests (4 per domain), a 0.25 s
randomised delay, AutoThrottle targeting a concurrency of 2, and retries on
`408, 429, 500, 502, 503, 504, 522, 524`. Cookies are disabled — the GET search endpoint is
stateless, and an ASP.NET session cookie would serialise the whole crawl.

Search result pages are ~810 KB each (mostly `__VIEWSTATE` and the legislation/topic option
lists), which dominates bandwidth: roughly 20 MB per 240-result body-partition.

---

## 7. Tests

```bash
.venv/Scripts/python -m pytest
.venv/Scripts/python -m ruff check .
.venv/Scripts/python -m ruff format --check .
```

No test touches the live site. `tests/fixtures/` holds real pages captured from
workplacerelations.ie; the search fixtures have only the `__VIEWSTATE` blob and the large
`<select>` option lists stripped, so every element the parsers read is byte-identical to
what the site served. MongoDB is doubled with `mongomock` and S3 with `moto`.

---

## 8. Deliberately not here

No Dagster, no transformation/cleaning stage, no API, no queue or cache, and no
repository/factory indirection over the single Mongo and single S3 implementation. The
application is not containerised — there is nothing yet that needs it.
