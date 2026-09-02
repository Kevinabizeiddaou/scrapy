# WRC Legal Documents Pipeline

A two-stage pipeline over Ireland's [Workplace Relations](https://www.workplacerelations.ie)
legal decisions: Scrapy ingestion into an **immutable Landing Zone**, then a transformation
stage that turns those raw documents into cleaned, identifier-named artefacts. Dagster
orchestrates the two stages in order; both also run standalone.

## Overview

```text
workplacerelations.ie  (decision search: bodies x date range, paginated)
        |
        v  Scrapy  -- live body discovery, monthly partitions, exhaustive pagination
MongoDB landing_documents  +  MinIO landing bucket        <-- immutable, append-only
        |
        v  transformation  -- reads only the Landing Zone
MongoDB transformed_documents  +  MinIO transformed bucket
```

Raw bytes are never altered on the way in. A changed decision becomes a *new* immutable
version beside the old one; nothing is ever updated or deleted in the Landing Zone.

## Architecture at a Glance

```text
wrc_pipeline/
  ingestion/       Scrapy spider -> item pipeline -> Mongo + MinIO      (stage 1)
  transformation/  Landing Zone -> cleaner/passthrough -> Mongo + MinIO (stage 2)
  storage/         one MongoDB layer, one S3-compatible object store
  orchestration/   Dagster job: ingestion -> transformation
```

Design decisions and the 50+ source scaling story live in [ARCHITECTURE.md](ARCHITECTURE.md).

## Requirements

- **Python 3.12**
- **Docker** + Docker Compose (MongoDB and MinIO)
- [uv](https://docs.astral.sh/uv/) optional; plain `venv` + `pip` works identically

## Quick Start

```bash
# 1. virtual environment  (uv: `uv venv --python 3.12 .venv`)
python -m venv .venv

# 2. activate it
source .venv/bin/activate          # macOS/Linux
# .venv\Scripts\activate           # Windows PowerShell
# source .venv/Scripts/activate    # Windows Git Bash

# 3. install the project and its dev tooling
python -m pip install -e ".[dev]"  # uv: `uv pip install -e ".[dev]"`

# 4. configuration -- required: it supplies the local MinIO credentials
cp .env.example .env

# 5. start MongoDB + MinIO
docker compose up -d

# 6. verify both are healthy
docker compose ps

# 7. run the test suite (no Docker or network needed)
pytest

# 8. ingest a small date range
python -m wrc_pipeline.ingestion.run \
  --start-date 2024-01-01 --end-date 2024-02-10 --bodies "Labour Court"

# 9. transform the same range
python -m wrc_pipeline.transformation.run \
  --start-date 2024-01-01 --end-date 2024-02-10

# 10. or run both through Dagster
dagster job execute -m wrc_pipeline.orchestration.definitions \
  -j wrc_landing_and_transformation -c dagster_run_config.example.yaml
```

MinIO console: <http://localhost:9001> (credentials from `.env`).

## Ingestion

```bash
python -m wrc_pipeline.ingestion.run \
  --start-date 2024-01-15 \
  --end-date   2024-04-10 \
  [--bodies "Labour Court,Equality Tribunal"] \
  [--partition-size monthly] \
  [--dry-run]
```

Dates are **inclusive** on both ends and partitioned into **whole calendar months by
default**, with the first and last partitions truncated to the requested interval and no
gaps or overlaps:

```text
2024-01-15 -> 2024-04-10  becomes  2024-01-15..2024-01-31
                                   2024-02-01..2024-02-29   (leap year)
                                   2024-03-01..2024-03-31
                                   2024-04-01..2024-04-10
```

`--dry-run` prints those partitions and exits. `--partition-size` accepts
`daily`/`weekly`/`monthly`/`yearly`. `--bodies` accepts body names (any casing) or the
site's numeric body ids; omit it to crawl every body the search form advertises. The bodies
are read from the live search form on every run, never hardcoded, and the run aborts if
they cannot be determined.

**Exit code** is `0` only when the run finished, no partition failed, and the record tally
balanced. Individual document failures are logged and counted but do not fail the process.

## Transformation

```bash
python -m wrc_pipeline.transformation.run \
  --start-date 2024-01-01 --end-date 2024-02-10 [--bodies "Labour Court"] [--dry-run]
```

Reads **only** MongoDB landing metadata and the landing object store — it never touches the
source site, and never writes to the Landing Zone. Records are selected by the decision's
own `published_date`, inclusive of both calendar dates.

- **PDF / DOC / DOCX / RTF** — passed through byte-for-byte. Nothing is parsed or
  converted, so `transformed_file_hash` equals the landing `file_hash`.
- **HTML** — the decision is extracted with BeautifulSoup/lxml from the page's
  `div.content` container plus its `h1.page-title`, then re-serialised as a minimal
  standalone HTML document. Site navigation, header, footer, search controls, buttons and
  scripts are gone; headings, paragraphs, tables, lists, links and inline emphasis remain.
  A page with no recognisable decision container **fails loudly** rather than storing the
  whole website page.

Transformed files are named `identifier.ext` (`ADJ-00054658.html`). Only characters that
cannot exist in a single path segment are percent-encoded, reversibly, so `UD570/2009`
becomes `UD570%2F2009.pdf` and the original identifier is always recoverable. Filenames are
deliberately **not** slugified.

## Dagster

```bash
dagster dev -m wrc_pipeline.orchestration.definitions          # UI at localhost:3000
```

```bash
dagster job execute -m wrc_pipeline.orchestration.definitions \
  -j wrc_landing_and_transformation -c dagster_run_config.example.yaml
```

The date range is configured once, on the ingestion op, and reaches transformation through
the op dependency, so the two stages cannot drift apart. Transformation runs only after
ingestion exits `0`. Both ops invoke the CLIs above in a subprocess — Scrapy's Twisted
reactor cannot be restarted in-process, which would break every Dagster run after the
first. See `dagster_run_config.example.yaml`; it holds dates only, never secrets.

## Storage Model

| MongoDB collection      | Rule                                                                    |
| ----------------------- | ----------------------------------------------------------------------- |
| `landing_documents`     | **Append-only.** One record per document version. Never updated.        |
| `landing_state`         | Mutable bookkeeping: latest hash, ETag, Last-Modified. Rebuildable.     |
| `transformed_documents` | Append-only transformation output, with lineage to its landing version. |

| MinIO bucket      | Key shape                                                                      |
| ----------------- | ------------------------------------------------------------------------------ |
| `wrc-landing`     | `landing/{body}/{partition_date}/{identifier}/{version_hash}.{ext}`            |
| `wrc-transformed` | `transformed/{body}/{identifier}/{landing_version_hash}/v{n}/{identifier.ext}` |

Every external string is sanitised before it reaches a key. Both stores are configurable,
and the object store is plain S3 via boto3 — pointing it at AWS S3 needs no code change.

## Idempotency

- **`file_hash`** — SHA-256 of the exact stored bytes. Always describes the object at
  `storage_key`, so it doubles as an integrity check.
- **`version_hash`** — the deduplication identity. Identical to `file_hash` for
  attachments. For HTML it ignores two volatile server markers the CMS appends after the
  closing markup (`<!-- Elapsed time: N -->`, and a cache-hit marker); without that, raw
  bytes differ on every fetch and every run would land a bogus "new version". Any real
  content change — a word, a figure, whitespace, an unrelated comment — changes it. The
  **stored bytes are never normalised**.
- **Unique indexes** enforce `(source, body, identifier, version_hash)` for landing and
  `(source, body, identifier, landing_version_hash, transformation_version)` for
  transformed. A duplicate is reported as *unchanged*, never as an error.
- **Attachments** (PDF/DOC) carry `ETag`, so a stored validator produces a conditional
  request and a real `304 Not Modified` — no body transferred.
- **HTML pages send no `ETag` or `Last-Modified`** (`Cache-Control: no-cache`), so their
  bytes must be fetched before equivalence can be determined. Unchanged HTML is therefore
  still *transferred*, but it is not re-persisted: no new landing version, no new object.
- **`TRANSFORMATION_VERSION`** — bumping it produces a new transformed version of every
  document, keyed and stored separately, without deleting the old one.

## Configuration

All configuration is environment-driven with the `WRC_` prefix, typed and validated at
startup by `wrc_pipeline/config.py`. See **[.env.example](.env.example)** for every setting
with a placeholder value; the groups are:

- **MongoDB** — URI, database, the three collection names
- **Object storage** — endpoint (leave empty for real AWS S3, so boto3 uses the standard
  credential chain), region, credentials, landing and transformed buckets
- **Partitioning** — default partition size
- **Scrapy** — concurrency, per-domain concurrency, delay, timeout, retries, AutoThrottle,
  `WRC_ROBOTSTXT_OBEY`, user agent
- **Logging** — level

Nonsense values are rejected on startup rather than surfacing later: zero concurrency, a
negative delay, an empty bucket name, an unknown log level, or a transformed bucket that
collides with the immutable landing bucket.

Secrets live only in `.env`, which is git-ignored. `.env.example` contains placeholders.

## Structured Logs

Every line is single-line JSON carrying `run_id`:

```json
{"timestamp": "2024-05-01T12:00:03+00:00", "level": "INFO", "logger": "wrc.ingestion",
 "message": "partition_completed", "event": "partition_completed", "run_id": "a1b2c3",
 "body": "Labour Court", "partition_date": "2024-01-01", "partition_end": "2024-01-31",
 "records_found": 45, "records_successful": 44, "records_unchanged": 1,
 "records_failed": 0, "accounting_balanced": true}
```

For every body/partition, and again for the run as a whole:

```text
records_found == records_successful + records_unchanged + records_failed
```

Nothing is dropped silently. A row the site promised but never rendered is booked as a
failure with a reason; every failure logs the identifier, URL, body, partition, HTTP status
where available, error reason and retry exhaustion.

## Testing

```bash
pytest                    # no Docker and no network required
ruff check .
ruff format --check .
```

Fixtures under `tests/fixtures/` are real pages captured from the live site. MongoDB is
doubled with `mongomock` and S3 with `moto`.

## Known Source-Site Constraints

Upstream behaviour we detect and handle rather than paper over:

- **Pagination is not a stable total order.** Across the page requests of one partition the
  site can return the same row twice while still claiming N results, which means another
  row it counted was never rendered. This cannot be prevented client-side, so it is
  detected and logged as `partition_pagination_unstable` with `records_possibly_missed`;
  the tally stays balanced and a repeat run picks up what was missed.
- **`robots.txt` disallows some attachment paths.** The mixed-case entries
  `/en/EAT_Import/` and `/en/Equality_Tribunal_Import/` are case-sensitive per RFC 9309,
  and Equality Tribunal decisions link their PDFs with the capitalised path — so those
  attachments are blocked. They are logged as `document_download_failed` and counted as
  failures. **We do not rewrite URL casing, substitute an equivalent URL, or disable robots
  to reach them.** `WRC_ROBOTSTXT_OBEY=false` exists for an operator with explicit
  authorisation; the default is to obey.
- **HTML pages expose no HTTP validators**, so conditional requests only save bandwidth for
  attachments (see [Idempotency](#idempotency)).
- **Search result pages are ~810 KB each**, mostly ASP.NET `__VIEWSTATE`. That dominates
  ingestion bandwidth, roughly 20 MB per 240-result body-partition.

## Repository Layout

```text
wrc_pipeline/
  config.py  logging.py
  ingestion/       partitions, parsing, accounting, items, spider, pipelines, settings, run
  transformation/  naming, html_cleaner, transformer, run
  storage/         mongo, object_store
  orchestration/   definitions        (Dagster)
tests/             unit tests + captured HTML fixtures
docker-compose.yml  pyproject.toml  .env.example  ARCHITECTURE.md
dagster_run_config.example.yaml
```
