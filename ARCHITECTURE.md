# Architecture Decisions

## Immutable Landing Zone, then transformation

Ingestion stores exactly what the server returned and never rewrites it; cleaning is a
separate stage reading only the Landing Zone, so a parser bug or a changed cleaning rule is
fixed by re-running transformation over bytes we already hold. Landing records are
append-only: a changed decision lands as a new version beside its predecessor.

## Why monthly date partitions

The WRC search takes a date range and returns a paginated result set, so the crawl must be
sliced. Months are the natural unit: they match how decisions are published, they keep each
partition's result count in the tens-to-hundreds (a few pages, not thousands), and they give
a stable re-run key — `partition_date` is always the partition's start, so "re-ingest March
2024" is unambiguous. Partitions are contiguous and non-overlapping by construction, first
and last truncated to the requested interval, so no date is crawled twice or skipped.
`WRC_PARTITION_SIZE` allows daily/weekly/yearly for denser bodies or sparse historical years.

Partitioning also makes accounting tractable. Each `(body, partition)` reports
`records_found` — the site's own total — against successful/unchanged/failed, and
`found == successful + unchanged + failed` is enforced per partition and per run. Pages come
from that total rather than from following pager links, because the site renders only a
ten-page window and following it would silently drop page 11 onward.

## Retries and rate limiting

Politeness is the default, not a flag. Scrapy's `AutoThrottle` adapts the delay to observed
latency over a fixed floor, per-domain concurrency is low, and `robots.txt` is obeyed.
Transient statuses (`408, 429, 500, 502, 503, 504, 522, 524`) are retried with backoff;
permanent ones are not. Concurrency, delay, timeout and retry count are environment
variables.

Retry *exhaustion* is a first-class outcome: the errback books the record as failed and logs
identifier, URL, body, partition, status, reason and retry count. Callbacks are wrapped too,
because Scrapy routes callback exceptions to `spider_error` rather than the errback, and an
unguarded one would leave a row counted but unresolved.

## Deduplication and idempotency

Identity is content-addressed. `file_hash` is the SHA-256 of the exact stored bytes.
`version_hash` is the dedup identity: equal to `file_hash` for attachments, but for HTML
computed with two volatile server markers excluded — the CMS appends a render-timing comment
and a cache-hit marker after the closing markup, so raw bytes are never identical twice and a
naive hash would land a fake new version every run. Stored bytes are never normalised.

Unique Mongo indexes on `(source, body, identifier, version_hash)` and, for output,
`(…, landing_version_hash, transformation_version)` make duplicates impossible rather than
unlikely; object keys embed the same hashes, so unchanged content maps to the identical key
and is not re-uploaded. Objects are written before metadata, so a record can never claim an
object that is not there, and the reverse failure self-heals next run. A mutable
`landing_state` collection holds each identifier's hash and HTTP validators, letting
attachments answer `304 Not Modified`; HTML sends no validators, so its bytes must be
fetched to compare and are simply not re-persisted.

## Mongo + MinIO

Heterogeneous documents with evolving metadata suit a document store, and the bytes belong
in object storage. MinIO speaks S3 through boto3, so moving to AWS S3 changes an endpoint
and credentials, not ingestion logic.

## Orchestration

Dagster runs the two stages as ops with a real data dependency, so the date range is
configured once and transformation cannot start unless ingestion exited cleanly. Both ops
shell out to the existing CLIs: Scrapy's Twisted reactor cannot restart in a process that
already ran one, so an in-process op would fail on the second run.

## What changes for 50+ sources

Little of the above. The shape that scales is a **common normalized document contract** —
the landing schema (`source, body, identifier, published_date, hashes, storage path,
partition`) is already source-agnostic — with a **per-source adapter** supplying only what
differs: request contract, result-row selectors, content container. That knowledge already
sits in `parsing.py` and `html_cleaner.py`, which become one module per source behind that
interface while storage, accounting, hashing and idempotency stay untouched.

Operationally `(source, body, partition)` is already an independent unit of work, so
throughput comes from **partition-level parallelism** with **per-source rate limits**, so a
polite budget for one site is never spent by another. If one machine stops being enough the
same units fan out to distributed Dagster workers — a deployment change, not a redesign.
Storage moves to managed Mongo and cloud S3 with no code change. **Backfill scheduling**
separates the historical sweep from a routine incremental job. **Central observability** is
worth building early: the JSON events and per-partition invariant already exist, so shipping
them to one place yields the per-source completeness dashboards that are what actually break
at fifty sources. Kafka, Kubernetes and a plugin framework are prerequisites for none of
this, and none are used here.
