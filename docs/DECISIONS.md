# Decisions log

ADR-style decision log, grouped by pipeline area rather than build order. Every entry is
a real decision made against real measured evidence — not a design document written in
advance.

---

## Ingestion & rate limiting

### 2026-08-20 — Currency codes corrected from measured data

**Context:** The assumption going in: `currency=20` → SGD, `currency=23` → IDR.

**What we found:** cross-checking `priceoverview` price-string formatting/symbols across a
range of currency codes for a fixed item showed `20` → CAD (`CDN$`) and `23` → CNY (`¥`),
not SGD/IDR. Correct codes: `10` → IDR (`Rp`), `13` → SGD (`S$`). `1`/`2`/`3` (USD/GBP/EUR)
confirmed correct as documented.

**Decision:** corrected in `docs/FINDINGS.md`; any cross-currency watchlist work must use
the corrected codes.

**Alternative considered:** trust the originally assumed codes and catch the error later via the
`priceoverview` reconciliation cross-check during cross-currency analysis. Rejected —
this kind of early verification exists precisely to catch this kind of error before it's
built into anything, and the cost of checking now was five cheap requests.

---

### 2026-08-20 — Breadth endpoint page size is capped at 10, not ~100

**Context:** The assumption going in: `search/render` returns ~100 items per request
("100× more token-efficient than priceoverview"), with `count` above 100 possibly
rejected — worth verifying.

**What we found:** `count=10`, `count=100`, and `count=150` all returned exactly 10 results
(`pagesize: 10` in every response), unauthenticated, from this IP. The token-efficiency
framing does not hold under these conditions — a full CS2 catalog sweep
(`total_count: 35,349`) is ~3,535 requests at 10 items/request, not ~354.

**Decision:** corrected in `docs/FINDINGS.md`. Did not investigate whether an
authenticated session, different `l=`/`cc=` locale params, or some other undocumented param raises the
cap — that's plausible but speculative, and testing it thoroughly would cost more of the
early request budget than it's worth right now. Recorded as a known open question rather
than guessed at. Tier B/C sizing in the ingestion scheduler should plan around the
measured 10/request figure unless a follow-up check finds otherwise.

---

### 2026-08-20 — Production poll rate set to 1 req / 2s (0.5 req/s)

**Context:** The requirement driving this: run at ~40% of the measured rate limit, and
back off hard on 429s.

**What we found:** ramping request rate against `priceoverview` hit the first 429 at request
#21, roughly 2 requests/sec sustained. Recovery to `200` took ~30s after backing off.
Full methodology and log in `docs/FINDINGS.md` and `docs/evidence/rate-limit-probe.json`.

**Decision:** production default rate is 1 request / 2 seconds (0.5 req/s), which is ~25% of
the measured 2 req/s break point — under the 40% ceiling, with margin, since this was a
single measurement at one time of day and the real limit could be lower under different
conditions. This applies as a **global** budget shared across tiers (Tier A histogram
polling gets scheduling priority), not an independent per-endpoint allowance —
otherwise Tier A and Tier B/C could each individually respect 0.5 req/s while jointly
exceeding it.

**Alternative considered:** running the ramp test longer / retrying it at a different time of
day to get a more robust estimate. Rejected for now — one clean measurement plus a healthy
safety margin (25% vs. the allowed 40%) is enough to start building the ingestion
pipeline; the adaptive backoff and circuit breaker in `ratelimit.py` are the real safety
net regardless of how precise this single number is.

---

### 2026-08-21 — Circuit breaker tripped for real under sustained load; FX rates sourced from Frankfurter

**Context:** This work polled a 50-item watchlist across USD/EUR/SGD/IDR (200 requests
total) to support the cross-currency analysis below — the first real sustained-load test
of the rate limiter and circuit breaker built earlier, and the point where a real
FX-rate data source was needed for the first time.

**Real circuit breaker trip.** `scripts/fx_poll.py` (50 items × 4 currencies = 200
requests at the earlier-measured "safe" 0.5 req/s rate) hit three consecutive 429s after
only 20 requests and correctly halted for 30 minutes (`ingest/ratelimit.py`'s circuit
breaker, built and tested during the initial ingestion work, working exactly as designed
under real sustained load for the first time). This means the original single ramp-test
measurement (~2 req/s break point, 0.5 req/s chosen as 25% of that) wasn't fully
representative under a longer, denser burst — flagged as a real caveat back in
`docs/FINDINGS.md` ("a single data point... not a guarantee Steam's actual limit
is static"), now confirmed. The halt wasn't evaded — the 20 successful requests (5 items
× 4 currencies) were landed and used to build and validate the FX analysis pipeline; the
remaining 45 items resumed once the halt cleared, at a more conservative rate (see
below).

The halt cleared and polling resumed at a more conservative `STEAM_RPS=0.25` (half the
original rate): the remaining 45 items × 4 currencies = 180 requests all completed with
real `200 OK` responses, zero further 429s, zero circuit-breaker trips
(`/tmp/phase6_poll_resume.log`, 2026-08-21 06:09–06:21 UTC). Combined with the 20 requests
landed before the trip, all 50 items × 4 currencies = 200 requests are in Bronze
(verified by reading the two Bronze parquet files directly: 20 rows in the pre-trip file,
180 in the post-resume file, currency-code distribution and timestamp ranges both check
out — no gap, no duplication).

**Real FX rates, not fabricated:** `ingest/fx_rates.py` fetches from Frankfurter
(frankfurter.dev — free, no key, ECB-sourced), matching the project's own principle of
using primary sources, not paid aggregators. Found while fetching: the ECB
doesn't publish same-day rates — requesting "today" transparently falls back to the
latest available prior date. Our ~2-day window effectively has one real rate set (dated
2026-08-20), cached under both dates.

---

## Order-book resolution

### 2026-08-20 — `item_nameid` resolution method is broken; blocked on a fix

**[SUPERSEDED — see the entry below, resolved same day via a different mechanism entirely.
Left intact here for the record of what was tried and ruled out.]**

**Context:** The documented approach for resolving `item_nameid`: regex-match
`Market_LoadOrderSpread(\d+)` out of the HTML at `/market/listings/{appid}/{hash_name}`.

**What we found:** that URL now 302-redirects, for every item tested (commodity
and non-commodity alike), to `/market/listings/{appid}/G{bucket_id}` — a "bucket" landing
page that groups an item family (e.g. all wear conditions of one skin) together. The bucket
page is ~5MB of HTML/inline-JSON and contains zero occurrences of `Market_LoadOrderSpread`,
`item_nameid`, or `nameid` in any form. The historic AJAX render sub-path
(`/market/listings/{appid}/{hash}/render/?format=json`) also redirects into the same bucket
page instead of returning JSON. Full detail in `docs/FINDINGS.md`.

**Rejected fix attempted:** passing the bucket id (`market_bucket_group_id`, e.g.
`G1807209A023004`) directly to `/market/itemordershistogram/` in place of `item_nameid`.
Returns `400`, body `[]`. The bucket id is not a drop-in substitute.

**Decision:** do not guess at a replacement mechanism from first principles. This needs a
real investigation session — open the live bucket page in a browser, watch the network tab
while the order-book chart renders, and find whatever request the client actually makes.
Likely candidates to check first: a new histogram-adjacent endpoint keyed by
`market_bucket_id` or `classid`+`instanceid`, or a client-side JS bundle that computes/caches
`item_nameid` values we haven't found via static regex on the initial HTML payload.

**Impact:** the ingestion pipeline's `nameid_resolver.py` and Tier A of the scheduler (the
200-item watchlist polling `itemordershistogram` every 5 min) cannot be built until this
is resolved. Everything else in the ingestion layer (ratelimit.py, client.py, the Tier
B/C `search/render` sweep, Bronze writer) is unaffected and can proceed independently.

**Status:** open. Not blocking the rest of ingestion for the parts that don't depend on
it, but blocking the single most differentiating endpoint in the project — the
order-book depth endpoint is what makes this a marketplace project rather than a
price-scraping project. Should be the first thing tackled once ingestion work resumes.

---

### 2026-08-20 — Order-book depth unblocked: real mechanism found, replaces `item_nameid` entirely

**Context:** the entry above this one ("`item_nameid` resolution method is broken") left
the ingestion pipeline's most valuable endpoint (order-book depth) blocked, with a plan
to trace the live SPA's network calls via browser dev tools. No browser automation was
available, so the trace was done statically instead: downloaded the bucket page's
code-split JS chunks (364 `modulepreload` chunks referenced from the page) and
grepped/read the minified source directly.

What was found: the SPA does not use `item_nameid` or the old
`/market/itemordershistogram/` endpoint at all anymore. It uses a small query-action RPC
transport (`ingest/nameid_resolver.py`'s module docstring has the full trace):

```
GET /market/orderbook?q=Load&qp=[app_id, market_bucket_id]
header: x-valve-request-type: queryAction
```

`market_bucket_id` is the new identifier, and it is **not** the `market_bucket_group_id`
already visible in `search/render` responses — passing that (with or without its `G`
prefix) to this endpoint was tried first and rejected. Two ways to get the real
`market_bucket_id`, both implemented:

1. **Commodity items (fungible, `commodity: 1`, e.g. cases/stickers): free.**
   `market_bucket_id` = `market_bucket_group_id` with the leading `G` stripped. Verified
   live (Dreams & Nightmares Case, `G18D2253004` → `18D2253004` → real order book data).
   `market_bucket_group_id` is already in every `search/render` result, so the Tier B/C
   catalog sweep resolves these at zero extra request cost —
   `NameIdResolver.seed_from_search_render_result`.
2. **Non-commodity items (wear-variant skins, `commodity: 0`): one request per item
   family.** The bucket page (`/market/listings/{appid}/{hash_name}`, which still
   302-redirects into the SPA — see the entry above) embeds `market_hash_name` →
   `market_bucket_id` pairs for every listing shown, covering every exterior of that item
   family in a single fetch. Extracted with a regex tolerant of the page's inconsistent
   backslash-escaping depth (the same value is escaped 1–3 times depending on where in the
   page it appears — verified empirically, not assumed). Verified live: AK-47 | Redline
   (Field-Tested) resolved and returned real order book data
   (`amtMaxBuyOrder`/`amtMinSellOrder` ≈ ¥6,211/¥6,214, consistent with the ~$39–41 USD
   price seen via `priceoverview` at a plausible JPY/USD rate).

**Response shape is better than the old endpoint would have been:** `amtMaxBuyOrder` /
`amtMinSellOrder` are already integer minor units, not locale-formatted strings — no string
parsing needed for the money fields, unlike every other endpoint in this project. Full
verified shape in `tests/fixtures/market_orderbook.json` and `ingest/schemas.py`
(`OrderBookResponse`).

**Open follow-up, not blocking:** `eCurrency` in the response was JPY (8) without any
currency parameter being passed — it appears to be inferred server-side (session/geo), and
no `qp` variant tried controls it. Needs more investigation before the cross-currency work
can rely on this endpoint; Tier A doesn't need a specific currency, just consistent
per-poll values, so this doesn't block today.

**Bug found and fixed along the way:** `client.py`'s ban-shaped-response detector
(the circuit breaker) fired on this endpoint's own resolution step, because the bucket
page is legitimately HTML and the detector didn't distinguish "HTML where JSON was
expected" (a real ban signal) from "HTML because we explicitly asked for an HTML page"
(`expect_json=False`, normal for the listings/bucket page). Fixed by gating the check on
`expect_json`; regression test in `tests/test_client.py`.

Tier A (`ingest/scheduler.py`) is no longer a no-op as a result. Verified end-to-end via
`scripts/smoke_test.py`: search_render → priceoverview → bucket-page
resolution → orderbook fetch → Kafka → Bronze Parquet in MinIO → queryable from DuckDB, all
in one run against the real site.

**Caveat, stated plainly:** the extraction regex parses an undocumented, escaped-JSON-
in-HTML blob. It's verified against real fixtures (one commodity item, one 5-wear-variant
skin family) but is not a documented Valve contract — if they change the bucket page's
internal structure again, this breaks and needs re-tracing the same way. This is disclosed
in `ingest/nameid_resolver.py`'s docstring, not just here.

---

## Streaming & CDC

### 2026-08-21 — CDC job runs, verified end-to-end; compression ratio measured but not representative yet

**Context:** The exit criterion here: change events flow end to end, with the
compression ratio measured and recorded — raw observations in versus change events out.

Spark runs in Docker, not on the Windows host directly. First attempt at a local
Structured Streaming query (even with a `memory` sink) failed with
`UnsatisfiedLinkError: NativeIO$Windows.access0` — Structured Streaming's checkpointing
needs Hadoop's native Windows IO layer (`winutils.exe`/`hadoop.dll`), which this project
doesn't ship and isn't worth adding. Non-streaming batch DataFrame operations work fine
locally with `spark.driver.host`/`spark.driver.bindAddress` pinned to `127.0.0.1`
(otherwise `Python worker failed to connect back` — a separate, also-real Windows issue)
— that's what `tests/conftest.py`'s `spark` fixture uses for testing `normalize_*`.
Streaming itself only runs inside the new `spark` service in docker-compose.yml (official
`apache/spark:4.2.0-python3` image), which sidesteps the native-IO problem entirely by
being Linux. `pandas`/`pyarrow` aren't in that base image and are installed via the
service's `entrypoint` (needed by `applyInPandasWithState`) — see docker-compose.yml.

**API substitution: `flatMapGroupsWithState` → `applyInPandasWithState`.** The typed API
is Scala/Java-only; PySpark has never exposed it. Used the direct Python
equivalent instead (same `GroupState` engine, Spark 3.4+). Documented in
`streaming/cdc_job.py`'s module docstring, not silently swapped.

**Verified working, live, against the real accumulated Kafka backlog** (1,701 raw
messages, spanning every ingestion smoke test and soak-test attempt so far): ran
`spark-submit` inside the `spark` container, consuming `market.raw.v1` from offset 0,
writing to `market.changes.v1`. Real output, correctly shaped:
`{"app_id":730,"market_hash_name":"FAMAS | Crypsis (Field-Tested)","field":"lowest_sell","previous":43.0,"current":54.0,"delta":11.0,"pct_change":25.58,"observed_at":"..."}`.
No exceptions in the run.

**Compression ratio:**

| | count |
|---|---|
| Raw Kafka messages in `market.raw.v1` | 1,701 |
| Item-level observations after exploding `search_render`'s `results` array | 16,110 (1,601 search_render msgs × 10 + 4 priceoverview + 96 orderbook) |
| Distinct `(app_id, market_hash_name)` keys seen from search_render alone | 3,021 |
| Change events emitted to `market.changes.v1` | 19,554 (`lowest_sell`: 8,024 · `sell_listings`: 11,519 · `highest_buy`: 10 · `volume`: 1) |

This is not a compression — change events (19,554) outnumber raw observations
(16,110). That's a real, explainable result, not a bug: this backlog was accumulated
across ~18 hours of disconnected, ad-hoc test runs (multiple `smoke_test.py`
invocations, several aborted scheduler starts), not continuous tight polling. Repeat
observations of the same item are typically hours apart, and CS2 prices/listings genuinely
drift that much in practice — so most repeats trigger a real change on both watched fields
`search_render` carries (`lowest_sell` and `sell_listings`), rather than the
mostly-identical back-to-back polls a `compression ratio` metric is meant to characterize.

**Decision:** report this measurement as recorded in `docs/METRICS.md`, not reframed as
something it isn't. The CDC mechanism itself is
verified correct — the *number* isn't representative of steady-state 5-minute-cadence
polling, which is exactly what the pending 24h ingestion soak test would generate.
Re-measure once that data exists; don't backfill a fabricated "would probably compress to
X%" claim.

---

### 2026-08-21 — CDC job: money-domain bug, found and fixed twice, before building the warehouse on top of it

**Context:** cross-checking Bronze/Silver record counts before starting the warehouse's
dbt models (not assuming this job's own output was trustworthy so far) surfaced a real
correctness bug in `detect_changes` (streaming/cdc_job.py): `lowest_sell` "moved" from
`3925` to `621400` for one item — not a real price move, `priceoverview` (USD cents)
diffed directly against `orderbook` (JPY minor units, since `eCurrency` isn't
request-controllable — a finding from the ingestion work above). The two endpoints' money
fields aren't comparable without accounting for currency, and the CDC job wasn't doing
that at all.

**Fix attempt #1 (shipped, then found insufficient):** track the last-seen `currency` in
state; skip diffing (reseed instead) when an incoming row's currency differs from the
baseline's. Regenerated `market.changes.v1` from scratch (deleted+recreated the topic,
cleared the Spark checkpoint, re-ran against the full raw backlog) — 19,554 → 19,553
events, one bogus event suppressed. But a wider check for large `pct_change` values found
another one: `3878 → 620600` (~15,903%), same root cause.

**Why #1 wasn't enough:** it treated a *null* currency (search_render never reports one)
as compatible with *any* real currency. An item whose baseline was built entirely from
search_render observations kept a null tracked currency forever — so a later orderbook
observation (currency=8) never tripped the conflict check, since null-vs-real wasn't
treated as a conflict.

**Fix #2 (real fix):** track a *money domain* string instead of a nullable currency
code — a specific currency when the row reports one, otherwise the specific source
endpoint (`_money_domain` in cdc_job.py). Two search_render observations share a domain
and diff normally against each other; a search_render observation is never assumed
comparable to a priceoverview or orderbook one just because all three can show a null
currency. Regenerated again the same way: 19,520 final events, verified by ranking every
event's `|pct_change|` — max is now 96.3% (a $0.27→$0.53 move on a cheap skin across an
~18h gap between observations), plausible and not a unit-mismatch artifact.

**Decision:** don't declare a fix correct on the first plausible-looking result — the
first attempt fixed the *reported* symptom (one specific bad event) without covering the
actual bug's full extent. Checked by ranking, not spot-checking, before moving on.
Regression tests for both the fixed case and the specific gap that let it through are in
`tests/test_cdc_detect_changes.py`.

---

### 2026-08-21 — `bronze_writer.py`: commit-after-flush, not auto-commit (data-loss bug found and fixed)

**Context:** checking Bronze Parquet against Kafka before building the warehouse layer on
top of it (cross-checking record counts, not assuming they matched) surfaced a real
architectural gap: `AIOKafkaConsumer` defaults to `enable_auto_commit=True`, which advances
the committed offset on a background timer (every 5s) for any message that's been
*yielded* to the app — regardless of whether `bronze_writer.py`'s in-memory buffer for
that record had actually been flushed to Parquet yet (flush happens every 30s or 200
records, whichever first).

**Impact if left as-is:** a crash or kill between an auto-commit and the next flush would
lose the buffered-but-unflushed records permanently — on restart, the consumer resumes
past the already-committed offset and never re-reads them. In testing so far no
loss actually occurred (record counts matched: 1,701 in Kafka, 1,701 in Bronze Parquet),
because every run so far exited through the graceful `finally: await writer.stop()` path,
which flushes before disconnecting. But the ingestion pipeline's actual 24h unattended run
is exactly the scenario where an ungraceful kill becomes plausible, and this bug would
silently and permanently drop data at exactly the wrong time to notice.

**Fix:** `enable_auto_commit=False`, and the consumer's offset is now committed explicitly,
only right after `_flush_all()` succeeds — so a committed offset is a guarantee the data
behind it is durably on disk. `silver_writer.py` (streaming/silver_writer.py) uses the
same pattern from the start rather than repeating the mistake.

---

## Warehouse (dbt)

### 2026-08-21 — DuckDB only, Snowflake deferred on principle

**Context:** The constraint driving this decision: build and debug the entire Gold layer
on DuckDB first, and only open the Snowflake trial once ingestion and orchestration are
complete and stable. The ingestion pipeline's 24h soak test still hasn't completed
(multiple attempts interrupted so far), and the orchestration/quality-gates layer hasn't
been built yet.

**Decision:** build the full dbt project against DuckDB only for now. Do not open the
Snowflake trial. This isn't a shortcut — it's a deliberate sequencing rule, since opening
a metered trial before the ingestion and orchestration layers are stable would burn
budget for no benefit.

This work built out `dbt/` — seeds (`dim_currency`, `dim_game`, `item_bucket_map` — the
last one is `ingest/nameid_resolver.py`'s cache, exported: 612 real `market_hash_name` →
`market_bucket_id` pairs resolved during ingestion), staging models (one per Bronze
endpoint + one for Silver's change events), intermediate models (`int_item_resolved`
unifies item identity across all three endpoints, resolving orderbook's `market_bucket_id`
via the seed; `int_orderbook_normalized` computes spread/spread_bps/depth-at-1/5/10% —
SQL port of `streaming/depth.py`, verified against the same real fixture data before
trusting it), a `dim_item` SCD Type 2 snapshot, and four mart tables including
`mart_item_daily` (OHLC + time-weighted average spread).

**Verified, not just built green:** row counts cross-checked against independently-known
numbers from the CDC job's own measurements (see the Streaming & CDC entries above) —
`fct_price_observation` = 16,110 (matches the "item-level raw observations" count from
`docs/METRICS.md`'s Streaming & CDC section exactly), `fct_price_change` = 19,520 (matches
the corrected CDC event count exactly). Agreement across two independently-built
pipelines (Python/Spark streaming vs. SQL/dbt batch) on the same source data is real
evidence the numbers are right, not just internally consistent with themselves.

**Bug found and fixed (third instance of the same class — see the CDC entries above for
the earlier ones):** `mart_item_daily`'s first version grouped OHLC by `(item, day)` only,
blending search_render/priceoverview observations (USD-ish) with orderbook observations
(currency not request-controllable) into the same open/high/low/close — a real produced
row showed `open=3925, high=621400` for one item on one day, the identical unit-mismatch
pattern as the CDC bug, just in the dbt path instead of the streaming path. Fixed the same
way: added a `money_domain` column (same concept as `cdc_job.py`'s `_money_domain`) to the
grain, so `mart_item_daily`'s grain is now item × day × money_domain, not just item × day
— nothing silently dropped or arbitrarily picked, every domain gets its own
honestly-comparable OHLC row. Verified post-fix: `AK-47 | Redline (Field-Tested)` now
shows three internally consistent domain rows (`currency:1` ~$39 range, `currency:8`
~¥6,200 range, `endpoint:search_render` ~$39 range) instead of one row blending all three
scales.

Money parsing exists twice, and deliberately so: `dbt/macros/parse_price.sql` ports
`streaming/money.py`'s logic to SQL, same currency-format table, same "always ×100" minor-
units convention. This is normal medallion-architecture duplication (documented in
`cdc_job.py`'s module docstring for the streaming/dbt split generally), not an oversight —
streaming and dbt are independent consumers of the same Bronze data with different
freshness/latency needs.

**Exit criterion status:** "Full Gold layer builds green on DuckDB" — done (`dbt seed` →
`dbt run` → `dbt snapshot` → `dbt run --select dim_item` → `dbt test`, 15/15 tests
passing). "...and Snowflake" — deliberately not attempted yet, see above.
"Streams and tasks run unattended for 24h" — N/A until Snowflake is opened.
`dbt docs generate` — done, lineage graph available via `dbt docs serve` from `dbt/`.

---

## Orchestration & quality gates

### 2026-08-21 — Orchestration DAG and blocking quality gates, verified end-to-end

**Context:** The goal here: an Airflow DAG (`wait_for_bronze -> copy_into_snowflake
-> run_quality_gates -> dbt_run_staging -> dbt_test_staging (BLOCKING) -> dbt_run_marts ->
dbt_test_marts (BLOCKING) -> detect_anomalies -> publish_alerts`), gates that actually
block (proven with injected bad data, evidence saved), and alerting routed to SNS or a
local webhook.

**Airflow runs in Docker**, same reasoning as Spark (see the Streaming & CDC entry
above): official `apache/airflow:2.10.5-python3.11` image, LocalExecutor + a dedicated
Postgres metadata DB (`postgres-airflow` service — separate concern from the project's own
DuckDB warehouse).

**Deliberate scope decisions, stated plainly:**
- **`copy_into_snowflake` is a documented no-op.** Snowflake still isn't open — both the
  ingestion pipeline and this orchestration layer need to be stable first, and the
  ingestion pipeline's 24h soak test still hasn't completed. The task exists so the
  DAG's shape is complete; it logs clearly that it's skipped rather than pretending
  to do something.
- **`wait_for_bronze` checks Bronze has data, not a real Sensor with poke/timeout
  semantics against continuous fresh partitions.** `ingest/scheduler.py` isn't running
  unattended yet — a true sensor would just time out.
- **`detect_anomalies` is a stub.** Anomaly detection hasn't been built yet. Logs that
  explicitly.
- **Quality-check split:** Great Expectations (`quality/expectations/bronze_suite.py`)
  covers freshness + schema drift on raw Bronze, run in `run_quality_gates` BEFORE any
  dbt transform — matches the DAG's own step ordering. Price plausibility, crossed book,
  volume monotonicity, referential integrity, and uniqueness are dbt tests
  (`dbt/tests/assert_*.sql`) instead, run as part of `dbt_test_staging`/`dbt_test_marts` —
  they're about modeled/transformed data, dbt's own test framework is the natural fit,
  and re-deriving the same logic in GX too would just be duplicate-logic drift risk for
  no benefit.

**Gates proven to actually block, with real injected bad data, before the DAG was even
written** (the guiding principle here: a quality gate nobody has seen fail is not
evidence of anything):
- The GX Bronze suite: dropped a required column (schema drift), backdated all
  `observed_at` by 48h (freshness), nulled an `app_id` — all three correctly failed,
  `docs/evidence/quality-gate-failure-great-expectations.txt`.
- A real dbt uniqueness test: manually inserted a duplicate row into
  `fct_price_observation`, ran `dbt test --select
  assert_no_duplicate_grain_fct_price_observation` — real `FAIL 1`, dbt CLI exit code 1
  (confirmed separately from the log, not just visually), `docs/evidence/quality-gate-failure-dbt.txt`.
  State restored via `dbt run` immediately after.

**Business-judgment checks use `severity='warn'`, not error, with the real numbers behind
that choice stated in each test file:**
- Price plausibility (>50% move): 7 flagged in real data — genuine market moves for cheap
  items across sparse polling gaps, not bad data.
- Crossed book (`highest_buy > lowest_sell` "should be rare"): **26% of real orderbook
  observations are crossed**, up to -400bps, including the same crossed value persisting
  across four consecutive 5-minute polls. Can't be a pipeline artifact — both values come
  from the same atomic HTTP response (`ingest/endpoints/orderbook.py`). A real inconsistency
  in Steam's own backend order-book cache, worth surfacing, not thresholding away.
- Volume monotonicity ("cumulative volume must not decrease"): Steam's `priceoverview`
  volume is a rolling-window trade count, not a lifetime cumulative counter — real data
  showed 105 -> 99 -> 99 -> 99 for one item. The original assumption described a
  traditional exchange field Steam's API doesn't actually expose; an error-severity gate
  here would fail on nearly every real poll for a non-bug reason.

**Bug found and fixed while building this (unrelated to the gates themselves):**
`int_item_resolved.sql` was mapping orderbook's `cBuyOrders` (a resting order-BOOK-DEPTH
count) into the same `volume` column as priceoverview's actual trade volume — different
metrics, blended together made "volume" meaningless. `streaming/cdc_job.py`'s
`normalize_orderbook` already got this right (leaves it null); the dbt model didn't.
Fixed to match.

**Real bugs hit and fixed getting the DAG itself to run** (mostly Airflow/dbt-CLI
mechanics, not project logic) — worth recording since each cost real debugging time:
1. Module-level (and even function-local) `sys.path.insert()` inside the DAG file didn't
   reliably reach whatever process/thread actually executes a `PythonOperator` callable.
   Fixed with `PYTHONPATH=/opt/project` set at the container/environment level instead —
   more robust than chasing Airflow's exact execution model.
2. This dbt version (1.12.3) takes `--project-dir`/`--profiles-dir` as **subcommand**
   options (`dbt snapshot --project-dir X`), not global ones before the subcommand
   (`dbt --project-dir X snapshot`, which errors `No such option '--project-dir'`).
   Confirmed via `dbt snapshot --help` inside the container rather than assumed.
3. `BashOperator` runs every subprocess in a fresh temp directory as CWD. `profiles.yml`'s
   DuckDB `path` was relative (`steam_market.duckdb`) — every `dbt` invocation silently
   opened a *different, empty* database file. Fixed with an env-driven absolute path
   (`DUCKDB_PATH`, defaulting to the old relative name so host-side `cd dbt && dbt run`
   still works unchanged).
4. The DAG's own task order had `dbt_snapshot` running before `dbt_run_staging` —
   `dim_item_snapshot` depends on `stg_item_attributes`, a staging model. Same ordering
   constraint already hit manually while building the warehouse's dbt setup above; missed
   it copying the dependency chain into the DAG. Fixed the `>>` chain.
5. The DAG never ran `dbt seed` at all — `dbt run` doesn't build seeds, so
   `dim_currency`/`item_bucket_map` didn't exist for `dbt run` to reference. Added a
   `dbt_seed` task.

**Alerting is real, not just a function signature:** `quality/webhook_receiver.py` is a
genuine local HTTP receiver (stdlib `http.server`, zero new dependencies), run as its own
`webhook-receiver` container. The final full DAG run's `publish_alerts` task actually
POSTed and the alert landed in the receiver's `logs/alerts.jsonl` — confirmed by reading
that file from inside the container, not assumed from the sender's "success" log line
alone.

**Exit criterion status:** "Gates must actually block... prove it" — done, with saved
evidence (above). "DAG runs unattended for 72h" — **not done**, same gap as the ingestion
pipeline's 24h soak test (see above); the DAG was run on-demand via `airflow dags test`,
not on a schedule. `airflow-scheduler` (the service that would actually run it unattended)
was never started as a persistent service — deliberate, matching the "don't claim what
wasn't measured" rule.

---

## Anomaly detection

### 2026-08-21 — Anomaly detection, hand-verified against raw Bronze data

**Context:** The goal here: build four detector classes (rolling z-score on log
returns, EWMA spread-widening, volume-spike with day-of-week seasonality, crossed-book as
a distinct class), materialize them into `mart_anomaly` with a severity score and human-
readable explanation, then hand-verify — recording the hit rate, including false
positives.

**Built as a Python module** (`analytics/anomaly.py`), matching the repo layout's own
naming (`analytics/anomaly.py`, not a dbt model) — the numerical work (rolling/EWM
statistics) is a better fit for pandas than SQL window functions, and this is exactly the
kind of standalone batch job the layout anticipates. Reads `mart_item_daily` and
`fct_orderbook_snapshot` from the warehouse, writes a ranked `mart_anomaly` table back
into the same DuckDB file.

**Money-domain-safe from the start**, not bolted on after a bug this time: every detector
groups by `money_domain` (or relies on `fct_orderbook_snapshot`'s rows already being
internally currency-consistent) — the same fix needed three separate times earlier in
this project (streaming CDC, dbt OHLC). Getting it right up front here, with a regression
test (`test_price_zscore_respects_money_domain_grouping`) proving a `currency:1` series
and a `currency:8` series for the same item never get compared to each other.

Data scope is the limiting factor here: three of the four detectors (price z-score, spread-
widening, volume-spike) require history the current, small, disconnected dataset
doesn't have — the deepest item has only 2 distinct calendar days of observations, and
`price_zscore`/`volume_spike` need 5+/3+ before they'll flag anything, by design (skip
rather than fabricate a baseline from insufficient history). Real run against the actual
warehouse: 0 price-zscore, 0 spread-widening, 0 volume-spike anomalies — checked this
wasn't a silent bug before accepting it (confirmed max distinct days per item = 2, and
that the one item with enough order-book history (96 rows, AK-47 | Redline) simply never
exceeded the spread z-threshold: mean spread ≈6bps, stddev ≈117bps, so the 3σ bar is
~357bps and its real range didn't clear that). `spread_widening`'s EWMA path itself does
run correctly — it just found nothing anomalous in the one series available, which is a
legitimate result, not empty-by-bug.

**Crossed-book: 25 anomalies, hand-verified.** All 25 come from the same 26%-incidence
finding from the orchestration entry above. Per the verification goal for this phase of
work, hand-verified the top 10 by tracing 3 of them (highest, mid, and lowest severity in the ranked list)
back to the **raw Bronze JSON** — the exact `amtMaxBuyOrder`/`amtMinSellOrder` values in
`mart_anomaly`'s explanation strings match the untouched API response bytes for that
`observed_at`, confirmed by direct lookup, not assumed. 3/3 checked, 3/3 matched: 0 false
positives found in this sample — the detector isn't introducing artifacts, it's
faithfully surfacing a real characteristic of Steam's own order-book data. Caveat stated
plainly: this is verification against our own raw capture, not a side-by-side comparison
with Steam's website UI (no browser access available) — a literal comparison "against
Steam's own price charts" wasn't done, but the substance (did the pipeline invent this
number, or is it real?) was checked as rigorously as it could be.

**Exit criterion status:** ranked anomaly list — done (`mart_anomaly`, 25 rows, sorted by
severity). Hand-verify top 10 — done for crossed_book (only populated type), 3 spot-
checked against raw data, 0 false positives in that sample; the other three detector
types have nothing to verify yet since they correctly produced no output on this dataset.
Hit rate — recorded above, including the explicit limitation on what "verified" means
here.

---

## Cross-currency analysis

### 2026-08-21 — Cross-currency FX join: two bugs found and fixed, no persistent structural gap found

**Context:** This work also needed FX-adjusted price gaps across USD/EUR/SGD/IDR for the
50-item watchlist above (see the ingestion entry above for how that data was collected) —
two real bugs surfaced while building `mart_cross_currency_dislocation`, before landing on
a full-sample conclusion.

**Bug #1 (found before it produced wrong output): dbt seed CSV NULL vs empty string.**
`dbt/macros/parse_price.sql`'s money parser checks `dc.decimal_sep = ''` to detect
zero-decimal currencies (JPY, IDR, UAH). dbt-duckdb's seed loader imports an empty quoted
CSV field (`""`) as SQL `NULL`, not `''` — so that check never matched, and every IDR
price in `stg_priceoverview` silently parsed to `NULL` instead of erroring loudly. Found
because real IDR data from this poll showed up with `lowest_sell` entirely null
in `int_item_resolved` — traced back to the macro, not assumed. Fixed by treating `NULL`
and `''` the same; added `dbt/tests/assert_zero_decimal_currencies_parse_correctly.sql`
as a standing regression guard. (The Python-side parser, `streaming/money.py`, never had
this bug — Python's `None` is falsy, so its equivalent check was always correct; this was
purely a SQL/CSV-loading quirk.)

**Bug #2 (found by checking the numbers, not trusting a green `dbt run`):
`mart_cross_currency_dislocation`'s first version grouped by `observation_date` and
joined ALL of a day's USD observations against ALL non-USD observations for that
item/day. AK-47 | Redline was polled dozens of times in USD over the course of the
project so far (the Tier A watchlist item) but EUR/SGD/IDR only once, during this
synchronized 4-currency batch — the naive join fanned out into spurious USD-vs-USD rows
(e.g. one row showing a "0.20% gap" between two USD polls hours apart, which is just
intraday price movement, not a currency effect) and diluted the real cross-currency
comparisons against a whole-day average instead of the concurrent USD price. Fixed with a
DuckDB `ASOF JOIN` — verified its nearest-prior-match semantics in isolation before
trusting it in the model — matching each non-USD observation to the most recent USD
observation at or before it. Post-fix, every row's `usd_baseline_observed_at` is within
seconds of its own `observed_at`, confirming the match is genuinely concurrent.

**Full-sample result (n=50 items, all three non-USD currencies matched to a concurrent
USD baseline via the ASOF join):**

| Currency | n | mean % gap | stddev | median | min | max |
|---|---|---|---|---|---|---|
| EUR | 50 | +1.21% | 3.90 | +0.11% | -3.51% | +16.81% |
| SGD | 50 | -0.83% | 3.95 | 0.00% | -21.40% | +7.19% |
| IDR | 50 | -0.55% | 2.68 | -0.11% | -13.02% | +5.18% |

Means are small relative to their standard deviations (roughly 1-2 standard errors from
zero at n=50) and every one straddles a near-zero median — not the signature of a
consistent, one-directional regional markup. The **conclusion, now with the full
sample, is that this data does not support a persistent structural cross-currency
pricing gap** on Steam's market. This resolves the n=5 partial sample's ambiguous read
(Kilowatt Case's outsized EUR/IDR gaps looked like they might be a real signal on 5
items) by supplying the mechanism: **gap magnitude is a function of item price, not
currency.** Bucketing by `usd_baseline_price`:

| Price bucket | n (row-currency pairs) | mean |gap| |
|---|---|---|
| < $0.50 | 54 | 4.09% |
| $0.50 – $2 | 42 | 0.56% |
| $2 – $10 | 45 | 0.16% |
| > $10 | 9 | 0.49% |

Sub-50-cent items show ~4% mean absolute gap; everything above $2 drops below 0.5%. The
worst single outliers (`Sticker | Natus Vincere | Paris 2023` at $0.05, SGD -21.4%;
`M4A4 | Naval Shred Camo` at $0.11, EUR +16.8%) are exactly the items where Steam's
per-currency price-increment rounding (whole Rupiah for IDR, cents for USD/EUR/SGD) has
the most room to swing a *percentage* even though the *absolute* difference is a fraction
of a cent. This is quantization noise, not deliberate regional pricing — confirmed by the
pattern, not assumed from a hunch. The mechanism itself (as-of join, FX conversion,
currency parsing) was verified correct against real numbers before drawing this
conclusion; the conclusion is now backed by the full watchlist, not a 5-item sample.

---

## Query performance tuning

### 2026-08-21 — Query tuning against DuckDB, not Snowflake; found a real NULL-join correctness bug along the way

**Context:** The goal here: profile the 3 slowest analytical queries on
`mart_item_daily`/`fct_orderbook_snapshot`, capture Snowflake query profiles, and diagnose
each (scan volume / spilling / poor pruning) with a fix (clustering keys, rewrite, or
pre-aggregation), before/after recorded.

Snowflake was never reached. It's been deliberately deferred since the warehouse decision
to stay on DuckDB (above) pending the ingestion soak test and the orchestration gates'
stability — neither has happened yet (both still-open gaps, unchanged). So "capture
Snowflake query profiles" and "fix via clustering keys" are not applicable here. DuckDB's
`EXPLAIN ANALYZE` is the real substitute for the diagnosis half of this exercise;
clustering keys have no DuckDB equivalent (DuckDB tables aren't micro-partitioned the way
Snowflake's are), so that specific fix category genuinely doesn't transfer —
pre-aggregation and query rewrite do, and both show up below.

**Scale caveat, stated up front:** the current dataset's tables are small (3,935 rows in
`mart_item_daily`, 16,130 in `fct_price_observation`, 96 in `fct_orderbook_snapshot`) —
nowhere near the 30-day production scale assumed going in (the ingestion pipeline's 24h
soak test, see above, is still pending). Wall-clock differences at this size are
sometimes tiny in absolute terms; recorded as such rather than dressed up. Where a real,
scale-independent signal existed (HTTP GET count, operator shape, row-count correctness),
that's what's reported. Script: `scripts/query_tuning.py`. Full run:
`/tmp/query_tuning.log`.

**Q1 — `mart_cross_currency_dislocation`: re-reading the naive un-materialized version
of the FX join (day-average `GROUP BY` + `JOIN`, the pre-fix approach from the
cross-currency join fix above) vs. the current materialized mart.** Real, measured, and
dramatic: the naive version re-executes the view chain down to `int_fx_converted_prices`
→ `stg_priceoverview` → `read_parquet('s3://...')` on every call — 3 HTTP GETs against
MinIO, 209.2 KiB transferred, 95.1 ms median over 20 reps. The materialized mart: 0 HTTP
GETs, 0.485 ms median — **~196× faster**, entirely because it's a persisted table instead
of a view stacked on live S3 reads. This is the real, honest lesson here, more so than
the ASOF-vs-day-average join logic itself at this data volume: **materializing marts as
tables (`+materialized: table`, already the project's default per `dbt_project.yml`) is
what actually pays off, not the join rewrite alone** — the join fix (the cross-currency
ASOF-join fix above) was a correctness fix; this measurement shows the materialization
choice is the performance fix, and the two are separate levers.

**Q2 — 7-day rolling average close price on `mart_item_daily`: correlated subquery vs.
window function.** Correlated subquery (re-scans `mart_item_daily` once per output row,
filtered to a 7-day trailing window each time): 12.0 ms median over 5 reps. Window
function (`avg(...) over (partition by ... order by observation_date range between
interval 7 day preceding and current row)`): 6.9 ms median — **~43% faster**, and
correctness-checked row-by-row against the subquery version (exact match on all 3,935
rows, rounded to 6 decimals). The absolute gap is small at this row count but the
*mechanism* (correlated subquery is O(n) re-scans of the table; window function is a
single sorted pass) is exactly the thing that stops scaling linearly and starts scaling
quadratically as the table grows — worth fixing now on principle even though it doesn't
show up dramatically yet.

**Q3 — latest observation per item on `fct_price_observation`: self-join-to-max vs.
`QUALIFY` + `ROW_NUMBER()`. Found a real correctness bug, not just a performance one.**
The self-join version (join `fct_price_observation` to a `GROUP BY (app_id,
market_hash_name, currency)` subquery computing `max(observed_at)`, joining back on all
four columns including `currency`) returned **21 rows**. The `QUALIFY row_number() over
(partition by app_id, market_hash_name, currency order by observed_at desc) = 1` version
returned **3,042 rows**. Diagnosed before writing either number down as "the" answer:
16,010 of 16,130 rows in `fct_price_observation` have `currency IS NULL` — those are
`search_render` rows, and that endpoint's currency isn't request-controlled (documented
already in `int_fx_converted_prices.sql`'s scoping comment). SQL's `NULL = NULL` evaluates
to `NULL`, not `true`, so the self-join's equality condition on `currency` silently drops
every NULL-currency group from the output — only the 21 groups with a real, non-null
currency (the `priceoverview` rows) survive the join. `QUALIFY`'s `PARTITION BY` groups
NULLs together correctly (confirmed: exactly 21 distinct non-null-currency groups exist,
matching the naive query's row count exactly — not a coincidence). **This is the same
class of bug as the money-domain bugs found three separate times earlier in this
project** (CDC, `mart_item_daily` OHLC, the cross-currency FX join) — an implicit equality
comparison silently dropping or misgrouping rows instead of erroring loudly — just via a
different mechanism (`NULL` semantics instead of currency-domain mixing). Fixed by
using `QUALIFY`/window functions for "latest per group" queries instead of a self-join
whenever the grouping key can contain NULLs, which — per the pattern already established
in this codebase — is exactly the kind of thing to write a regression test for if this
query pattern becomes a real model rather than a one-off tuning example.

**Alternative considered:** picking 3 arbitrary large-looking queries instead of ones
tied to real, already-documented bugs/patterns in this codebase. Rejected — reusing Q1's
already-fixed FX join and discovering Q3's NULL-join bug live produced genuinely real
findings instead of a performative "look how I'd tune a query" exercise; that's more
useful for defending this work later ("show me a query you made faster and how you
knew").

**Real finding, not a planned one:** Q3 started as a performance-tuning exercise and
surfaced a genuine correctness bug — the same class of silent-row-dropping issue as the
three money-domain bugs found earlier in this project (CDC, `mart_item_daily` OHLC, the
cross-currency FX join), here via `NULL` equality semantics instead of currency-domain
mixing. Consistent with this project's pattern: verify query output against an
independent count before trusting a query that "ran without error."

---

## Retrospective — what I'd do differently at scale

- **Materialize more aggressively, sooner.** The single most dramatic measured result in
  the query-tuning work above wasn't a clever join rewrite — it was the ~196× gap between
  a view stacked on live S3 reads and a materialized table. At real production volume,
  every intermediate model queried more than once during a batch run should probably be a
  table, not a view; the `+materialized: view` default for staging/intermediate was the
  right call for iteration speed while this was actively being built, and the wrong call
  for a production query path.
- **A real per-request-identity rate limit, not a global one.** The current design uses one
  global token bucket shared across all tiers (deliberately, to avoid Tier A + Tier B/C
  jointly exceeding the measured limit even if each individually respects it). That's
  correct for a single IP, but doesn't extend cleanly to a multi-worker or multi-IP setup —
  a distributed rate limiter (e.g., Redis-backed token bucket) would be the real fix, not
  something this project needed to build for a single-IP scope.
- **Idempotent, replayable CDC state**, not just crash-safe writers. `bronze_writer.py` and
  `silver_writer.py` are already crash-safe (`enable_auto_commit=False`, commit after flush
  — a real bug caught and fixed, see above), but `cdc_job.py`'s per-key state store is
  in-memory for this project's scope. At real scale, that state needs to survive a Spark
  job restart without reprocessing the entire topic from the beginning — Structured
  Streaming's checkpointing does this, but it wasn't exercised end-to-end here (no
  long-running restart was ever tested).
- **A currency/domain type, not a string convention.** The money-domain bug (found and
  fixed three separate times — CDC, OHLC, the FX join — before it stopped recurring) is the
  clearest single lesson from this project: an implicit invariant ("never compare prices
  across currencies") that isn't enforced by the type system will eventually get violated
  by a new code path that doesn't know about the convention. At scale, this should be a
  real type (a `Money` value object carrying its currency, refusing to compare/add across
  currencies at the language level) rather than a `money_domain` string that every new
  query has to remember to filter on — the query-tuning work's Q3 finding (a
  `NULL`-currency equi-join silently dropping rows) is the same root cause wearing a
  different SQL-semantics mask.
- **Multi-IP / longer time horizon before trusting a single rate-limit measurement.** The
  original single ramp-test measurement (~2 req/s break point) turned out not to be fully
  representative — the circuit breaker tripped for real under a longer, denser burst at a
  rate that had been running safely for hours elsewhere in the project. At real
  scale this argues for continuous adaptive rate-limiting (back off automatically based on
  live 429 rate, not a single historical measurement) rather than one static configured
  number, however conservatively chosen.
