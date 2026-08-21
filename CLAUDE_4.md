# CLAUDE.md — Steam Marketplace Intelligence Pipeline

## 0. Purpose of this document

This is the working spec for `steam-market-pipeline`. You (Claude Code) are building a
production-shaped streaming data platform over the Steam Community Market.

**This is a portfolio project with a specific commercial target.** It exists to demonstrate,
to a hiring manager at an ecommerce data company, that the author can build and operate a
real data pipeline: streaming ingestion, a lakehouse, warehouse modelling with incremental
processing, orchestration, and enforced data quality.

That target shapes every decision below. When you face a design fork, prefer the option that
produces **defensible interview material** over the option that is merely clever. If a choice
would produce a claim the author cannot explain under questioning, do not make that choice.

### Non-negotiable honesty rule

Every number that ends up in the README or on a CV must be **measured, not estimated**.
Instrument first, claim second. If a metric was not measured, write `TBD` and say so.
Never invent throughput figures, latency figures, or row counts. If a phase fails to produce
its metric, that is a finding to document, not a gap to fill with plausible-sounding numbers.

---

## 1. What we are building

A pipeline that continuously observes the Steam Community Market — a genuine two-sided
marketplace with buy orders, sell orders, and trade volume across hundreds of thousands of
tradable items — and turns it into a queryable warehouse that surfaces:

- **Price and spread history** per item, per game, per currency.
- **Order book depth** — not just the last price, but the shape of resting bids and asks.
- **Anomalies** — price dislocations, spread blowouts, volume spikes, crossed books.
- **Cross-region price dislocation** — the same item priced in USD, EUR, SGD, IDR, where
  Steam's regional pricing means the gaps are *not* pure FX.

The last two bullets are what differentiate this from every other "I scraped some prices"
project. Protect them.

### Target architecture

```
  Steam Community Market (5 endpoints, no auth, hard IP rate limit)
        │
        ▼
  Async Python poller — token bucket, tiered schedules, adaptive backoff
        │  raw JSON envelopes
        ▼
  Kafka  ──►  topic: market.raw.v1        (every poll response, verbatim)
        │
        ▼
  Spark Structured Streaming — stateful CDC
        │  diffs each snapshot against prior state
        ▼
  Kafka  ──►  topic: market.changes.v1    (only genuine price/book movements)
        │
        ├──►  Bronze:  S3 / MinIO, Parquet, partitioned dt/hour/app_id/endpoint
        │
        ▼
  Snowflake external stage ──► STREAM ──► TASK ──► incremental MERGE  (Silver)
        │
        ▼
  dbt: staging → intermediate → marts                                  (Gold)
        │
        ▼
  Airflow DAG — orchestrates batch path, dbt tests as BLOCKING gates
        │
        ▼
  Anomaly detection ──► alerting (SNS / local webhook)
```

---

## 2. Rules of engagement (read before writing any HTTP code)

These are not style preferences. Violating them gets the author's home IP banned for days
and kills the project.

1. **Single IP. No proxies, no proxy rotation, no VPN cycling.** If we get rate limited, we
   slow down. We do not evade.
2. **Respect `robots.txt` and Steam's terms.** We read public, unauthenticated endpoints at
   a polite rate. We never log in, never touch inventory endpoints, never trade.
3. **Default to far slower than the measured limit.** Phase 0 measures the real threshold;
   we then run at ~40% of it. Throughput is not the goal — an unbroken 30-day history is.
4. **Cache anything immutable, forever.** `item_nameid` never changes for a given item.
   Fetch once, persist to disk, never fetch again.
5. **Every 429 is a first-class event.** Log it, emit a metric, trigger exponential backoff
   with jitter, and *reduce the token bucket refill rate for the rest of the hour*. Never
   retry a 429 tightly.
6. **Never republish raw scraped data.** The GitHub repo contains code, schemas, dbt models,
   docs, and aggregate metrics. It does not contain a dump of Steam's market data. Add
   `data/`, `*.parquet`, `*.duckdb`, `.env` to `.gitignore` on the first commit.
7. **Circuit breaker.** If we see 3 consecutive 429s or any 403, halt all polling for 30
   minutes and write a loud log line. If we see a ban-shaped response (HTML error page
   instead of JSON), halt for 6 hours and require manual restart.

---

## 3. Data sources

Base: `https://steamcommunity.com`. All endpoints are unauthenticated and undocumented.
Treat every schema below as **unverified until Phase 0 confirms it**.

### 3.1 Breadth — `/market/search/render/`

```
GET /market/search/render/?query=&start=0&count=100&search_descriptions=0
    &sort_column=popular&sort_dir=desc&appid=730&norender=1
```

Returns items with `name`, `hash_name`, `sell_price` (integer cents), `sell_listings`,
`sale_price_text`, and an `asset_description` block.

**[CORRECTED IN PHASE 0, 2026-08-20]** `pagesize` is hard-capped at **10 items/request**
unauthenticated, regardless of the requested `count` — tested `count=10/100/150`, all three
returned exactly 10 results. The original "~100 items per request, 100× more
token-efficient than `priceoverview`" framing does not hold under these conditions; a full
CS2 catalog sweep is ~3,535 requests at 10 items/request (`total_count` was 35,349 for
`appid=730`), not ~354. Still meaningfully more efficient than `priceoverview` (10 items vs.
1 per request), just not 100×. Whether an authenticated session or an undocumented param
raises the cap is unverified — flagged as an open question in `docs/DECISIONS.md`, not
assumed. See `docs/PHASE0_FINDINGS.md` §3.1 for the raw data.

### 3.2 Depth — `/market/itemordershistogram/`

```
GET /market/itemordershistogram/?country=US&language=english&currency=1&item_nameid=176582392
```

Returns the **full order book**: `buy_order_graph`, `sell_order_graph` (each an array of
`[price, cumulative_quantity, label]`), plus `highest_buy_order` and `lowest_sell_order`.

This is the most valuable endpoint in the project — it is what makes this a marketplace
project rather than a price-scraping project. Spend tokens here on a curated watchlist.

Requires `item_nameid`, which is **not** in any JSON response.

**[SUPERSEDED, PHASE 1, 2026-08-20]** This exact endpoint no longer exists in the live
front-end. Order-book depth is still available, via a different mechanism:

```
GET /market/orderbook?q=Load&qp=[app_id, market_bucket_id]
header: x-valve-request-type: queryAction
```

`market_bucket_id` replaces `item_nameid` (see the corrected §3.3 below for how to get it).
Response fields (`amtMaxBuyOrder`, `amtMinSellOrder`, `rgCompactBuyOrders`,
`rgCompactSellOrders`, `cBuyOrders`, `cSellOrders`, `eCurrency`) are integer minor units
already — better than this section's original assumption, no string parsing needed. Full
trace and verification in `docs/DECISIONS.md`. Implemented:
`ingest/endpoints/orderbook.py`.

### 3.3 `item_nameid` resolution — `/market/listings/{appid}/{hash_name}`

Returns HTML. The id appears in an inline script call resembling
`Market_LoadOrderSpread( 176582392 )`. Extract with a regex, then **persist to
`data/cache/item_nameids.json` and never fetch that item again.**

Resolution is expensive (1 request per item, HTML-sized). Resolve the watchlist once during
Phase 0/1 and treat the cache as permanent infrastructure.

**[CORRECTED, PHASE 1, 2026-08-20]** This URL still 302-redirects (for every item tested,
commodity and non-commodity alike) to `/market/listings/{appid}/G{bucket_id}`, a "bucket"
landing page grouping the item family — confirmed still true, and `Market_LoadOrderSpread`
is indeed gone from that page. But the redirect target page turns out to still be useful:
it embeds `market_hash_name` → `market_bucket_id` pairs (a *different* id from
`market_bucket_group_id`) for every listing shown, covering every exterior of an item
family in one fetch. Two resolution paths, both implemented in
`ingest/nameid_resolver.py`:

1. **Commodity items (`commodity: 1`), free:** `market_bucket_id` = the
   `market_bucket_group_id` already returned by `search/render`, with its leading `G`
   stripped. No extra request.
2. **Non-commodity/wear-variant items, one request per family:** fetch the bucket page
   (as above) and regex-extract `market_hash_name` → `market_bucket_id` pairs from its
   embedded listing data.

Full trace (how this was found by reading the SPA's own JS bundles, without a browser) in
`docs/DECISIONS.md`. Both paths verified live against real items.

### 3.4 Point price — `/market/priceoverview/`

```
GET /market/priceoverview/?appid=730&currency=1&market_hash_name=AK-47%20%7C%20Redline%20(Field-Tested)
```

Returns `lowest_price`, `median_price`, `volume` (as display strings — parse carefully,
they carry currency symbols and locale-specific separators).

One item per request. Use sparingly, mainly as a **cross-check** against `search/render`
to prove the reconciliation logic in §7.3 works.

### 3.5 Activity — `/market/itemordersactivity/`

Recent order events for an item. Optional; explore in Phase 0 and use only if it yields
genuine event-level data. If it does, it is the closest thing to a true push feed we have.

**[UNRESOLVED, PHASE 1, 2026-08-20]** §3.2's blocker was resolved via a different mechanism
(see above), but this endpoint wasn't part of that investigation and no modern replacement
has been found for it specifically. Still not exercised. Optional per this section's own
framing either way.

### 3.6 Games to cover

| appid  | game    | why |
|--------|---------|-----|
| 730    | CS2     | deepest liquidity, most items, sharpest price action |
| 570    | Dota 2  | different item taxonomy — forces real schema normalization |
| 440    | TF2     | oldest economy, quirky legacy naming — good dirty-data source |
| 252490 | Rust    | mid-size, different seasonality |
| 578080 | PUBG    | mid-size, sparse liquidity — tests our empty-book handling |

Multi-app coverage is deliberate: heterogeneous schemas across sources is the exact problem
this project claims to solve. Do not collapse to CS2 alone.

### 3.7 Currencies

`currency=1` USD, `2` GBP, `3` EUR, `10` IDR, `13` SGD.

**[CORRECTED IN PHASE 0, 2026-08-20]** The original codes here (`20` for SGD, `23` for IDR)
were wrong — measured via price-string cross-check, `20` → CAD (`CDN$`) and `23` → CNY
(`¥`). Corrected above. `1`/`2`/`3` confirmed correct. Full table in
`docs/PHASE0_FINDINGS.md` §3.7.

Poll multiple currencies for a small watchlist only (Phase 6). Steam uses regional pricing,
so cross-currency gaps are not pure FX — that dislocation is a real analytical finding, not
a bug.

---

## 4. Tech stack

| Layer | Local dev | Production-shaped | Notes |
|---|---|---|---|
| Streaming bus | Redpanda | Kafka | Redpanda is Kafka-API compatible. **Always describe this as "Kafka (Redpanda)" in the README** — never claim bare Kafka. |
| Stream processing | Spark Structured Streaming | same | `flatMapGroupsWithState` for CDC |
| Object store | MinIO | AWS S3 | identical `s3a://` paths, swap endpoint only |
| Warehouse | DuckDB | Snowflake | see §4.1 |
| Transform | dbt-duckdb | dbt-snowflake | same models, different profile |
| Orchestration | Airflow (LocalExecutor) | same | |
| Quality | dbt tests + Great Expectations | same | |
| Language | Python 3.11+ | | `uv` for dependency management |

### 4.1 Snowflake trial discipline — important

The Snowflake trial is 30 days / $400 credit. Burning it debugging dbt syntax would be a
serious own goal, because Snowflake **streams and tasks** are the single most differentiating
item this project puts on a CV.

Therefore: **build and debug the entire Gold layer on DuckDB first.** Every dbt model must
run green locally. Only when Phases 1–4 are complete and stable do we open the Snowflake
trial, and we use it exclusively for:

- external stage → `COPY INTO` ingestion from S3
- `STREAM` + `TASK` incremental MERGE (the headline capability)
- query profile analysis and tuning with clustering keys (§8.2)
- capturing before/after performance numbers

Keep a `SNOWFLAKE_BUDGET.md` logging credit spend after each session. Use an XS warehouse
with `AUTO_SUSPEND = 60`.

---

## 5. Repository layout

```
steam-market-pipeline/
├── CLAUDE.md
├── README.md                     # written LAST, in Phase 7
├── SNOWFLAKE_BUDGET.md
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── ingest/
│   ├── ratelimit.py              # token bucket + adaptive backoff + circuit breaker
│   ├── client.py                 # async HTTP client, retry, 429 handling
│   ├── endpoints/                # one module per endpoint, each returns a typed envelope
│   ├── schemas.py                # Pydantic models for every raw payload
│   ├── nameid_resolver.py
│   └── scheduler.py              # tiered polling loop
├── streaming/
│   ├── cdc_job.py                # stateful snapshot→change-event diffing
│   └── bronze_writer.py          # Parquet sink to S3/MinIO
├── warehouse/
│   ├── snowflake/                # DDL, external stage, streams, tasks
│   └── duckdb/
├── dbt/
│   ├── models/{staging,intermediate,marts}/
│   └── tests/
├── quality/
│   └── expectations/
├── airflow/dags/
├── analytics/
│   └── anomaly.py
├── docs/
│   ├── PHASE0_FINDINGS.md        # measured rate limits, verified schemas
│   ├── DECISIONS.md              # ADR-style log
│   └── METRICS.md                # every measured number, with how it was measured
└── tests/
```

---

## 6. Working conventions

- **Type everything.** Pydantic models for all external payloads. `mypy --strict` on `ingest/`.
- **Never trust the source.** Every field from Steam is optional until proven otherwise.
  Prices arrive as locale-formatted strings; parse them in exactly one place.
- **Store money as integer minor units** plus an explicit currency code. Never floats.
- **All timestamps UTC, timezone-aware.** Record both `observed_at` (when we polled) and any
  source-provided time. These are different things and conflating them corrupts the history.
- **Tests before the next phase.** `pytest` must be green before advancing. Use recorded
  HTTP fixtures — tests must never hit Steam.
- **Commit per logical unit** with a real message. The commit history is part of the portfolio.
- **Log the decision, not just the code.** Append to `docs/DECISIONS.md` whenever a
  non-obvious choice is made, with the alternative rejected and why.

When you are uncertain about a schema or a limit, **stop and check empirically**. Do not
write code against a guessed shape.

---

## 7. Build phases

Each phase has an exit criterion. Do not advance until it is met.

### Phase 0 — Recon and calibration (do this first, do not skip)

Nothing else in this spec is trustworthy until Phase 0 confirms it.

1. Write a single throwaway script that hits each endpoint in §3 **once**, pretty-prints the
   response, and saves it to `tests/fixtures/`.
2. Confirm or correct every field name and type in §3. Correct this document where it is wrong.
3. **Measure the rate limit.** Slowly ramp request rate against one cheap endpoint until the
   first 429. Record: requests before limit, the window, and how long recovery takes. Then
   stop immediately and wait out the cooldown.
4. Verify the currency codes in §3.7 by comparing returned price strings.
5. Check whether `pricehistory` returns useful data without a login cookie. It may not.
   Record the answer either way.

**Exit:** `docs/PHASE0_FINDINGS.md` contains measured limits and verified schemas, and §3 of
this file has been corrected. The chosen production rate is ≤40% of the measured limit.

### Phase 1 — Ingestion and Bronze

1. `ratelimit.py`: async token bucket, per-endpoint budgets, adaptive refill reduction on
   429, jittered exponential backoff, circuit breaker per §2.7.
2. `client.py`: async httpx client, sane timeouts, structured logging of every request.
3. Endpoint modules returning validated Pydantic envelopes:
   `{source, endpoint, app_id, currency, observed_at, request_params, raw_payload, ingest_id}`.
4. `nameid_resolver.py` with the permanent on-disk cache.
5. Tiered scheduler:
   - **Tier A** — ~200-item watchlist, `itemordershistogram`, every 5 min
   - **Tier B** — ~5,000-item catalog, `search/render`, every 60 min
   - **Tier C** — full catalog sweep, `search/render`, daily
   Tiers share one global budget; Tier A has priority.
6. Publish envelopes to Kafka `market.raw.v1`, keyed by `{app_id}:{market_hash_name}`.
7. `bronze_writer.py`: consume and write Parquet to
   `s3a://steam-lake/bronze/dt=YYYY-MM-DD/hour=HH/app_id=NNN/endpoint=X/`.

**Exit:** 24 hours of continuous polling with zero 429s and no gaps. Bronze is queryable
from DuckDB. Record measured events/hour in `docs/METRICS.md`.

### Phase 2 — Streaming CDC and Silver

This phase is what lets the project honestly use the word "streaming".

1. `cdc_job.py`: Spark Structured Streaming reading `market.raw.v1`, keyed per item, holding
   last-known state via `flatMapGroupsWithState`. Emit to `market.changes.v1` **only** when a
   watched field actually moves: `lowest_sell`, `highest_buy`, `sell_listings`, `volume`, or
   a material shift in book shape. Emit `{previous, current, delta, pct_change, observed_at}`.
2. Handle late and out-of-order data with a watermark. Document the chosen watermark and why.
3. Silver normalization: unify CS2 / Dota 2 / TF2 / Rust / PUBG into one schema. This will be
   genuinely awkward — that awkwardness is the point. Document each mapping decision.
4. Parse all money to integer minor units + currency code here, once.
5. Derive: `spread = lowest_sell - highest_buy`, `spread_bps`, book depth at 1/5/10% from mid.

**Exit:** Change events flow end to end. **Measure and record the compression ratio** —
raw observations in versus change events out. That ratio is a strong README number and it is
the honest answer to "was this really streaming?".

### Phase 3 — Warehouse and Gold

Build on DuckDB first (§4.1).

**dbt model layers:**

- `staging/` — one model per source stream, light typing only
- `intermediate/` — `int_item_resolved`, `int_orderbook_normalized`
- `marts/`:
  - `dim_item` — **SCD Type 2**, tracking name/type/rarity changes over time
  - `dim_game`, `dim_currency`
  - `fct_price_observation` — grain: item × currency × observed_at
  - `fct_orderbook_snapshot` — grain: item × currency × observed_at, with depth measures
  - `fct_price_change` — from the CDC stream
  - `mart_item_daily` — OHLC + volume + time-weighted average spread
  - `mart_anomaly` — Phase 6 output

Then, in Snowflake:

1. External stage over the S3 bronze/silver prefixes; `COPY INTO` landing tables.
2. `CREATE STREAM` on each landing table.
3. `CREATE TASK` consuming each stream and performing incremental `MERGE` into Silver, on a
   schedule, with task dependencies forming a DAG.
4. Explicitly demonstrate the stream's offset-advance semantics — that consuming the stream
   inside a transaction advances it — and write it up in `docs/DECISIONS.md`. This is the
   detail that proves the capability is real rather than copy-pasted.

**Exit:** Full Gold layer builds green on DuckDB *and* Snowflake. Streams and tasks run
unattended for 24h. `dbt docs generate` produces a lineage graph; screenshot it for the README.

### Phase 4 — Orchestration and quality gates

1. Airflow DAG `steam_market_batch`:
   ```
   wait_for_bronze  →  copy_into_snowflake  →  run_quality_gates
        →  dbt_run_staging  →  dbt_test_staging  (BLOCKING)
        →  dbt_run_marts    →  dbt_test_marts    (BLOCKING)
        →  detect_anomalies →  publish_alerts
   ```
2. **Gates must actually block.** A failing test halts downstream loading. Then *prove it*:
   deliberately inject a bad batch, capture the DAG failing at the gate, screenshot it. A
   quality gate nobody has seen fail is not evidence of anything.
3. Quality checks to implement:
   - **Freshness** — no partition older than the tier's SLA
   - **Schema drift** — unexpected/missing fields in raw payloads
   - **Price plausibility** — >50% single-interval move flagged, not silently dropped
   - **Crossed book** — `highest_buy > lowest_sell` should be rare; each occurrence is
     either a genuine arbitrage window or bad data, and we must distinguish them
   - **Volume monotonicity** — cumulative volume must not decrease
   - **Referential integrity** — every fact row resolves to a `dim_item`
   - **Uniqueness** — no duplicate grain in any fact table
4. Route failures to SNS (or a local webhook if staying off AWS).

**Exit:** DAG runs unattended for 72h. At least one gate has been shown failing on injected
bad data, with evidence saved.

### Phase 5 — Anomaly detection

1. Rolling z-score on log returns per item (30-day window, per-item volatility).
2. EWMA spread-widening detector — a sudden spread blowout usually precedes a price move.
3. Volume spike detection with day-of-week seasonality control (Steam sales and esports
   events create strong, real periodicity — do not treat it as anomalous).
4. Crossed-book detection as a distinct class from the above.
5. Materialize into `mart_anomaly` with a severity score and a human-readable explanation
   string. The explanation matters — an alert nobody can interpret is noise.

**Exit:** Detector runs over accumulated history, produces a ranked anomaly list, and the
author can hand-verify the top 10 against Steam's own price charts. Record the hit rate
honestly, including false positives.

### Phase 6 — Cross-currency dislocation (optional, high value)

Poll a 50-item watchlist across USD/EUR/SGD/IDR. Join to daily FX rates. Compute FX-adjusted
price gaps. Because Steam prices regionally, persistent structural gaps should appear.

This is the most genuinely *interesting* analytical output in the project, and it maps
directly to multi-market ecommerce pricing work. It also mirrors real prior experience with
multi-market operational data, which makes it a strong thing to be asked about.

### Phase 7 — Query tuning, metrics, README

1. **Query tuning, measured:** take the 3 slowest analytical queries on `mart_item_daily`
   and `fct_orderbook_snapshot`. Capture Snowflake query profiles. Diagnose (scan volume?
   spilling? poor pruning?). Fix via clustering keys, rewrite, or pre-aggregation. Record
   before/after p95 runtime and bytes scanned in `docs/METRICS.md`.
2. Consolidate every measured number into `docs/METRICS.md` with methodology for each.
3. Write `README.md`: architecture diagram, dbt lineage screenshot, the quality-gate failure
   screenshot, real metrics, and a frank **"what I'd do differently at scale"** section.

**Exit:** A stranger can read the README and understand what was built, what was measured,
and what its limits are.

---

## 8. Interview defence — the questions to build against

Build so these have real answers. If a phase would leave one unanswerable, the phase is
not done.

1. *"Is this actually streaming, or a cron job?"* → Phase 2. Answer: CDC over periodic
   snapshots, with the measured compression ratio, and an honest account of why the source
   is poll-based.
2. *"Why Kafka for this volume?"* → Have a real answer about decoupling, replay, and
   consumer independence. Also be willing to say where it is overkill.
3. *"What happens when Steam changes a field?"* → Schema drift gate, Phase 4.
4. *"How do you know your data is right?"* → The reconciliation between `search/render` and
   `priceoverview`, plus the gate suite.
5. *"Explain Snowflake streams vs. a timestamp watermark."* → Phase 3.4.
6. *"Show me a query you made faster and how you knew."* → Phase 7.1.
7. *"What broke?"* → `docs/DECISIONS.md`. Have a real failure story ready. Rate limiting will
   supply one.

---

## 9. Definition of done

**[STATUS — 2026-08-21, end of Phase 7]** Honest checklist against what this session
actually built and measured, not what was intended. See `docs/DECISIONS.md` and
`docs/METRICS.md` for the evidence behind every line.

- [ ] 30 consecutive days of ingestion with documented uptime and zero IP bans — **not
      met.** Built in a single working session; the 24h soak test itself never ran (Phase
      1, still open). This is the root cause of most of the other partial/TBD items below.
- [ ] Bronze → Silver → Gold builds green on both DuckDB and Snowflake — **DuckDB only.**
      12/12 (later 14/14) models build green on DuckDB. Snowflake was never opened —
      deliberately deferred pending the item above and the 72h DAG item below.
- [ ] Snowflake streams + tasks running unattended — **not met**, same reason.
- [x] Airflow DAG with a blocking gate demonstrated failing, with evidence — **met.**
      2/2 gate layers (Great Expectations Bronze suite, dbt uniqueness test) proven to
      block on deliberately injected bad data. `docs/PHASE4_GATE_FAILURE_EVIDENCE.txt`,
      `_dbt.txt`. (72h *unattended-schedule* run is a separate, still-open item — this
      checkbox is about the gate actually blocking, which is proven.)
- [x] Anomaly detector with hand-verified results and an honest false-positive rate —
      **met.** 25 `crossed_book` anomalies, 3/3 hand-verified against raw Bronze JSON, 0
      false positives in that sample — with the explicit caveat that the other 3 detector
      classes never fired on this session's short dataset (recorded, not hidden).
- [x] `docs/METRICS.md` where every number is measured and traceable — **met**, and
      actively maintained every phase, including this one.
- [x] README a stranger can follow — **met** (this file's sibling, `README.md`, written
      end of Phase 7).
- [x] Repo contains no scraped data, no credentials — **met.** `data/`, `*.duckdb`, `.env`
      all gitignored throughout; verified via `git status` before every stage.

---

## 10. Start here

Begin with **Phase 0 only**. Write the recon script, run each endpoint once, save the
fixtures, and report back with the actual response shapes and the measured rate limit before
writing any pipeline code.

Do not scaffold the full repo yet. Do not write the poller yet. Confirm reality first.
