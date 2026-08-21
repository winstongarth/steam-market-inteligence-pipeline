# Metrics

Every number here is measured, with the method that produced it. Per CLAUDE.md's
non-negotiable honesty rule: unmeasured numbers are recorded as `TBD`, never guessed.

## Phase 0

| Metric | Value | Method |
|---|---|---|
| Search/render page size (unauthenticated) | 10 items/request (not the assumed ~100) | Requested `count=10/100/150` against `search/render`, all returned `pagesize: 10`. See `docs/PHASE0_FINDINGS.md` §3.1. |
| CS2 (`appid=730`) total catalog size | 35,349 items | `total_count` field, `search/render` response, 2026-08-20. |
| Rate limit — requests before first 429 | 21 requests | Ramp test, 2.0s→0.5s step-down, `priceoverview` endpoint. See `docs/PHASE0_RATELIMIT.json`. |
| Rate limit — approximate break point | ~2 req/s sustained | Same ramp test; 429 occurred 4 requests into the 0.5s-interval (2 req/s) step. |
| Recovery time after 429 | ~30 seconds | Backoff probe: still 429 at t+5s and t+15s, recovered (200) at t+30s. |
| Chosen production rate | 1 request / 2s (0.5 req/s, ~25% of measured limit) | Derived: 40% ceiling per CLAUDE.md §2.3 applied to the ~2 req/s measured break point, rounded down for margin. |
| `pricehistory` without login | HTTP 400, `[]` | Single unauthenticated request, 2026-08-20. Confirms login required; endpoint out of scope. |

## Phase 1

| Metric | Value | Method |
|---|---|---|
| `mypy --strict` on `ingest/` | 0 errors, 13 source files | `uv run mypy ingest`, 2026-08-20 |
| Test suite | 15/15 passing | `uv run pytest -v`, 2026-08-20 — all against recorded fixtures or mocked I/O, never hits Steam |
| End-to-end smoke test | search_render → priceoverview → orderbook (Tier A path) → Kafka → Bronze Parquet → queryable from DuckDB, all real network calls | `scripts/phase1_smoke_test.py`, 2026-08-20 |
| Bucket-page resolution efficiency | 31 `market_hash_name → market_bucket_id` pairs from 1 request | One AK-47 \| Redline bucket-page fetch resolved every wear/StatTrak/Souvenir variant present on the page in a single call — see `docs/DECISIONS.md` |
| Commodity fast-path resolution cost | 0 extra requests | Verified: Dreams & Nightmares Case resolved directly from a `search/render` result already being fetched for Tier B/C |

**Not yet measured — needs a longer unattended run, out of scope for a single interactive
session:**
- 24h continuous polling with zero 429s and no gaps (Phase 1 exit criterion)
- Measured events/hour at steady state
- Bronze partition growth rate / file sizes at real cadence

To collect these: run `uv run python -m ingest.scheduler` (after `docker compose up -d` and
`docker exec steam-redpanda rpk topic create market.raw.v1`) and `uv run python -m
streaming.bronze_writer` concurrently under a process manager for 24h, then query
`docs/PHASE0_RATELIMIT.json`-style logs for 429 counts and Bronze partition row counts via
DuckDB.

## Phase 2

Numbers below are from the corrected run, after fixing the money-domain bug documented in
`docs/DECISIONS.md` ("CDC job: money-domain bug, found and fixed twice") — two earlier
runs (19,554 and 19,553 events) contained bogus multi-thousand-percent "price changes"
caused by diffing across incompatible currencies/endpoints and are superseded.

| Metric | Value | Method |
|---|---|---|
| CDC job — exceptions during real run | 0 | `spark-submit` inside the `spark` container against the full real `market.raw.v1` backlog (1,701 messages), 2026-08-21 |
| Raw Kafka messages consumed | 1,701 | `rpk topic describe market.raw.v1 -p` |
| Item-level raw observations (post-explode) | 16,110 | 1,601 search_render × 10 results + 4 priceoverview + 96 orderbook |
| Distinct items seen (search_render alone) | 3,021 | Counted `(app_id, hash_name)` pairs across all search_render messages |
| Change events emitted | 19,520 | `rpk topic describe market.changes.v1 -p`, high-watermark, post-fix |
| Change events by field | lowest_sell: 8,003 · sell_listings: 11,510 · highest_buy: 6 · volume: 1 | Parsed all 19,520 messages from `market.changes.v1` |
| Largest `\|pct_change\|` in the corrected data | 96.3% ($0.27 → $0.53, M249 \| Hypnosis, Well-Worn) | Ranked every event's `\|pct_change\|` after the fix — plausible for a cheap item across an ~18h gap, not a unit-mismatch artifact (pre-fix max was ~15,903%) |
| Compression ratio | **19,520 out vs 16,110 in — not a compression in this dataset** | See `docs/DECISIONS.md` for why: this backlog spans ~18h of disconnected ad-hoc test runs, not continuous tight polling, so most repeat observations show genuine real-world drift rather than being near-duplicates. Re-measure once continuous Tier-cadence data exists (pending Phase 1's 24h soak test above). |
| Test suite (money/depth/normalize/detect_changes) | 47/47 passing | `uv run pytest tests/test_money.py tests/test_depth.py tests/test_cdc_normalize.py tests/test_cdc_detect_changes.py -v`, 2026-08-21 |

**Not yet measured:** a compression ratio representative of steady-state 5-minute-cadence
polling — needs Phase 1's 24h soak test data, not this ad-hoc backlog.

## Phase 3

DuckDB only this phase — Snowflake deliberately deferred, see `docs/DECISIONS.md`.

| Metric | Value | Method |
|---|---|---|
| dbt models built green | 12/12 (7 staging/intermediate views, 5 mart/fact tables) | `dbt seed && dbt run --exclude dim_item && dbt snapshot && dbt run --select dim_item`, 2026-08-21 |
| dbt tests passing | 15/15 | `dbt test`, 2026-08-21 |
| `item_bucket_map` seed size | 612 rows | Exported from `ingest/nameid_resolver.py`'s on-disk cache (`data/cache/market_bucket_ids.json`) — every item this session's Tier A / bucket-page resolution actually resolved |
| `fct_price_observation` row count | 16,110 | Matches Phase 2's independently-measured "item-level raw observations" count exactly — cross-validated across two independently-built pipelines (Spark streaming vs. dbt/DuckDB batch) on the same source data |
| `fct_price_change` row count | 19,520 | Matches Phase 2's corrected CDC event count exactly |
| `fct_orderbook_snapshot` row count | 96 | Matches Bronze's orderbook message count exactly — every orderbook observation's `market_bucket_id` resolved via the seed |
| `dim_item` row count (SCD2, current rows) | 3,021 | Matches distinct search_render items — one current row per item at seed time |
| `mart_item_daily` row count | 3,913 | item × day × money_domain grain, post money-domain fix |
| Bug found and fixed: OHLC currency-mixing | `open=3925, high=621400` for one item/day, pre-fix | Third instance of the money-domain bug class (see Phase 2 above and `docs/DECISIONS.md`) — found by spot-checking a real `mart_item_daily` row before trusting the model, not assumed correct because tests passed |
| Bug verified fixed | `AK-47 \| Redline (FT)`: `currency:1` ~$39 range, `currency:8` ~¥6,200 range, `endpoint:search_render` ~$39 range — three internally consistent rows instead of one blended row | Re-queried the same item post-fix |

**Not yet done (deliberately, per spec sequencing):** Snowflake external stage, streams,
tasks, 24h unattended run. Pending Phase 1's soak test and Phase 4's stability, per
CLAUDE.md §4.1.

## Phase 4

| Metric | Value | Method |
|---|---|---|
| DAG tasks, full run | 11/11 succeeded | `airflow dags test steam_market_batch 2026-08-21` inside `steam-airflow-webserver`, 2026-08-21. Evidence: `docs/PHASE4_DAG_SUCCESS_EVIDENCE.txt` |
| Blocking gates that actually blocked on injected bad data | 2/2 tested, both blocked | GX Bronze suite (3 injected failure modes) + a real dbt uniqueness test (injected duplicate row, dbt exit code 1). Evidence: `docs/PHASE4_GATE_FAILURE_EVIDENCE.txt`, `_dbt.txt` |
| dbt tests, full suite | 21 total: 18 pass, 3 warn (by design), 0 error | `dbt test` against real Phase 3 warehouse, 2026-08-21 |
| Crossed-book incidence in real data | 25 / 96 orderbook observations (26%) | `dbt/tests/assert_crossed_book_rare.sql` — see `docs/DECISIONS.md` for why this is a real Steam-backend finding, not a pipeline bug |
| Price-plausibility flags (>50% move) | 7 events | `dbt/tests/assert_price_plausibility.sql` |
| Volume-monotonicity flags | 1 decrease | `dbt/tests/assert_volume_monotonicity.sql` — expected, see `docs/DECISIONS.md` on why this field isn't a true cumulative counter |
| Alert delivery, end-to-end | 1/1 delivered and confirmed logged | `publish_alerts` POSTed to `webhook-receiver`; confirmed by reading `logs/alerts.jsonl` inside that container, not just the sender's log line |
| Real bugs found/fixed building this phase | 6 (1 data-modeling bug, 5 Airflow/dbt-CLI mechanics) | Full list in `docs/DECISIONS.md`'s Phase 4 entry |
| Test suite | 68/68 passing | `uv run pytest -q`, 2026-08-21 |
| `mypy --strict` on `ingest/` | 0 errors | `uv run mypy ingest`, 2026-08-21 |

**Not yet done (deliberately):** DAG running unattended on a schedule for 72h.
`airflow-scheduler` was never started as a persistent service this session — the DAG was
run on-demand via `airflow dags test` against real data instead. Same honest gap as
Phase 1's still-pending 24h soak test.

## Phase 5

| Metric | Value | Method |
|---|---|---|
| Total anomalies detected | 25 (all `crossed_book`) | `python -m analytics.anomaly` against the real warehouse, 2026-08-21 |
| price_zscore / spread_widening / volume_spike anomalies | 0 / 0 / 0 | Confirmed not a silent bug: max distinct calendar days per item = 2 (need 5+/3+); the one item with enough order-book history for spread EWMA (96 rows) simply never exceeded the 3σ threshold (mean spread ≈6bps, stddev ≈117bps, ≈357bps bar) |
| Hand-verification against raw Bronze | 3/3 spot-checked (highest, mid, lowest severity), 3/3 matched exactly | Traced each `mart_anomaly` row's `amtMaxBuyOrder`/`amtMinSellOrder` values back to the untouched raw API JSON for that `observed_at` — confirmed by direct lookup |
| False positives found | 0 (in the 3-row verified sample) | See `docs/DECISIONS.md` for the explicit caveat: verified against our own raw capture, not Steam's website UI (no browser access) |
| Test suite (analytics) | 10/10 passing | `uv run pytest tests/test_anomaly.py -v`, 2026-08-21 |
| `mypy --strict` on `analytics/` | 0 errors | `uv run mypy analytics --ignore-missing-imports`, 2026-08-21 |

**Not yet measured:** real hit/false-positive rates for price_zscore, spread_widening, and
volume_spike — all three are mechanically correct (unit-tested against synthetic data
with known-good outcomes) but haven't fired on real data yet, since this session's
dataset doesn't span enough calendar time. Re-run once Phase 1's 24h+ soak test produces
real continuous history.

## Phase 6

| Metric | Value | Method |
|---|---|---|
| Watchlist requests, first batch | 20/20 succeeded, then 3 consecutive 429s | `scripts/phase6_fx_poll.py` at `STEAM_RPS=0.5`, halted by the real circuit breaker after item 5. `docs/DECISIONS.md`. |
| Watchlist requests, resumed batch | 180/180 succeeded, 0 errors | Same script, `RESUME_FROM_INDEX=5 STEAM_RPS=0.25`, 2026-08-21 06:09-06:21 UTC. `/tmp/phase6_poll_resume.log`. |
| Total watchlist coverage | 50/50 items × 4 currencies = 200/200 requests landed in Bronze | Verified by reading both Bronze parquet files directly (20 rows + 180 rows), not just trusting script exit code 0. |
| dbt seed CSV bug found | 1 (`parse_price_sql`, NULL vs `''` for zero-decimal currencies) | Found via real IDR data showing `lowest_sell = NULL`; fixed, regression test `assert_zero_decimal_currencies_parse_correctly.sql` added and passing. |
| Cross-currency join bug found | 1 (day-average fan-out instead of concurrent match) | Found by checking `mart_cross_currency_dislocation` output values, not by a test failure; fixed with DuckDB `ASOF JOIN`, verified nearest-prior-match semantics before trusting it. |
| Cross-currency gap, EUR (n=50, all matched) | mean +1.21%, stddev 3.90, median +0.11% | `mart_cross_currency_dislocation`, full watchlist, 2026-08-21. |
| Cross-currency gap, SGD (n=50, all matched) | mean -0.83%, stddev 3.95, median 0.00% | Same. |
| Cross-currency gap, IDR (n=50, all matched) | mean -0.55%, stddev 2.68, median -0.11% | Same. |
| Mean absolute gap by price bucket | <$0.50: 4.09% · $0.50-2: 0.56% · $2-10: 0.16% · >$10: 0.49% | Same table, bucketed by `usd_baseline_price`. Shows gap magnitude tracks item price (rounding/quantization noise), not currency — no persistent structural regional-pricing gap found. |
| dbt tests, full suite | 26 total: 23 pass, 3 warn (pre-existing, by design), 0 error | `dbt test` after Phase 6 rebuild, 2026-08-21. |

**Conclusion:** on this watchlist, Steam Community Market does not show a persistent,
currency-driven structural pricing gap once item price is controlled for — the apparent
gaps in the n=5 partial sample (documented mid-phase) were rounding noise on cheap items,
not a real signal. This is a negative result, stated plainly rather than reframed as a
positive finding.

## Phase 7

**Not measured: Snowflake query profiles / clustering-key tuning.** Snowflake was never
reached this session (deferred since Phase 3, pending Phase 1's soak test and Phase 4's
stability, both still open). DuckDB `EXPLAIN ANALYZE` used instead — see method column.
Full run: `scripts/phase7_query_tuning.py`, output in `/tmp/phase7_tuning.log`.

| Query | Before | After | Change | Method |
|---|---|---|---|---|
| Q1 — cross-currency FX join (`mart_cross_currency_dislocation`) | 95.1 ms median (20 reps), 3 HTTP GETs to MinIO, 209.2 KiB transferred | 0.485 ms median (20 reps), 0 HTTP GETs | ~196× faster | Materialized table vs. un-materialized view stacked on live S3 reads. `EXPLAIN ANALYZE`, 2026-08-21. |
| Q2 — 7-day rolling avg close price (`mart_item_daily`) | 12.0 ms median (5 reps), correlated subquery (O(n) re-scans) | 6.9 ms median (5 reps), window function (single sorted pass) | ~43% faster | Correctness-checked: exact match vs. subquery version on all 3,935 rows (6-decimal rounding). `EXPLAIN ANALYZE`, 2026-08-21. |
| Q3 — latest observation per item (`fct_price_observation`) | 21 rows returned (WRONG — self-join on `currency` silently drops all NULL-currency groups; `NULL = NULL` is never true in SQL) | 3,042 rows returned (correct — matches independently-counted distinct-group total) | Correctness bug found and fixed, not just a speed difference | Diagnosed: 16,010/16,130 rows have `currency IS NULL` (`search_render` rows, uncontrolled currency per `int_fx_converted_prices.sql`). `QUALIFY row_number() ... partition by` groups NULLs correctly; self-join equi-join does not. `docs/DECISIONS.md`. |

**Real finding, not a planned one:** Q3 started as a performance-tuning exercise and
surfaced a genuine correctness bug — the same class of silent-row-dropping issue as the
three money-domain bugs found earlier in this project (CDC, `mart_item_daily` OHLC, the
Phase 6 FX join), here via `NULL` equality semantics instead of currency-domain mixing.
Consistent with this project's pattern: verify query output against an independent count
before trusting a query that "ran without error."

| Metric | Value | Method |
|---|---|---|
| Test suite | 81/81 passing | `uv run pytest -q`, 2026-08-21 |
| `mypy --strict` on `ingest/` | 0 errors | `uv run mypy ingest`, 2026-08-21 |
| `mypy` on `streaming`/`analytics` | 0 errors | `uv run mypy streaming analytics --ignore-missing-imports`, 2026-08-21 |
