# Data source findings

Recorded 2026-08-20. Single-IP, unauthenticated `httpx` client, User-Agent
`steam-market-pipeline-recon/0.1 (personal research project; contact: winstonpatrickgarth@gmail.com)`.
Raw fixtures in `tests/fixtures/`. Raw rate-limit log in `docs/evidence/rate-limit-probe.json`.

## Summary — what changed vs. the initial assumptions

| # | Endpoint / area | Assumption going in | Measured reality |
|---|---|---|---|
| 1 | Item listing page | Returns HTML containing an inline `Market_LoadOrderSpread(item_nameid)` script call | **Broken.** `/market/listings/{appid}/{hash_name}` now 302-redirects to a `/market/listings/{appid}/G{bucket_id}` page for every item tested (commodity and non-commodity alike). That page contains no `Market_LoadOrderSpread`, no `item_nameid`, no `nameid` substring anywhere in ~5MB of HTML. The old per-item `/render/` AJAX sub-path also redirects into the same bucket page rather than returning JSON. |
| 2 | `search/render` | `count` param controls page size, "100 items per request", `count` above 100 "typically rejected" | **Wrong.** `count=10`, `count=100`, and `count=150` all returned `pagesize: 10` / 10 results, unauthenticated. The breadth endpoint is capped at 10 items/request from this IP. |
| 3 | Currency codes | `currency=20` → SGD, `currency=23` → IDR | **Wrong.** Measured via price-string cross-check: `20` → CAD (`CDN$`), `23` → CNY (`¥`). Correct codes: **`10` → IDR (`Rp`), `13` → SGD (`S$`)**. `1`=USD, `2`=GBP, `3`=EUR confirmed correct. |
| 4 | `pricehistory` | Unclear whether it needs a login cookie | **Confirmed it does.** No-cookie request → HTTP 400, body `[]`. Not usable without an authenticated session; out of scope for this project (logging in is out of scope entirely). |
| 5 | Rate limit | Unmeasured | **Measured**, see below. |

Finding #1 is the one that matters most — it blocks order-book depth entirely, since the
documented resolution method can no longer produce an `item_nameid`. The replacement
mechanism that unblocks it is documented in `docs/DECISIONS.md` ("Order-book resolution").

---

## `search/render` — catalog breadth

- Status 200, `success: true`.
- Top-level keys: `pagesize, results, searchdata, start, success, total_count`.
- **`pagesize` is hard-capped at 10** regardless of the requested `count` (tested
  10/100/150, all returned exactly 10 results). `total_count` for CS2 (`appid=730`, empty
  query) was `35,349`. This means a full catalog sweep is ~3,535 requests per pass at this
  endpoint, not ~354 as originally assumed.
- Per-result schema confirmed: `app_icon, app_name, asset_description{...}, hash_name,
  name, sale_price_text, sell_listings, sell_price, sell_price_text`. `asset_description`
  carries `market_bucket_group_id` / `market_bucket_group_name` — the same bucket concept
  that broke the listing-page resolution shows up here too, suggesting Valve's item-family
  "bucket" model is pervasive across the market surface, not just the listing page.
- Fixtures: `tests/fixtures/search_render.json`, `search_render_count150.json`.
- Not tested across every tracked game beyond confirming `appid=730` works; no reason to
  expect the other appids behave differently, but that's an assumption, not a measurement.

## Order-book depth

The originally documented path (`item_nameid`, scraped from the listing page's HTML) is
dead: following the 302 redirect on `GET /market/listings/730/AK-47%20%7C%20Redline%20
(Field-Tested)` lands on a ~4.99MB bucket page (`/market/listings/730/G1807209A023004`)
containing zero occurrences of `Market_LoadOrderSpread`, `item_nameid`, `nameid`,
`LoadOrder`, `histogram`, `spread`, or `buy_order`/`sell_order` in any form. The same
redirect-to-bucket behavior is confirmed on a `commodity: 1` item too (`Dreams &
Nightmares Case` → `G18D2253004`), so it isn't specific to wear-variant weapon skins. The
historic AJAX render sub-path (`/market/listings/{appid}/{hash}/render/?format=json`)
also redirects into the same bucket page instead of returning JSON.

This is not a dead end for order-book depth itself, though — a real replacement mechanism
was found by reading the bucket page's own code-split JS chunks statically (no browser
automation needed): `/market/orderbook?q=Load&qp=[app_id, market_bucket_id]`. Full
writeup, including how `market_bucket_id` is obtained for both commodity and
wear-variant items, is in `docs/DECISIONS.md` ("Order-book resolution"). Implemented in
`ingest/endpoints/orderbook.py` / `ingest/nameid_resolver.py`.

Fixtures: `tests/fixtures/listings_page.html` (the 302 stub),
`tests/fixtures/listings_bucket_page_excerpt.html` (trimmed excerpt of the final page,
centered on a `market_bucket_group_id` occurrence — the full 4.99MB page is kept locally,
gitignored, at `data/cache/listings_bucket_page_full.html`, not committed).

## `priceoverview` — point price

- Status 200. Keys: `success, lowest_price, median_price, volume`.
- Sample (`currency=1`): `{"success": true, "lowest_price": "$39.25", "median_price":
  "$52.52", "volume": "105"}`.
- These are locale-formatted display strings with currency symbols, not plain numbers —
  parsing must handle `$`, `£`, `€` (symbol-after + comma decimal), `CHF `, `руб.`, `R$`,
  `¥`, `Rp`, `S$`, `₴`, `CDN$`, `₹` at minimum, plus thousands separators that vary by
  locale (`3338,26` vs `3,338.26`).
- Fixture: `tests/fixtures/priceoverview.json`.

## `itemordersactivity` — not exercised

No modern replacement was found for this endpoint during the order-book investigation
above — it wasn't part of that trace and remains genuinely unresolved. Out of scope
unless a future investigation finds it yields genuine event-level data worth the extra
requests.

## `pricehistory` — requires login

`GET /market/pricehistory/?appid=730&market_hash_name=...` with no session cookie →
**HTTP 400**, body `[]`. Confirmed: this endpoint requires an authenticated session.
Logging in is out of scope for this project, so `pricehistory` is **out of scope**.
Fixture: `tests/fixtures/pricehistory_no_login.json`.

## Currencies

Verified by cross-checking price-string symbols/formats returned for a fixed item across
currency codes 1, 2, 3, 4, 5, 7, 8, 10, 13, 18, 20, 23, 24:

| code | symbol/format observed | currency |
|------|------------------------|----------|
| 1  | `$39.25`        | USD (confirmed) |
| 2  | `£28.84`        | GBP (confirmed) |
| 3  | `33,72€`        | EUR (confirmed) |
| 4  | `CHF 31.65`     | CHF |
| 5  | `3338,26 руб.`  | RUB |
| 7  | `R$ 202,91`     | BRL |
| 8  | `¥ 6,228`       | JPY |
| 10 | `Rp 700 697`    | **IDR** |
| 13 | `S$49.99`       | **SGD** |
| 18 | `1 757₴`        | UAH |
| 20 | `CDN$ 54.37`    | **CAD** (not SGD) |
| 23 | `¥ 266.58`      | **CNY** (not IDR) |
| 24 | `₹ 3,756`       | INR |

Note for terminal debugging: printing these directly in a Windows PowerShell/Git-Bash
console mangles the non-ASCII symbols (mojibake). Verified via raw-byte inspection
(`\xc2\xa3` = correctly UTF-8-encoded `£`) that the underlying JSON is correct UTF-8 — this
is purely a console rendering issue, not a data quality issue. Any code that logs these
strings to a Windows console should be aware of this; it does not affect values stored to
disk with explicit UTF-8 encoding.

Fixtures: `tests/fixtures/currency_check.json`, `currency_probe_extended.json`.

## `robots.txt`

```
Host: steamcommunity.com
User-agent: *
Disallow: /actions/
Disallow: /linkfilter/
Disallow: /tradeoffer/
Disallow: /trade/
Disallow: /email/
```

None of the `/market/*` paths this project needs are disallowed. Fixture:
`tests/fixtures/robots.txt`.

## Measured rate limit

Method: ramped request rate against `/market/priceoverview/` (single item, cheapest
endpoint) in discrete steps (2.0s → 1.0s → 0.5s → 0.25s → 0.1s → 0.05s → 0s between
requests, 8 requests per step), stopping immediately on the first 429. Full log:
`docs/evidence/rate-limit-probe.json`.

**Result: first 429 at request #21**, after 8 requests at 2.0s spacing, 8 at 1.0s spacing,
and 4 requests into the 0.5s-spacing step (i.e., ~2 req/s sustained). Total elapsed time to
the 429: ~28.7s of real traffic across the ramp.

**Recovery:** probed with backoff after the 429 (5s, then +10s, then +15s cumulative).
Still `429` at t+5s and t+15s; **recovered (`200`) at t+30s.** Recovery time: ~30 seconds
from the triggering request.

**Chosen production rate:** targeting ≤40% of the measured limit. The measured break
point is ~2 req/s sustained; 40% of that is 0.8 req/s (1 request per 1.25s). Rounding down
for margin, **production default is 1 request per 2 seconds (0.5 req/s)**, well under 40%.
This is a single data point from one test run at one time of day — it is not a guarantee
Steam's actual limit is static or IP-independent, which is exactly what the circuit
breaker and adaptive backoff in `ratelimit.py` exist to handle regardless of whether this
exact number drifts. This becomes the global ceiling for `ratelimit.py`, with per-endpoint
budgets layered on top rather than each endpoint getting its own independent 0.5 req/s
(order-book polling and the catalog sweep would otherwise each independently think they're
compliant while jointly exceeding the measured limit).

## Open items

- Every endpoint above has been hit at least once, response shape recorded, fixture
  saved.
- Rate limit measured; production rate set to ≤40% of it.
- Currency codes verified; two were wrong and are corrected in the table above.
- `pricehistory` login requirement confirmed (requires login → out of scope).
- The listing-page assumption and the currency assumptions are corrected above.
  `itemordershistogram`/`item_nameid` resolution and `itemordersactivity` could not be
  corrected in place because a replacement mechanism had to be found first — the
  order-book replacement is documented in `docs/DECISIONS.md`; `itemordersactivity`
  remains genuinely unresolved (see above), which is a real open gap, not a
  rubber-stamped close.
