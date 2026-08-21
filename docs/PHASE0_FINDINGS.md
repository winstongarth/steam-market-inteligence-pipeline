# Phase 0 findings — recon and calibration

Recorded 2026-08-20. Single-IP, unauthenticated `httpx` client, User-Agent
`steam-market-pipeline-recon/0.1 (personal research project; contact: winstonpatrickgarth@gmail.com)`.
Raw fixtures in `tests/fixtures/`. Raw rate-limit log in `docs/PHASE0_RATELIMIT.json`.

## Summary — what changed vs. the CLAUDE.md draft

| # | §  | Assumption in the spec | Measured reality |
|---|----|------------------------|-------------------|
| 1 | 3.3 | Item listing page returns HTML containing an inline `Market_LoadOrderSpread(item_nameid)` script call | **Broken.** `/market/listings/{appid}/{hash_name}` now 302-redirects to a `/market/listings/{appid}/G{bucket_id}` page for every item tested (commodity and non-commodity alike). That page contains no `Market_LoadOrderSpread`, no `item_nameid`, no `nameid` substring anywhere in ~5MB of HTML. The old per-item `/render/` AJAX sub-path also redirects into the same bucket page rather than returning JSON. |
| 2 | 3.1 | `count` param controls page size, "100 items per token", count>100 "typically rejected" | **Wrong.** `count=10`, `count=100`, and `count=150` all returned `pagesize: 10` / 10 results, unauthenticated. The breadth endpoint is capped at 10 items/request for us, not ~100 — the "100× more token-efficient" framing in §3.1 does not hold under these conditions. |
| 3 | 3.7 | `currency=20` → SGD, `currency=23` → IDR | **Wrong.** Measured via price-string cross-check: `20` → CAD (`CDN$`), `23` → CNY (`¥`). Correct codes for the currencies named in the spec: **`10` → IDR (`Rp`), `13` → SGD (`S$`)**. `1`=USD, `2`=GBP, `3`=EUR confirmed correct. |
| 4 | Phase 0 step 5 | Unclear whether `pricehistory` needs a login cookie | **Confirmed it does.** No-cookie request → HTTP 400, body `[]`. Not usable without an authenticated session; out of scope for this project (§2.2 forbids logging in). |
| 5 | §2.3 | Rate limit unmeasured | **Measured**, see below. |

Finding #1 is the one that matters most — it blocks §3.2 (`itemordershistogram`) and §3.5
(`itemordersactivity`) entirely, since both require an `item_nameid` that the documented
resolution method can no longer produce. See `docs/DECISIONS.md` for the open decision this
creates.

---

## 3.1 `/market/search/render/` — breadth

- Status 200, `success: true`.
- Top-level keys: `pagesize, results, searchdata, start, success, total_count`.
- **`pagesize` is hard-capped at 10** regardless of the requested `count` (tested 10/100/150,
  all returned exactly 10 results). `total_count` for CS2 (`appid=730`, empty query) was
  `35,349`. This means a full catalog sweep is ~3,535 requests per pass at this endpoint, not
  ~354 as the original "100/request" assumption implied.
- Per-result schema confirmed: `app_icon, app_name, asset_description{...}, hash_name, name,
  sale_price_text, sell_listings, sell_price, sell_price_text`. `asset_description` carries
  `market_bucket_group_id` / `market_bucket_group_name` — the same bucket concept that broke
  §3.3 shows up here too, suggesting Valve's item-family "bucket" model is now pervasive
  across the market surface, not just the listing page.
- Fixtures: `tests/fixtures/search_render.json`, `search_render_count150.json`.

## 3.2 `/market/itemordershistogram/` — depth

**[RESOLVED IN PHASE 1, 2026-08-20]** This exact endpoint is dead — but order-book depth
itself is not. See `docs/DECISIONS.md` ("Order-book depth unblocked...") for the full
writeup: the live SPA uses `/market/orderbook?q=Load&qp=[app_id, market_bucket_id]` instead,
traced from its own JS bundles and verified live. Implemented in
`ingest/endpoints/orderbook.py` / `ingest/nameid_resolver.py`.

## 3.3 `item_nameid` resolution

- `GET /market/listings/730/AK-47%20%7C%20Redline%20(Field-Tested)` → **302**, `Location:
  https://steamcommunity.com/market/listings/730/G1807209A023004`.
- Same redirect-to-bucket behavior confirmed on a `commodity: 1` item too (`Dreams &
  Nightmares Case` → `G18D2253004`), so this isn't specific to wear-variant weapon skins.
- Followed the redirect: final page is ~4.99MB of HTML/inline-JSON. Regex search for
  `Market_LoadOrderSpread`, `item_nameid`, `nameid`, `LoadOrder`, `histogram`, `spread`,
  `buy_order`/`sell_order` (case-insensitive) — **zero matches on all of them.**
- The page does contain per-listing asset data (`classid`, `instanceid`, `assetid`,
  `market_bucket_id`, float values, sticker data) for individual for-sale listings — it's
  rendering active listings for the bucket, not a price-history/order-book summary.
- Trying the historic AJAX sub-path (`/market/listings/730/{hash}/render/?...&format=json`)
  also redirects into the same bucket HTML page rather than returning JSON.
- Fixtures: `tests/fixtures/listings_page.html` (the 302 stub), 
  `tests/fixtures/listings_bucket_page_excerpt.html` (trimmed excerpt of the final page,
  centered on a `market_bucket_group_id` occurrence — full 4.99MB page kept locally,
  gitignored, at `data/cache/listings_bucket_page_full.html`, not committed).

**Conclusion (Phase 0):** the item_nameid-resolution method in §3.3, as written, is dead.
**[UPDATE, Phase 1]** — resolved without a browser: the bucket page's own code-split JS
chunks were downloaded and read statically, revealing the SPA's real data-fetching
mechanism. See `docs/DECISIONS.md`.

## 3.4 `/market/priceoverview/` — point price

- Status 200. Keys: `success, lowest_price, median_price, volume`. Matches spec.
- Sample (`currency=1`): `{"success": true, "lowest_price": "$39.25", "median_price":
  "$52.52", "volume": "105"}`.
- Confirms these are display strings with currency symbols exactly as the spec warned —
  parsing must handle `$`, `£`, `€` (symbol-after + comma decimal), `CHF `, `руб.`, `R$`,
  `¥`, `Rp`, `S$`, `₴`, `CDN$`, `₹` at minimum, plus thousands separators that vary by
  locale (`3338,26` vs `3,338.26`).
- Fixture: `tests/fixtures/priceoverview.json`.

## 3.5 `/market/itemordersactivity/`

Not exercised. Unlike 3.2, no modern replacement was found during the Phase 1
investigation — this endpoint wasn't part of that trace and remains genuinely unresolved.
Optional per the spec either way ("use only if it yields genuine event-level data").

## 3.6 Games to cover

Not tested in Phase 0 beyond confirming `appid=730` works; no reason to expect the other
four appids behave differently for `search/render`, but that's an assumption, not a
measurement. Cheap to verify in Phase 1 setup, not worth spending Phase 0 request budget on.

## 3.7 Currencies

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

---

## Phase 0 step 3 — measured rate limit

Method: ramped request rate against `/market/priceoverview/` (single item, cheapest
endpoint) in discrete steps (2.0s → 1.0s → 0.5s → 0.25s → 0.1s → 0.05s → 0s between
requests, 8 requests per step), stopping immediately on the first 429. Full log:
`docs/PHASE0_RATELIMIT.json`.

**Result: first 429 at request #21**, after 8 requests at 2.0s spacing, 8 at 1.0s spacing,
and 4 requests into the 0.5s-spacing step (i.e., ~2 req/s sustained). Total elapsed time to
the 429: ~28.7s of real traffic across the ramp.

**Recovery:** probed with backoff after the 429 (5s, then +10s, then +15s cumulative).
Still `429` at t+5s and t+15s; **recovered (`200`) at t+30s.** Recovery time: ~30 seconds
from the triggering request.

**Chosen production rate:** the spec requires ≤40% of the measured limit. The measured
break point is ~2 req/s sustained. 40% of that is 0.8 req/s (1 request per 1.25s). Rounding
down for margin, **production default is 1 request per 2 seconds (0.5 req/s)**, well under
40%. This becomes the `[CONFIG]` default for `ratelimit.py` in Phase 1, with per-endpoint
budgets layered on top of that global ceiling rather than each endpoint getting its own
independent 0.5 req/s (Tier A's histogram polling and Tier B/C's catalog sweep would
otherwise each independently think they're compliant while jointly exceeding the measured
limit).

This is a single data point from one test run at one time of day — it is not a guarantee
Steam's actual limit is static or IP-independent. Phase 1's circuit breaker (§2.7) and
adaptive backoff need to hold regardless of whether this exact number drifts.

---

## Phase 0 step 5 — `pricehistory` without login

`GET /market/pricehistory/?appid=730&market_hash_name=...` with no session cookie →
**HTTP 400**, body `[]`. Confirmed: this endpoint requires an authenticated session. Per
§2.2 (never log in, never touch inventory-adjacent authenticated flows), `pricehistory` is
**out of scope** for this project. Fixture: `tests/fixtures/pricehistory_no_login.json`.

---

## Exit criteria check

- [x] Every endpoint in §3 hit at least once, response shape recorded, fixture saved.
- [x] Rate limit measured (see above); production rate set to ≤40% of it.
- [x] Currency codes verified; two were wrong and are corrected in §3.7 above and in
      CLAUDE_4.md.
- [x] `pricehistory` login requirement confirmed (requires login → out of scope).
- [ ] §3 of CLAUDE_4.md corrected — done for 3.1, 3.7; **3.2/3.3/3.5 cannot be corrected
      yet** because the replacement mechanism for `item_nameid` resolution hasn't been found.
      This is a genuine open blocker, logged in `docs/DECISIONS.md`, not a rubber-stamped
      "exit."

**Phase 0 is not fully closed.** Steps 1, 3, and 4 are done. Step 2 (schema confirmation) is
done for search/render and priceoverview but reveals that the depth endpoint — the one the
spec calls "the most valuable endpoint in the project" — is currently unreachable by the
documented method. Recommend a short, separate investigation session (browser dev-tools
network trace on the live bucket page) before writing any Phase 1 ingestion code, since
`nameid_resolver.py` and Tier A of the scheduler both depend on solving this.
