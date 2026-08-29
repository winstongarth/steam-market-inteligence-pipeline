{{ config(severity='warn') }}

-- Crossed book: "highest_buy > lowest_sell should be rare; each
-- occurrence is either a genuine arbitrage window or bad data, and we must distinguish
-- them." Real finding, 2026-08-21: in this project's actual data, crossed-book rows are
-- NOT rare — 25 of 96 orderbook observations (26%) are crossed, with spread_bps as
-- extreme as -400bps, including the *same* crossed value persisting across four
-- consecutive polls 5 minutes apart (595700/620100 for AK-47 | Redline FT,
-- 11:30–11:45 on 2026-08-20).
--
-- This can't be a pipeline-introduced staleness artifact: amtMinSellOrder and
-- amtMaxBuyOrder both come from the SAME atomic HTTP response
-- (ingest/endpoints/orderbook.py issues one request; Steam returns both values together).
-- So a persistent 4%+ cross reflects an inconsistency in Steam's OWN backend order-book
-- cache, not ours — a genuinely interesting finding for a market-intelligence tool to
-- surface, not something to threshold away. `severity='warn'` deliberately: at this
-- incidence rate, an error-level gate would block essentially every run without being
-- actionable. Revisit the threshold once more data (esp. the pending 24h soak test)
-- shows whether 26% is typical or this session's small sample was unusual.

select
    app_id, market_hash_name, observed_at, currency, lowest_sell, highest_buy, spread, spread_bps
from {{ ref('fct_orderbook_snapshot') }}
where highest_buy > lowest_sell
