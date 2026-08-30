-- Unifies item-identity across all three raw endpoints into one observation stream keyed
-- by (app_id, market_hash_name). search_render/priceoverview already carry a hash_name;
-- orderbook only carries market_bucket_id (see stg_orderbook.sql), resolved here via
-- seeds/item_bucket_map.csv — the exported nameid_resolver.py cache (ingest/nameid_resolver.py).
--
-- Orderbook rows whose bucket_id isn't in that cache are silently dropped here (inner
-- join) — they're only the ones this session actually resolved via a bucket-page fetch
-- (612 items at seed time), not the full catalog. That's an expected, honest limitation
-- of a cache built incrementally by Tier A polling a watchlist, not a bug.
--
-- Bug found and fixed (2026-08-21, spotted while building the volume-monotonicity
-- check): this model used to map orderbook's `buy_orders_count` (cBuyOrders — a resting
-- order-book DEPTH count) into the shared `volume` column alongside priceoverview's
-- actual trade volume. Those are different metrics entirely — cBuyOrders was ~53,000+ for
-- a popular item while priceoverview's volume was ~100, and blending them made "volume"
-- meaningless (a monotonicity check on the blended column would have failed constantly,
-- for the wrong reason). streaming/cdc_job.py's normalize_orderbook already got this
-- right — it leaves orderbook's `volume` null. This model now matches that.

with search_render as (
    select app_id, market_hash_name, observed_at, lowest_sell, highest_buy, sell_listings, volume, currency, source_endpoint
    from {{ ref('stg_search_render') }}
),

priceoverview as (
    select app_id, market_hash_name, observed_at, lowest_sell, highest_buy, sell_listings, volume, currency, source_endpoint
    from {{ ref('stg_priceoverview') }}
    where market_hash_name is not null
),

orderbook_resolved as (
    select
        ob.app_id,
        m.market_hash_name,
        ob.observed_at,
        ob.lowest_sell,
        ob.highest_buy,
        ob.sell_listings,
        cast(null as bigint) as volume,
        ob.currency,
        ob.source_endpoint
    from {{ ref('stg_orderbook') }} ob
    inner join {{ ref('item_bucket_map') }} m
        on ob.app_id = try_cast(m.app_id as bigint)
        and ob.market_bucket_id = m.market_bucket_id
)

select * from search_render
union all
select * from priceoverview
union all
select * from orderbook_resolved
