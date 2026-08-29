-- Order-book depth. Not the original itemordershistogram endpoint — that endpoint is dead;
-- this is the real replacement mechanism found in Phase 1 (docs/DECISIONS.md,
-- "Order-book depth unblocked"). Money fields are already integer minor units, no string
-- parsing needed here — unlike every other endpoint. Keyed by market_bucket_id, not
-- market_hash_name (that identifier doesn't exist in this response at all); resolving it
-- to an item name is int_item_resolved's job, one layer up, by design (staging stays
-- light-typing only, per the layer rules (README §05)).

with source as (
    select app_id, observed_at, raw_payload, request_params
    from {{ source('bronze', 'raw_envelopes') }}
    where endpoint = 'orderbook'
),

typed as (
    select
        app_id,
        observed_at,
        json_extract_string(request_params, '$.market_bucket_id') as market_bucket_id,
        json_transform(
            raw_payload,
            '{"data": {"success": "BOOLEAN", "data": {"amtMaxBuyOrder": "BIGINT", "amtMinSellOrder": "BIGINT", "eCurrency": "INTEGER", "cBuyOrders": "BIGINT", "cSellOrders": "BIGINT", "rgCompactBuyOrders": ["BIGINT"], "rgCompactSellOrders": ["BIGINT"]}}}'
        ) as payload
    from source
)

select
    app_id,
    market_bucket_id,
    cast(observed_at as timestamp) as observed_at,
    payload.data.data.amtMinSellOrder as lowest_sell,
    payload.data.data.amtMaxBuyOrder as highest_buy,
    payload.data.data.cSellOrders as sell_listings,
    payload.data.data.cBuyOrders as buy_orders_count,
    payload.data.data.eCurrency as currency,
    payload.data.data.rgCompactBuyOrders as compact_buy_orders,
    payload.data.data.rgCompactSellOrders as compact_sell_orders,
    'orderbook' as source_endpoint
from typed
where payload.data.success = true
