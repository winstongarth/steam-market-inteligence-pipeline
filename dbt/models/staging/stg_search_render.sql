-- search/render, exploded to one row per item. Measured pagesize=10, not the originally
-- assumed ~100 — see docs/FINDINGS.md.
--
-- No currency here: search/render doesn't take or return a currency parameter. Its
-- sell_price empirically looks USD-cents-scaled but that's unconfirmed — see
-- streaming/cdc_job.py's Silver normalization table, which documents the same open
-- question on the streaming side.

with source as (
    select app_id, observed_at, raw_payload
    from {{ source('bronze', 'raw_envelopes') }}
    where endpoint = 'search_render'
),

typed as (
    select
        app_id,
        observed_at,
        json_transform(
            raw_payload,
            '{"results": [{"hash_name":"VARCHAR","sell_price":"BIGINT","sell_listings":"BIGINT"}]}'
        ) as payload
    from source
),

exploded as (
    select
        app_id,
        observed_at,
        unnest(payload.results) as item
    from typed
)

select
    app_id,
    item.hash_name as market_hash_name,
    cast(observed_at as timestamp) as observed_at,
    item.sell_price as lowest_sell,
    cast(null as bigint) as highest_buy,
    item.sell_listings as sell_listings,
    cast(null as bigint) as volume,
    cast(null as integer) as currency,
    'search_render' as source_endpoint
from exploded
where item.hash_name is not null
