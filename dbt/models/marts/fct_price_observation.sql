-- Grain: item x currency x observed_at. Every raw price/listing observation across all
-- three endpoints, identity-resolved (dbt/models/intermediate/int_item_resolved.sql).

select
    app_id,
    market_hash_name,
    currency,
    observed_at,
    lowest_sell,
    highest_buy,
    sell_listings,
    volume,
    source_endpoint
from {{ ref('int_item_resolved') }}
