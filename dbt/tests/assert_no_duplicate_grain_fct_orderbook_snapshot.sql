-- Uniqueness. Grain: item x observed_at (orderbook is single-currency
-- per response, no source_endpoint ambiguity like fct_price_observation has).

select
    app_id, market_hash_name, observed_at,
    count(*) as n
from {{ ref('fct_orderbook_snapshot') }}
group by 1, 2, 3
having count(*) > 1
