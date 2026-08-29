-- Order-book depth measures: spread, spread_bps, depth at 1/5/10% from mid. Only
-- orderbook rows carry the full compact arrays needed for this — see
-- streaming/depth.py and dbt/macros/depth_within_pct.sql (same math, ported to SQL for
-- the warehouse-side path; see cdc_job.py's module docstring for why streaming and dbt
-- independently derive from the same Bronze data).

with resolved as (
    select
        ob.app_id,
        m.market_hash_name,
        ob.market_bucket_id,
        ob.observed_at,
        ob.currency,
        ob.lowest_sell,
        ob.highest_buy,
        ob.sell_listings,
        ob.buy_orders_count,
        ob.compact_buy_orders,
        ob.compact_sell_orders
    from {{ ref('stg_orderbook') }} ob
    inner join {{ ref('item_bucket_map') }} m
        on ob.app_id = try_cast(m.app_id as bigint)
        and ob.market_bucket_id = m.market_bucket_id
)

select
    app_id,
    market_hash_name,
    market_bucket_id,
    observed_at,
    currency,
    lowest_sell,
    highest_buy,
    sell_listings,
    buy_orders_count,
    (lowest_sell - highest_buy) as spread,
    case
        when (lowest_sell + highest_buy) = 0 then null
        else (lowest_sell - highest_buy)::double / ((lowest_sell + highest_buy) / 2.0) * 10000
    end as spread_bps,
    {{ depth_within_pct('compact_buy_orders', '(lowest_sell + highest_buy) / 2.0', 0.01, 'buy') }} as depth_buy_1pct,
    {{ depth_within_pct('compact_buy_orders', '(lowest_sell + highest_buy) / 2.0', 0.05, 'buy') }} as depth_buy_5pct,
    {{ depth_within_pct('compact_buy_orders', '(lowest_sell + highest_buy) / 2.0', 0.10, 'buy') }} as depth_buy_10pct,
    {{ depth_within_pct('compact_sell_orders', '(lowest_sell + highest_buy) / 2.0', 0.01, 'sell') }} as depth_sell_1pct,
    {{ depth_within_pct('compact_sell_orders', '(lowest_sell + highest_buy) / 2.0', 0.05, 'sell') }} as depth_sell_5pct,
    {{ depth_within_pct('compact_sell_orders', '(lowest_sell + highest_buy) / 2.0', 0.10, 'sell') }} as depth_sell_10pct
from resolved
