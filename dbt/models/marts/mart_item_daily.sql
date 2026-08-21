-- OHLC + volume + time-weighted average spread (CLAUDE.md Phase 3).
--
-- Money-domain-partitioned (bug found and fixed here too, docs/DECISIONS.md — the same
-- class of bug as the CDC job's, see "CDC job: money-domain bug, found and fixed twice"):
-- a first version grouped OHLC by (item, day) only, blending search_render/priceoverview
-- observations (USD-ish) with orderbook observations (currency not request-controllable,
-- e.g. JPY) into the same open/high/low/close — producing rows like
-- open=3925, high=621400 for the same item on the same day, a unit-mismatch artifact, not
-- a real price range. Fixed by adding a money_domain column (same concept as
-- cdc_job.py's `_money_domain`: a specific currency when the observation reports one,
-- otherwise the specific source endpoint) to the grain. Grain is now item x day x
-- money_domain, not just item x day — nothing is silently dropped or arbitrarily picked;
-- every domain gets its own honestly-comparable OHLC row.
--
-- Time-weighted average spread: each observation's spread is weighted by the time until
-- the NEXT observation of that item on the same day (i.e. "how long did this spread
-- value hold"). An item's last observation of the day gets zero weight — we don't know
-- how long that spread persisted past the last poll, so it's excluded rather than guessed
-- (verified against a synthetic case before writing this into the model: 10 for 1h, 20 for
-- 2h, 30 for 0h -> 16.67, matches hand computation). Days with only one observation get a
-- null TWAP rather than a division-by-zero or a fabricated single-point "average".
-- fct_orderbook_snapshot rows are already single-domain internally (lowest_sell/
-- highest_buy/spread all come from the same orderbook response), so no further
-- domain-mixing risk there — it just needs tagging to join back to the right OHLC row.

with daily_prices as (
    select
        app_id,
        market_hash_name,
        coalesce('currency:' || currency::varchar, 'endpoint:' || source_endpoint) as money_domain,
        date_trunc('day', observed_at) as observation_date,
        observed_at,
        lowest_sell,
        volume
    from {{ ref('fct_price_observation') }}
    where lowest_sell is not null
),

ohlc as (
    select
        app_id,
        market_hash_name,
        money_domain,
        observation_date,
        first(lowest_sell order by observed_at) as open_price,
        max(lowest_sell) as high_price,
        min(lowest_sell) as low_price,
        last(lowest_sell order by observed_at) as close_price,
        sum(coalesce(volume, 0)) as total_volume,
        count(*) as observation_count
    from daily_prices
    group by 1, 2, 3, 4
),

spread_series as (
    select
        app_id,
        market_hash_name,
        'currency:' || currency::varchar as money_domain,
        date_trunc('day', observed_at) as observation_date,
        observed_at,
        spread,
        lead(observed_at) over (
            partition by app_id, market_hash_name, currency, date_trunc('day', observed_at)
            order by observed_at
        ) as next_observed_at
    from {{ ref('fct_orderbook_snapshot') }}
    where spread is not null
),

twap_spread as (
    select
        app_id,
        market_hash_name,
        money_domain,
        observation_date,
        sum(spread * extract(epoch from (coalesce(next_observed_at, observed_at) - observed_at)))
            / nullif(sum(extract(epoch from (coalesce(next_observed_at, observed_at) - observed_at))), 0)
            as time_weighted_avg_spread
    from spread_series
    group by 1, 2, 3, 4
)

select
    o.app_id,
    o.market_hash_name,
    o.money_domain,
    o.observation_date,
    o.open_price,
    o.high_price,
    o.low_price,
    o.close_price,
    o.total_volume,
    o.observation_count,
    t.time_weighted_avg_spread
from ohlc o
left join twap_spread t
    on o.app_id = t.app_id
    and o.market_hash_name = t.market_hash_name
    and o.money_domain = t.money_domain
    and o.observation_date = t.observation_date
