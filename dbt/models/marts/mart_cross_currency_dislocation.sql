-- Cross-currency dislocation: FX-adjusted price gaps, per non-USD
-- observation, against the nearest-in-time USD observation for the same item. Because
-- Steam prices regionally, persistent structural gaps should appear — this is the number
-- that answers "is this really regional pricing, or just FX noise."
--
-- Bug found and fixed (2026-08-21): a first version grouped by observation_date and
-- averaged/joined ALL of a day's USD observations against ALL non-USD observations for
-- that item/day. AK-47 | Redline was polled dozens of times in USD across the whole
-- session (it's the Tier A watchlist item) but EUR/SGD/IDR only once each, during Phase
-- 6's synchronized 4-currency batch — the naive join fanned out into spurious USD-vs-USD
-- rows (comparing one poll's price against a DIFFERENT poll's price, hours apart, with a
-- nonzero "gap" that was really just intraday price movement, not FX/regional pricing)
-- and diluted the real EUR/SGD/IDR comparisons against a whole-day average instead of the
-- actual concurrent USD price. Fixed with an ASOF JOIN: each non-USD observation is
-- matched to the most recent USD observation AT OR BEFORE it — the real "what would this
-- have cost in USD, at roughly the same moment" comparison, verified against DuckDB's
-- documented ASOF JOIN semantics before trusting it (nearest-prior match, not nearest-any).

with converted as (
    select * from {{ ref('int_fx_converted_prices') }}
),

usd_observations as (
    select app_id, market_hash_name, observed_at, usd_equivalent as usd_price
    from converted
    where iso_code = 'USD'
),

non_usd as (
    select * from converted where iso_code != 'USD'
)

select
    n.app_id,
    n.market_hash_name,
    n.observation_date,
    n.observed_at,
    n.iso_code,
    n.local_price_major_units,
    n.rate_to_usd,
    n.usd_equivalent,
    u.usd_price as usd_baseline_price,
    u.observed_at as usd_baseline_observed_at,
    (n.usd_equivalent - u.usd_price) as usd_gap,
    case
        when u.usd_price is null or u.usd_price = 0 then null
        else (n.usd_equivalent - u.usd_price) / u.usd_price * 100
    end as pct_gap
from non_usd n
asof left join usd_observations u
    on n.app_id = u.app_id
    and n.market_hash_name = u.market_hash_name
    and n.observed_at >= u.observed_at
