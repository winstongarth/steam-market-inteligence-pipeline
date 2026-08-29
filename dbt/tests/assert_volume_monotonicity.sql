{{ config(severity='warn') }}

-- Volume monotonicity: "cumulative volume must not decrease." Real
-- finding, 2026-08-21: Steam's priceoverview `volume` field is NOT a lifetime cumulative
-- counter — it's a rolling-window trade count (empirically, real data for one item showed
-- 105 -> 99 -> 99 -> 99 across successive polls, a real decrease). The assumption going in
-- described a traditional exchange's cumulative volume field; Steam's actual API doesn't
-- expose one. `severity='warn'` deliberately, not error — an error-level gate here would
-- fail on essentially every real poll, for a reason that isn't a data quality problem.
-- This test still has value: it flags decreases so they're visible and reviewable, and a
-- genuinely alarming pattern (e.g. volume dropping to exactly zero, or a huge single-step
-- drop) would still show up here for a human to look at, even though "any decrease" isn't
-- inherently wrong for this field.

select
    app_id, market_hash_name, observed_at, volume,
    lag(volume) over (partition by app_id, market_hash_name order by observed_at) as previous_volume
from {{ ref('fct_price_observation') }}
where volume is not null
qualify volume < lag(volume) over (partition by app_id, market_hash_name order by observed_at)
