-- Uniqueness (CLAUDE.md Phase 4): no duplicate grain in any fact table. Grain here is
-- item x currency x observed_at x source_endpoint (source_endpoint included because two
-- different endpoints CAN legitimately report for the same item at the exact same
-- millisecond only by coincidence — including it makes this a true duplicate-row check,
-- not a false positive on rapid legitimate polling).
-- A passing test returns zero rows; dbt fails the test if this query returns any.

select
    app_id, market_hash_name, currency, observed_at, source_endpoint,
    count(*) as n
from {{ ref('fct_price_observation') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
