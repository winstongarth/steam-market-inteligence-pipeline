-- Grain: item x currency x observed_at, order-book-specific: full depth measures
-- (spread, spread_bps, depth at 1/5/10% from mid). See
-- dbt/models/intermediate/int_orderbook_normalized.sql for the depth math.

select * from {{ ref('int_orderbook_normalized') }}
