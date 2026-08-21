-- Referential integrity (CLAUDE.md Phase 4): every fact row resolves to a dim_item.
-- Checked against the CURRENT dim_item row per (app_id, market_hash_name) — an item that
-- existed historically but was renamed should still resolve via its current identity row,
-- not fail just because its exact old attributes aren't "current" anymore.

select f.app_id, f.market_hash_name, count(*) as orphan_rows
from {{ ref('fct_price_observation') }} f
left join {{ ref('dim_item') }} d
    on f.app_id = d.app_id
    and f.market_hash_name = d.market_hash_name
    and d.is_current
where d.app_id is null
group by 1, 2
