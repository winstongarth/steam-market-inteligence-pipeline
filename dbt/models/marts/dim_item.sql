-- Gold-facing view over the SCD2 snapshot (dbt/snapshots/dim_item_snapshot.sql). Requires
-- `dbt snapshot` to have been run at least once before this has any data — see
-- dbt/README or docs/DECISIONS.md for the run order.

select
    app_id,
    market_hash_name,
    item_name,
    item_type,
    rarity_color,
    commodity,
    market_bucket_group_id,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_current
from {{ ref('dim_item_snapshot') }}
