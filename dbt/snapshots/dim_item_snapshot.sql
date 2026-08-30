{#
  SCD Type 2 for item identity — dim_item, tracking name/type/rarity changes over time.
  `check` strategy on item_name/item_type/
  rarity_color: a new dbt_valid_from/dbt_valid_to row pair is written whenever any of
  those change for a given (app_id, market_hash_name).

  Honest caveat: this session's Bronze data is a small, disconnected accumulation from
  ad-hoc test runs (see docs/METRICS.md), not a genuinely continuous
  production feed. The source query below takes each item's MOST RECENT observed
  attributes as "the
  current snapshot" so `dbt snapshot` has something meaningful to compare against on
  successive runs — the SCD2 mechanism itself is real and will correctly version any
  future name/type/rarity change once this runs against live, evolving data (e.g. after
  the scheduler has been running for a while and Valve renames or re-rarities an
  item, which does happen with CS2 skins).
#}

{% snapshot dim_item_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key="app_id || ':' || market_hash_name",
        strategy='check',
        check_cols=['item_name', 'item_type', 'rarity_color'],
    )
}}

select
    app_id,
    market_hash_name,
    item_name,
    item_type,
    rarity_color,
    commodity,
    market_bucket_group_id
from {{ ref('stg_item_attributes') }}
qualify row_number() over (partition by app_id, market_hash_name order by observed_at desc) = 1

{% endsnapshot %}
