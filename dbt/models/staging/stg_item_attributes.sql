-- Item descriptive attributes (name/type/rarity), exploded from search_render's embedded
-- asset_description. Feeds dim_item_snapshot's SCD Type 2 tracking — the money/listing
-- fields live in stg_search_render instead, this model is identity/attributes only.

with source as (
    select app_id, observed_at, raw_payload
    from {{ source('bronze', 'raw_envelopes') }}
    where endpoint = 'search_render'
),

typed as (
    select
        app_id,
        observed_at,
        json_transform(
            raw_payload,
            '{"results": [{"hash_name":"VARCHAR","asset_description":{"name":"VARCHAR","type":"VARCHAR","name_color":"VARCHAR","commodity":"BIGINT","market_bucket_group_id":"VARCHAR"}}]}'
        ) as payload
    from source
),

exploded as (
    select app_id, observed_at, unnest(payload.results) as item
    from typed
)

select
    app_id,
    item.hash_name as market_hash_name,
    cast(observed_at as timestamp) as observed_at,
    item.asset_description.name as item_name,
    item.asset_description.type as item_type,
    item.asset_description.name_color as rarity_color,
    item.asset_description.commodity as commodity,
    item.asset_description.market_bucket_group_id as market_bucket_group_id
from exploded
where item.hash_name is not null
