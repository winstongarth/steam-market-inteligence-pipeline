-- §3.4 priceoverview. market_hash_name isn't in the response body — it's in the request
-- params we stored alongside the envelope. Money strings parsed via macros/parse_price.sql,
-- joined against dim_currency using the envelope's own `currency` field (what we
-- requested), never reverse-inferred from the string — see docs/PHASE0_FINDINGS.md §3.7.

with source as (
    select app_id, currency, observed_at, raw_payload, request_params
    from {{ source('bronze', 'raw_envelopes') }}
    where endpoint = 'priceoverview'
),

parsed as (
    select
        app_id,
        currency,
        cast(observed_at as timestamp) as observed_at,
        json_extract_string(request_params, '$.market_hash_name') as market_hash_name,
        json_extract_string(raw_payload, '$.lowest_price') as lowest_price_text,
        json_extract_string(raw_payload, '$.volume') as volume_text
    from source
)

select
    p.app_id,
    p.market_hash_name,
    p.observed_at,
    {{ parse_price_sql('p.lowest_price_text') }} as lowest_sell,
    cast(null as bigint) as highest_buy,
    cast(null as bigint) as sell_listings,
    try_cast(regexp_replace(p.volume_text, '[^0-9]', '', 'g') as bigint) as volume,
    p.currency,
    'priceoverview' as source_endpoint
from parsed p
left join {{ ref('dim_currency') }} dc on p.currency = dc.currency_code
