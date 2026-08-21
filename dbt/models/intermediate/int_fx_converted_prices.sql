-- Converts each priceoverview observation's local-currency price to a USD equivalent,
-- using real ECB rates (seeds/dim_fx_rate.csv, fetched via ingest/fx_rates.py). Only
-- priceoverview rows carry a real, request-controlled currency (search_render's is
-- undocumented, orderbook's isn't currency-controllable — see cdc_job.py's Silver
-- normalization table) — this model is deliberately scoped to the one source we can
-- trust for this.

with priceoverview_obs as (
    select
        app_id,
        market_hash_name,
        observed_at,
        date_trunc('day', observed_at) as observation_date,
        currency,
        lowest_sell
    from {{ ref('int_item_resolved') }}
    where source_endpoint = 'priceoverview' and lowest_sell is not null
),

with_iso as (
    select p.*, dc.iso_code
    from priceoverview_obs p
    left join {{ ref('dim_currency') }} dc on p.currency = dc.currency_code
),

with_fx as (
    select w.*, fx.rate_to_usd
    from with_iso w
    left join {{ ref('dim_fx_rate') }} fx
        on fx.rate_date = w.observation_date and fx.currency_iso = w.iso_code
)

select
    app_id,
    market_hash_name,
    observed_at,
    observation_date,
    currency,
    iso_code,
    lowest_sell,
    lowest_sell / 100.0 as local_price_major_units,
    rate_to_usd,
    (lowest_sell / 100.0) / rate_to_usd as usd_equivalent
from with_fx
where rate_to_usd is not null
