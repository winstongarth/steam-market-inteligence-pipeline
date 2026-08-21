-- Regression test for the parse_price_sql bug (docs/DECISIONS.md, Phase 6): currencies
-- with no decimal separator (JPY, IDR, UAH — decimal_sep is blank in dim_currency.csv,
-- which dbt-duckdb's seed loader imports as NULL) used to silently parse to NULL instead
-- of a real value. Fails if any priceoverview row for one of these currencies has a null
-- lowest_sell where the raw data clearly wasn't null (i.e., stg_priceoverview parsed
-- something for lowest_price_text but stg_priceoverview's downstream lowest_sell is null).

select
    app_id, market_hash_name, currency, observed_at
from {{ ref('stg_priceoverview') }}
where currency in (8, 10, 18)  -- JPY, IDR, UAH
    and lowest_sell is null
