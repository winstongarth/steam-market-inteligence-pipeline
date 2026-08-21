{{ config(severity='warn') }}

-- Price plausibility (CLAUDE.md Phase 4): ">50% single-interval move flagged, not
-- silently dropped." Deliberately `severity='warn'`, not error — the spec's own wording
-- says "flagged", and a >50% move is a real, interesting signal for this project (sudden
-- price dislocations are literally one of the things §1 says the pipeline should surface),
-- not automatically bad data. Warn makes it visible in every `dbt test` run without
-- halting the DAG over something that might just be a genuine market event.

select
    app_id, market_hash_name, field, previous, current, pct_change, observed_at
from {{ ref('fct_price_change') }}
where field in ('lowest_sell', 'highest_buy')
    and abs(pct_change) > 50
