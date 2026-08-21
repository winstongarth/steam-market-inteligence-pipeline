-- CDC output from streaming/cdc_job.py, landed by streaming/silver_writer.py. Already
-- normalized and money-domain-safe (docs/DECISIONS.md, "CDC job: money-domain bug, found
-- and fixed twice") — this model is just light typing, no re-derivation.

select
    app_id,
    market_hash_name,
    field,
    previous,
    current,
    delta,
    pct_change,
    cast(observed_at as timestamp) as observed_at
from {{ source('silver', 'change_events') }}
