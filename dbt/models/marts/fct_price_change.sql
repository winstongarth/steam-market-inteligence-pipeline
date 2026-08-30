-- From the CDC stream (streaming/cdc_job.py -> market.changes.v1 ->
-- streaming/silver_writer.py). Already deduplicated to genuine field-level changes,
-- money-domain-safe (docs/DECISIONS.md, "CDC job: money-domain bug, found and fixed twice").

select * from {{ ref('stg_price_changes') }}
