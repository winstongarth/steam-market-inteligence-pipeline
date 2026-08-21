{#
  Ports streaming/money.py's parsing logic to SQL for the warehouse-side (dbt) path.
  Both implementations exist because Phase 2's CDC job (Python/Spark) and Phase 3's dbt
  models are independent consumers of the same Bronze data — see cdc_job.py's module
  docstring for why that duplication is normal in a medallion architecture, not an
  oversight. Kept in sync deliberately: same currency-format table (docs/PHASE0_FINDINGS.md
  §3.7), same "always store money as amount*100" convention.

  Requires a `dim_currency` row aliased `dc` to be joined into the calling query (columns
  `dc.thousands_sep`, `dc.decimal_sep`) — this macro only builds the expression, it doesn't
  join. See dbt/seeds/dim_currency.csv.

  Bug found and fixed (2026-08-21, Phase 6): dbt-duckdb's seed loader imports an empty
  quoted CSV field ("") as SQL NULL, not empty string — currencies with no decimal
  separator (JPY, IDR, UAH) have `decimal_sep` blank in the seed on purpose (see
  dim_currency.csv), but the original `when dc.decimal_sep = ''` check never matched a
  NULL, so every IDR/JPY/UAH price silently parsed to NULL instead of erroring loudly.
  Found because Phase 6's real IDR poll data showed up with `lowest_sell` entirely NULL
  in int_item_resolved — traced back here, not assumed. Fixed by treating NULL and ''
  the same.
#}
{% macro parse_price_sql(price_col) %}
    case
        when dc.thousands_sep is null then null
        when dc.decimal_sep is null or dc.decimal_sep = '' then
            try_cast(
                replace(regexp_replace({{ price_col }}, '[^0-9' || dc.thousands_sep || ']', '', 'g'), dc.thousands_sep, '')
                as bigint
            ) * 100
        else
            round(
                try_cast(
                    replace(
                        replace(
                            regexp_replace({{ price_col }}, '[^0-9' || dc.thousands_sep || dc.decimal_sep || ']', '', 'g'),
                            dc.thousands_sep, ''
                        ),
                        dc.decimal_sep, '.'
                    ) as double
                ) * 100
            )::bigint
    end
{% endmacro %}
