"""Phase 7.1 — query tuning, measured against the real DuckDB warehouse.

No Snowflake query profiles here: the Snowflake trial was deliberately deferred (see
docs/DECISIONS.md, Phase 3/4) pending Phase 1's soak test and Phase 4's stability, neither
of which happened this session. DuckDB's own EXPLAIN ANALYZE is the honest substitute —
same diagnostic questions (scan volume, join fan-out, redundant scans), same fix
categories worth checking (pre-aggregation, rewrite), minus the one technique that's
Snowflake-specific and doesn't map onto DuckDB at all: clustering keys. DuckDB tables here
aren't micro-partitioned, so "add a clustering key" isn't an available lever — noted
plainly in docs/METRICS.md rather than faked.

Also note: this session's real dataset is small (thousands of rows, not the 30-day/
production scale assumed going in), so wall-clock time differences between a naive and a
tuned query are often sub-millisecond either way. Where that's true, the real, scale-
independent signal is operator shape and rows-scanned/produced from EXPLAIN ANALYZE, not
wall time — recorded as such, not dressed up as a dramatic speedup that didn't happen.

Run: PYTHONPATH=. uv run python scripts/phase7_query_tuning.py
"""

from __future__ import annotations

import sys
import time

import duckdb

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = "dbt/steam_market.duckdb"


def run(con: duckdb.DuckDBPyConnection, label: str, sql: str, reps: int = 20) -> None:
    # warm up (first exec pays parse/plan cost)
    result = con.execute(sql).fetchdf()
    times = []
    for _ in range(reps):
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        times.append(time.perf_counter() - t0)
    times.sort()
    p50 = times[len(times) // 2]
    print(f"--- {label} ---")
    print(f"rows returned: {len(result)}")
    print(f"median wall time over {reps} reps: {p50*1000:.3f} ms")
    print("EXPLAIN ANALYZE:")
    plan = con.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
    for row in plan:
        print(row[1] if len(row) > 1 else row[0])
    print()


def main() -> None:
    con = duckdb.connect(DB_PATH, read_only=True)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_endpoint='localhost:9000';")
    con.execute("SET s3_access_key_id='minioadmin';")
    con.execute("SET s3_secret_access_key='minioadmin';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")

    print("=" * 70)
    print("Q1 — cross-currency dislocation join: day-average fan-out vs ASOF")
    print("=" * 70)

    naive_q1 = """
        with converted as (select * from int_fx_converted_prices),
        usd_day as (
            select app_id, market_hash_name, observation_date, avg(usd_equivalent) as usd_price
            from converted where iso_code = 'USD'
            group by 1, 2, 3
        ),
        non_usd as (select * from converted where iso_code != 'USD')
        select n.app_id, n.market_hash_name, n.observation_date, n.iso_code,
               n.usd_equivalent, u.usd_price,
               (n.usd_equivalent - u.usd_price) / u.usd_price * 100 as pct_gap
        from non_usd n
        join usd_day u
            on n.app_id = u.app_id and n.market_hash_name = u.market_hash_name
            and n.observation_date = u.observation_date
    """
    tuned_q1 = "select * from mart_cross_currency_dislocation"

    run(con, "Q1 naive (day-average GROUP BY join)", naive_q1)
    run(con, "Q1 tuned (ASOF JOIN, materialized mart)", tuned_q1)

    print("=" * 70)
    print("Q2 — 7-day rolling avg close price: correlated subquery vs window function")
    print("=" * 70)

    naive_q2 = """
        select
            d.app_id, d.market_hash_name, d.money_domain, d.observation_date, d.close_price,
            (select avg(d2.close_price) from mart_item_daily d2
             where d2.app_id = d.app_id and d2.market_hash_name = d.market_hash_name
               and d2.money_domain = d.money_domain
               and d2.observation_date <= d.observation_date
               and d2.observation_date > d.observation_date - interval 7 day
            ) as rolling_7d_avg
        from mart_item_daily d
    """
    tuned_q2 = """
        select
            app_id, market_hash_name, money_domain, observation_date, close_price,
            avg(close_price) over (
                partition by app_id, market_hash_name, money_domain
                order by observation_date
                range between interval 7 day preceding and current row
            ) as rolling_7d_avg
        from mart_item_daily
    """

    run(con, "Q2 naive (correlated subquery per row)", naive_q2, reps=5)
    run(con, "Q2 tuned (window function)", tuned_q2, reps=5)

    # correctness check: naive vs tuned must agree
    naive_df = con.execute(naive_q2).fetchdf().sort_values(
        ["app_id", "market_hash_name", "money_domain", "observation_date"]
    ).reset_index(drop=True)
    tuned_df = con.execute(tuned_q2).fetchdf().sort_values(
        ["app_id", "market_hash_name", "money_domain", "observation_date"]
    ).reset_index(drop=True)
    import pandas as pd
    match = (naive_df["rolling_7d_avg"].round(6) == tuned_df["rolling_7d_avg"].round(6)).all()
    print(f"Q2 correctness check (naive == tuned, all rows): {match}\n")

    print("=" * 70)
    print("Q3 — latest observation per item: self-join-to-max vs QUALIFY/ROW_NUMBER")
    print("NOTE: the correctness check below is expected to print False. That's a real")
    print("finding, not a script bug: 16,010/16,130 fct_price_observation rows have a")
    print("NULL currency (search_render rows — that endpoint's currency isn't request-")
    print("controlled, see int_fx_converted_prices.sql). SQL's NULL = NULL is never")
    print("true, so the naive equi-join silently drops every NULL-currency group (only")
    print("21 of 3042 groups have non-null currency and survive the join); QUALIFY's")
    print("PARTITION BY groups NULLs together correctly. See docs/DECISIONS.md.")
    print("=" * 70)

    naive_q3 = """
        select f.*
        from fct_price_observation f
        join (
            select app_id, market_hash_name, currency, max(observed_at) as max_observed_at
            from fct_price_observation
            group by 1, 2, 3
        ) latest
            on f.app_id = latest.app_id
            and f.market_hash_name = latest.market_hash_name
            and f.currency = latest.currency
            and f.observed_at = latest.max_observed_at
    """
    tuned_q3 = """
        select *
        from fct_price_observation
        qualify row_number() over (
            partition by app_id, market_hash_name, currency
            order by observed_at desc
        ) = 1
    """

    run(con, "Q3 naive (self-join to per-group max)", naive_q3)
    run(con, "Q3 tuned (QUALIFY + ROW_NUMBER window)", tuned_q3)

    naive_df3 = con.execute(naive_q3).fetchdf().sort_values(
        ["app_id", "market_hash_name", "currency"]
    ).reset_index(drop=True)
    tuned_df3 = con.execute(tuned_q3).fetchdf().sort_values(
        ["app_id", "market_hash_name", "currency"]
    ).reset_index(drop=True)
    match3 = len(naive_df3) == len(tuned_df3) and (
        naive_df3["observed_at"].values == tuned_df3["observed_at"].values
    ).all()
    print(f"Q3 correctness check (naive == tuned, all rows): {match3}\n")


if __name__ == "__main__":
    main()
