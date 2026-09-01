"""Dashboard sync — DuckDB gold marts -> Postgres, for Grafana to query directly.

Grafana has no first-party DuckDB datasource, and the community plugins for it are
unsigned (extra install/signing friction, less predictable offline). Postgres is a
first-party Grafana datasource with zero plugins — the same reasoning already used for
`postgres-airflow` in docker-compose.yml, just serving gold-mart data instead of Airflow's
own metadata. DuckDB (`dbt/steam_market.duckdb`) stays the single source of truth; this
module is a one-way, full-refresh sync into `postgres-marts` that Grafana reads from.

Full-refresh (`if_exists="replace"`) rather than incremental upsert: matches this
project's existing pattern of `CREATE OR REPLACE TABLE` in analytics/anomaly.py — marts
are cheap to fully recompute at this data volume, so there's no reconciliation logic to
get wrong.

## Game scope

The pipeline tracks five games (ingest/config.py APP_IDS), but the dashboards this syncs
for are scoped to CS2 by default — `DASHBOARD_APP_IDS` [CONFIG: default "730"]. Pass a
comma-separated list (or unset it) to widen the sync to other tracked games; the
underlying tables and dashboards keep `app_id` as a real column/filter rather than
assuming CS2, so nothing has to change structurally to widen scope later.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger("analytics.export_to_grafana")

# Every gold mart the dashboards read from. Order doesn't matter — no FK enforcement on
# the Postgres side, Grafana just queries these as independent tables.
MART_TABLES = [
    "dim_item",
    "mart_item_daily",
    "fct_orderbook_snapshot",
    "mart_anomaly",
    "mart_cross_currency_dislocation",
]


def parse_app_ids(raw: str | None) -> list[int] | None:
    """`None`/empty means "no filter, sync every tracked game". Comma-separated string of
    app_ids (e.g. "730" or "730,570") means restrict the sync to those games."""
    if raw is None or not raw.strip():
        return None
    return [int(part.strip()) for part in raw.split(",") if part.strip()]


def filter_by_app_ids(df: pd.DataFrame, app_ids: list[int] | None) -> pd.DataFrame:
    """Restricts `df` to the given app_ids. A no-op if app_ids is None, df is empty, or df
    has no app_id column (defensive — every mart here has one, but this keeps the function
    safe to call generically over MART_TABLES without a per-table special case)."""
    if not app_ids or df.empty or "app_id" not in df.columns:
        return df
    return df[df["app_id"].isin(app_ids)].reset_index(drop=True)


def stamp_synced_at(df: pd.DataFrame, synced_at: datetime) -> pd.DataFrame:
    """Adds a `synced_at` column so a Grafana panel (or a human) can tell how fresh the
    dashboard's data is relative to the last successful sync, independent of any
    per-row `computed_at`/`observed_at` column the mart itself carries."""
    out = df.copy()
    out["synced_at"] = synced_at
    return out


def read_marts(con: object, app_ids: list[int] | None, tables: list[str] = MART_TABLES) -> dict[str, pd.DataFrame]:
    """Reads each mart from the DuckDB connection `con`, filtered to `app_ids`. A table
    that doesn't exist yet (e.g. `mart_anomaly` before the first anomaly-detection run)
    logs a warning and is skipped rather than failing the whole sync — the other marts are
    still worth syncing even if one hasn't been built yet."""
    result: dict[str, pd.DataFrame] = {}
    for name in tables:
        try:
            df = con.execute(f"SELECT * FROM main.{name}").fetchdf()  # type: ignore[attr-defined]
        except Exception as exc:
            logger.warning("skipping %s: not readable yet (%s)", name, exc)
            continue
        result[name] = filter_by_app_ids(df, app_ids)
    return result


def write_to_postgres(tables: dict[str, pd.DataFrame], engine: object, synced_at: datetime) -> None:
    """Writes each DataFrame to a same-named Postgres table, replacing it wholesale."""
    for name, df in tables.items():
        stamped = stamp_synced_at(df, synced_at)
        stamped.to_sql(name, engine, schema="public", if_exists="replace", index=False)
        logger.info("synced %s: %d row(s)", name, len(stamped))


def main() -> None:
    import duckdb
    from sqlalchemy import create_engine

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    default_duckdb_path = os.path.join(os.path.dirname(__file__), "..", "dbt", "steam_market.duckdb")
    duckdb_path = os.environ.get("DUCKDB_PATH", default_duckdb_path)
    pg_url = os.environ.get(
        "GRAFANA_POSTGRES_URL", "postgresql+psycopg2://marts:marts@localhost:5433/marts"
    )
    app_ids = parse_app_ids(os.environ.get("DASHBOARD_APP_IDS", "730"))

    logger.info(
        "syncing marts from %s to %s (app_ids=%s)", duckdb_path, pg_url, app_ids or "ALL"
    )

    # read_only=True: this runs after dbt_run_marts/detect_anomalies have finished writing
    # (see airflow/dags/steam_market_batch.py's task order), and DuckDB allows concurrent
    # read-only connections but only one writer — no reason to take a write lock here.
    con = duckdb.connect(duckdb_path, read_only=True)
    engine = create_engine(pg_url)
    synced_at = datetime.now(timezone.utc)

    try:
        tables = read_marts(con, app_ids)
        if not tables:
            logger.warning("no marts were readable — nothing synced. Has `dbt run` completed?")
            return
        write_to_postgres(tables, engine, synced_at)
    finally:
        con.close()
        engine.dispose()


if __name__ == "__main__":
    main()
