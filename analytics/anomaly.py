"""Anomaly detection.

Reads Phase 3's dbt marts (mart_item_daily, fct_orderbook_snapshot) from the DuckDB
warehouse, computes four anomaly classes, and writes a ranked list to `mart_anomaly` in
the same database.

## Honest data-scope note

The default windows here (30-day z-score window, day-of-week volume seasonality) assume
months of continuous daily history. This session's accumulated data spans roughly two
calendar days, for a catalog sweep that mostly touches each item once or twice — only a
handful of frequently-resurfacing items (a few popular cases, and the one Tier A
watchlist item) have enough repeat observations to compute anything. The detectors below
are written to be correct and general (configurable windows, `min_periods` guards that
skip output rather than fabricate a baseline from insufficient history) — the actual
numbers reported in docs/METRICS.md reflect this small real dataset, not a validated
30-day run. Re-run once Phase 1's 24h+ soak test (still pending) produces real continuous
history.

## Money-domain safety

Every detector here groups by `money_domain` (or, for the order-book detectors, relies on
`fct_orderbook_snapshot` rows already being internally currency-consistent) — the same
fix applied three times already this session (streaming CDC, dbt OHLC, see
docs/DECISIONS.md) after real bugs blended USD-ish and JPY-minor-units values into one
series. Not repeating that mistake a fourth time here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
import pandas as pd

logger = logging.getLogger("analytics.anomaly")

ANOMALY_COLUMNS = [
    "app_id", "market_hash_name", "anomaly_type", "severity",
    "explanation", "detected_at", "computed_at",
]


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --- 1. Rolling z-score on log returns per item -----------------------------------------


def detect_price_zscore_anomalies(
    daily_df: pd.DataFrame,
    window: int = 30,
    min_periods: int = 5,
    z_threshold: float = 3.0,
) -> pd.DataFrame:
    """`daily_df` columns: app_id, market_hash_name, money_domain, observation_date,
    close_price (from mart_item_daily). `window`/`min_periods` are in TRADING DAYS
    (calendar days with an observation), matching the spec's "30-day window" phrasing —
    not raw poll counts, since polling cadence is irregular.

    The rolling mean/std are computed over the window PRIOR to the current day (shifted by
    one) so a day's own return never leaks into its own baseline.
    """
    if daily_df.empty:
        return pd.DataFrame(columns=ANOMALY_COLUMNS)

    rows = []
    computed_at = _now()

    for (app_id, market_hash_name, money_domain), group in daily_df.groupby(
        ["app_id", "market_hash_name", "money_domain"]
    ):
        group = group.sort_values("observation_date").reset_index(drop=True)
        if len(group) < min_periods + 1:
            continue

        log_return = np.log(group["close_price"] / group["close_price"].shift(1))
        rolling_mean = log_return.rolling(window, min_periods=min_periods).mean().shift(1)
        rolling_std = log_return.rolling(window, min_periods=min_periods).std().shift(1)
        z = (log_return - rolling_mean) / rolling_std

        for i in range(len(group)):
            if pd.isna(z.iloc[i]) or rolling_std.iloc[i] == 0 or pd.isna(rolling_std.iloc[i]):
                continue
            if abs(z.iloc[i]) < z_threshold:
                continue
            pct_move = (np.exp(log_return.iloc[i]) - 1) * 100
            direction = "up" if pct_move > 0 else "down"
            rows.append((
                app_id, market_hash_name, "price_zscore",
                min(abs(z.iloc[i]) / 10.0, 1.0),
                f"{market_hash_name} moved {direction} {abs(pct_move):.1f}% on "
                f"{group['observation_date'].iloc[i]} ({money_domain}), "
                f"{abs(z.iloc[i]):.1f}σ from its trailing {window}-day average move",
                group["observation_date"].iloc[i],
                computed_at,
            ))

    return pd.DataFrame(rows, columns=ANOMALY_COLUMNS)


# --- 2. EWMA spread-widening detector ----------------------------------------------------


def detect_spread_widening_anomalies(
    orderbook_df: pd.DataFrame,
    span: int = 20,
    min_periods: int = 10,
    z_threshold: float = 3.0,
) -> pd.DataFrame:
    """`orderbook_df` columns: app_id, market_hash_name, observed_at, spread_bps (from
    fct_orderbook_snapshot). One-sided: only flags WIDENING (current spread above its
    EWMA baseline), not narrowing — "a sudden spread blowout usually precedes a price
    move", narrowing isn't the signal being asked for.
    """
    if orderbook_df.empty:
        return pd.DataFrame(columns=ANOMALY_COLUMNS)

    rows = []
    computed_at = _now()

    for (app_id, market_hash_name), group in orderbook_df.groupby(["app_id", "market_hash_name"]):
        group = group.dropna(subset=["spread_bps"]).sort_values("observed_at").reset_index(drop=True)
        if len(group) < min_periods + 1:
            continue

        prior_mean = group["spread_bps"].ewm(span=span, min_periods=min_periods).mean().shift(1)
        prior_std = group["spread_bps"].ewm(span=span, min_periods=min_periods).std().shift(1)
        z = (group["spread_bps"] - prior_mean) / prior_std

        for i in range(len(group)):
            if pd.isna(z.iloc[i]) or prior_std.iloc[i] == 0 or pd.isna(prior_std.iloc[i]):
                continue
            if z.iloc[i] < z_threshold:  # one-sided: widening only
                continue
            rows.append((
                app_id, market_hash_name, "spread_widening",
                min(z.iloc[i] / 10.0, 1.0),
                f"{market_hash_name} spread widened to {group['spread_bps'].iloc[i]:.0f}bps at "
                f"{group['observed_at'].iloc[i]}, {z.iloc[i]:.1f}σ above its EWMA(span={span}) "
                f"baseline of {prior_mean.iloc[i]:.0f}bps",
                group["observed_at"].iloc[i],
                computed_at,
            ))

    return pd.DataFrame(rows, columns=ANOMALY_COLUMNS)


# --- 3. Volume spike detection, day-of-week seasonality control -------------------------


def detect_volume_spike_anomalies(
    daily_df: pd.DataFrame,
    min_weekday_occurrences: int = 3,
    z_threshold: float = 3.0,
) -> pd.DataFrame:
    """`daily_df` columns: app_id, market_hash_name, money_domain, observation_date,
    total_volume (from mart_item_daily). Baseline is per (item, day-of-week) — "Steam
    sales and esports events create strong, real periodicity" — so a Saturday
    is only compared against other Saturdays, never against a Tuesday.

    Requires at least `min_weekday_occurrences` PRIOR observations of that same weekday
    before flagging anything — with under ~3 weeks of history, every (item, weekday) has
    at most one occurrence, so this returns empty rather than compare a single point
    against itself. That's correct behavior on today's data, not a bug — see the module
    docstring's data-scope note.
    """
    if daily_df.empty:
        return pd.DataFrame(columns=ANOMALY_COLUMNS)

    df = daily_df.copy()
    df["day_of_week"] = pd.to_datetime(df["observation_date"]).dt.dayofweek

    rows = []
    computed_at = _now()

    for (app_id, market_hash_name, money_domain, dow), group in df.groupby(
        ["app_id", "market_hash_name", "money_domain", "day_of_week"]
    ):
        group = group.sort_values("observation_date").reset_index(drop=True)
        volume = group["total_volume"].astype(float)

        for i in range(len(group)):
            prior = volume.iloc[:i]
            if len(prior) < min_weekday_occurrences:
                continue
            prior_mean, prior_std = prior.mean(), prior.std()
            if prior_std == 0 or pd.isna(prior_std):
                continue
            z = (volume.iloc[i] - prior_mean) / prior_std
            if abs(z) < z_threshold:
                continue
            rows.append((
                app_id, market_hash_name, "volume_spike",
                min(abs(z) / 10.0, 1.0),
                f"{market_hash_name} volume was {volume.iloc[i]:.0f} on "
                f"{group['observation_date'].iloc[i]} ({money_domain}), {abs(z):.1f}σ from its "
                f"typical day-of-week ({group['observation_date'].iloc[i].strftime('%A')}) volume of "
                f"{prior_mean:.0f}",
                group["observation_date"].iloc[i],
                computed_at,
            ))

    return pd.DataFrame(rows, columns=ANOMALY_COLUMNS)


# --- 4. Crossed-book detection -----------------------------------------------------------


def detect_crossed_book_anomalies(orderbook_df: pd.DataFrame) -> pd.DataFrame:
    """`orderbook_df` columns: app_id, market_hash_name, observed_at, highest_buy,
    lowest_sell, spread_bps (from fct_orderbook_snapshot). A distinct class from the
    price/spread/volume detectors above — it's a separate class on purpose, and it's
    genuinely a different phenomenon (a book-state inconsistency, not a return/spread
    statistic). See docs/DECISIONS.md: in this project's real data, 26% of orderbook
    observations are crossed — not rare, and traced to Steam's own backend, not our
    pipeline (both values come from one atomic API response)."""
    if orderbook_df.empty:
        return pd.DataFrame(columns=ANOMALY_COLUMNS)

    crossed = orderbook_df[orderbook_df["highest_buy"] > orderbook_df["lowest_sell"]]
    computed_at = _now()

    rows = []
    for _, row in crossed.iterrows():
        severity = min(abs(row["spread_bps"]) / 1000.0, 1.0) if pd.notna(row["spread_bps"]) else 0.5
        rows.append((
            row["app_id"], row["market_hash_name"], "crossed_book", severity,
            f"{row['market_hash_name']} crossed book at {row['observed_at']}: "
            f"highest_buy ({row['highest_buy']}) > lowest_sell ({row['lowest_sell']}), "
            f"spread {row['spread_bps']:.0f}bps — either a genuine arbitrage window or a "
            f"Steam backend order-book cache inconsistency (see docs/DECISIONS.md)",
            row["observed_at"], computed_at,
        ))

    return pd.DataFrame(rows, columns=ANOMALY_COLUMNS)


@dataclass
class AnomalyRunSummary:
    total_anomalies: int
    by_type: dict[str, int]


def build_mart_anomaly(
    daily_df: pd.DataFrame,
    orderbook_df: pd.DataFrame,
    price_zscore_window: int = 30,
    price_zscore_min_periods: int = 5,
    spread_ewm_span: int = 20,
    spread_ewm_min_periods: int = 10,
    volume_min_weekday_occurrences: int = 3,
    z_threshold: float = 3.0,
) -> pd.DataFrame:
    """Runs all four detectors and returns one ranked (by severity desc) DataFrame ready
    to materialize as mart_anomaly."""
    parts = [
        detect_price_zscore_anomalies(
            daily_df, window=price_zscore_window, min_periods=price_zscore_min_periods, z_threshold=z_threshold
        ),
        detect_spread_widening_anomalies(
            orderbook_df, span=spread_ewm_span, min_periods=spread_ewm_min_periods, z_threshold=z_threshold
        ),
        detect_volume_spike_anomalies(
            daily_df, min_weekday_occurrences=volume_min_weekday_occurrences, z_threshold=z_threshold
        ),
        detect_crossed_book_anomalies(orderbook_df),
    ]
    non_empty = [p for p in parts if not p.empty]
    combined = pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame(columns=ANOMALY_COLUMNS)
    if not combined.empty:
        combined = combined.sort_values("severity", ascending=False).reset_index(drop=True)
    return combined


def summarize(mart_anomaly_df: pd.DataFrame) -> AnomalyRunSummary:
    if mart_anomaly_df.empty:
        return AnomalyRunSummary(total_anomalies=0, by_type={})
    return AnomalyRunSummary(
        total_anomalies=len(mart_anomaly_df),
        by_type=mart_anomaly_df["anomaly_type"].value_counts().to_dict(),
    )


def main() -> None:
    """Reads mart_item_daily/fct_orderbook_snapshot from the DuckDB warehouse, runs all
    four detectors, and materializes the ranked result as `mart_anomaly` in the same
    database. Same DUCKDB_PATH convention as dbt/profiles.yml and the Airflow DAG."""
    import os

    import duckdb

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    default_path = os.path.join(os.path.dirname(__file__), "..", "dbt", "steam_market.duckdb")
    db_path = os.environ.get("DUCKDB_PATH", default_path)
    con = duckdb.connect(db_path)

    daily_df = con.execute("SELECT * FROM main.mart_item_daily").fetchdf()
    orderbook_df = con.execute("SELECT * FROM main.fct_orderbook_snapshot").fetchdf()

    result = build_mart_anomaly(daily_df, orderbook_df)
    summary = summarize(result)
    logger.info("detected %d anomalies: %s", summary.total_anomalies, summary.by_type)

    con.execute("CREATE OR REPLACE TABLE main.mart_anomaly AS SELECT * FROM result")
    logger.info("wrote main.mart_anomaly (%d rows) to %s", len(result), db_path)
    con.close()


if __name__ == "__main__":
    main()
