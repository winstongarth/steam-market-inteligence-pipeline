"""Tests for analytics/anomaly.py's four detectors, using synthetic data — no DuckDB/
warehouse dependency, so these run fast and independent of whatever's currently in the
real database.
"""

import datetime

import numpy as np
import pandas as pd
import pytest

from analytics.anomaly import (
    detect_crossed_book_anomalies,
    detect_price_zscore_anomalies,
    detect_spread_widening_anomalies,
    detect_volume_spike_anomalies,
)


def _dates(n, start="2026-01-01"):
    return pd.date_range(start, periods=n, freq="D")


def test_price_zscore_flags_a_real_outlier_move():
    rng = np.random.default_rng(42)
    n = 40
    prices = [100.0]
    for _ in range(n - 1):
        prices.append(prices[-1] * (1 + rng.normal(0, 0.01)))  # ~1% daily noise
    # Inject one huge move on the last day.
    prices[-1] = prices[-2] * 1.5  # +50%

    df = pd.DataFrame({
        "app_id": 730,
        "market_hash_name": "Test Item",
        "money_domain": "currency:1",
        "observation_date": _dates(n),
        "close_price": prices,
    })

    result = detect_price_zscore_anomalies(df, window=30, min_periods=5, z_threshold=3.0)

    assert len(result) >= 1
    last_day_flagged = result[result["detected_at"] == df["observation_date"].iloc[-1]]
    assert len(last_day_flagged) == 1
    assert "up 50" in last_day_flagged.iloc[0]["explanation"] or "50.0%" in last_day_flagged.iloc[0]["explanation"]


def test_price_zscore_no_flags_on_stable_series():
    df = pd.DataFrame({
        "app_id": 730,
        "market_hash_name": "Stable Item",
        "money_domain": "currency:1",
        "observation_date": _dates(15),
        "close_price": [100.0 + (i % 2) * 0.5 for i in range(15)],  # tiny alternating noise
    })
    result = detect_price_zscore_anomalies(df, window=30, min_periods=5, z_threshold=3.0)
    assert result.empty


def test_price_zscore_respects_money_domain_grouping():
    """A currency:1 series and a currency:8 series for the SAME item must never be
    compared to each other — the exact bug class fixed three times already this session."""
    n = 20
    df = pd.concat([
        pd.DataFrame({
            "app_id": 730, "market_hash_name": "X", "money_domain": "currency:1",
            "observation_date": _dates(n), "close_price": [100.0] * n,
        }),
        pd.DataFrame({
            "app_id": 730, "market_hash_name": "X", "money_domain": "currency:8",
            "observation_date": _dates(n), "close_price": [15000.0] * n,  # huge "jump" vs currency:1 if blended
        }),
    ])
    result = detect_price_zscore_anomalies(df, window=30, min_periods=5, z_threshold=3.0)
    assert result.empty  # each domain is flat internally; no cross-domain "jump" should appear


def test_spread_widening_flags_a_real_blowout():
    n = 30
    spreads = [50.0 + (i % 3) for i in range(n)]  # tight, stable spread
    spreads[-1] = 800.0  # sudden blowout

    df = pd.DataFrame({
        "app_id": 730,
        "market_hash_name": "Test Item",
        "observed_at": pd.date_range("2026-01-01", periods=n, freq="h"),
        "spread_bps": spreads,
    })

    result = detect_spread_widening_anomalies(df, span=20, min_periods=10, z_threshold=3.0)
    assert len(result) >= 1
    assert result.iloc[0]["anomaly_type"] == "spread_widening"
    assert "800" in result.iloc[0]["explanation"]


def test_spread_widening_is_one_sided_narrowing_not_flagged():
    n = 30
    spreads = [500.0 + (i % 3) for i in range(n)]
    spreads[-1] = 10.0  # sudden narrowing — should NOT be flagged (widening-only)

    df = pd.DataFrame({
        "app_id": 730,
        "market_hash_name": "Test Item",
        "observed_at": pd.date_range("2026-01-01", periods=n, freq="h"),
        "spread_bps": spreads,
    })
    result = detect_spread_widening_anomalies(df, span=20, min_periods=10, z_threshold=3.0)
    assert result.empty


def test_volume_spike_requires_minimum_weekday_history():
    """With fewer than min_weekday_occurrences prior same-weekday points, nothing should
    be flagged — this is the honest "not enough history yet" behavior, not a bug."""
    df = pd.DataFrame({
        "app_id": 730, "market_hash_name": "X", "money_domain": "currency:1",
        "observation_date": _dates(2),  # only 2 days total, can't have 3 same-weekday priors
        "total_volume": [100, 100000],
    })
    result = detect_volume_spike_anomalies(df, min_weekday_occurrences=3, z_threshold=3.0)
    assert result.empty


def test_volume_spike_flags_after_sufficient_weekday_history():
    # 4 consecutive Wednesdays with stable volume, then a 5th with a huge spike.
    wednesdays = [datetime.date(2026, 1, 7) + datetime.timedelta(weeks=i) for i in range(5)]
    volumes = [100, 105, 98, 102, 5000]
    df = pd.DataFrame({
        "app_id": 730, "market_hash_name": "X", "money_domain": "currency:1",
        "observation_date": pd.to_datetime(wednesdays),
        "total_volume": volumes,
    })
    result = detect_volume_spike_anomalies(df, min_weekday_occurrences=3, z_threshold=3.0)
    assert len(result) == 1
    assert result.iloc[0]["anomaly_type"] == "volume_spike"


def test_crossed_book_flags_only_crossed_rows():
    df = pd.DataFrame({
        "app_id": [730, 730],
        "market_hash_name": ["X", "X"],
        "observed_at": pd.date_range("2026-01-01", periods=2, freq="h"),
        "highest_buy": [100, 200],
        "lowest_sell": [110, 190],  # row 0 normal, row 1 crossed (buy > sell)
        "spread_bps": [-909.0, 526.0],
    })
    result = detect_crossed_book_anomalies(df)
    assert len(result) == 1
    assert result.iloc[0]["anomaly_type"] == "crossed_book"
    assert "200" in result.iloc[0]["explanation"]


def test_crossed_book_severity_scales_with_magnitude():
    df = pd.DataFrame({
        "app_id": [730, 730],
        "market_hash_name": ["A", "B"],
        "observed_at": pd.date_range("2026-01-01", periods=2, freq="h"),
        "highest_buy": [200, 200],
        "lowest_sell": [190, 190],
        "spread_bps": [-50.0, -900.0],  # A: small cross, B: big cross
    })
    result = detect_crossed_book_anomalies(df)
    severity_a = result[result["market_hash_name"] == "A"].iloc[0]["severity"]
    severity_b = result[result["market_hash_name"] == "B"].iloc[0]["severity"]
    assert severity_b > severity_a


def test_all_detectors_return_empty_on_empty_input():
    empty = pd.DataFrame()
    assert detect_price_zscore_anomalies(empty).empty
    assert detect_spread_widening_anomalies(empty).empty
    assert detect_volume_spike_anomalies(empty).empty
    assert detect_crossed_book_anomalies(empty).empty
