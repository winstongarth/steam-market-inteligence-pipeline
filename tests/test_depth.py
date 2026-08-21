"""Spread/depth calculation tests. Pure functions — no Spark needed."""

import json
from pathlib import Path

from streaming.depth import depth_within_pct, spread, spread_bps

FIXTURES = Path(__file__).parent / "fixtures"


def test_spread_basic():
    assert spread(lowest_sell=110, highest_buy=100) == 10


def test_spread_none_when_missing_side():
    assert spread(None, 100) is None
    assert spread(110, None) is None


def test_spread_bps():
    # mid = 105, spread = 10 -> 10/105 * 10000 ≈ 952.38
    result = spread_bps(lowest_sell=110, highest_buy=100)
    assert result is not None
    assert round(result, 2) == 952.38


def test_spread_bps_zero_mid_is_none():
    assert spread_bps(0, 0) is None


def test_depth_within_pct_buy_side_sums_levels_in_band():
    # price levels at 100 (qty 5) and 95 (qty 3), mid=100, 10% band -> floor=90, both included
    compact = [100, 5, 95, 3]
    assert depth_within_pct(compact, mid=100, pct=0.10, side="buy") == 8


def test_depth_within_pct_excludes_levels_outside_band():
    # floor at 1% of mid=100 is 99 -> only the 100 level qualifies, 95 excluded
    compact = [100, 5, 95, 3]
    assert depth_within_pct(compact, mid=100, pct=0.01, side="buy") == 5


def test_depth_within_pct_sell_side():
    compact = [100, 5, 105, 3]
    assert depth_within_pct(compact, mid=100, pct=0.10, side="sell") == 8
    assert depth_within_pct(compact, mid=100, pct=0.01, side="sell") == 5


def test_depth_within_pct_empty_or_missing():
    assert depth_within_pct(None, 100, 0.10, "buy") is None
    assert depth_within_pct([], 100, 0.10, "buy") is None
    assert depth_within_pct([100, 5], None, 0.10, "buy") is None
    assert depth_within_pct([100, 5], 0, 0.10, "buy") is None


def test_depth_against_real_orderbook_fixture():
    """Cross-check against the real recorded orderbook response — not just synthetic pairs.
    Quantities were verified non-monotonic (see streaming/depth.py's _pairs docstring), so
    this also guards against reintroducing a wrong cumulative-max assumption."""
    data = json.loads((FIXTURES / "market_orderbook.json").read_text(encoding="utf-8"))
    inner = data["data"]["data"]
    buys = inner["rgCompactBuyOrders"]
    sells = inner["rgCompactSellOrders"]
    mid = (inner["amtMaxBuyOrder"] + inner["amtMinSellOrder"]) / 2

    depth_1pct = depth_within_pct(buys, mid, 0.01, "buy")
    depth_10pct = depth_within_pct(buys, mid, 0.10, "buy")
    assert depth_1pct is not None and depth_10pct is not None
    # a wider band can never hold less resting quantity than a narrower one
    assert depth_10pct >= depth_1pct

    sell_depth_1pct = depth_within_pct(sells, mid, 0.01, "sell")
    assert sell_depth_1pct is not None and sell_depth_1pct >= 0
