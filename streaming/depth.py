"""Spread and order-book depth calculations.

Pure functions, no Spark dependency — testable directly, wrapped as UDFs in cdc_job.py
where needed. Depth-at-X%-from-mid only applies to orderbook-sourced records, since only
that endpoint carries the full compact order arrays (rgCompactBuyOrders/rgCompactSellOrders).
"""

from __future__ import annotations


def spread(lowest_sell: int | None, highest_buy: int | None) -> int | None:
    if lowest_sell is None or highest_buy is None:
        return None
    return lowest_sell - highest_buy


def spread_bps(lowest_sell: int | None, highest_buy: int | None) -> float | None:
    """Spread in basis points of the mid price. None if either side is missing or mid is 0."""
    if lowest_sell is None or highest_buy is None:
        return None
    mid = (lowest_sell + highest_buy) / 2
    if mid == 0:
        return None
    return (lowest_sell - highest_buy) / mid * 10_000


def _pairs(compact_orders: list[int] | None) -> list[tuple[int, int]]:
    """rgCompactBuyOrders/rgCompactSellOrders are flat [price, qty_at_price, price,
    qty_at_price, ...] arrays — pair them up.

    Verified against real data (tests/fixtures/market_orderbook.json), not assumed: the
    qty values are NOT monotonic as price moves away from the touch (e.g. 24, 278, 1196,
    184, 247, ...) — this is per-price-level resting quantity, not a running cumulative
    total. That's notably different from the OLD spec's documented `buy_order_graph`/
    `sell_order_graph` shape, which described cumulative_quantity explicitly. Worth
    knowing if this module is ever compared against that old (now-dead) format's semantics.
    """
    if not compact_orders:
        return []
    return list(zip(compact_orders[0::2], compact_orders[1::2]))


def depth_within_pct(compact_orders: list[int] | None, mid: float | None, pct: float, side: str) -> int | None:
    """Total quantity resting within `pct` (e.g. 0.01 for 1%) of `mid` — a sum across all
    price levels in the band, since each level's qty is standalone, not cumulative (see
    `_pairs`).

    side="buy": prices at or above mid*(1-pct) (buy orders step down from the touch).
    side="sell": prices at or below mid*(1+pct) (sell orders step up from the touch).
    """
    if not compact_orders or mid is None or mid <= 0:
        return None
    pairs = _pairs(compact_orders)
    if not pairs:
        return None

    if side == "buy":
        floor = mid * (1 - pct)
        in_band = [qty for price, qty in pairs if price >= floor]
    elif side == "sell":
        ceiling = mid * (1 + pct)
        in_band = [qty for price, qty in pairs if price <= ceiling]
    else:
        raise ValueError(f"side must be 'buy' or 'sell', got {side!r}")

    return sum(in_band)
