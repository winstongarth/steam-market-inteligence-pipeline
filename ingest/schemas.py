"""Pydantic models for raw Steam Market payloads.

Every field from Steam is optional until proven otherwise (a rule this project applies throughout) — models here are
deliberately permissive. Money stays as the locale-formatted string Steam returns; parsing
to integer minor units happens exactly once, downstream in the Silver normalization pass
(Phase 2), not here.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class RawEnvelope(BaseModel):
    """The unit published to Kafka `market.raw.v1`. Wraps every poll response, verbatim."""

    source: str = "steamcommunity"
    endpoint: str
    app_id: int
    currency: int | None = None
    observed_at: datetime = Field(default_factory=_utcnow)
    request_params: dict[str, Any]
    raw_payload: Any
    ingest_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    def kafka_key(self, market_hash_name: str | None) -> str:
        return f"{self.app_id}:{market_hash_name or ''}"


# --- Confirmed shapes (Phase 0, tests/fixtures/) --------------------------------------


class AssetDescription(BaseModel):
    appid: int | None = None
    classid: str | None = None
    background_color: str | None = None
    icon_url: str | None = None
    tradable: int | None = None
    name: str | None = None
    name_color: str | None = None
    type: str | None = None
    market_name: str | None = None
    market_hash_name: str | None = None
    commodity: int | None = None
    market_bucket_group_name: str | None = None
    market_bucket_group_id: str | None = None

    model_config = {"extra": "allow"}


class SearchRenderResult(BaseModel):
    name: str | None = None
    hash_name: str | None = None
    sell_listings: int | None = None
    sell_price: int | None = None  # integer cents
    sell_price_text: str | None = None
    app_icon: str | None = None
    app_name: str | None = None
    asset_description: AssetDescription | None = None
    sale_price_text: str | None = None

    model_config = {"extra": "allow"}


class SearchRenderResponse(BaseModel):
    success: bool | None = None
    start: int | None = None
    pagesize: int | None = None
    total_count: int | None = None
    results: list[SearchRenderResult] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class PriceOverviewResponse(BaseModel):
    success: bool | None = None
    lowest_price: str | None = None  # locale-formatted, e.g. "$39.25" or "33,72€"
    median_price: str | None = None
    volume: str | None = None  # display string, may contain thousands separators

    model_config = {"extra": "allow"}


# --- Order book (Phase 1 — see ingest/endpoints/orderbook.py, docs/DECISIONS.md) -------
#
# This replaces the spec's original itemordershistogram shape entirely — different
# endpoint, different fields, different (better: pre-parsed integer minor units) money
# representation. Verified against a real response, tests/fixtures/market_orderbook.json.


class OrderBookData(BaseModel):
    amtMaxBuyOrder: int | None = None  # integer minor units
    amtMinSellOrder: int | None = None  # integer minor units
    eCurrency: int | None = None  # NOT request-controllable — see orderbook.py docstring
    cBuyOrders: int | None = None
    cSellOrders: int | None = None
    rgCompactBuyOrders: list[int] = Field(default_factory=list)  # flat [price, cum_qty, ...] pairs
    rgCompactSellOrders: list[int] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class OrderBookInner(BaseModel):
    success: bool | None = None
    data: OrderBookData | None = None

    model_config = {"extra": "allow"}


class OrderBookResponse(BaseModel):
    data: OrderBookInner | None = None

    model_config = {"extra": "allow"}


# --- Unverified / blocked (see docs/DECISIONS.md) ---------------------------------------
#
# itemordersactivity: optional per the spec ("use only if it yields genuine event-level
# data"). No modern replacement found during the orderbook investigation; not wired into
# the scheduler. Left here in case a future session finds one.


class ItemOrdersActivityResponse(BaseModel):
    """UNVERIFIED — shape unknown; kept fully open."""

    model_config = {"extra": "allow"}
