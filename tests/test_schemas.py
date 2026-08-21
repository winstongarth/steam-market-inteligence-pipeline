"""Validates Pydantic models against real recorded fixtures (tests/fixtures/). Never hits Steam."""

import json
from pathlib import Path

from ingest.schemas import OrderBookResponse, PriceOverviewResponse, SearchRenderResponse

FIXTURES = Path(__file__).parent / "fixtures"


def test_search_render_matches_recorded_fixture():
    data = json.loads((FIXTURES / "search_render.json").read_text(encoding="utf-8"))
    parsed = SearchRenderResponse.model_validate(data)
    assert parsed.success is True
    assert parsed.pagesize == 10
    assert parsed.total_count == 35349
    assert len(parsed.results) == 10
    first = parsed.results[0]
    assert first.hash_name == "Dreams & Nightmares Case"
    assert first.asset_description is not None
    assert first.asset_description.market_bucket_group_id == "G18D2253004"


def test_priceoverview_matches_recorded_fixture():
    data = json.loads((FIXTURES / "priceoverview.json").read_text(encoding="utf-8"))
    parsed = PriceOverviewResponse.model_validate(data)
    assert parsed.success is True
    assert parsed.lowest_price == "$39.25"
    assert parsed.median_price == "$52.52"
    assert parsed.volume == "105"


def test_orderbook_matches_recorded_fixture():
    data = json.loads((FIXTURES / "market_orderbook.json").read_text(encoding="utf-8"))
    parsed = OrderBookResponse.model_validate(data)
    assert parsed.data is not None
    assert parsed.data.success is True
    assert parsed.data.data is not None
    assert parsed.data.data.amtMaxBuyOrder is not None
    assert parsed.data.data.amtMinSellOrder is not None
    assert parsed.data.data.amtMaxBuyOrder < parsed.data.data.amtMinSellOrder
    assert len(parsed.data.data.rgCompactBuyOrders) > 0
    assert len(parsed.data.data.rgCompactSellOrders) > 0
