"""Tests for streaming/cdc_job.py's normalization functions, using real recorded fixtures
as the raw_payload content — batch-mode Spark only (see conftest.py). Streaming itself
(checkpointing, applyInPandasWithState) is validated inside the Docker container, not here.
"""

import datetime
import json
from pathlib import Path

import pytest

from streaming.cdc_job import normalize, normalize_orderbook, normalize_priceoverview, normalize_search_render

FIXTURES = Path(__file__).parent / "fixtures"

ENVELOPE_COLUMNS = ["kafka_key", "endpoint", "app_id", "request_currency", "observed_at", "raw_payload_json"]


def _envelope_rows():
    now = datetime.datetime(2026, 8, 20, 12, 0, 0)
    return [
        (
            "730:",
            "search_render",
            730,
            None,
            now,
            (FIXTURES / "search_render.json").read_text(encoding="utf-8"),
        ),
        (
            "730:AK-47 | Redline (Field-Tested)",
            "priceoverview",
            730,
            1,
            now,
            (FIXTURES / "priceoverview.json").read_text(encoding="utf-8"),
        ),
        (
            "730:AK-47 | Redline (Field-Tested)",
            "orderbook",
            730,
            None,
            now,
            (FIXTURES / "market_orderbook.json").read_text(encoding="utf-8"),
        ),
    ]


@pytest.fixture
def envelope_df(spark):
    return spark.createDataFrame(_envelope_rows(), ENVELOPE_COLUMNS)


def test_normalize_search_render_explodes_results(envelope_df):
    rows = normalize_search_render(envelope_df).collect()
    # search_render.json has 10 results (Phase 0 measured page-size cap)
    assert len(rows) == 10
    first = next(r for r in rows if r.market_hash_name == "Dreams & Nightmares Case")
    assert first.lowest_sell == 167  # sell_price in the fixture, already integer cents
    assert first.highest_buy is None
    assert first.source_endpoint == "search_render"


def test_normalize_priceoverview_parses_money(envelope_df):
    rows = normalize_priceoverview(envelope_df).collect()
    assert len(rows) == 1
    row = rows[0]
    assert row.market_hash_name == "AK-47 | Redline (Field-Tested)"
    assert row.lowest_sell == 3925  # "$39.25" at currency=1 (USD)
    assert row.volume == 105
    assert row.currency == 1
    assert row.highest_buy is None


def test_normalize_orderbook_reads_integer_minor_units_directly(envelope_df):
    rows = normalize_orderbook(envelope_df).collect()
    assert len(rows) == 1
    row = rows[0]
    assert row.market_hash_name == "AK-47 | Redline (Field-Tested)"
    assert row.lowest_sell == 26100  # amtMinSellOrder from the fixture
    assert row.highest_buy == 26000  # amtMaxBuyOrder from the fixture
    assert row.sell_listings is not None
    assert row.currency == 8  # eCurrency observed as JPY in this fixture


def test_normalize_unions_all_three_endpoints(envelope_df):
    rows = normalize(envelope_df).collect()
    endpoints = {r.source_endpoint for r in rows}
    assert endpoints == {"search_render", "priceoverview", "orderbook"}
    # 10 exploded search_render rows + 1 priceoverview + 1 orderbook
    assert len(rows) == 12
