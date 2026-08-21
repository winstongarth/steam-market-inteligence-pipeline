"""Tests for ingest/fx_rates.py. Mocks the HTTP call — no real network."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from ingest.fx_rates import FxRateCache


def _mock_response(date: str, rates: dict) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"amount": 1.0, "base": "USD", "date": date, "rates": rates}
    return resp


@pytest.mark.asyncio
async def test_fetch_caches_and_returns_rates(tmp_path):
    cache = FxRateCache(cache_path=tmp_path / "fx_rates.json")
    client = AsyncMock()
    client.get.return_value = _mock_response("2026-08-20", {"EUR": 0.85609, "SGD": 1.2722, "IDR": 17797})

    rates = await cache.fetch(client, "2026-08-20")

    assert rates == {"USD": 1.0, "EUR": 0.85609, "SGD": 1.2722, "IDR": 17797}
    client.get.assert_called_once()


@pytest.mark.asyncio
async def test_second_fetch_uses_cache_not_network(tmp_path):
    cache = FxRateCache(cache_path=tmp_path / "fx_rates.json")
    client = AsyncMock()
    client.get.return_value = _mock_response("2026-08-20", {"EUR": 0.85609, "SGD": 1.2722, "IDR": 17797})

    await cache.fetch(client, "2026-08-20")
    client.get.reset_mock()
    rates = await cache.fetch(client, "2026-08-20")

    assert rates == {"USD": 1.0, "EUR": 0.85609, "SGD": 1.2722, "IDR": 17797}
    client.get.assert_not_called()


@pytest.mark.asyncio
async def test_ecb_fallback_date_caches_under_both_requested_and_actual_date(tmp_path):
    """ECB has no same-day rates — requesting 2026-08-21 can transparently fall back to
    2026-08-20's rates. Both dates should be cached so a later lookup for either hits."""
    cache = FxRateCache(cache_path=tmp_path / "fx_rates.json")
    client = AsyncMock()
    client.get.return_value = _mock_response("2026-08-20", {"EUR": 0.85609, "SGD": 1.2722, "IDR": 17797})

    await cache.fetch(client, "2026-08-21")

    assert cache.get_cached("2026-08-21") == {"USD": 1.0, "EUR": 0.85609, "SGD": 1.2722, "IDR": 17797}
    assert cache.get_cached("2026-08-20") == {"USD": 1.0, "EUR": 0.85609, "SGD": 1.2722, "IDR": 17797}
