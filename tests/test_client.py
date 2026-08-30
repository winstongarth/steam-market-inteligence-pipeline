"""SteamMarketClient tests. Mocks the underlying httpx call — no network."""

from unittest.mock import AsyncMock

import httpx
import pytest

from ingest.client import SteamMarketClient
from ingest.ratelimit import ManualRestartRequiredError


def _html_response(status: int = 200) -> httpx.Response:
    return httpx.Response(
        status_code=status,
        content=b"<html><body>a real HTML page, not a ban</body></html>",
        headers={"content-type": "text/html; charset=utf-8"},
        request=httpx.Request("GET", "https://steamcommunity.com/market/listings/730/Foo"),
    )


@pytest.mark.asyncio
async def test_expect_json_false_does_not_trip_ban_detection_on_html():
    """Regression test: the listings/bucket page is legitimately HTML. Bug found
    2026-08-20 — the ban-shape check used to fire on ANY non-JSON body regardless
    of what the caller actually expected, halting the circuit breaker for 6h on a normal
    200 OK page fetch."""
    client = SteamMarketClient()
    client._client.get = AsyncMock(return_value=_html_response())  # type: ignore[method-assign]

    status, body = await client.get_json("https://steamcommunity.com/market/listings/730/Foo", expect_json=False)

    assert status == 200
    assert "real HTML page" in body
    await client.close()


@pytest.mark.asyncio
async def test_expect_json_true_still_trips_ban_detection_on_html():
    client = SteamMarketClient()
    client._client.get = AsyncMock(return_value=_html_response())  # type: ignore[method-assign]

    with pytest.raises(ManualRestartRequiredError):
        await client.get_json("https://steamcommunity.com/market/priceoverview/", expect_json=True)

    await client.close()
