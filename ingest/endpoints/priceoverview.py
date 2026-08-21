"""§3.4 — /market/priceoverview/, one item per request. Used sparingly, mainly as a
cross-check against search/render (§7.3 reconciliation)."""

from __future__ import annotations

from ingest.client import SteamMarketClient
from ingest.schemas import RawEnvelope

URL = "https://steamcommunity.com/market/priceoverview/"


async def fetch(
    client: SteamMarketClient,
    app_id: int,
    market_hash_name: str,
    currency: int = 1,
) -> RawEnvelope:
    params = {"appid": app_id, "currency": currency, "market_hash_name": market_hash_name}
    status, body = await client.get_json(URL, params=params)
    return RawEnvelope(
        endpoint="priceoverview",
        app_id=app_id,
        currency=currency,
        request_params=params,
        raw_payload=body,
    )
