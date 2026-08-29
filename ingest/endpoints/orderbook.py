"""Order-book depth — the real, working replacement for
`/market/itemordershistogram/`. See ingest/nameid_resolver.py for how this session
discovered it (traced from the live SPA's own JS bundles) and docs/DECISIONS.md for the
full writeup.

    GET /market/orderbook?q=Load&qp=[app_id, market_bucket_id]
    header: x-valve-request-type: queryAction

Response money fields (`amtMaxBuyOrder`, `amtMinSellOrder`) are already integer minor
units — no locale-string parsing needed, unlike every other endpoint in this project.
`eCurrency` in the response is NOT controllable by any request parameter we've found; it
appears to be inferred server-side (session/geo). Flagged as an open follow-up in
docs/DECISIONS.md — do not assume a fixed currency without checking this field.
"""

from __future__ import annotations

import json

from ingest.client import SteamMarketClient
from ingest.schemas import RawEnvelope

URL = "https://steamcommunity.com/market/orderbook"
HEADERS = {"x-valve-request-type": "queryAction"}


async def fetch(client: SteamMarketClient, app_id: int, market_bucket_id: str) -> RawEnvelope:
    params = {"q": "Load", "qp": json.dumps([app_id, market_bucket_id])}
    status, body = await client.get_json(URL, params=params, headers=HEADERS)
    return RawEnvelope(
        endpoint="orderbook",
        app_id=app_id,
        request_params={"market_bucket_id": market_bucket_id},
        raw_payload=body,
    )
