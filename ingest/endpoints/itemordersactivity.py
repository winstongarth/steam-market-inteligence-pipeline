"""§3.5 — /market/itemordersactivity/, optional recent-order-events endpoint.

UNVERIFIED. §3.2's depth endpoint was unblocked in Phase 1 via a different mechanism
entirely (see ingest/endpoints/orderbook.py, docs/DECISIONS.md) — this endpoint wasn't
part of that investigation and no modern replacement for it has been found. Not wired into
the scheduler; the classic `item_nameid` param below is untested against the current site
and likely doesn't work, per the same regression that broke itemordershistogram.
"""

from __future__ import annotations

from ingest.client import SteamMarketClient
from ingest.schemas import RawEnvelope

URL = "https://steamcommunity.com/market/itemordersactivity/"


async def fetch(
    client: SteamMarketClient,
    app_id: int,
    item_nameid: str,
    country: str = "US",
    language: str = "english",
    currency: int = 1,
) -> RawEnvelope:
    params = {
        "country": country,
        "language": language,
        "currency": currency,
        "item_nameid": item_nameid,
    }
    status, body = await client.get_json(URL, params=params)
    return RawEnvelope(
        endpoint="itemordersactivity",
        app_id=app_id,
        currency=currency,
        request_params=params,
        raw_payload=body,
    )
