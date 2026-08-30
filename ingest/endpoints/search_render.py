"""/market/search/render/, the breadth (catalog sweep) endpoint.

Measured correction: pagesize is capped at 10/request regardless of the requested `count`
(see docs/FINDINGS.md) — this is verified fact, not the originally assumed ~100.
"""

from __future__ import annotations

from ingest.client import SteamMarketClient
from ingest.schemas import RawEnvelope

URL = "https://steamcommunity.com/market/search/render/"


async def fetch(
    client: SteamMarketClient,
    app_id: int,
    start: int = 0,
    count: int = 10,
    query: str = "",
    sort_column: str = "popular",
    sort_dir: str = "desc",
) -> RawEnvelope:
    params = {
        "query": query,
        "start": start,
        "count": count,
        "search_descriptions": 0,
        "sort_column": sort_column,
        "sort_dir": sort_dir,
        "appid": app_id,
        "norender": 1,
    }
    status, body = await client.get_json(URL, params=params)
    return RawEnvelope(
        endpoint="search_render",
        app_id=app_id,
        request_params=params,
        raw_payload=body,
    )
