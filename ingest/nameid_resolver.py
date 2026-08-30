"""Resolving the identifier needed for order-book depth, with a permanent on-disk cache.

STATUS (resolved 2026-08-20): the originally documented mechanism (regex
`Market_LoadOrderSpread` out of the listings page) is dead — Valve redirects that page to a
"bucket" SPA (see docs/DECISIONS.md, "item_nameid resolution method is broken"). The SPA's
own network calls were traced statically (downloading its code-split JS chunks and reading
the minified source) to a *different* mechanism entirely:

    GET /market/orderbook?q=Load&qp=[app_id, market_bucket_id]
    header: x-valve-request-type: queryAction

`market_bucket_id` — not `item_nameid` — is the identifier this needs. Two ways to get it,
both implemented below:

1. **Commodity fast path, zero extra requests.** For fungible items (`commodity: 1` in
   `asset_description`, e.g. cases, stickers, sealed containers), `market_bucket_id` equals
   `market_bucket_group_id` with its leading `G` stripped. `market_bucket_group_id` is
   already present in every `search/render` result — Tier B/C get this for free while
   sweeping the catalog, no listings-page fetch needed at all. See
   `seed_from_search_render_result`.

2. **Wear-variant items, one request per item family.** Skins with multiple exteriors
   (`commodity: 0`) need a per-exterior suffix that isn't derivable from the group id alone.
   Fetching `/market/listings/{appid}/{hash_name}` (which redirects into the bucket SPA page)
   and regex-extracting `market_hash_name` → `market_bucket_id` pairs from the embedded
   listing data resolves **every exterior of that item family in one request** — cheaper than
   the originally assumed "1 request per exact hash_name". See `_resolve_uncached`.

Caveat: the extraction regex parses an undocumented, escaped-JSON-in-HTML blob that Valve
could change without notice. It's verified against two real fixtures (one commodity + one
wear-variant item) but is best-effort, not a stable contract — if Valve changes the
bucket page's internal structure, this breaks again and needs re-tracing.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

from ingest.client import SteamMarketClient

logger = logging.getLogger("ingest.nameid_resolver")

DEFAULT_CACHE_PATH = Path(__file__).resolve().parent.parent / "data" / "cache" / "market_bucket_ids.json"

_BS = chr(92)
_ONE_OR_MORE_BACKSLASHES = _BS + _BS + "+"  # matches Valve's escaped-JSON-in-HTML quoting
_BACKSLASH_IN_CLASS = _BS + _BS

# Extracts every {market_hash_name: market_bucket_id} pair embedded in a bucket page's
# listing data, tolerant of the page's inconsistent backslash-escaping depth.
_HASH_NAME_TO_BUCKET_ID_RE = re.compile(
    re.escape("market_hash_name") + _ONE_OR_MORE_BACKSLASHES + '":' + _ONE_OR_MORE_BACKSLASHES + '"'
    + f'([^"{_BACKSLASH_IN_CLASS}]+)' + _ONE_OR_MORE_BACKSLASHES + '"'
    + ".{0,600}?"
    + re.escape("market_bucket_id") + _ONE_OR_MORE_BACKSLASHES + '":' + _ONE_OR_MORE_BACKSLASHES + '"'
    + "([0-9A-Fa-f]+)" + _ONE_OR_MORE_BACKSLASHES + '"',
    re.DOTALL,
)


class NameIdResolutionError(RuntimeError):
    """Raised when market_bucket_id cannot be resolved by any currently-working method."""


class NameIdResolver:
    def __init__(self, cache_path: Path = DEFAULT_CACHE_PATH) -> None:
        self.cache_path = cache_path
        self._cache = self._load_cache()

    def _load_cache(self) -> dict[str, str]:
        if self.cache_path.exists():
            loaded: dict[str, str] = json.loads(self.cache_path.read_text(encoding="utf-8"))
            return loaded
        return {}

    def _save_cache(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(self._cache, indent=2), encoding="utf-8")

    @staticmethod
    def _cache_key(app_id: int, market_hash_name: str) -> str:
        return f"{app_id}:{market_hash_name}"

    def get_cached(self, app_id: int, market_hash_name: str) -> str | None:
        return self._cache.get(self._cache_key(app_id, market_hash_name))

    def seed(self, app_id: int, market_hash_name: str, market_bucket_id: str) -> None:
        """Manually seed a known market_bucket_id (e.g. found via an out-of-band method)."""
        self._cache[self._cache_key(app_id, market_hash_name)] = market_bucket_id
        self._save_cache()

    def seed_from_search_render_result(self, app_id: int, result: dict[str, Any]) -> str | None:
        """Fast path (see module docstring, method 1): free resolution for commodity items
        found while sweeping search/render (Tier B/C). Returns the bucket id if applicable."""
        asset = result.get("asset_description") or {}
        if asset.get("commodity") != 1:
            return None
        group_id: str | None = asset.get("market_bucket_group_id")
        hash_name: str | None = result.get("hash_name")
        if not group_id or not hash_name:
            return None
        bucket_id: str = group_id[1:] if group_id.startswith("G") else group_id
        self.seed(app_id, hash_name, bucket_id)
        return bucket_id

    async def resolve(self, client: SteamMarketClient, app_id: int, market_hash_name: str) -> str:
        cached = self.get_cached(app_id, market_hash_name)
        if cached is not None:
            return cached

        await self._resolve_uncached(client, app_id, market_hash_name)

        resolved = self.get_cached(app_id, market_hash_name)
        if resolved is None:
            raise NameIdResolutionError(
                f"Could not resolve market_bucket_id for {app_id}:{market_hash_name!r}. "
                "The bucket page was fetched successfully but no market_hash_name -> "
                "market_bucket_id pair matching this item was found in its listing data "
                "(the item may have zero active listings right now, or Valve changed the "
                "page's internal structure again). See docs/DECISIONS.md."
            )
        return resolved

    async def _resolve_uncached(self, client: SteamMarketClient, app_id: int, market_hash_name: str) -> None:
        """Fetches the bucket page (method 2, module docstring) and caches EVERY
        market_hash_name -> market_bucket_id pair found, not just the one requested — a
        single request resolves an entire item family (all wear exteriors at once)."""
        url = f"https://steamcommunity.com/market/listings/{app_id}/{quote(market_hash_name)}"
        status, body = await client.get_json(url, expect_json=False)
        text = body if isinstance(body, str) else ""

        found = 0
        for match in _HASH_NAME_TO_BUCKET_ID_RE.finditer(text):
            hash_name, bucket_id = match.groups()
            if self.get_cached(app_id, hash_name) is None:
                self._cache[self._cache_key(app_id, hash_name)] = bucket_id
                found += 1
        if found:
            self._save_cache()
        logger.info(
            "resolved %d market_hash_name -> market_bucket_id pair(s) from one bucket-page fetch (app_id=%d, requested=%r)",
            found, app_id, market_hash_name,
        )
