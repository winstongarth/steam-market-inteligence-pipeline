"""Phase 6 — daily FX rates for cross-currency dislocation analysis.

Real rates from Frankfurter (https://frankfurter.dev), a free, no-key, ECB-sourced FX API
— chosen over a paid provider since the whole project's ethos is primary/free data
sources (CLAUDE.md §3: "not from paid aggregators"). Cached on disk like
ingest/nameid_resolver.py's cache — fetch once, never re-fetch the same date.

Note (found while fetching, 2026-08-21): the ECB doesn't publish same-day rates —
requesting "today" transparently falls back to the latest available prior date. Our
~2-day data collection window (2026-08-20 to 2026-08-21) effectively has one real rate
set (dated 2026-08-20), not two distinct daily rates. Documented, not hidden — see
docs/DECISIONS.md.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import httpx

logger = logging.getLogger("ingest.fx_rates")

FRANKFURTER_URL = "https://api.frankfurter.dev/v1"
DEFAULT_CACHE_PATH = Path(__file__).resolve().parent.parent / "data" / "cache" / "fx_rates.json"

# Steam currency code -> ISO code, matching dbt/seeds/dim_currency.csv (Phase 0-corrected
# mapping — docs/PHASE0_FINDINGS.md §3.7).
STEAM_CURRENCY_TO_ISO = {1: "USD", 3: "EUR", 13: "SGD", 10: "IDR"}


class FxRateCache:
    def __init__(self, cache_path: Path = DEFAULT_CACHE_PATH) -> None:
        self.cache_path = cache_path
        self._cache: dict[str, dict[str, float]] = self._load()

    def _load(self) -> dict[str, dict[str, float]]:
        if self.cache_path.exists():
            loaded: dict[str, dict[str, float]] = json.loads(self.cache_path.read_text(encoding="utf-8"))
            return loaded
        return {}

    def _save(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(self._cache, indent=2), encoding="utf-8")

    def get_cached(self, date: str) -> dict[str, float] | None:
        return self._cache.get(date)

    async def fetch(self, client: httpx.AsyncClient, date: str, base: str = "USD", symbols: tuple[str, ...] = ("EUR", "SGD", "IDR")) -> dict[str, float]:
        cached = self.get_cached(date)
        if cached is not None:
            return cached

        response = await client.get(f"{FRANKFURTER_URL}/{date}", params={"base": base, "symbols": ",".join(symbols)})
        response.raise_for_status()
        data = response.json()
        actual_date = data["date"]  # may differ from requested `date` — ECB has no same-day rates
        rates = {base: 1.0, **data["rates"]}

        self._cache[date] = rates
        if actual_date != date:
            logger.info("requested FX rates for %s, ECB's latest available was %s — cached under both dates", date, actual_date)
            self._cache[actual_date] = rates
        self._save()
        return rates
