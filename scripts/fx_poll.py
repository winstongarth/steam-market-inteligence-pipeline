"""Cross-currency dislocation: poll a 50-item watchlist across USD/EUR/SGD/IDR.

Throwaway script — reuses the existing ingest pipeline (SteamMarketClient ->
priceoverview -> Kafka market.raw.v1) rather than building a parallel path, so this data
flows through Bronze/Silver/Gold exactly like everything else.

Currency codes are the measured, corrected ones (docs/FINDINGS.md), NOT the commonly
assumed wrong mapping (20=SGD, 23=IDR): USD=1, EUR=3, SGD=13, IDR=10.

Run: PYTHONPATH=. uv run python scripts/fx_poll.py
(50 items x 4 currencies = 200 requests, ~400s at the measured 0.5 req/s rate)

RESUME_FROM_INDEX env var (default 0) skips already-polled items — used on 2026-08-21 to
resume after the circuit breaker tripped at item 5 (see docs/DECISIONS.md, "Ingestion &
rate limiting").
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

from ingest.client import SteamMarketClient
from ingest.endpoints import priceoverview
from ingest.kafka_producer import EnvelopePublisher

logger = logging.getLogger("scripts.fx_poll")

APP_ID = 730
CURRENCIES = {1: "USD", 3: "EUR", 13: "SGD", 10: "IDR"}
WATCHLIST_PATH = Path(__file__).parent / "watchlist.json"
RESUME_FROM_INDEX = int(os.environ.get("RESUME_FROM_INDEX", "0"))


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    watchlist = json.loads(WATCHLIST_PATH.read_text(encoding="utf-8"))
    remaining = watchlist[RESUME_FROM_INDEX:]
    logger.info(
        "resuming from index %d: %d items x %d currencies = %d requests",
        RESUME_FROM_INDEX, len(remaining), len(CURRENCIES), len(remaining) * len(CURRENCIES),
    )

    published = 0
    async with SteamMarketClient() as client, EnvelopePublisher() as publisher:
        for i, item in enumerate(remaining):
            for currency_code, currency_name in CURRENCIES.items():
                envelope = await priceoverview.fetch(client, app_id=APP_ID, market_hash_name=item, currency=currency_code)
                await publisher.publish(envelope, market_hash_name=item)
                published += 1
                payload = envelope.raw_payload if isinstance(envelope.raw_payload, dict) else {}
                logger.info(
                    "[%d/%d] %s (%s): lowest=%s median=%s",
                    published, len(remaining) * len(CURRENCIES), item, currency_name,
                    payload.get("lowest_price"), payload.get("median_price"),
                )

    logger.info("done, published %d envelopes", published)


if __name__ == "__main__":
    asyncio.run(main())
