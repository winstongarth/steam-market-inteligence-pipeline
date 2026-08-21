"""Phase 1 smoke test — throwaway script, not part of the pipeline.

Proves the wiring end-to-end with a small number of real (rate-limited) requests:
fetch search_render + priceoverview once each -> publish to Kafka (Redpanda) ->
bronze_writer consumes and flushes real Parquet to MinIO. This is NOT the 24h soak test
required for Phase 1 exit — it's a correctness check that every hop actually works before
handing the scheduler an unattended multi-day run.

Run: uv run python scripts/phase1_smoke_test.py
(requires `docker compose up -d` first)
"""

from __future__ import annotations

import asyncio
import logging

from ingest.client import SteamMarketClient
from ingest.endpoints import orderbook, priceoverview, search_render
from ingest.kafka_producer import EnvelopePublisher
from ingest.nameid_resolver import NameIdResolver
from streaming.bronze_writer import BronzeWriter

WATCHLIST_ITEM = "AK-47 | Redline (Field-Tested)"


async def produce_a_few_envelopes() -> None:
    async with SteamMarketClient() as client, EnvelopePublisher() as publisher:
        env1 = await search_render.fetch(client, app_id=730, start=0, count=10)
        await publisher.publish(env1, market_hash_name=None)
        print(f"published search_render envelope, ingest_id={env1.ingest_id}, "
              f"payload success={env1.raw_payload.get('success') if isinstance(env1.raw_payload, dict) else None}")

        env2 = await priceoverview.fetch(client, app_id=730, market_hash_name=WATCHLIST_ITEM)
        await publisher.publish(env2, market_hash_name=WATCHLIST_ITEM)
        print(f"published priceoverview envelope, ingest_id={env2.ingest_id}, "
              f"payload={env2.raw_payload}")

        resolver = NameIdResolver()
        bucket_id = await resolver.resolve(client, 730, WATCHLIST_ITEM)
        env3 = await orderbook.fetch(client, app_id=730, market_bucket_id=bucket_id)
        await publisher.publish(env3, market_hash_name=WATCHLIST_ITEM)
        inner = env3.raw_payload.get("data", {}).get("data", {}) if isinstance(env3.raw_payload, dict) else {}
        print(f"published orderbook envelope (Tier A path), ingest_id={env3.ingest_id}, "
              f"bucket_id={bucket_id}, amtMaxBuyOrder={inner.get('amtMaxBuyOrder')}, "
              f"amtMinSellOrder={inner.get('amtMinSellOrder')}")


async def consume_and_flush(timeout_seconds: float = 15.0) -> BronzeWriter:
    writer = BronzeWriter()
    await writer.start()
    try:
        await asyncio.wait_for(writer.run_forever(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        pass
    finally:
        await writer.stop()
    return writer


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    await produce_a_few_envelopes()
    writer = await consume_and_flush()
    print(f"\nbronze_writer: {writer.records_written} records written across {writer.files_written} file(s)")


if __name__ == "__main__":
    asyncio.run(main())
