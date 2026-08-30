"""Tiered scheduler (7.1.5): Tier A (watchlist, 5min), Tier B (~top-5000, 60min),
Tier C (full sweep, daily). All requests funnel through one SteamMarketClient — one token
bucket, one circuit breaker — via a single priority-queue dispatcher, so Tier A jobs queued
are always pulled ahead of already-queued Tier B/C jobs. That's what gives Tier A priority
over the shared global budget.

Tier A polls the order-book depth endpoint (ingest/endpoints/orderbook.py) using
market_bucket_id, resolved via ingest/nameid_resolver.py — either free (commodity items,
seeded straight from Tier B/C's search/render results) or via one bucket-page fetch per
non-commodity item family. Tier B/C also opportunistically seed the resolver's cache with
every commodity item they see, at zero extra request cost.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from ingest import config
from ingest.client import SteamMarketClient
from ingest.endpoints import orderbook, search_render
from ingest.kafka_producer import EnvelopePublisher
from ingest.nameid_resolver import NameIdResolutionError, NameIdResolver

logger = logging.getLogger("ingest.scheduler")

TIER_A_PRIORITY = 0
TIER_B_PRIORITY = 1
TIER_C_PRIORITY = 2
TIER_B_CATALOG_SIZE = 5000
SEARCH_RENDER_PAGE_SIZE = 10  # Measured cap — see docs/FINDINGS.md

Job = Callable[[], Awaitable[None]]


@dataclass(order=True)
class _QueueItem:
    priority: int
    seq: int
    job: Job = field(compare=False)


class TieredScheduler:
    def __init__(
        self,
        client: SteamMarketClient,
        publisher: EnvelopePublisher,
        resolver: NameIdResolver,
        watchlist: list[tuple[int, str]],
        catalog_app_ids: list[int] | None = None,
    ) -> None:
        self.client = client
        self.publisher = publisher
        self.resolver = resolver
        self.watchlist = watchlist
        self.catalog_app_ids = catalog_app_ids or list(config.APP_IDS.keys())
        self._queue: asyncio.PriorityQueue[_QueueItem] = asyncio.PriorityQueue()
        self._seq = itertools.count()
        self._nameid_resolution_failures_logged: set[tuple[int, str]] = set()

    def _enqueue(self, priority: int, job: Job) -> None:
        self._queue.put_nowait(_QueueItem(priority=priority, seq=next(self._seq), job=job))

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self._dispatch_loop(), name="dispatcher"),
            asyncio.create_task(self._tier_a_loop(), name="tier_a"),
            asyncio.create_task(self._tier_b_loop(), name="tier_b"),
            asyncio.create_task(self._tier_c_loop(), name="tier_c"),
        ]
        await asyncio.gather(*tasks)

    async def _dispatch_loop(self) -> None:
        while True:
            item = await self._queue.get()
            try:
                await item.job()
            except Exception:
                logger.exception("scheduled job raised — continuing (queue size=%d)", self._queue.qsize())
            self._queue.task_done()

    # --- Tier A: watchlist depth, every 5 min --------------------------------------

    async def _tier_a_loop(self) -> None:
        while True:
            logger.info("Tier A: enqueueing %d watchlist items", len(self.watchlist))
            for app_id, hash_name in self.watchlist:
                self._enqueue(TIER_A_PRIORITY, self._make_tier_a_job(app_id, hash_name))
            await asyncio.sleep(config.TIER_A_INTERVAL_SECONDS)

    def _make_tier_a_job(self, app_id: int, hash_name: str) -> Job:
        async def job() -> None:
            try:
                market_bucket_id = await self.resolver.resolve(self.client, app_id, hash_name)
            except NameIdResolutionError as exc:
                key = (app_id, hash_name)
                if key not in self._nameid_resolution_failures_logged:
                    logger.warning("Tier A: could not resolve %s: %s", key, exc)
                    self._nameid_resolution_failures_logged.add(key)
                return
            envelope = await orderbook.fetch(self.client, app_id, market_bucket_id)
            await self.publisher.publish(envelope, market_hash_name=hash_name)

        return job

    # --- Tier B: top-~5000 catalog by popularity, every 60 min ---------------------

    async def _tier_b_loop(self) -> None:
        while True:
            logger.info("Tier B: enqueueing top-%d sweep for %d app(s)", TIER_B_CATALOG_SIZE, len(self.catalog_app_ids))
            for app_id in self.catalog_app_ids:
                for start in range(0, TIER_B_CATALOG_SIZE, SEARCH_RENDER_PAGE_SIZE):
                    self._enqueue(TIER_B_PRIORITY, self._make_search_render_job(app_id, start))
            await asyncio.sleep(config.TIER_B_INTERVAL_SECONDS)

    # --- Tier C: full catalog sweep, daily ------------------------------------------

    async def _tier_c_loop(self) -> None:
        while True:
            for app_id in self.catalog_app_ids:
                total = await self._catalog_total_count(app_id)
                logger.info("Tier C: enqueueing full sweep for app_id=%d (%d items)", app_id, total)
                for start in range(0, total, SEARCH_RENDER_PAGE_SIZE):
                    self._enqueue(TIER_C_PRIORITY, self._make_search_render_job(app_id, start))
            await asyncio.sleep(config.TIER_C_INTERVAL_SECONDS)

    def _make_search_render_job(self, app_id: int, start: int) -> Job:
        async def job() -> None:
            envelope = await search_render.fetch(self.client, app_id, start=start, count=SEARCH_RENDER_PAGE_SIZE)
            await self.publisher.publish(envelope, market_hash_name=None)

            payload = envelope.raw_payload
            if isinstance(payload, dict):
                for result in payload.get("results", []):
                    if isinstance(result, dict):
                        self.resolver.seed_from_search_render_result(app_id, result)

        return job

    async def _catalog_total_count(self, app_id: int) -> int:
        envelope = await search_render.fetch(self.client, app_id, start=0, count=SEARCH_RENDER_PAGE_SIZE)
        payload = envelope.raw_payload
        if isinstance(payload, dict):
            return int(payload.get("total_count", 0))
        return 0


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    watchlist: list[tuple[int, str]] = [
        (730, "AK-47 | Redline (Field-Tested)"),
    ]

    async with SteamMarketClient() as client, EnvelopePublisher() as publisher:
        resolver = NameIdResolver()
        scheduler = TieredScheduler(client, publisher, resolver, watchlist)
        await scheduler.run()


if __name__ == "__main__":
    asyncio.run(main())
