"""Consumes market.changes.v1 (Phase 2's CDC output) and writes Parquet to S3/MinIO,
partitioned dt=YYYY-MM-DD/hour=HH/app_id=NNN/ — this is the landing step Phase 3's dbt
staging models read from (fct_price_change).

Same buffering/flush and crash-safe commit-after-flush design as bronze_writer.py (see
docs/DECISIONS.md, "bronze_writer.py: commit-after-flush, not auto-commit" — applied here
from the start rather than repeating that bug).
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from aiokafka import AIOKafkaConsumer

from ingest import config
from streaming.bronze_writer import _s3_client, ensure_bucket

logger = logging.getLogger("streaming.silver_writer")

FLUSH_INTERVAL_SECONDS = 30.0
FLUSH_SIZE_THRESHOLD = 200

KAFKA_CHANGES_TOPIC = "market.changes.v1"


def _partition_key(event: dict[str, Any]) -> tuple[str, str, int]:
    observed_at = datetime.fromisoformat(event["observed_at"].replace("Z", "+00:00"))
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return observed_at.strftime("%Y-%m-%d"), observed_at.strftime("%H"), event["app_id"]


def _partition_prefix(dt: str, hour: str, app_id: int) -> str:
    return f"silver/change_events/dt={dt}/hour={hour}/app_id={app_id}/"


class SilverWriter:
    def __init__(self, bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS) -> None:
        self.bootstrap_servers = bootstrap_servers
        self._consumer: AIOKafkaConsumer | None = None
        self._s3 = _s3_client()
        ensure_bucket(self._s3)
        self._buffers: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
        self._last_flush = time.monotonic()
        self.records_written = 0
        self.files_written = 0

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            KAFKA_CHANGES_TOPIC,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
            group_id="silver-writer",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await self._consumer.start()
        logger.info("silver writer consuming %s from %s", KAFKA_CHANGES_TOPIC, self.bootstrap_servers)

    async def stop(self) -> None:
        await self._flush_all()
        if self._consumer is not None:
            await self._consumer.stop()

    async def run_forever(self) -> None:
        assert self._consumer is not None
        async for msg in self._consumer:
            event = msg.value
            key = _partition_key(event)
            self._buffers[key].append(event)

            if len(self._buffers[key]) >= FLUSH_SIZE_THRESHOLD or (
                time.monotonic() - self._last_flush > FLUSH_INTERVAL_SECONDS
            ):
                await self._flush_all()

    async def _flush_all(self) -> None:
        for key in list(self._buffers.keys()):
            self._flush_partition(key)
        self._last_flush = time.monotonic()
        if self._consumer is not None:
            await self._consumer.commit()

    def _flush_partition(self, key: tuple[str, str, int]) -> None:
        records = self._buffers.pop(key, [])
        if not records:
            return
        dt, hour, app_id = key

        table = pa.Table.from_pylist(records)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        object_key = _partition_prefix(dt, hour, app_id) + f"part-{uuid.uuid4().hex}.parquet"
        self._s3.put_object(Bucket=config.S3_BUCKET, Key=object_key, Body=buf.getvalue())

        self.records_written += len(records)
        self.files_written += 1
        logger.info("flushed %d records -> s3://%s/%s", len(records), config.S3_BUCKET, object_key)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    writer = SilverWriter()
    await writer.start()
    try:
        await writer.run_forever()
    finally:
        await writer.stop()


if __name__ == "__main__":
    asyncio.run(main())
