"""Consumes market.raw.v1 and writes Parquet to S3/MinIO, partitioned
dt=YYYY-MM-DD/hour=HH/app_id=NNN/endpoint=X/ (§5, Phase 1 step 7).

Buffers records per partition key in memory and flushes on a timer or size threshold —
simple and sufficient for Phase 1's ~0.5 req/s ingest volume. Phase 2's cdc_job.py is the
real streaming-semantics component; this writer's job is just "land Bronze reliably."
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

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from aiokafka import AIOKafkaConsumer

from ingest import config

logger = logging.getLogger("streaming.bronze_writer")

FLUSH_INTERVAL_SECONDS = 30.0
FLUSH_SIZE_THRESHOLD = 200


def _s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT_URL,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
    )


def ensure_bucket(s3: Any, bucket: str = config.S3_BUCKET) -> None:
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        logger.info("created bucket %s", bucket)


def _partition_key(envelope: dict[str, Any]) -> tuple[str, str, int, str]:
    observed_at = datetime.fromisoformat(envelope["observed_at"])
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    dt = observed_at.strftime("%Y-%m-%d")
    hour = observed_at.strftime("%H")
    return dt, hour, envelope["app_id"], envelope["endpoint"]


def _partition_prefix(dt: str, hour: str, app_id: int, endpoint: str) -> str:
    return f"bronze/dt={dt}/hour={hour}/app_id={app_id}/endpoint={endpoint}/"


class BronzeWriter:
    def __init__(self, bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS) -> None:
        self.bootstrap_servers = bootstrap_servers
        self._consumer: AIOKafkaConsumer | None = None
        self._s3 = _s3_client()
        ensure_bucket(self._s3)
        self._buffers: dict[tuple[str, str, int, str], list[dict[str, Any]]] = defaultdict(list)
        self._last_flush = time.monotonic()
        self.records_written = 0
        self.files_written = 0

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            config.KAFKA_RAW_TOPIC,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
            group_id="bronze-writer",
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # see run_forever — we commit only after a successful flush
        )
        await self._consumer.start()
        logger.info("bronze writer consuming %s from %s", config.KAFKA_RAW_TOPIC, self.bootstrap_servers)

    async def stop(self) -> None:
        await self._flush_all()
        if self._consumer is not None:
            await self._consumer.stop()

    async def run_forever(self) -> None:
        """`enable_auto_commit=False`: offsets are only committed right after a successful
        flush to Parquet, never on a background timer. Auto-commit would advance the offset
        for messages merely buffered in memory — a crash between that commit and the next
        flush would silently lose those records forever, since the consumer would resume
        past them on restart. Explicit commit-after-flush makes this genuinely crash-safe."""
        assert self._consumer is not None
        async for msg in self._consumer:
            envelope = msg.value
            key = _partition_key(envelope)
            self._buffers[key].append(envelope)

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

    def _flush_partition(self, key: tuple[str, str, int, str]) -> None:
        records = self._buffers.pop(key, [])
        if not records:
            return
        dt, hour, app_id, endpoint = key

        table = pa.Table.from_pylist(
            [{**r, "raw_payload": json.dumps(r["raw_payload"]), "request_params": json.dumps(r["request_params"])} for r in records]
        )
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        object_key = _partition_prefix(dt, hour, app_id, endpoint) + f"part-{uuid.uuid4().hex}.parquet"
        self._s3.put_object(Bucket=config.S3_BUCKET, Key=object_key, Body=buf.getvalue())

        self.records_written += len(records)
        self.files_written += 1
        logger.info("flushed %d records -> s3://%s/%s", len(records), config.S3_BUCKET, object_key)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    writer = BronzeWriter()
    await writer.start()
    try:
        await writer.run_forever()
    finally:
        await writer.stop()


if __name__ == "__main__":
    asyncio.run(main())
