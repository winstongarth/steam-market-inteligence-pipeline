"""Publishes RawEnvelopes to Kafka (Redpanda) topic market.raw.v1, keyed by {app_id}:{market_hash_name}."""

from __future__ import annotations

import logging

from aiokafka import AIOKafkaProducer

from ingest import config
from ingest.schemas import RawEnvelope

logger = logging.getLogger("ingest.kafka_producer")


class EnvelopePublisher:
    def __init__(self, bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS, topic: str = config.KAFKA_RAW_TOPIC) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda env: env.model_dump_json().encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
        )
        await self._producer.start()
        logger.info("kafka producer connected to %s, topic %s", self.bootstrap_servers, self.topic)

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()

    async def publish(self, envelope: RawEnvelope, market_hash_name: str | None = None) -> None:
        if self._producer is None:
            raise RuntimeError("EnvelopePublisher.start() must be called before publish()")
        key = envelope.kafka_key(market_hash_name)
        await self._producer.send_and_wait(self.topic, value=envelope, key=key)
        logger.debug("published envelope endpoint=%s key=%s ingest_id=%s", envelope.endpoint, key, envelope.ingest_id)

    async def __aenter__(self) -> "EnvelopePublisher":
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()
