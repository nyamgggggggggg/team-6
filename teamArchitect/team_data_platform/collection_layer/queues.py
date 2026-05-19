from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..foundation_layer.config import PlatformConfig


def _bootstrap_servers(raw: str) -> list[str]:
    servers = [item.strip() for item in raw.split(",") if item.strip()]
    return servers or ["127.0.0.1:9092"]


def _import_kafka_symbols() -> tuple[Any, Any, Any]:
    try:
        from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    except ImportError as exc: # pragma: no cover - runtime env dependency
        raise RuntimeError(
            "Kafka backend requires `kafka-python`. Install it with `pip install kafka-python`."
        ) from exc
    return KafkaProducer, KafkaConsumer, TopicPartition


class KafkaQueue:
    """
    Kafka-backed queue.

    This implementation assumes source topics use a small partition count
    (ideally one) for deterministic replay in this reference project.
    """

    def __init__(
        self,
        config: PlatformConfig,
        known_topics: list[str] | None = None,
    ) -> None:
        self.config = config
        self.known_topics = sorted(set(known_topics or []))
        KafkaProducer, KafkaConsumer, TopicPartition = _import_kafka_symbols()
        self._KafkaConsumer = KafkaConsumer
        self._TopicPartition = TopicPartition
        self._producer = KafkaProducer(
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            **self._producer_kwargs(),
        )

    def _security_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "security_protocol": self.config.kafka_security_protocol,
        }

        if self.config.kafka_sasl_mechanism:
            kwargs["sasl_mechanism"] = self.config.kafka_sasl_mechanism
        if self.config.kafka_sasl_username:
            kwargs["sasl_plain_username"] = self.config.kafka_sasl_username
        if self.config.kafka_sasl_password:
            kwargs["sasl_plain_password"] = self.config.kafka_sasl_password
        if self.config.kafka_ssl_cafile:
            kwargs["ssl_cafile"] = self.config.kafka_ssl_cafile
        return kwargs

    def _producer_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "bootstrap_servers": _bootstrap_servers(self.config.kafka_bootstrap_servers),
            "request_timeout_ms": self.config.kafka_request_timeout_ms,
        }

        kwargs.update(self._security_kwargs())
        return kwargs

    def _consumer_kwargs(self, group_id: str | None = None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "bootstrap_servers": _bootstrap_servers(self.config.kafka_bootstrap_servers),
            "group_id": group_id,
            "auto_offset_reset": self.config.kafka_auto_offset_reset,
            "enable_auto_commit": False,
            "request_timeout_ms": self.config.kafka_request_timeout_ms,
            "value_deserializer": lambda value: json.loads(value.decode("utf-8")),
        }

        kwargs.update(self._security_kwargs())
        return kwargs

    def _build_consumer(self, group_id: str | None = None) -> Any:
        return self._KafkaConsumer(**self._consumer_kwargs(group_id=group_id))

    def _topic_partitions(self, consumer: Any, topic: str) -> list[Any]:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return []
        return [self._TopicPartition(topic, idx) for idx in sorted(partitions)]

    def publish(self, topic: str, message: dict[str, Any]) -> None:
        future = self._producer.send(topic, value=message)
        timeout_sec = max(1.0, self.config.kafka_request_timeout_ms / 1000.0)
        future.get(timeout=timeout_sec)

    def read(self, topic: str) -> list[dict[str, Any]]:
        return self.read_from(topic=topic, offset=0, limit=self.config.kafka_read_max_records)

    def read_from(
        self,
        topic: str,
        offset: int = 0,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        max_records = self.config.kafka_read_max_records if limit is None else max(0, limit)
        if max_records == 0:
            return []

        consumer = self._build_consumer(group_id=None)
        try:
            topic_partitions = self._topic_partitions(consumer, topic)
            if not topic_partitions:
                return []

            consumer.assign(topic_partitions)
            beginning_offsets = consumer.beginning_offsets(topic_partitions)
            seek_offset = max(0, int(offset))
            for topic_partition in topic_partitions:
                start = max(seek_offset, int(beginning_offsets.get(topic_partition, 0)))
                consumer.seek(topic_partition, start)

            records = consumer.poll(
                timeout_ms=max(100, self.config.kafka_poll_timeout_ms),
                max_records=max_records,
            )
            rows: list[dict[str, Any]] = []
            for topic_partition in sorted(records.keys(), key=lambda item: item.partition):
                rows.extend(record.value for record in records[topic_partition])
            return rows[:max_records]
        finally:
            consumer.close()

    def size(self, topic: str) -> int:
        consumer = self._build_consumer(group_id=None)
        try:
            topic_partitions = self._topic_partitions(consumer, topic)
            if not topic_partitions:
                return 0
            consumer.assign(topic_partitions)
            start_offsets = consumer.beginning_offsets(topic_partitions)
            end_offsets = consumer.end_offsets(topic_partitions)
            return sum(
                max(0, int(end_offsets[tp]) - int(start_offsets.get(tp, 0)))
                for tp in topic_partitions
            )
        finally:
            consumer.close()

    def topics(self) -> list[str]:
        if self.known_topics:
            return list(self.known_topics)
        consumer = self._build_consumer(group_id=None)
        try:
            return sorted(consumer.topics())
        finally:
            consumer.close()

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()


class InboundQueue:
    """Inbound queue for data collection events."""

    def __init__(self, config: PlatformConfig) -> None:
        self._queue = KafkaQueue(config=config, known_topics=list(config.source_keys.keys()))

    def publish(self, topic: str, message: dict[str, Any]) -> None:
        self._queue.publish(topic, message)

    def read(self, topic: str) -> list[dict[str, Any]]:
        return self._queue.read(topic)

    def read_from(
        self,
        topic: str,
        offset: int = 0,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._queue.read_from(topic, offset, limit)

    def size(self, topic: str) -> int:
        return self._queue.size(topic)

    def topics(self) -> list[str]:
        return self._queue.topics()

    def close(self) -> None:
        self._queue.close()


class OutboundQueue:
    """Outbound queue for consumer notifications."""

    def __init__(self, config: PlatformConfig) -> None:
        known_topics = ["data-ready", *[f"consumer-{key}" for key in config.consumer_keys]]
        self._queue = KafkaQueue(config=config, known_topics=known_topics)

    def publish(self, topic: str, message: dict[str, Any]) -> None:
        self._queue.publish(topic, message)

    def read(self, topic: str) -> list[dict[str, Any]]:
        return self._queue.read(topic)

    def read_from(
        self,
        topic: str,
        offset: int = 0,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._queue.read_from(topic, offset, limit)

    def size(self, topic: str) -> int:
        return self._queue.size(topic)

    def topics(self) -> list[str]:
        return self._queue.topics()

    def close(self) -> None:
        self._queue.close()