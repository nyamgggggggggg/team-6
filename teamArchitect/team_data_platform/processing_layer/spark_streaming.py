from __future__ import annotations

from typing import Any

from ..collection_layer.queues import InboundQueue, OutboundQueue
from ..foundation_layer.models import ChangeEvent, utcnow_iso
from ..storage_layer.state import StateRepository
from .spark import SparkPipeline, SparkProcessResult


class SparkStreamingService:
    """Spark streaming consumer that processes inbound Kafka topics."""

    def __init__(
        self,
        inbound_queue: InboundQueue,
        outbound_queue: OutboundQueue,
        state_repository: StateRepository,
        spark_pipeline: SparkPipeline,
    ) -> None:
        self.inbound_queue = inbound_queue
        self.outbound_queue = outbound_queue
        self.state_repository = state_repository
        self.spark_pipeline = spark_pipeline

    def consume_topic(self, source_system: str, limit: int = 100) -> dict[str, Any]:
        topic = source_system.strip()
        if not topic:
            return {
                "status": "invalid-topic",
                "topic": topic,
                "consumed": 0,
                "results": [],
            }

        start_offset = self.state_repository.get_stream_offset(topic)
        rows = self.inbound_queue.read_from(topic, start_offset, limit)
        results: list[dict[str, Any]] = []

        for row in rows:
            event = ChangeEvent.from_dict(row)
            if self.state_repository.is_processed(event.event_id, event.version):
                results.append(
                    {
                        "status": "duplicate",
                        "event_id": event.event_id,
                        "record_id": event.record_id,
                    }
                )
                continue

            if event.operation == "DELETE":
                spark_result = self.spark_pipeline.delete(event)
                self.state_repository.mark_processed(event.event_id, event.version)
                self._publish_outbound_notifications(event, spark_result, "record.deleted")
                results.append(
                    {
                        "status": spark_result.status,
                        "event_id": spark_result.event_id,
                        "record_id": spark_result.record_id,
                        "serving_targets": spark_result.serving_targets,
                        "source_mode": spark_result.source_mode,
                    }
                )
                continue

            spark_result = self.spark_pipeline.process(event)
            self.state_repository.mark_processed(event.event_id, event.version)
            self._publish_outbound_notifications(event, spark_result, "record.updated")
            results.append(
                {
                    "status": spark_result.status,
                    "event_id": spark_result.event_id,
                    "record_id": spark_result.record_id,
                    "serving_targets": spark_result.serving_targets,
                    "source_mode": spark_result.source_mode,
                }
            )

        new_offset = start_offset + len(rows)
        self.state_repository.set_stream_offset(topic, new_offset)
        return {
            "status": "consumed",
            "topic": topic,
            "start_offset": start_offset,
            "end_offset": new_offset,
            "consumed": len(rows),
            "results": results,
        }

    def consume_all(self, limit_per_topic: int = 100) -> dict[str, Any]:
        topic_results: list[dict[str, Any]] = []
        total = 0
        for topic in self.inbound_queue.topics():
            result = self.consume_topic(topic, limit_per_topic)
            topic_results.append(result)
            total += int(result.get("consumed", 0))
        return {
            "status": "consumed",
            "consumed": total,
            "topics": topic_results,
        }

    def _publish_outbound_notifications(
        self,
        event: ChangeEvent,
        spark_result: SparkProcessResult,
        event_type: str,
    ) -> None:
        payload = {
            "event_type": event_type,
            "record_id": event.record_id,
            "source_system": event.source_system,
            "version": event.version,
            "serving_targets": spark_result.serving_targets,
            "source_mode": spark_result.source_mode,
            "published_at": utcnow_iso(),
        }
        self.outbound_queue.publish("data-ready", payload)
        for consumer_id in spark_result.serving_targets:
            self.outbound_queue.publish(
                f"consumer-{consumer_id}",
                {
                    **payload,
                    "consumer_id": consumer_id,
                },
            )
