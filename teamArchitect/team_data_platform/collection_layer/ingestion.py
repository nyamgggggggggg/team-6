from __future__ import annotations

from typing import Any

from ..processing_layer.spark_streaming import SparkStreamingService
from .source_publisher import SourceEventPublisherService

class IngestionService:
    """Facade for source publish + spark streaming consume flow."""

    def __init__(
        self,
        source_publisher_service: SourceEventPublisherService,
        spark_streaming_service: SparkStreamingService,
    ) -> None:
        self.source_publisher_service = source_publisher_service
        self.spark_streaming_service = spark_streaming_service

    def publish_change_event(
        self,
        source_system: str,
        source_key: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self.source_publisher_service.publish_change_event(
            source_system,
            source_key,
            payload,
        )

    def consume_topic(self, source_system: str, limit: int = 100) -> dict[str, Any]:
        return self.spark_streaming_service.consume_topic(source_system, limit)

    def consume_all(self, limit_per_topic: int = 100) -> dict[str, Any]:
        return self.spark_streaming_service.consume_all(limit_per_topic)