from __future__ import annotations

from typing import Any

from ..foundation_layer.config import PlatformConfig
from ..foundation_layer.models import ChangeEvent, utcnow_iso
from .queues import InboundQueue


class SourceEventPublisherService:
    """Source-facing publisher that writes change events directly to inbound Kafka topics."""

    def __init__(self, config: PlatformConfig, inbound_queue: InboundQueue) -> None:
        self.config = config
        self.inbound_queue = inbound_queue

    def publish_change_event(
        self,
        source_system: str,
        source_key: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        expected_key = self.config.source_keys.get(source_system)
        if expected_key != source_key:
            raise PermissionError("Invalid source credentials")

        event_input = dict(payload)
        event_input["source_system"] = source_system
        event = ChangeEvent.from_dict(event_input)

        queued_event = {
            **event.to_dict(),
            "accepted_at": utcnow_iso(),
        }
        self.inbound_queue.publish(source_system, queued_event)
        queue_size = self.inbound_queue.size(source_system)

        return {
            "status": "published",
            "topic": source_system,
            "event_id": event.event_id,
            "record_id": event.record_id,
            "operation": event.operation,
            "version": event.version,
            "queued_messages": queue_size,
        }