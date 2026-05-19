from __future__ import annotations

from ..foundation_layer.config import PlatformConfig
from ..serving_layer.adapter_registry import ConsumerAdapterRegistry
from ..serving_layer.consumer_adapter import PolicyViolation


class GatewayAuthService:
    def __init__(
        self,
        config: PlatformConfig,
        adapter_registry: ConsumerAdapterRegistry,
    ) -> None:
        self.config = config
        self._registry = adapter_registry

    def authenticate_source(self, source_system: str, source_key: str) -> None:
        expected = self.config.source_keys.get(source_system)
        if expected != source_key:
            raise PermissionError("Invalid source credentials")

    def authenticate_consumer(self, consumer_id: str, consumer_key: str) -> None:
        try:
            adapter = self._registry.get(consumer_id)
        except PolicyViolation:
            raise PermissionError("Invalid consumer credentials")
        if adapter.auth_key != consumer_key:
            raise PermissionError("Invalid consumer credentials")