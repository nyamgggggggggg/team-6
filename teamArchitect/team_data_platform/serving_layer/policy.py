"""Access policy service — delegates to the consumer adapter registry."""
from __future__ import annotations

from typing import Any

from .adapter_registry import ConsumerAdapterRegistry
from .consumer_adapter import PolicyViolation

__all__ = ["AccessPolicyService", "PolicyViolation"]


class AccessPolicyService:
    """Authorizes consumer requests and filters document fields.

    All policy rules are driven by :class:`ConsumerAdapterRegistry`.
    To add or modify a consumer policy, edit ``consumer_policies.yaml``
    — no changes to this class are required.
    """

    def __init__(self, registry: ConsumerAdapterRegistry) -> None:
        self._registry = registry

    def authorize(
        self,
        consumer_id: str,
        purpose: str,
        document: dict[str, Any],
    ) -> dict[str, Any]:
        adapter = self._registry.get(consumer_id)
        return adapter.authorize(purpose, document)