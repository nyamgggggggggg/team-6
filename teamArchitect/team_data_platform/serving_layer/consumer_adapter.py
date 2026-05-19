"""Consumer adapter abstractions and YAML-driven implementation."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class PolicyViolation(Exception):
    """Raised when a consumer attempts an unauthorized access."""


class ConsumerAdapter(ABC):
    """Abstract interface every consumer system adapter must implement.

    Adding a new consumer system means creating a concrete subclass
    (or registering a YAML-driven entry) — no changes to the core
    serving layer are required.
    """

    @property
    @abstractmethod
    def consumer_id(self) -> str:
        """Unique identifier for this consumer system."""

    @property
    @abstractmethod
    def auth_key(self) -> str:
        """Pre-shared authentication key for this consumer."""

    @property
    @abstractmethod
    def purposes(self) -> set[str]:
        """Set of allowed query purposes (e.g. 'search', 'analytics')."""

    @property
    @abstractmethod
    def allowed_fields(self) -> set[str]:
        """Set of document fields this consumer is permitted to receive."""

    def authorize(self, purpose: str, document: dict[str, Any]) -> dict[str, Any]:
        """Validate purpose and return only the permitted fields.

        Subclasses may override this to implement custom projection or
        post-processing logic (e.g. field renaming, derived fields).
        """
        if purpose not in self.purposes:
            raise PolicyViolation(
                f"Purpose '{purpose}' is not allowed for consumer '{self.consumer_id}'. "
                f"Allowed: {sorted(self.purposes)}"
            )
        return {
            key: value
            for key, value in document.items()
            if key in self.allowed_fields
        }


class YamlConsumerAdapter(ConsumerAdapter):
    """Config-file-driven adapter. Loaded automatically from consumer_policies.yaml.

    This is the default adapter for consumers defined declaratively in YAML.
    For consumers requiring custom projection logic, subclass ConsumerAdapter
    directly and register via ConsumerAdapterRegistry.register().
    """

    def __init__(
        self,
        consumer_id: str,
        auth_key: str,
        purposes: set[str],
        allowed_fields: set[str],
    ) -> None:
        self._consumer_id = consumer_id
        self._auth_key = auth_key
        self._purposes = purposes
        self._allowed_fields = allowed_fields

    @property
    def consumer_id(self) -> str:
        return self._consumer_id

    @property
    def auth_key(self) -> str:
        return self._auth_key

    @property
    def purposes(self) -> set[str]:
        return self._purposes

    @property
    def allowed_fields(self) -> set[str]:
        return self._allowed_fields
