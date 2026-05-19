"""Consumer adapter registry — loads and manages all consumer adapters."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .consumer_adapter import ConsumerAdapter, PolicyViolation, YamlConsumerAdapter

_DEFAULT_POLICY_FILE = Path(__file__).resolve().parents[2] / "config" / "consumer_policies.yaml"


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML without requiring a third-party library (stdlib only).

    Falls back to a minimal line-by-line parser so the project has
    zero mandatory non-stdlib dependencies.  If PyYAML is installed it
    will be preferred for correctness.
    """
    try:
        import yaml  # type: ignore[import]
        with path.open(encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except ImportError:
        pass

    # Minimal YAML subset parser: handles the indented block structure
    # produced by consumer_policies.yaml (no anchors, no multi-line scalars).
    import re
    result: dict[str, Any] = {}
    stack: list[tuple[int, Any]] = [(-1, result)]

    with path.open(encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.rstrip()
            stripped = line.lstrip()
            if not stripped or stripped.startswith("#"):
                continue
            indent = len(line) - len(stripped)
            while len(stack) > 1 and stack[-1][0] >= indent:
                stack.pop()

            parent = stack[-1][1]
            if stripped.endswith(":"):
                key = stripped[:-1].strip()
                new_dict: dict[str, Any] = {}
                if isinstance(parent, dict):
                    parent[key] = new_dict
                stack.append((indent, new_dict))
            elif stripped.startswith("- "):
                value = stripped[2:].strip()
                if isinstance(parent, dict):
                    # convert last key's value to list
                    last_key = list(parent.keys())[-1]
                    if not isinstance(parent[last_key], list):
                        parent[last_key] = []
                    parent[last_key].append(value)
                elif isinstance(parent, list):
                    parent.append(value)
            elif ":" in stripped:
                key, _, val = stripped.partition(":")
                if isinstance(parent, dict):
                    parent[key.strip()] = val.strip()

    return result


class ConsumerAdapterRegistry:
    """Central registry for all consumer system adapters.

    Loaded automatically from ``consumer_policies.yaml`` by default.
    Custom adapters can be registered programmatically with
    :meth:`register`, which overrides any YAML-defined entry for the
    same ``consumer_id``.

    Usage example — adding a new consumer without touching core code::

        registry = ConsumerAdapterRegistry.from_yaml()
        registry.register(MyCustomAdapter())
    """

    def __init__(self) -> None:
        self._adapters: dict[str, ConsumerAdapter] = {}

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, path: Path | None = None) -> ConsumerAdapterRegistry:
        """Build a registry from a YAML policy file.

        The path defaults to ``team_data_platform/consumer_policies.yaml``.
        Override via the ``TEAM_PLATFORM_CONSUMER_POLICY_FILE`` env var.
        """
        env_path = os.environ.get("TEAM_PLATFORM_CONSUMER_POLICY_FILE", "")
        resolved = Path(env_path) if env_path else (path or _DEFAULT_POLICY_FILE)

        raw = _load_yaml(resolved)
        consumers: dict[str, Any] = raw.get("consumers", {})

        registry = cls()
        for consumer_id, cfg in consumers.items():
            adapter = YamlConsumerAdapter(
                consumer_id=consumer_id,
                auth_key=str(cfg.get("key", "")),
                purposes=set(cfg.get("purposes", [])),
                allowed_fields=set(cfg.get("allowed_fields", [])),
            )
            registry.register(adapter)
        return registry

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, adapter: ConsumerAdapter) -> None:
        """Register (or replace) an adapter for a given consumer_id."""
        self._adapters[adapter.consumer_id] = adapter

    # ------------------------------------------------------------------
    # Lookup helpers used by AccessPolicyService
    # ------------------------------------------------------------------

    def get(self, consumer_id: str) -> ConsumerAdapter:
        adapter = self._adapters.get(consumer_id)
        if adapter is None:
            raise PolicyViolation(f"Unknown consumer: '{consumer_id}'")
        return adapter

    def consumer_keys(self) -> dict[str, str]:
        """Return {consumer_id: auth_key} mapping — replaces config hardcoding."""
        return {cid: a.auth_key for cid, a in self._adapters.items()}

    def consumer_ids(self) -> list[str]:
        return sorted(self._adapters.keys())
