from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(slots=True)
class ChangeEvent:
    event_id: str
    source_system: str
    entity_type: str
    record_id: str
    operation: str
    version: int
    occurred_at: str
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChangeEvent":
        source_system = data.get("source_system") or data.get("topic") or "mail"
        return cls(
            event_id=str(data["event_id"]),
            source_system=str(source_system),
            entity_type=str(data.get("entity_type", "generic")),
            record_id=str(data["record_id"]),
            operation=str(data.get("operation", "UPSERT")).upper(),
            version=int(data.get("version", 1)),
            occurred_at=str(data.get("occurred_at", utcnow_iso())),
            payload=dict(data.get("payload", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

