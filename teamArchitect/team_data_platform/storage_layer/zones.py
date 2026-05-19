from __future__ import annotations

from pathlib import Path
from typing import Any

from .json_store import atomic_write_json, list_json_files, load_json


class RawZoneRepository:
    def __init__(self, root: Path) -> None:
        self.root = root

    def upsert(self, source_system: str, record_id: str, document: dict[str, Any]) -> None:
        atomic_write_json(self.root / source_system / f"{record_id}.json", document)

    def get(self, source_system: str, record_id: str) -> dict[str, Any] | None:
        path = self.root / source_system / f"{record_id}.json"
        return load_json(path, None)


class StandardizedZoneRepository:
    def __init__(self, root: Path) -> None:
        self.root = root

    def upsert(self, record_id: str, document: dict[str, Any]) -> None:
        atomic_write_json(self.root / f"{record_id}.json", document)

    def get(self, record_id: str) -> dict[str, Any] | None:
        return load_json(self.root / f"{record_id}.json", None)

    def delete(self, record_id: str) -> None:
        path = self.root / f"{record_id}.json"
        if path.exists():
            path.unlink()


class ServingZoneRepository:
    def __init__(self, root: Path) -> None:
        self.root = root

    def upsert_many(self, record_id: str, projections: dict[str, dict[str, Any]]) -> None:
        for consumer_id, projection in projections.items():
            atomic_write_json(self.root / consumer_id / f"{record_id}.json", projection)

    def get(self, consumer_id: str, record_id: str) -> dict[str, Any] | None:
        return load_json(self.root / consumer_id / f"{record_id}.json", None)

    def list(self, consumer_id: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for path in list_json_files(self.root / consumer_id):
            records.append(load_json(path, {}))
        return records

    def delete_all(self, record_id: str) -> None:
        for consumer_dir in self.root.iterdir() if self.root.exists() else []:
            if not consumer_dir.is_dir():
                continue
            path = consumer_dir / f"{record_id}.json"
            if path.exists():
                path.unlink()
