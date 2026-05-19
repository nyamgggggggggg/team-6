from __future__ import annotations

from pathlib import Path

from .json_store import atomic_write_json, load_json


class StateRepository:
    def __init__(self, state_dir: Path) -> None:
        self._processed_path = state_dir / "processed_events.json"
        self._offset_path = state_dir / "spark_stream_offsets.json"

    def is_processed(self, event_id: str, version: int) -> bool:
        state = load_json(self._processed_path, {})
        return state.get(event_id) == version

    def mark_processed(self, event_id: str, version: int) -> None:
        state = load_json(self._processed_path, {})
        state[event_id] = version
        atomic_write_json(self._processed_path, state)

    def get_stream_offset(self, topic: str) -> int:
        offsets = load_json(self._offset_path, {})
        raw = offsets.get(topic, 0)
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = 0
        return max(0, value)

    def set_stream_offset(self, topic: str, offset: int) -> None:
        offsets = load_json(self._offset_path, {})
        offsets[topic] = max(0, int(offset))
        atomic_write_json(self._offset_path, offsets)