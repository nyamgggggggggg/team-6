from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


def atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(path)

def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def list_json_files(path: Path) -> Iterable[Path]:
    if not path.exists():
        return []
    return sorted(path.glob("*.json"))