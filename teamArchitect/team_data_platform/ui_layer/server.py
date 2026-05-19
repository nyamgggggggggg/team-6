from __future__ import annotations

import json
import os
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from ..collection_layer.queues import InboundQueue, OutboundQueue
from ..collection_layer.knox import KnoxSourceService
from ..foundation_layer.config import PlatformConfig
from ..storage_layer.json_store import list_json_files, load_json


UI_ROOT = Path(__file__).resolve().parent
INDEX_FILE = UI_ROOT / "index.html"


@dataclass(slots=True)
class UiRuntimeConfig:
    backend_url: str
    host: str
    port: int
    platform_config: PlatformConfig
    ui_start_step: int


RUNTIME = UiRuntimeConfig(
    backend_url=os.environ.get("TEAM_PLATFORM_GATEWAY_URL", "http://127.0.0.1:8080"),
    host="127.0.0.1",
    port=8090,
    platform_config=PlatformConfig(),
    ui_start_step=1,
)


def _resolve_ui_start_step(config: PlatformConfig) -> int:
    raw_value = os.environ.get("TEAM_PLATFORM_UI_START_STEP", "").strip()
    if raw_value:
        try:
            step = int(raw_value)
        except ValueError:
            step = 1
        return min(4, max(1, step))
    if config.is_test_mode():
        return 2
    return 1


def _tail(rows: list[dict[str, Any]], size: int = 20) -> list[dict[str, Any]]:
    if len(rows) <= size:
        return rows
    return rows[-size:]


def _guess_latest_record_id(config: PlatformConfig) -> str | None:
    candidates = list(list_json_files(config.iceberg_catalog_dir))
    if not candidates:
        return None
    latest = max(candidates, key=lambda path: path.stat().st_mtime)
    return latest.stem

def _build_storage_state(
    config: PlatformConfig,
    source_system: str,
    record_id: str | None,
) -> dict[str, Any]:
    resolved_record_id = record_id or _guess_latest_record_id(config)

    inbound_topic = source_system or "mail"
    inbound_events: list[dict[str, Any]]
    outbound_data_ready: list[dict[str, Any]]
    consumer_topics: dict[str, list[dict[str, Any]]] = {}

    inbound_queue = InboundQueue(config=config)
    outbound_queue = OutboundQueue(config=config)
    try:
        inbound_events = _tail(inbound_queue.read(inbound_topic))
        outbound_data_ready = _tail(outbound_queue.read("data-ready"))

        serving_records: dict[str, dict[str, Any] | None] = {}
        for consumer_id in sorted(config.consumer_keys.keys()):
            consumer_topics[consumer_id] = _tail(outbound_queue.read(f"consumer-{consumer_id}"))
            if resolved_record_id:
                serving_records[consumer_id] = load_json(
                    config.serving_dir / consumer_id / f"{resolved_record_id}.json",
                    None,
                )
            else:
                serving_records[consumer_id] = None

        raw_record = None
        standardized_record = None
        catalog_entry = None
        if resolved_record_id:
            raw_record = load_json(
                config.raw_dir / inbound_topic / f"{resolved_record_id}.json",
                None,
            )
            standardized_record = load_json(
                config.standardized_dir / f"{resolved_record_id}.json",
                None,
            )
            catalog_entry = load_json(
                config.iceberg_catalog_dir / f"{resolved_record_id}.json",
                None,
            )

        return {
            "base_dir": str(config.base_dir),
            "source_system": inbound_topic,
            "record_id": resolved_record_id,
            "processed_state": load_json(config.state_dir / "processed_events.json", {}),
            "inbound_events": inbound_events,
            "raw_record": raw_record,
            "standardized_record": standardized_record,
            "catalog_entry": catalog_entry,
            "serving_records": serving_records,
            "outbound_data_ready": outbound_data_ready,
            "outbound_by_consumer": consumer_topics,
        }
    finally:
        inbound_queue.close()
        outbound_queue.close()

class UiHandler(BaseHTTPRequestHandler):
    server_version = "TeamDataPlatformUI/1.0"

    def _json_response(self, status: int, payload: dict[str, Any] | list[Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _text_response(self, status: int, text: str, content_type: str = "text/plain; charset=utf-8") -> None:
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            html = INDEX_FILE.read_text(encoding="utf-8")
            self._text_response(HTTPStatus.OK, html, "text/html; charset=utf-8")
            return

        if parsed.path == "/api/ui/config":
            self._json_response(
                HTTPStatus.OK,
                {
                    "backend_url": RUNTIME.backend_url,
                    "base_dir": str(RUNTIME.platform_config.base_dir),
                    "runtime_mode": RUNTIME.platform_config.runtime_mode,
                    "ui_start_step": RUNTIME.ui_start_step,
                },
            )
            return

        if parsed.path == "/api/ui/state":
            params = parse_qs(parsed.query)
            source_system = params.get("source_system", ["mail"])[0]
            record_id = params.get("record_id", [""])[0] or None
            self._json_response(
                HTTPStatus.OK,
                _build_storage_state(
                    RUNTIME.platform_config,
                    source_system=source_system,
                    record_id=record_id,
                ),
            )
            return

        if parsed.path == "/api/gateway/outbound-events":
            topic = parse_qs(parsed.query).get("topic", ["data-ready"])[0]
            self._proxy_gateway(
                method="GET",
                path=f"/api/v1/admin/outbound-events?topic={topic}",
                payload=None,
            )
            return

        if parsed.path == "/api/ui/records":
            records = []
            for path in list_json_files(RUNTIME.platform_config.iceberg_catalog_dir):
                entry = load_json(path, {})
                if entry:
                    records.append(entry)
            records.sort(key=lambda r: r.get("catalog_updated_at", ""), reverse=True)
            self._json_response(HTTPStatus.OK, records)
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)

        if parsed.path == "/api/ui/knox/register":
            payload = self._read_json()
            source_system = str(payload.get("source_system", "")).strip()
            record_id = str(payload.get("record_id", "")).strip()
            source_payload = payload.get("payload", {})
            if not source_system or not record_id or not isinstance(source_payload, dict):
                self._json_response(
                    HTTPStatus.BAD_REQUEST,
                    {"error": "source_system, record_id, payload(object) are required"},
                )
                return
            knox = KnoxSourceService(RUNTIME.platform_config.knox_dir)
            knox.register_record(source_system, record_id, source_payload)
            self._json_response(
                HTTPStatus.OK,
                {
                    "status": "registered",
                    "source_system": source_system,
                    "record_id": record_id,
                    "knox_file": str(
                        RUNTIME.platform_config.knox_dir / source_system / f"{record_id}.json"
                    ),
                },
            )
            return

        if parsed.path == "/api/platform/consume":
            request_body = self._read_json()
            source_system = str(request_body.get("source_system", "")).strip()
            limit = int(request_body.get("limit", 100))
            self._proxy_gateway(
                method="POST",
                path="/api/v1/admin/stream/consume",
                payload={
                    "source_system": source_system,
                    "limit": limit,
                },
                headers=None,
            )
            return

        if parsed.path == "/api/platform/consume-all":
            request_body = self._read_json()
            limit_per_topic = int(request_body.get("limit_per_topic", 100))
            self._proxy_gateway(
                method="POST",
                path="/api/v1/admin/stream/consume-all",
                payload={"limit_per_topic": limit_per_topic},
                headers=None,
            )
            return

        if parsed.path == "/api/ui/ingest":
            request_body = self._read_json()
            source_system = str(request_body.pop("source_system", "mail"))
            source_key = str(request_body.pop("source_key", ""))
            self._proxy_gateway(
                method="POST",
                path="/api/v1/ingest/events",
                payload=request_body,
                headers={
                    "X-Source-System": source_system,
                    "X-Source-Key": source_key,
                },
            )
            return

        if parsed.path == "/api/gateway/query/record":
            request_body = self._read_json()
            consumer_id = str(request_body.get("consumer_id", "")).strip()
            consumer_key = str(request_body.get("consumer_key", "")).strip()
            purpose = str(request_body.get("purpose", "")).strip()
            record_id = str(request_body.get("record_id", "")).strip()

            self._proxy_gateway(
                method="POST",
                path="/api/v1/query/record",
                payload={"purpose": purpose, "record_id": record_id},
                headers={
                    "X-Consumer-Id": consumer_id,
                    "X-Consumer-Key": consumer_key,
                },
            )
            return

        if parsed.path == "/api/gateway/query/search":
            request_body = self._read_json()
            consumer_id = str(request_body.get("consumer_id", "")).strip()
            consumer_key = str(request_body.get("consumer_key", "")).strip()
            purpose = str(request_body.get("purpose", "")).strip()
            query_text = str(request_body.get("query", ""))
            limit = int(request_body.get("limit", 10))

            self._proxy_gateway(
                method="POST",
                path="/api/v1/query/search",
                payload={"purpose": purpose, "query": query_text, "limit": limit},
                headers={
                    "X-Consumer-Id": consumer_id,
                    "X-Consumer-Key": consumer_key,
                },
            )
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def _proxy_gateway(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
        headers: dict[str, str] | None = None,
    ) -> None:
        target = f"{RUNTIME.backend_url.rstrip('/')}{path}"
        request_headers = {"Content-Type": "application/json; charset=utf-8"}
        if headers:
            request_headers.update(headers)
        body = None
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        request = Request(target, data=body, method=method, headers=request_headers)
        try:
            with urlopen(request, timeout=20) as response:
                raw = response.read().decode("utf-8")
                status = response.status
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            status = exc.code
        except URLError as exc:
            self._json_response(
                HTTPStatus.BAD_GATEWAY,
                {
                    "error": "gateway connection failed",
                    "detail": str(exc),
                    "target": target,
                },
            )
            return

        try:
            payload_obj = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload_obj = {"raw": raw}
        self._json_response(status, payload_obj)

    def log_message(self, format: str, *args: object) -> None:
        return


def serve(
    host: str = "127.0.0.1",
    port: int = 8090,
    backend_url: str | None = None,
    base_dir: Path | None = None,
) -> None:
    RUNTIME.host = host
    RUNTIME.port = port
    if backend_url:
        RUNTIME.backend_url = backend_url
    if base_dir is not None:
        RUNTIME.platform_config = PlatformConfig(base_dir=base_dir)
    else:
        RUNTIME.platform_config = PlatformConfig(base_dir=RUNTIME.platform_config.base_dir)
    RUNTIME.platform_config.ensure_directories()
    RUNTIME.ui_start_step = _resolve_ui_start_step(RUNTIME.platform_config)

    server = ThreadingHTTPServer((host, port), UiHandler)
    print(f"UI listening on http://{host}:{port}")
    print(f"Gateway target: {RUNTIME.backend_url}")
    print(
        f"UI runtime mode: {RUNTIME.platform_config.runtime_mode} "
        f"(default start step: {RUNTIME.ui_start_step})"
    )
    server.serve_forever()


if __name__ == "__main__":
    serve()