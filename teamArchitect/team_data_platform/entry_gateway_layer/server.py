from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from ..foundation_layer.bootstrap import Application, build_application
from ..processing_layer.streaming_runner import SparkStreamingRunner
from ..serving_layer.policy import PolicyViolation


APP: Application = build_application()
STREAMING_RUNNER: SparkStreamingRunner | None = None


class PlatformHandler(BaseHTTPRequestHandler):
    server_version = "TeamDataPlatform/1.0"

    def _json_response(self, status: int, payload: dict | list) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._json_response(HTTPStatus.OK, {"status": "ok"})
            return

        if parsed.path == "/api/v1/admin/runtime":
            runner_status = STREAMING_RUNNER.status() if STREAMING_RUNNER is not None else None
            self._json_response(
                HTTPStatus.OK,
                {
                    "runtime_mode": APP.config.runtime_mode,
                    "continuous_consume_enabled": APP.config.is_production_mode(),
                    "streaming_runner": runner_status,
                },
            )
            return

        if parsed.path == "/api/v1/admin/outbound-events":
            topic = parse_qs(parsed.query).get("topic", ["data-ready"])[0]
            self._json_response(HTTPStatus.OK, APP.outbound_queue.read(topic))
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        try:
            if self.path == "/api/v1/query/record":
                self._handle_record_query()
                return

            if self.path == "/api/v1/query/search":
                self._handle_search()
                return

            if self.path == "/api/v1/admin/stream/consume":
                self._handle_stream_consume()
                return

            if self.path == "/api/v1/admin/stream/consume-all":
                self._handle_stream_consume_all()
                return

            if self.path == "/api/v1/ingest/events":
                self._handle_ingest_event()
                return

            self._json_response(HTTPStatus.NOT_FOUND, {"error": "not found"})
        except PermissionError as exc:
            self._json_response(HTTPStatus.UNAUTHORIZED, {"error": str(exc)})
        except PolicyViolation as exc:
            self._json_response(HTTPStatus.FORBIDDEN, {"error": str(exc)})
        except KeyError as exc:
            self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover - defensive handler
            self._json_response(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": str(exc)},
            )

    def _handle_record_query(self) -> None:
        consumer_id = self.headers.get("X-Consumer-Id", "")
        consumer_key = self.headers.get("X-Consumer-Key", "")
        APP.auth_service.authenticate_consumer(consumer_id, consumer_key)

        payload = self._read_json()
        result = APP.query_service.get_record(
            consumer_id=consumer_id,
            purpose=str(payload["purpose"]),
            record_id=str(payload["record_id"]),
        )
        self._json_response(HTTPStatus.OK, result)

    def _handle_search(self) -> None:
        consumer_id = self.headers.get("X-Consumer-Id", "")
        consumer_key = self.headers.get("X-Consumer-Key", "")
        APP.auth_service.authenticate_consumer(consumer_id, consumer_key)

        payload = self._read_json()
        result = APP.query_service.search(
            consumer_id=consumer_id,
            purpose=str(payload["purpose"]),
            query_text=str(payload.get("query", "")),
            limit=int(payload.get("limit", 10)),
        )
        self._json_response(HTTPStatus.OK, result)

    def _handle_stream_consume(self) -> None:
        payload = self._read_json()
        topic = str(payload.get("source_system", payload.get("topic", "mail"))).strip()
        limit = int(payload.get("limit", 100))
        result = APP.spark_streaming_service.consume_topic(topic, limit)
        self._json_response(HTTPStatus.OK, result)

    def _handle_stream_consume_all(self) -> None:
        payload = self._read_json()
        limit_per_topic = int(payload.get("limit_per_topic", 100))
        result = APP.spark_streaming_service.consume_all(limit_per_topic)
        self._json_response(HTTPStatus.OK, result)

    def _handle_ingest_event(self) -> None:
        source_system = self.headers.get("X-Source-System", "")
        source_key = self.headers.get("X-Source-Key", "")
        payload = self._read_json()
        result = APP.source_publisher_service.publish_change_event(
            source_system=source_system,
            source_key=source_key,
            payload=payload,
        )
        self._json_response(HTTPStatus.OK, result)

    def log_message(self, format: str, *args: object) -> None:
        return



def serve(host: str = "127.0.0.1", port: int = 8080, base_dir: Path | None = None) -> None:
    global APP
    global STREAMING_RUNNER
    APP = build_application(base_dir)

    if APP.config.is_production_mode():
        STREAMING_RUNNER = SparkStreamingRunner(
            spark_streaming_service=APP.spark_streaming_service,
            poll_interval_seconds=APP.config.streaming_poll_interval_seconds,
            batch_limit=APP.config.streaming_batch_limit,
        )
        STREAMING_RUNNER.start()
        print(
            "Runtime mode: production "
            f"(continuous consume ON, poll={APP.config.streaming_poll_interval_seconds}s, "
            f"batch={APP.config.streaming_batch_limit})"
        )
    else:
        STREAMING_RUNNER = None
        print("Runtime mode: test (continuous consume OFF, use admin consume endpoints)")

    server = ThreadingHTTPServer((host, port), PlatformHandler)
    print(f"Listening on http://{host}:{port}")
    try:
        server.serve_forever()
    finally:
        if STREAMING_RUNNER is not None:
            STREAMING_RUNNER.stop()
        APP.inbound_queue.close()
        APP.outbound_queue.close()

if __name__ == "__main__":
    serve()

