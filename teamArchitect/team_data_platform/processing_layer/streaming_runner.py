from __future__ import annotations

import threading
from typing import Any

from .spark_streaming import SparkStreamingService


class SparkStreamingRunner:
    """Background polling runner for production-like continuous consume."""

    def __init__(
        self,
        spark_streaming_service: SparkStreamingService,
        poll_interval_seconds: float = 2.0,
        batch_limit: int = 100,
    ) -> None:
        self.spark_streaming_service = spark_streaming_service
        self.poll_interval_seconds = max(0.2, float(poll_interval_seconds))
        self.batch_limit = max(1, int(batch_limit))
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_result: dict[str, Any] | None = None
        self._last_error: str | None = None

    def start(self) -> None:
        if self.is_running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="spark-streaming-runner",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status(self) -> dict[str, Any]:
        return {
            "running": self.is_running,
            "poll_interval_seconds": self.poll_interval_seconds,
            "batch_limit": self.batch_limit,
            "last_result": self._last_result,
            "last_error": self._last_error,
        }

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._last_result = self.spark_streaming_service.consume_all(self.batch_limit)
                self._last_error = None
            except Exception as exc:  # pragma: no cover - defensive worker
                self._last_error = str(exc)
            self._stop_event.wait(self.poll_interval_seconds)