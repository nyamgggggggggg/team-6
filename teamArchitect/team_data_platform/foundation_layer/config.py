from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


RuntimeMode = Literal["test", "production"]


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(slots=True)
class PlatformConfig:
    base_dir: Path = field(
        default_factory=lambda: Path(
            os.environ.get(
                "TEAM_PLATFORM_HOME",
                Path(__file__).resolve().parents[2] / "var",
            )
        )
    )
    runtime_mode: RuntimeMode = field(
        default_factory=lambda: (
            "production"
            if os.environ.get("TEAM_PLATFORM_RUNTIME_MODE", "test").strip().lower()
            in {"prod", "production", "live", "ops"}
            else "test"
        )
    )
    streaming_poll_interval_seconds: float = field(
        default_factory=lambda: _env_float("TEAM_PLATFORM_STREAM_POLL_INTERVAL_SECONDS", 2.0)
    )
    streaming_batch_limit: int = field(
        default_factory=lambda: _env_int("TEAM_PLATFORM_STREAM_BATCH_LIMIT", 100)
    )
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.environ.get(
            "TEAM_PLATFORM_KAFKA_BOOTSTRAP_SERVERS",
            "127.0.0.1:9092",
        )
    )
    kafka_consumer_group_id: str = field(
        default_factory=lambda: os.environ.get(
            "TEAM_PLATFORM_KAFKA_CONSUMER_GROUP_ID",
            "team-data-platform-stream",
        )
    )
    kafka_auto_offset_reset: str = field(
        default_factory=lambda: os.environ.get(
            "TEAM_PLATFORM_KAFKA_AUTO_OFFSET_RESET",
            "earliest",
        )
    )
    kafka_poll_timeout_ms: int = field(
        default_factory=lambda: _env_int("TEAM_PLATFORM_KAFKA_POLL_TIMEOUT_MS", 3000)
    )
    kafka_request_timeout_ms: int = field(
        default_factory=lambda: _env_int("TEAM_PLATFORM_KAFKA_REQUEST_TIMEOUT_MS", 10000)
    )
    kafka_read_max_records: int = field(
        default_factory=lambda: _env_int("TEAM_PLATFORM_KAFKA_READ_MAX_RECORDS", 500)
    )
    kafka_security_protocol: str = field(
        default_factory=lambda: os.environ.get(
            "TEAM_PLATFORM_KAFKA_SECURITY_PROTOCOL",
            "PLAINTEXT",
        ).strip()
    )
    kafka_sasl_mechanism: str = field(
        default_factory=lambda: os.environ.get(
            "TEAM_PLATFORM_KAFKA_SASL_MECHANISM",
            "",
        ).strip()
    )

    kafka_sasl_username: str = field(
        default_factory=lambda: os.environ.get(
            "TEAM_PLATFORM_KAFKA_SASL_USERNAME",
            "",
        ).strip()
    )
    kafka_sasl_password: str = field(
        default_factory=lambda: os.environ.get(
            "TEAM_PLATFORM_KAFKA_SASL_PASSWORD",
            "",
        ).strip()
    )
    kafka_ssl_cafile: str = field(
        default_factory=lambda: os.environ.get(
            "TEAM_PLATFORM_KAFKA_SSL_CAFILE",
            "",
        ).strip()
    )
    pii_secret: str = field(
        default_factory=lambda: os.environ.get(
            "TEAM_PLATFORM_SECRET",
            "team-platform-reference-secret",
        )
    )

    source_keys: dict[str, str] = field(
        default_factory=lambda: {
            "mail": "source-mail-key",
            "schedule": "source-schedule-key",
            "payment": "source-payment-key",
            "board": "source-board-key",
        }
    )
    source_fetch_urls: dict[str, str] = field(
        default_factory=lambda: {
            "mail":     os.environ.get("TEAM_PLATFORM_SOURCE_FETCH_URL_MAIL",     "http://mail-service:8000"),
            "schedule": os.environ.get("TEAM_PLATFORM_SOURCE_FETCH_URL_SCHEDULE", "http://schedule-service:8001"),
            "payment":  os.environ.get("TEAM_PLATFORM_SOURCE_FETCH_URL_PAYMENT",  "http://payment-service:8002"),
            "board":    os.environ.get("TEAM_PLATFORM_SOURCE_FETCH_URL_BOARD",    "http://board-service:8003"),
        }
    )
    source_fetch_timeout_seconds: int = field(
        default_factory=lambda: _env_int("TEAM_PLATFORM_SOURCE_FETCH_TIMEOUT_SECONDS", 10)
    )
    source_fetch_api_key: str = field(
        default_factory=lambda: os.environ.get("TEAM_PLATFORM_SOURCE_FETCH_API_KEY", "").strip()
    )
    consumer_keys: dict[str, str] = field(
        default_factory=lambda: {
            "search": "consumer-search-key",
            "mobile": "consumer-mobile-key",
            "rag": "consumer-rag-key",
            "graph": "consumer-graph-key",
            "analytics": "consumer-analytics-key",
        }
    )
    consumer_purposes: dict[str, set[str]] = field(
        default_factory=lambda: {
            "search": {"search"},
            "mobile": {"mobile"},
            "rag": {"rag"},
            "graph": {"graph"},
            "analytics": {"analytics"},
        }
    )

    @property
    def raw_dir(self) -> Path:
        return self.base_dir / "data_lake" / "raw"

    @property
    def standardized_dir(self) -> Path:
        return self.base_dir / "data_lake" / "standardized"

    @property
    def serving_dir(self) -> Path:
        return self.base_dir / "data_lake" / "serving"

    @property
    def iceberg_catalog_dir(self) -> Path:
        return self.base_dir / "catalog" / "iceberg"

    @property
    def knox_dir(self) -> Path:
        return self.base_dir / "sources" / "knox"

    @property
    def inbound_dir(self) -> Path:
        return self.base_dir / "mq" / "inbound"

    @property
    def outbound_dir(self) -> Path:
        return self.base_dir / "mq" / "outbound"

    @property
    def state_dir(self) -> Path:
        return self.base_dir / "state"

    def ensure_directories(self) -> None:
        for path in (
                self.raw_dir,
                self.standardized_dir,
                self.serving_dir,
                self.iceberg_catalog_dir,
                self.knox_dir,
                self.inbound_dir,
                self.outbound_dir,
                self.state_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def is_test_mode(self) -> bool:
        return self.runtime_mode == "test"

    def is_production_mode(self) -> bool:
        return self.runtime_mode == "production"
