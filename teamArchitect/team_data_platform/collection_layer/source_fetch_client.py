from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

@runtime_checkable
class SourceFetchClient(Protocol):

    def fetch_record(
        self,
        source_system: str,
        record_id: str,
    ) -> dict[str, Any] | None:
        """공급 시스템에서 레코드 원본 데이터를 조회한다.

        Args:
            source_system: 공급 시스템 식별자 (예: "mail", "schedule")
            record_id: 조회할 레코드 ID

        Returns:
            원본 레코드 dict, 레코드가 없으면 None.
        """
        ...

class HttpSourceFetchClient:
    DEFAULT_TIMEOUT_SECONDS: int = 10

    def __init__(
        self,
        source_fetch_urls: dict[str, str],
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        api_key: str = "",
    ) -> None:
        self._base_urls: dict[str, str] = {
            system: url.rstrip("/")
            for system, url in source_fetch_urls.items()
        }
        self._timeout = timeout_seconds
        self._api_key = api_key

    def fetch_record(
        self,
        source_system: str,
        record_id: str,
    ) -> dict[str, Any] | None:
        base_url = self._base_urls.get(source_system)
        if not base_url:
            logger.warning(
                "source_fetch_url not configured for source_system=%r; "
                "falling back to event payload.",
                source_system,
            )
            return None

        url = f"{base_url}/records/{record_id}"
        logger.info(
            "[SourceFetchClient] → HTTP GET %s  (source_system=%r, record_id=%r)",
            url,
            source_system,
            record_id,
        )

        req = urllib.request.Request(url, method="GET")
        if self._api_key:
            req.add_header("Authorization", f"Bearer {self._api_key}")
        req.add_header("Accept", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                body = resp.read().decode("utf-8")
                data: dict[str, Any] = json.loads(body)
                logger.info(
                    "[SourceFetchClient] ← 200 OK  source_system=%r, record_id=%r",
                    source_system,
                    record_id,
                )
                return data
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                logger.info(
                    "[SourceFetchClient] ← 404 Not Found  source_system=%r, record_id=%r",
                    source_system,
                    record_id,
                )
                return None
            logger.error(
                "[SourceFetchClient] HTTP error %d fetching record_id=%r from %r: %s",
                exc.code,
                record_id,
                source_system,
                exc,
            )
            raise
        except urllib.error.URLError as exc:
            logger.error(
                "[SourceFetchClient] Connection error fetching record_id=%r from %r: %s",
                record_id,
                source_system,
                exc,
            )
            raise


