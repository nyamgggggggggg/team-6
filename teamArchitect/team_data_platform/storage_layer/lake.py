from __future__ import annotations

from typing import Any

from .zones import RawZoneRepository, ServingZoneRepository, StandardizedZoneRepository


class S3DataLakeRepository:
    """File-backed data lake facade for raw, standardized, and serving zones."""

    def __init__(
        self,
        raw_zone: RawZoneRepository,
        standardized_zone: StandardizedZoneRepository,
        serving_zone: ServingZoneRepository,
    ) -> None:
        self.raw_zone = raw_zone
        self.standardized_zone = standardized_zone
        self.serving_zone = serving_zone

    def store_raw_source_record(
        self,
        source_system: str,
        record_id: str,
        document: dict[str, Any],
    ) -> None:
        self.raw_zone.upsert(source_system, record_id, document)

    def store_standardized_record(self, record_id: str, document: dict[str, Any]) -> None:
        self.standardized_zone.upsert(record_id, document)

    def delete_standardized_record(self, record_id: str) -> None:
        self.standardized_zone.delete(record_id)

    def store_serving_views(
        self,
        record_id: str,
        projections: dict[str, dict[str, Any]],
    ) -> None:
        self.serving_zone.upsert_many(record_id, projections)

    def get_serving_view(self, consumer_id: str, record_id: str) -> dict[str, Any] | None:
        return self.serving_zone.get(consumer_id, record_id)

    def list_serving_views(self, consumer_id: str) -> list[dict[str, Any]]:
        return self.serving_zone.list(consumer_id)

    def delete_serving_views(self, record_id: str) -> None:
        self.serving_zone.delete_all(record_id)