from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..collection_layer.knox import KnoxSourceService
from ..foundation_layer.models import ChangeEvent, utcnow_iso
from ..storage_layer.catalog import IcebergCatalogRepository
from ..storage_layer.lake import S3DataLakeRepository
from .deidentification import DeidentificationService
from .projections import ProjectionBuilder
from .standardization import StandardizationService


@dataclass(slots=True)
class SparkProcessResult:
    status: str
    record_id: str
    event_id: str
    serving_targets: list[str]
    source_mode: str


class SparkPipeline:
    """Processing component that pulls source records and materializes lake views."""

    def __init__(
        self,
        knox_service: KnoxSourceService,
        data_lake: S3DataLakeRepository,
        iceberg_catalog: IcebergCatalogRepository,
        standardization_service: StandardizationService,
        deidentification_service: DeidentificationService,
        projection_builder: ProjectionBuilder,
    ) -> None:
        self.knox_service = knox_service
        self.data_lake = data_lake
        self.iceberg_catalog = iceberg_catalog
        self.standardization_service = standardization_service
        self.deidentification_service = deidentification_service
        self.projection_builder = projection_builder

    def process(self, event: ChangeEvent) -> SparkProcessResult:
        source_payload, source_mode = self._resolve_source_payload(event)
        source_event = ChangeEvent(
            event_id=event.event_id,
            source_system=event.source_system,
            entity_type=event.entity_type,
            record_id=event.record_id,
            operation=event.operation,
            version=event.version,
            occurred_at=event.occurred_at,
            payload=source_payload,
        )
        self.data_lake.store_raw_source_record(
            source_system=event.source_system,
            record_id=event.record_id,
            document={
                **source_event.to_dict(),
                "stored_at": utcnow_iso(),
                "deleted": False,
                "source_mode": source_mode,
            },
        )

        standardized = self.standardization_service.standardize(source_event)
        deidentified = self.deidentification_service.apply(standardized)
        self.data_lake.store_standardized_record(event.record_id, deidentified)

        projections = self.projection_builder.build(deidentified)
        self.data_lake.store_serving_views(event.record_id, projections)

        serving_targets = sorted(projections.keys())
        self.iceberg_catalog.upsert_entry(
            event.record_id,
            {
                "record_id": event.record_id,
                "source_system": event.source_system,
                "entity_type": event.entity_type,
                "current_version": event.version,
                "serving_targets": serving_targets,
                "source_mode": source_mode,
                "catalog_updated_at": utcnow_iso(),
            },
        )

        return SparkProcessResult(
            status="processed",
            record_id=event.record_id,
            event_id=event.event_id,
            serving_targets=serving_targets,
            source_mode=source_mode,
        )

    def delete(self, event: ChangeEvent) -> SparkProcessResult:
        existing_entry = self.iceberg_catalog.get_entry(event.record_id) or {}
        serving_targets = sorted(
            existing_entry.get(
                "serving_targets",
                self.projection_builder.supported_consumers(),
            )
        )

        self.data_lake.store_raw_source_record(
            source_system=event.source_system,
            record_id=event.record_id,
            document={
                **event.to_dict(),
                "stored_at": utcnow_iso(),
                "deleted": True,
                "source_mode": "delete-tombstone",
            },
        )
        self.data_lake.delete_standardized_record(event.record_id)
        self.data_lake.delete_serving_views(event.record_id)
        self.iceberg_catalog.delete_entry(event.record_id)

        return SparkProcessResult(
            status="deleted",
            record_id=event.record_id,
            event_id=event.event_id,
            serving_targets=serving_targets,
            source_mode="delete-tombstone",
        )

    def _resolve_source_payload(self, event: ChangeEvent) -> tuple[dict[str, Any], str]:
        pulled_payload = self.knox_service.pull_record(event.source_system, event.record_id)
        if pulled_payload is not None:
            return pulled_payload, "knox-pull"
        return dict(event.payload), "event-payload"

class SparkSqlService:
    """Query facade that models Data Serving pulling through Spark SQL."""

    def __init__(self, data_lake: S3DataLakeRepository) -> None:
        self.data_lake = data_lake

    def get_record(self, consumer_id: str, record_id: str) -> dict[str, Any] | None:
        return self.data_lake.get_serving_view(consumer_id, record_id)

    def search(
        self,
        consumer_id: str,
        query_text: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        lowered = query_text.lower().strip()
        for document in self.data_lake.list_serving_views(consumer_id):
            haystack = " ".join(str(value) for value in document.values()).lower()
            if not lowered or lowered in haystack:
                matches.append(document)
            if len(matches) >= limit:
                break
        return matches