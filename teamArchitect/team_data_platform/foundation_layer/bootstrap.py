from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..collection_layer.ingestion import IngestionService
from ..collection_layer.knox import KnoxSourceService
from ..collection_layer.queues import InboundQueue, OutboundQueue
from ..collection_layer.source_fetch_client import HttpSourceFetchClient, SourceFetchClient
from ..collection_layer.source_publisher import SourceEventPublisherService
from ..entry_gateway_layer.auth import GatewayAuthService
from ..processing_layer.deidentification import DeidentificationService
from ..processing_layer.projections import ProjectionBuilder
from ..processing_layer.spark import SparkPipeline, SparkSqlService
from ..processing_layer.spark_streaming import SparkStreamingService
from ..processing_layer.standardization import StandardizationService
from ..serving_layer.data_serving import DataServingService
from ..serving_layer.adapter_registry import ConsumerAdapterRegistry
from ..serving_layer.policy import AccessPolicyService
from ..storage_layer.catalog import IcebergCatalogRepository
from ..storage_layer.lake import S3DataLakeRepository
from ..storage_layer.state import StateRepository
from ..storage_layer.zones import (
    RawZoneRepository,
    ServingZoneRepository,
    StandardizedZoneRepository,
)
from .config import PlatformConfig


@dataclass(slots=True)
class Application:
    config: PlatformConfig
    auth_service: GatewayAuthService
    knox_service: KnoxSourceService
    source_fetch_client: SourceFetchClient
    source_publisher_service: SourceEventPublisherService
    spark_pipeline: SparkPipeline
    spark_streaming_service: SparkStreamingService
    ingestion_service: IngestionService
    data_serving_service: DataServingService
    query_service: DataServingService
    inbound_queue: InboundQueue
    outbound_queue: OutboundQueue
    iceberg_catalog: IcebergCatalogRepository
    data_lake: S3DataLakeRepository

def build_application(base_dir: Path | None = None) -> Application:
    config = PlatformConfig(base_dir=base_dir or PlatformConfig().base_dir)
    config.ensure_directories()

    inbound_queue = InboundQueue(config=config)
    outbound_queue = OutboundQueue(config=config)
    raw_zone = RawZoneRepository(config.raw_dir)
    standardized_zone = StandardizedZoneRepository(config.standardized_dir)
    serving_zone = ServingZoneRepository(config.serving_dir)
    data_lake = S3DataLakeRepository(raw_zone, standardized_zone, serving_zone)
    iceberg_catalog = IcebergCatalogRepository(config.iceberg_catalog_dir)
    knox_service = KnoxSourceService(config.knox_dir)
    state_repository = StateRepository(config.state_dir)
    standardization_service = StandardizationService()
    deidentification_service = DeidentificationService(config.pii_secret)
    projection_builder = ProjectionBuilder()
    adapter_registry = ConsumerAdapterRegistry.from_yaml()
    access_policy_service = AccessPolicyService(adapter_registry)

    source_fetch_client: SourceFetchClient = HttpSourceFetchClient(
        source_fetch_urls=config.source_fetch_urls,
        timeout_seconds=config.source_fetch_timeout_seconds,
        api_key=config.source_fetch_api_key,
    )

    spark_pipeline = SparkPipeline(
        source_fetch_client=source_fetch_client,
        data_lake=data_lake,
        iceberg_catalog=iceberg_catalog,
        standardization_service=standardization_service,
        deidentification_service=deidentification_service,
        projection_builder=projection_builder,
    )
    data_serving_service = DataServingService(
        SparkSqlService(data_lake),
        access_policy_service,
    )
    source_publisher_service = SourceEventPublisherService(config, inbound_queue)
    spark_streaming_service = SparkStreamingService(
        inbound_queue=inbound_queue,
        outbound_queue=outbound_queue,
        state_repository=state_repository,
        spark_pipeline=spark_pipeline,
    )
    ingestion_service = IngestionService(
        source_publisher_service=source_publisher_service,
        spark_streaming_service=spark_streaming_service,
    )

    return Application(
        config=config,
        auth_service=GatewayAuthService(config, adapter_registry),
        knox_service=knox_service,
        source_fetch_client=source_fetch_client,
        source_publisher_service=source_publisher_service,
        spark_pipeline=spark_pipeline,
        spark_streaming_service=spark_streaming_service,
        ingestion_service=ingestion_service,
        data_serving_service=data_serving_service,
        query_service=data_serving_service,
        inbound_queue=inbound_queue,
        outbound_queue=outbound_queue,
        iceberg_catalog=iceberg_catalog,
        data_lake=data_lake,
    )



