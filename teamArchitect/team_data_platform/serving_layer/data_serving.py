from __future__ import annotations

from typing import Any

from ..processing_layer.spark import SparkSqlService
from .policy import AccessPolicyService


class DataServingService:
    def __init__(
        self,
        spark_sql_service: SparkSqlService,
        access_policy_service: AccessPolicyService,
    ) -> None:
        self.spark_sql_service = spark_sql_service
        self.access_policy_service = access_policy_service

    def get_record(self, consumer_id: str, purpose: str, record_id: str) -> dict[str, Any]:
        document = self.spark_sql_service.get_record(consumer_id, record_id)
        if document is None:
            raise KeyError(f"Record not found: {record_id}")
        return self.access_policy_service.authorize(consumer_id, purpose, document)

    def search(
        self,
        consumer_id: str,
        purpose: str,
        query_text: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        return [
            self.access_policy_service.authorize(consumer_id, purpose, document)
            for document in self.spark_sql_service.search(consumer_id, query_text, limit)
        ]