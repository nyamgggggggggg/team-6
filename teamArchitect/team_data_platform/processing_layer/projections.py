from __future__ import annotations

from typing import Any


class ProjectionBuilder:
    def supported_consumers(self) -> list[str]:
        return ["analytics", "graph", "mobile", "rag", "search"]

    def build(self, record: dict[str, Any]) -> dict[str, dict[str, Any]]:
        preview = record.get("body_text", "")[:120]
        base = {
            "record_id": record["record_id"],
            "source_system": record["source_system"],
            "occurred_at": record["occurred_at"],
            "version": record["version"],
        }

        return {
            "search": {
                **base,
                "subject": record.get("subject", ""),
                "preview": preview,
                "body_text": record.get("body_text", ""),
                "attachment_text": record.get("attachment_text", ""),
                "user_name_masked": record.get("user_name_masked", ""),
                "status": record.get("status", ""),
                "tags": record.get("tags", []),
            },
            "mobile": {
                **base,
                "subject": record.get("subject", ""),
                "preview": preview,
                "status": record.get("status", ""),
                "user_name_masked": record.get("user_name_masked", ""),
                "phone_masked": record.get("phone_masked", ""),
            },
            "rag": {
                **base,
                "subject": record.get("subject", ""),
                "context_text": record.get("combined_text", ""),
                "attachment_text": record.get("attachment_text", ""),
                "user_name_masked": record.get("user_name_masked", ""),
                "tags": record.get("tags", []),
            },
            "graph": {
                **base,
                "graph_nodes": [
                    {"type": "source_system", "value": record.get("source_system", "")},
                    {"type": "status", "value": record.get("status", "")},
                ],
                "graph_edges": [
                    {
                        "from": record["record_id"],
                        "to": record.get("source_system", ""),
                        "type": "generated_by",
                    }
                ],
                "tags": record.get("tags", []),
            },
            "analytics": {
                **base,
                "status": record.get("status", ""),
                "tags": record.get("tags", []),
                "user_name_token": record.get("user_name_token", ""),
                "email_token": record.get("email_token", ""),
                "phone_token": record.get("phone_token", ""),
                "has_attachments": record.get("metadata", {}).get("has_attachments", False),
            },
        }