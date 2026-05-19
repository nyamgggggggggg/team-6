from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from team_data_platform.foundation_layer.bootstrap import build_application


def main() -> None:
    with TemporaryDirectory() as temp_dir:
        app = build_application(Path(temp_dir))

        app.knox_service.register_record(
            "mail",
            "mail-100",
            {
                "subject": "Payment complaint",
                "body_html": "<p>Alice alice@example.com 01012345678</p>",
                "user_name": "Alice",
                "email": "alice@example.com",
                "phone": "01012345678",
                "status": "OPEN",
                "tags": ["complaint", "payment"],
                "attachments": [{"filename": "memo.txt", "text": "payment memo"}],
            },
        )

        publish_result = app.source_publisher_service.publish_change_event(
            source_system="mail",
            source_key="source-mail-key",
            payload={
                "event_id": "evt-001",
                "source_system": "mail",
                "entity_type": "mail",
                "record_id": "mail-100",
                "operation": "UPSERT",
                "version": 1,
                "occurred_at": "2026-05-11T09:00:00Z",
                "payload": {"subject": "stale message bus payload"},
            },
        )
        print("SOURCE PUBLISH RESULT")
        print(json.dumps(publish_result, ensure_ascii=False, indent=2))

        consume_result = app.spark_streaming_service.consume_topic("mail", limit=100)
        print("\nSPARK CONSUME RESULT")
        print(json.dumps(consume_result, ensure_ascii=False, indent=2))

        query_result = app.query_service.get_record(
            consumer_id="search",
            purpose="search",
            record_id="mail-100",
        )
        print("\nSEARCH VIEW")
        print(json.dumps(query_result, ensure_ascii=False, indent=2))

        analytics_result = app.query_service.get_record(
            consumer_id="analytics",
            purpose="analytics",
            record_id="mail-100",
        )
        print("\nANALYTICS VIEW")
        print(json.dumps(analytics_result, ensure_ascii=False, indent=2))

        print("\nOUTBOUND SEARCH TOPIC")
        print(json.dumps(app.outbound_queue.read("consumer-search"), ensure_ascii=False, indent=2))

        print("\nDATA ROOT")
        print(temp_dir)

        app.inbound_queue.close()
        app.outbound_queue.close()

if __name__ == "__main__":
    main()
