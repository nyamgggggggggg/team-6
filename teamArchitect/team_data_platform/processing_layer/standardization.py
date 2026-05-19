from __future__ import annotations

import re
from email.header import decode_header
from typing import Any

from ..foundation_layer.models import ChangeEvent


TAG_PATTERN = re.compile(r"<[^>]+>")


def decode_mime_words(value: str) -> str:
    parts = []
    for chunk, encoding in decode_header(value):
        if isinstance(chunk, bytes):
            parts.append(chunk.decode(encoding or "utf-8", errors="replace"))
        else:
            parts.append(chunk)
    return "".join(parts)


def strip_html(value: str) -> str:
    no_tags = TAG_PATTERN.sub(" ", value)
    return " ".join(no_tags.split())


def extract_attachment_text(payload: dict[str, Any]) -> str:
    texts: list[str] = []
    for attachment in payload.get("attachments", []):
        text = attachment.get("text")
        if text:
            texts.append(str(text))
    return "\n".join(texts)


class StandardizationService:
    def standardize(self, event: ChangeEvent) -> dict[str, Any]:
        payload = event.payload
        subject = decode_mime_words(str(payload.get("subject", payload.get("title", ""))))
        body_html = str(payload.get("body_html", ""))
        body_text = str(payload.get("body_text", payload.get("body", "")))
        attachment_text = extract_attachment_text(payload)
        cleaned_body = strip_html(body_html) if body_html else body_text

        combined_text = "\n".join(
            value for value in (subject, cleaned_body, attachment_text) if value
        )

        return {
            "record_id": event.record_id,
            "source_system": event.source_system,
            "entity_type": event.entity_type,
            "version": event.version,
            "occurred_at": event.occurred_at,
            "status": payload.get("status", "OPEN"),
            "subject": subject,
            "body_text": cleaned_body,
            "attachment_text": attachment_text,
            "combined_text": combined_text,
            "tags": list(payload.get("tags", [])),
            "user_name": payload.get("user_name", ""),
            "email": payload.get("email", ""),
            "phone": payload.get("phone", payload.get("contact", "")),
            "metadata": {
                "has_attachments": bool(payload.get("attachments")),
                "attachment_count": len(payload.get("attachments", [])),
            },
        }