from __future__ import annotations

import hashlib
import hmac
import re
from typing import Any


EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_PATTERN = re.compile(r"\b\d{2,3}[- ]?\d{3,4}[- ]?\d{4}\b")


def _mask_name(value: str) -> str:
    value = value.strip()
    if len(value) <= 1:
        return "*"
    if len(value) == 2:
        return value[0] + "*"
    return value[0] + "*" * (len(value) - 2) + value[-1]

def _mask_email(value: str) -> str:
    if "@" not in value:
        return "***"
    local, domain = value.split("@", 1)
    if len(local) <= 2:
        local_masked = local[0] + "*"
    else:
        local_masked = local[:2] + "*" * max(1, len(local) - 2)
    return f"{local_masked}@{domain}"


def _mask_phone(value: str) -> str:
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) < 7:
        return "***"
    return f"{digits[:3]}****{digits[-4:]}"


class DeidentificationService:
    def __init__(self, secret: str) -> None:
        self.secret = secret.encode("utf-8")

    def _tokenize(self, value: str) -> str:
        digest = hmac.new(self.secret, value.encode("utf-8"), hashlib.sha256).hexdigest()
        return digest[:24]

    def apply(self, standardized: dict[str, Any]) -> dict[str, Any]:
        user_name = str(standardized.get("user_name", ""))
        email = str(standardized.get("email", ""))
        phone = str(standardized.get("phone", ""))

        masked = dict(standardized)
        masked["user_name_masked"] = _mask_name(user_name) if user_name else ""
        masked["email_masked"] = _mask_email(email) if email else ""
        masked["phone_masked"] = _mask_phone(phone) if phone else ""
        masked["user_name_token"] = self._tokenize(user_name) if user_name else ""
        masked["email_token"] = self._tokenize(email) if email else ""
        masked["phone_token"] = self._tokenize(phone) if phone else ""

        replacements = {
            user_name: masked["user_name_masked"],
            email: masked["email_masked"],
            phone: masked["phone_masked"],
        }
        for field_name in ("subject", "body_text", "attachment_text", "combined_text"):
            value = str(masked.get(field_name, ""))
            for source_value, masked_value in replacements.items():
                if source_value:
                    value = value.replace(source_value, masked_value)
            value = EMAIL_PATTERN.sub("[EMAIL_MASKED]", value)
            value = PHONE_PATTERN.sub("[PHONE_MASKED]", value)
            masked[field_name] = value

        masked.pop("user_name", None)
        masked.pop("email", None)
        masked.pop("phone", None)
        return masked

