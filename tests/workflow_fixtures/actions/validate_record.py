"""Validate a fetched record."""

from __future__ import annotations

from typing import Any

from justflow.sdk.base_action import BaseAction

ALLOWED_ITEMS = frozenset({"alpha", "beta"})


class ValidateRecord(BaseAction):
    async def validate_record(self, input: Any) -> dict[str, Any]:
        items = input.get("items", [])
        allowed = [item for item in items if item in ALLOWED_ITEMS]
        return {
            "allowed": allowed,
            "disallowed": [item for item in items if item not in ALLOWED_ITEMS],
            "is_valid": bool(allowed),
        }
