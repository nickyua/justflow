"""Validate one item during a for-each step."""

from __future__ import annotations

from typing import Any

from justflow.sdk.base_action import BaseAction

ALLOWED_ITEMS = frozenset({"alpha", "beta"})


class CheckItem(BaseAction):
    async def check_item(self, input: Any) -> dict[str, Any]:
        item = self.globals.get("item", input)
        return {
            "item": item,
            "is_valid": item in ALLOWED_ITEMS if isinstance(item, str) else False,
        }
