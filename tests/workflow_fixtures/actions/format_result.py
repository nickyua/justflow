"""Format the final workflow result."""

from __future__ import annotations

from typing import Any

from justflow.sdk.base_action import BaseAction


class FormatResult(BaseAction):
    async def format_result(self, input: Any) -> dict[str, Any]:
        return {
            "summary": (
                f"Record {input.get('record_id', '?')} - "
                f"Category: {input.get('category', '?')} - "
                f"Accepted: {input.get('accepted', False)}"
            ),
            "details": input,
        }
