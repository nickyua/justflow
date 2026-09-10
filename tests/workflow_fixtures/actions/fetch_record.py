"""Return deterministic source data for workflow tests."""

from __future__ import annotations

from typing import Any

from justflow.sdk.base_action import BaseAction

FIXTURE_ITEMS = ["alpha", "beta", "gamma"]


class FetchRecord(BaseAction):
    async def fetch_record(self, input: Any) -> dict[str, Any]:
        return {
            "record_id": "R001",
            "name": "Example record",
            "items": FIXTURE_ITEMS,
        }
