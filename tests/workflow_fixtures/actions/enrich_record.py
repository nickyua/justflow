"""Enrich a record with deterministic fixture data."""

from __future__ import annotations

from typing import Any

from justflow.sdk.base_action import BaseAction

FIXTURE_SCORE = 0.42


class EnrichRecord(BaseAction):
    async def enrich_record(self, input: Any) -> dict[str, Any]:
        return {
            **input,
            "enriched": True,
            "score": FIXTURE_SCORE,
        }
