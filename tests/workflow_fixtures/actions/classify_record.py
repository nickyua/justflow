"""Classify an enriched record."""

from __future__ import annotations

from typing import Any

from justflow.sdk.base_action import BaseAction

LOW_SCORE_THRESHOLD = 0.5
ACCEPTANCE_THRESHOLD = 0.7


class ClassifyRecord(BaseAction):
    async def classify_record(self, input: Any) -> dict[str, Any]:
        score = input.get("score", 0)
        return {
            "score": score,
            "category": "low" if score < LOW_SCORE_THRESHOLD else "high",
            "accepted": score < ACCEPTANCE_THRESHOLD,
        }
