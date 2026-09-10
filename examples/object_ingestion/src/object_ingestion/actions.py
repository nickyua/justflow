"""Deterministic local actions for the object-ingestion example."""

from __future__ import annotations

from justflow.sdk.base_action import BaseAction
from object_ingestion.contracts import ObjectReference, ObjectResult


class LocalObjectService(BaseAction):
    async def inspect_object(self, input: object) -> dict[str, object]:
        reference = ObjectReference.model_validate(input)
        return reference.model_dump(mode="json")

    async def archive_object(self, input: object) -> dict[str, object]:
        reference = ObjectReference.model_validate(input)
        result = ObjectResult(
            source_key=reference.key,
            archive_key=f"archive/{reference.key.removeprefix('incoming/')}",
            state="archived",
        )
        return result.model_dump(mode="json")
