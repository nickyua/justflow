"""Public data contracts for the object-ingestion example."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ExampleContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class ObjectReference(ExampleContract):
    bucket: str = Field(pattern=r"^example-[a-z0-9-]+$", max_length=63)
    key: str = Field(pattern=r"^incoming/.+\.json$", max_length=256)
    sequencer: str = Field(min_length=1, max_length=128)
    size: int | None = Field(default=None, ge=0)


class ObjectResult(ExampleContract):
    source_key: str = Field(pattern=r"^incoming/.+\.json$", max_length=256)
    archive_key: str = Field(pattern=r"^archive/.+\.json$", max_length=256)
    state: str = Field(pattern=r"^archived$")
