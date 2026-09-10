"""Public data contracts for the scheduled-reporting example."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class ExampleContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class ReportRequest(ExampleContract):
    report_date: date


class ReportArtifact(ExampleContract):
    report_id: str = Field(pattern=r"^daily-[0-9]{4}-[0-9]{2}-[0-9]{2}$")
    title: str = Field(min_length=1, max_length=80)
    row_count: int = Field(ge=0, le=10_000)
