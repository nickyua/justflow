"""Public contracts for the host-application example."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class HostExampleContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class GreetingRequest(HostExampleContract):
    subject_ref: str = Field(pattern=r"^subject-[a-z0-9-]+$", max_length=64)


class GreetingResult(HostExampleContract):
    subject_ref: str = Field(pattern=r"^subject-[a-z0-9-]+$", max_length=64)
    message: str = Field(min_length=1, max_length=160)
    state: Literal["rendered"] = "rendered"
