"""Public data contracts for the product-onboarding example."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool


class ExampleContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class OnboardingRequest(ExampleContract):
    customer_ref: str = Field(pattern=r"^customer-[a-z0-9-]+$", max_length=64)
    plan: Literal["starter", "growth"]
    simulate_provisioning_failure: StrictBool = False


class ProvisionedWorkspace(ExampleContract):
    workspace_id: str = Field(pattern=r"^workspace-[a-z0-9-]+$", max_length=80)
    plan: Literal["starter", "growth"]
    state: Literal["provisioned"] = "provisioned"
