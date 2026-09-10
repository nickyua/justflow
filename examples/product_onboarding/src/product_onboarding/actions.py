"""Credential-free local service used by the product-onboarding example."""

from __future__ import annotations

from justflow.sdk.base_action import BaseAction
from product_onboarding.contracts import (
    OnboardingRequest,
    ProvisionedWorkspace,
)


class LocalOnboardingService(BaseAction):
    async def validate_request(self, input: object) -> dict[str, object]:
        request = OnboardingRequest.model_validate(input)
        return request.model_dump(mode="json")

    async def provision_workspace(self, input: object) -> dict[str, object]:
        request = OnboardingRequest.model_validate(input)
        if request.simulate_provisioning_failure:
            raise RuntimeError("Synthetic provisioning failure")
        workspace = ProvisionedWorkspace(
            workspace_id=f"workspace-{request.customer_ref.removeprefix('customer-')}",
            plan=request.plan,
        )
        return workspace.model_dump(mode="json")

    async def finalize_onboarding(self, input: object) -> dict[str, object]:
        workspace = ProvisionedWorkspace.model_validate(input)
        return {
            "workspace_id": workspace.workspace_id,
            "state": "ready",
        }

    async def record_compensation(self, input: object) -> dict[str, object]:
        request = OnboardingRequest.model_validate(input)
        return {
            "customer_ref": request.customer_ref,
            "state": "compensated",
        }
