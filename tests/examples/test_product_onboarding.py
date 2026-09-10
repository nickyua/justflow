"""Behavior checks for the product-onboarding example package."""

from __future__ import annotations

import pytest
from product_onboarding.actions import LocalOnboardingService

REQUEST = {
    "customer_ref": "customer-demo",
    "plan": "starter",
    "simulate_provisioning_failure": False,
}


async def test_local_onboarding_service_provisions_and_finalizes() -> None:
    service = LocalOnboardingService()

    validated = await service.validate_request(REQUEST)
    workspace = await service.provision_workspace(REQUEST)
    finalized = await service.finalize_onboarding(workspace)

    assert validated == REQUEST
    assert workspace == {
        "workspace_id": "workspace-demo",
        "plan": "starter",
        "state": "provisioned",
    }
    assert finalized == {"workspace_id": "workspace-demo", "state": "ready"}


async def test_local_onboarding_service_exposes_synthetic_failure_and_compensation() -> None:
    failed_request = {**REQUEST, "simulate_provisioning_failure": True}
    service = LocalOnboardingService()

    with pytest.raises(RuntimeError, match="Synthetic provisioning failure"):
        await service.provision_workspace(failed_request)

    assert await service.record_compensation(failed_request) == {
        "customer_ref": "customer-demo",
        "state": "compensated",
    }
