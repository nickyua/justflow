"""Direct action service using the host-owned resource contract."""

from __future__ import annotations

from host_application.contracts import GreetingRequest, GreetingResult
from justflow.resources import ConfigReader, ResourceError
from justflow.sdk.base_action import BaseAction

TENANT_SETTINGS_RESOURCE = "tenant_settings"
GREETING_PREFIX_KEY = "greeting_prefix"


class HostGreetingService(BaseAction):
    async def render_greeting(self, input: object) -> dict[str, object]:
        request = GreetingRequest.model_validate(input)
        resource = self.resources[TENANT_SETTINGS_RESOURCE]
        if not isinstance(resource, ConfigReader):
            raise ResourceError("Tenant settings do not provide the configuration capability")
        prefix = resource.get(GREETING_PREFIX_KEY)
        if not isinstance(prefix, str) or not prefix:
            raise ResourceError("Greeting prefix is unavailable")
        return GreetingResult(
            subject_ref=request.subject_ref,
            message=f"{prefix}, {request.subject_ref}",
        ).model_dump(mode="json")
