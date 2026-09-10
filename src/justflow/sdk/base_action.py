"""BaseAction - base class for all action handlers across services."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from justflow.resources.registry import ResourceGrant
from justflow.sdk.service_context import ServiceCallContext


@dataclass
class ActionContext:
    request_id: str
    flow_name: str
    step_name: str
    action: str
    service_call: ServiceCallContext = field(default_factory=ServiceCallContext)


class BaseAction:
    """Base class for all service action handlers.

    Provides consistent access to globals, resources, logger, and request context.
    Subclasses define named methods matching the action field in step definitions.
    """

    def __init__(
        self,
        globals: dict[str, Any] | None = None,
        resources: Mapping[str, Any] | None = None,
        context: ActionContext | None = None,
    ):
        self.globals: dict[str, Any] = globals or {}
        self.resources: Mapping[str, object] = ResourceGrant(resources or {})
        self.context: ActionContext = context or ActionContext(
            request_id="", flow_name="", step_name="", action=""
        )
        self.log = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")

    def get_resource(self, name: str) -> Any:
        return self.resources[name]
