"""Direct transport - in-process function calls via class import + method dispatch."""

from __future__ import annotations

import asyncio
import importlib
import logging
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, NoReturn

from pydantic import ConfigDict, Field

from justflow.config.models import MAX_PATH_LENGTH
from justflow.resources.base import ResourceAccessError, ResourceOperationError
from justflow.resources.registry import ResourceGrant
from justflow.sdk.base_action import ActionContext, BaseAction
from justflow.transports.base import (
    ActionDispatchError,
    Completed,
    DispatchTimeoutError,
    ServiceError,
    StrictTransportConfig,
    TransportError,
    TransportRequest,
)

logger = logging.getLogger(__name__)
STARTUP_VALIDATION_TIMEOUT_SECONDS = 1


class DirectTransportConfig(StrictTransportConfig):
    class_: str = Field(alias="class", min_length=1, max_length=MAX_PATH_LENGTH)

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class DirectTransport:
    """Direct transport calls action handlers in-process.

    Imports the class specified in the service config, instantiates it,
    and dispatches to the method matching the action name.
    """

    def __init__(
        self,
        config: DirectTransportConfig,
        *,
        dispatch_timeout_sec: int,
        resources: Mapping[str, Any] | None = None,
    ):
        self._config = config
        self._resources = MappingProxyType(dict(resources or {}))
        self._dispatch_timeout_sec = dispatch_timeout_sec
        self._class_cache: dict[str, type[BaseAction]] = {}

    async def send(self, request: TransportRequest) -> Completed:
        action_cls = self._import_class(self._config.class_)

        context = ActionContext(
            request_id=request.request_id,
            flow_name=request.flow_name,
            step_name=request.step_name,
            action=request.action,
            service_call=request.service_call_context,
        )

        try:
            granted_resources = ResourceGrant(
                {name: self._resources[name] for name in request.required_resources}
            )
        except KeyError as exc:
            raise ActionDispatchError(
                f"Direct action resource grant '{exc.args[0]}' is not loaded"
            ) from exc

        handler = action_cls(
            globals=request.globals,
            resources=granted_resources,
            context=context,
        )

        method = getattr(handler, request.action, None)
        if method is None:
            raise ActionDispatchError(
                f"{action_cls.__name__} has no action method '{request.action}'"
            )

        deadline = asyncio.timeout(self._dispatch_timeout_sec)
        try:
            async with deadline:
                result = await method(request.input)
        except TimeoutError as exc:
            if deadline.expired():
                raise DispatchTimeoutError(
                    f"Direct action '{request.action}' exceeded its dispatch deadline"
                ) from exc
            self._raise_service_error(action_cls, request, exc)
        except ResourceAccessError as exc:
            raise ActionDispatchError(
                f"Direct action '{request.action}' attempted denied resource access"
            ) from exc
        except TransportError:
            raise
        except ResourceOperationError as exc:
            raise ServiceError(
                "Direct action resource operation failed",
                code=type(exc).__name__,
                retryable=exc.retryable,
            ) from exc
        except Exception as e:  # noqa: BLE001 - user action boundary normalizes arbitrary failures
            self._raise_service_error(action_cls, request, e)

        return Completed(data=result)

    @staticmethod
    def _raise_service_error(
        action_cls: type[BaseAction],
        request: TransportRequest,
        error: Exception,
    ) -> NoReturn:
        logger.error(
            "Direct action failed",
            extra={
                "action_class": action_cls.__name__,
                "action": request.action,
                "exception_type": type(error).__name__,
            },
        )
        raise ServiceError("Direct action failed", code=type(error).__name__) from error

    async def close(self) -> None:
        self._class_cache.clear()

    def _import_class(self, dotted_path: str) -> type[BaseAction]:
        if dotted_path not in self._class_cache:
            try:
                module_path, class_name = dotted_path.rsplit(".", 1)
                module = importlib.import_module(module_path)
                cls = getattr(module, class_name)
            except (ImportError, AttributeError, ValueError) as e:
                raise ActionDispatchError(f"Cannot import action class '{dotted_path}': {e}") from e
            if not isinstance(cls, type) or not issubclass(cls, BaseAction):
                raise ActionDispatchError(f"Action class '{dotted_path}' must extend BaseAction")
            self._class_cache[dotted_path] = cls
        return self._class_cache[dotted_path]


def validate_direct_startup(config: DirectTransportConfig) -> str | None:
    try:
        DirectTransport(
            config,
            dispatch_timeout_sec=STARTUP_VALIDATION_TIMEOUT_SECONDS,
        )._import_class(config.class_)
    except ActionDispatchError as exc:
        return str(exc)
    return None
