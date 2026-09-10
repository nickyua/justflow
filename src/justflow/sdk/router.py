"""Router - generic message router that dispatches to action handlers."""

from __future__ import annotations

import importlib
import inspect
import logging
import pkgutil
from pathlib import Path
from typing import Any

from justflow.sdk.base_action import ActionContext, BaseAction

logger = logging.getLogger(__name__)


class Router:
    """Generic router that discovers BaseAction subclasses and dispatches by action name."""

    def __init__(self, resources: dict[str, Any] | None = None):
        self._actions: dict[str, type[BaseAction]] = {}
        self._resources = resources or {}

    @property
    def registered_actions(self) -> list[str]:
        return list(self._actions.keys())

    def register(self, action_name: str, action_cls: type[BaseAction]) -> None:
        self._actions[action_name] = action_cls

    def discover(self, actions_package: str) -> None:
        """Auto-discover BaseAction subclasses from a package path.

        Registers one action per public async method (the method-per-action
        convention shared with the direct transport): a class ProcessRecord
        with `async def process_record` handles the 'process_record' action.
        """
        module = importlib.import_module(actions_package)
        if module.__file__ is None:
            raise ValueError(f"Actions package '{actions_package}' has no file path")
        package_path = Path(module.__file__).parent

        for module_info in pkgutil.iter_modules([str(package_path)]):
            mod = importlib.import_module(f"{actions_package}.{module_info.name}")
            for _name, obj in inspect.getmembers(mod, inspect.isclass):
                if issubclass(obj, BaseAction) and obj is not BaseAction:
                    for action_name in _action_methods(obj):
                        self.register(action_name, obj)
                        logger.info(f"Registered action: {action_name} -> {obj.__name__}")

    async def dispatch(
        self,
        action: str,
        input: Any,
        globals: dict[str, Any] | None = None,
        context: ActionContext | None = None,
    ) -> Any:
        """Dispatch a request to the action method named after the action."""
        if action not in self._actions:
            raise ValueError(f"Unknown action: {action}. Available: {list(self._actions.keys())}")

        action_cls = self._actions[action]
        handler = action_cls(
            globals=globals or {},
            resources=self._resources,
            context=context,
        )
        method = getattr(handler, action, None)
        if method is None:
            raise ValueError(f"{action_cls.__name__} has no action method '{action}'")
        return await method(input)


def _action_methods(action_cls: type[BaseAction]) -> list[str]:
    """Public coroutine methods defined on the subclass (not BaseAction itself)."""
    return [
        name
        for name, member in inspect.getmembers(action_cls, inspect.iscoroutinefunction)
        if not name.startswith("_") and not hasattr(BaseAction, name)
    ]
