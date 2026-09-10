"""Lifecycle owner for resources resolved through an explicit provider registry."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass

from justflow.optional_dependencies import OptionalDependencyError
from justflow.resources.base import (
    ManagedResource,
    ResourceCapability,
    ResourceCapabilityError,
    ResourceFactoryContext,
    ResourceNotFoundError,
)
from justflow.resources.registry import (
    LoadedResource,
    ResolvedResource,
    ResourceCollection,
    ResourceFactoryError,
    ResourceRegistry,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class ResourceCleanupFailure:
    resource_name: str
    method_name: str
    cause: Exception


class ResourceCleanupError(Exception):
    def __init__(self, failures: tuple[ResourceCleanupFailure, ...]) -> None:
        self.failures = failures
        details = "; ".join(
            f"{failure.resource_name}.{failure.method_name}: {type(failure.cause).__name__}"
            for failure in failures
        )
        super().__init__(f"Resource cleanup failed: {details}")


class ResourceLoadError(Exception):
    """A validated resource could not be constructed or initialized."""

    def __init__(self, resource_name: str, provider_name: str, reason: str) -> None:
        self.resource_name = resource_name
        self.provider_name = provider_name
        super().__init__(
            f"Resource '{resource_name}' from provider '{provider_name}' failed to load: {reason}"
        )


class ResourceLoader:
    """Builds typed resources and owns their reverse-order lifecycle."""

    def __init__(
        self,
        registry: ResourceRegistry,
        context: ResourceFactoryContext | None = None,
    ) -> None:
        self._registry = registry
        self._loaded: dict[str, LoadedResource] = {}
        self._context = (context or ResourceFactoryContext()).with_resource_resolver(
            self._resolve_loaded_resource
        )
        self._exit_stack = AsyncExitStack()
        self._cleanup_failures: list[ResourceCleanupFailure] = []
        self._closed = False
        self._load_started = False

    @property
    def resources(self) -> ResourceCollection:
        return ResourceCollection(self._loaded)

    async def load(self, resources: Mapping[str, ResolvedResource]) -> None:
        if self._closed:
            raise RuntimeError("A closed ResourceLoader cannot load resources")
        if self._load_started:
            raise RuntimeError("ResourceLoader.load() may only be called once")
        self._load_started = True

        try:
            for name in self._registry.resource_load_order(resources):
                definition = resources[name]
                try:
                    resource = self._registry.build(definition, self._context)
                    self._exit_stack.push_async_callback(self._cleanup_resource, name, resource)
                    await resource.initialize()
                except OptionalDependencyError as exc:
                    raise ResourceLoadError(name, definition.provider_name, str(exc)) from exc
                except ResourceFactoryError as exc:
                    raise ResourceLoadError(name, definition.provider_name, str(exc)) from exc
                except Exception as exc:
                    raise ResourceLoadError(
                        name,
                        definition.provider_name,
                        f"initialization raised {type(exc).__name__}",
                    ) from exc

                self._loaded[name] = LoadedResource(
                    definition=definition,
                    instance=resource,
                )
        except BaseException as exc:
            try:
                await self.close()
            except ResourceCleanupError as cleanup_exc:
                exc.add_note(str(cleanup_exc))
            raise

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._exit_stack.aclose()
        self._loaded.clear()
        if self._cleanup_failures:
            raise ResourceCleanupError(tuple(self._cleanup_failures))

    def get(self, name: str) -> ManagedResource:
        try:
            resource = self._loaded[name].instance
        except KeyError as exc:
            raise KeyError(f"Resource '{name}' not loaded") from exc
        if not isinstance(resource, ManagedResource):
            raise TypeError(f"Loaded resource '{name}' has no managed lifecycle")
        return resource

    def _resolve_loaded_resource(self, name: str, capability: ResourceCapability) -> object:
        try:
            loaded = self._loaded[name]
        except KeyError as exc:
            raise ResourceNotFoundError(f"Resource dependency '{name}' is not initialized") from exc
        if capability not in loaded.definition.capabilities:
            raise ResourceCapabilityError(
                f"Resource dependency '{name}' does not provide '{capability.value}'"
            )
        return loaded.instance

    async def _cleanup_resource(
        self,
        resource_name: str,
        resource: ManagedResource,
    ) -> None:
        try:
            await resource.close()
        except Exception as exc:  # noqa: BLE001 - resource lifecycle boundary
            logger.error(
                "Resource cleanup failed",
                extra={
                    "resource_name": resource_name,
                    "method_name": "close",
                    "exception_type": type(exc).__name__,
                },
            )
            self._cleanup_failures.append(
                ResourceCleanupFailure(
                    resource_name=resource_name,
                    method_name="close",
                    cause=exc,
                )
            )
