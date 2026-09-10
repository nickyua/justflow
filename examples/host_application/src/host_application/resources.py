"""Typed host-owned resource provider and its read-only facade."""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import Field, StringConstraints

from justflow.resources import (
    ResourceCapability,
    ResourceError,
    ResourceFactoryContext,
    ResourceProvider,
    ResourceRegistry,
    StrictResourceConfig,
    builtin_resource_registry,
)

PROVIDER_NAME = "host_tenant_settings"
PROVIDER_CONTRACT_VERSION = "1"
MAX_SETTING_VALUE_LENGTH = 80
MAX_SETTING_KEY_LENGTH = 64
MAX_SETTING_COUNT = 16

SettingKey = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=MAX_SETTING_KEY_LENGTH,
        pattern=r"^[a-z][a-z0-9_]*$",
    ),
]
SettingValue = Annotated[
    str,
    StringConstraints(min_length=1, max_length=MAX_SETTING_VALUE_LENGTH),
]


class TenantSettingsConfig(StrictResourceConfig):
    values: dict[SettingKey, SettingValue] = Field(
        default_factory=dict,
        max_length=MAX_SETTING_COUNT,
    )


class TenantSettingsUnavailableError(ResourceError):
    """The host-owned settings facade is outside its managed lifecycle."""


class TenantSettings:
    def __init__(self, config: TenantSettingsConfig) -> None:
        self._values = dict(config.values)
        self._available = False

    async def initialize(self) -> None:
        self._available = True

    async def close(self) -> None:
        self._available = False

    def get(self, key: str, default: Any = None) -> Any:
        if not self._available:
            raise TenantSettingsUnavailableError("Tenant settings are unavailable")
        return self._values.get(key, default)


def _build_tenant_settings(
    config: TenantSettingsConfig,
    _context: ResourceFactoryContext,
) -> TenantSettings:
    return TenantSettings(config)


TENANT_SETTINGS_PROVIDER = ResourceProvider(
    name=PROVIDER_NAME,
    contract_version=PROVIDER_CONTRACT_VERSION,
    config_model=TenantSettingsConfig,
    capabilities=frozenset({ResourceCapability.CONFIG}),
    factory=_build_tenant_settings,
)


def create_resource_registry() -> ResourceRegistry:
    registry = builtin_resource_registry()
    registry.register(TENANT_SETTINGS_PROVIDER)
    return registry


__all__ = [
    "PROVIDER_CONTRACT_VERSION",
    "PROVIDER_NAME",
    "TENANT_SETTINGS_PROVIDER",
    "TenantSettings",
    "TenantSettingsConfig",
    "TenantSettingsUnavailableError",
    "create_resource_registry",
]
