"""Deterministic safe-YAML rendering for configuration bundles."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

import yaml
from pydantic import ValidationError

from justflow.config.loader import ConfigLoadError, load_bounded_yaml
from justflow.configuration.errors import ConfigurationError, ConfigurationLimitError
from justflow.configuration.models import ConfigurationBundle, TenantConfiguration

MAX_CONFIGURATION_YAML_BYTES = 2_097_152
TENANT_CONFIGURATION_SOURCE = "tenant-configuration.yaml"


def render_configuration_yaml(bundle: ConfigurationBundle) -> Mapping[str, bytes]:
    values: dict[str, object] = {
        "resources.yaml": bundle.resources.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "services.yaml": bundle.services.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "triggers.yaml": bundle.triggers.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
    }
    values.update(
        {
            f"workflows/{name}.yaml": workflow.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            )
            for name, workflow in bundle.workflows.items()
        }
    )
    try:
        rendered = {
            path: yaml.safe_dump(
                value,
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=True,
            ).encode("utf-8")
            for path, value in sorted(values.items())
        }
    except (TypeError, ValueError, yaml.YAMLError) as exc:
        raise ConfigurationError("Configuration cannot be rendered as safe YAML") from exc
    if sum(len(payload) for payload in rendered.values()) > MAX_CONFIGURATION_YAML_BYTES:
        raise ConfigurationLimitError("Rendered configuration YAML exceeds its byte limit")
    return MappingProxyType(rendered)


def parse_tenant_configuration_yaml(payload: bytes) -> TenantConfiguration:
    try:
        value = load_bounded_yaml(payload, source=TENANT_CONFIGURATION_SOURCE)
        return TenantConfiguration.model_validate(value)
    except ConfigLoadError as exc:
        raise ConfigurationError("Tenant configuration YAML is invalid") from exc
    except ValidationError as exc:
        raise ConfigurationError("Tenant configuration declaration is invalid") from exc


def render_tenant_configuration_yaml(configuration: TenantConfiguration) -> bytes:
    try:
        payload = yaml.safe_dump(
            configuration.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, yaml.YAMLError) as exc:
        raise ConfigurationError("Tenant configuration cannot be rendered as safe YAML") from exc
    if len(payload) > MAX_CONFIGURATION_YAML_BYTES:
        raise ConfigurationLimitError("Rendered tenant configuration exceeds its byte limit")
    return payload
