"""Explicit local publication and activation operations for the host example."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

from host_application.resources import create_resource_registry
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.config.settings import Settings, load_settings
from justflow.config.validator import ConfigValidator
from justflow.configuration import (
    ConfigurationConflictError,
    FileConfigurationSource,
    RevisionIdentity,
    SqliteConfigurationStore,
)
from justflow.configuration.models import ConfigurationBundle
from justflow.definitions import build_definition_manifests
from justflow.definitions.configuration import configured_catalog_store
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope
from justflow.transports.builtins import builtin_transport_registry

logger = logging.getLogger(__name__)
CONFIG_ROOT = Path(__file__).resolve().parent / "configs"


class HostConfigurationConflictError(Exception):
    """The example configuration changed across an explicit concurrency boundary."""


@dataclass(frozen=True, kw_only=True)
class HostConfigurationSummary:
    workflow_count: int
    service_count: int
    resource_count: int
    trigger_count: int


def publish_local_configuration(
    database: str | Path,
    config_dir: str | Path,
    *,
    scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
    expected_active_revision: RevisionIdentity | None,
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
) -> RevisionIdentity:
    bundle, _ = _read_and_validate_configuration(config_dir, scope=scope, limits=limits)
    store = SqliteConfigurationStore(database)
    try:
        active = store.read_active(scope)
        current_revision = active.revision_id if active is not None else None
        if current_revision != expected_active_revision:
            raise HostConfigurationConflictError("Active configuration changed before publication")
        revision = store.create_revision(
            scope,
            bundle,
            parent_revision_id=expected_active_revision,
        )
        latest = store.read_active(scope)
        latest_revision = latest.revision_id if latest is not None else None
        if latest_revision != expected_active_revision:
            raise HostConfigurationConflictError("Active configuration changed during publication")
        return revision.revision_id
    finally:
        store.close()


def activate_local_configuration(
    database: str | Path,
    revision_id: RevisionIdentity,
    *,
    scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
    expected_active_revision: RevisionIdentity | None,
) -> RevisionIdentity:
    store = SqliteConfigurationStore(database)
    try:
        active = store.read_active(scope)
        if active is not None and active.revision_id == revision_id:
            return revision_id
        try:
            store.compare_and_swap_active(
                scope,
                revision_id,
                expected_revision_id=expected_active_revision,
                expected_version=active.version if active is not None else None,
            )
        except ConfigurationConflictError as exc:
            raise HostConfigurationConflictError(
                "Active configuration changed before activation"
            ) from exc
        return revision_id
    finally:
        store.close()


def initialize_local_configuration_database(
    database: str | Path,
    config_dir: str | Path = CONFIG_ROOT,
    *,
    scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
) -> RevisionIdentity:
    bundle, _ = _read_and_validate_configuration(config_dir, scope=scope, limits=limits)
    store = SqliteConfigurationStore(database)
    try:
        active = store.read_active(scope)
        if active is not None:
            revision = store.read_revision(scope, active.revision_id)
            if revision.bundle != bundle:
                raise HostConfigurationConflictError(
                    "The active configuration differs from the bundled configuration"
                )
            return active.revision_id
    finally:
        store.close()

    revision_id = publish_local_configuration(
        database,
        config_dir,
        scope=scope,
        expected_active_revision=None,
        limits=limits,
    )
    return activate_local_configuration(
        database,
        revision_id,
        scope=scope,
        expected_active_revision=None,
    )


def _read_and_validate_configuration(
    config_dir: str | Path,
    *,
    scope: RuntimeScope,
    limits: RuntimeLimits,
) -> tuple[ConfigurationBundle, ConfigValidator]:
    source = FileConfigurationSource(config_dir, scope=scope)
    bundle = source.read(scope).bundle
    transport_registry = builtin_transport_registry()
    resource_registry = create_resource_registry()
    validator = ConfigValidator(
        bundle.resources,
        bundle.services,
        bundle.workflows,
        transport_registry=transport_registry,
        resource_registry=resource_registry,
        limits=limits,
        config_dir=config_dir,
        workflow_sources=source.workflow_sources,
        triggers=bundle.triggers,
    )
    validator.validate().raise_if_invalid()
    return bundle, validator


def _validated_configuration(
    settings: Settings,
    config_dir: str | Path,
) -> tuple[ConfigurationBundle, ConfigValidator]:
    return _read_and_validate_configuration(
        config_dir,
        scope=settings.runtime.scope,
        limits=settings.limits.snapshot(),
    )


def validate_configuration(
    settings: Settings,
    config_dir: str | Path = CONFIG_ROOT,
) -> HostConfigurationSummary:
    bundle, _ = _validated_configuration(settings, config_dir)
    return HostConfigurationSummary(
        workflow_count=len(bundle.workflows),
        service_count=len(bundle.services.services),
        resource_count=len(bundle.resources.resources),
        trigger_count=len(bundle.triggers.triggers),
    )


def publish_definitions(settings: Settings, config_dir: str | Path) -> None:
    bundle, validator = _validated_configuration(settings, config_dir)
    manifests = build_definition_manifests(
        bundle.workflows,
        dict(validator.resolved_services),
        settings.limits.snapshot(),
        resources=dict(validator.resolved_resources),
    )
    configured_catalog_store(
        settings.catalog,
        settings.paths.catalog_dir or config_dir,
        scope=settings.runtime.scope,
    ).publish(manifests)
    logger.info(
        "Host definitions published",
        extra={"workflow_count": len(manifests), "scope_digest": settings.runtime.scope.digest},
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage host-owned runtime configuration")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser(
        "validate",
        help="validate host configuration with every host-owned provider",
    )
    validate.add_argument("--source-dir", required=True)
    initialize = commands.add_parser(
        "initialize",
        help="idempotently publish and activate the bundled local configuration",
    )
    initialize.add_argument("--source-dir", required=True)
    initialize.add_argument("--database", required=True)
    publish = commands.add_parser(
        "publish-definitions",
        help="validate and publish immutable workflow definitions",
    )
    publish.add_argument("--source-dir", required=True)
    args = parser.parse_args()
    settings = load_settings()
    if args.command == "validate":
        summary = validate_configuration(settings, args.source_dir)
        logger.info(
            "Host configuration is valid",
            extra={
                "workflow_count": summary.workflow_count,
                "service_count": summary.service_count,
                "resource_count": summary.resource_count,
                "trigger_count": summary.trigger_count,
            },
        )
        return
    if args.command == "initialize":
        initialize_local_configuration_database(
            args.database,
            args.source_dir,
            scope=settings.runtime.scope,
            limits=settings.limits.snapshot(),
        )
        return
    if args.command == "publish-definitions":
        publish_definitions(settings, args.source_dir)
        return
    raise RuntimeError("Unknown host configuration command")


if __name__ == "__main__":
    main()
