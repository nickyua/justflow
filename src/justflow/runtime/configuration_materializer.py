"""Host-owned materialization of published configuration into immutable runtime targets."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from justflow.config.models import ResourcesConfig, ServicesConfig
from justflow.config.settings import Settings
from justflow.configuration.activation import ActivationOutcomeCode
from justflow.configuration.errors import ConfigurationError
from justflow.configuration.models import (
    ConfigurationBundle,
    ConfigurationSnapshot,
    RevisionRecord,
)
from justflow.configuration.policy import validate_tenant_authoring
from justflow.configuration.ports import ConfigurationStore, PlatformComponentCatalogSource
from justflow.configuration.publication import TenantAuthoringPolicySource
from justflow.definitions.catalog import (
    CatalogConflictError,
    DefinitionCatalog,
    DefinitionCatalogStore,
)
from justflow.definitions.environment import build_execution_environment_snapshots
from justflow.definitions.manifest import build_definition_manifests
from justflow.definitions.routing import WorkerDeployment, WorkerDeploymentRouter
from justflow.definitions.runtime import prepare_definitions
from justflow.provenance import ExecutionConfigurationIdentity, provenance_digest
from justflow.resources.registry import ResourceRegistry
from justflow.runtime.configuration_activation import (
    ActivationControllerError,
    PreparedActivation,
    PreparedWorkerDeployment,
)
from justflow.runtime.schedule_configuration import prepare_schedules
from justflow.scope import RuntimeScope
from justflow.transports.registry import TransportRegistry


@dataclass(frozen=True, kw_only=True)
class ActivationRuntimeRegistration:
    scope: RuntimeScope
    settings: Settings
    catalog_store: DefinitionCatalogStore
    worker_deployment: WorkerDeployment

    def __post_init__(self) -> None:
        if self.settings.runtime.scope != self.scope:
            raise ValueError("Activation runtime registration has a different settings scope")
        artifact = self.worker_deployment.artifact_identity
        deployment = self.settings.deployment
        if (
            deployment.name != artifact.deployment_name
            or deployment.build_id != artifact.build_id
            or deployment.artifact_digest != artifact.artifact_digest
            or deployment.package_version != artifact.package_version
            or deployment.source_revision != artifact.source_revision
        ):
            raise ValueError("Activation runtime registration has a different worker artifact")


class ConfigurationActivationMaterializer:
    def __init__(
        self,
        *,
        configuration_store: ConfigurationStore,
        policy_source: TenantAuthoringPolicySource,
        component_catalog_source: PlatformComponentCatalogSource,
        platform_resources: ResourcesConfig,
        platform_services: ServicesConfig,
        transport_registry: TransportRegistry,
        resource_registry: ResourceRegistry,
        registrations: Mapping[RuntimeScope, ActivationRuntimeRegistration],
        config_dir: str | Path,
    ) -> None:
        indexed: dict[str, ActivationRuntimeRegistration] = {}
        for scope, registration in registrations.items():
            if scope != registration.scope:
                raise ValueError("Activation runtime registration key is inconsistent")
            if scope.digest in indexed:
                raise ValueError("Activation materializer has a duplicate runtime scope")
            indexed[scope.digest] = registration
        self._configuration_store = configuration_store
        self._policy_source = policy_source
        self._component_catalog_source = component_catalog_source
        self._platform_resources = platform_resources
        self._platform_services = platform_services
        self._transport_registry = transport_registry
        self._resource_registry = resource_registry
        self._registrations = indexed
        self._config_dir = Path(config_dir)

    def prepare(
        self,
        scope: RuntimeScope,
        revision: RevisionRecord,
    ) -> PreparedActivation:
        registration = self._registration(scope)
        if revision.scope_digest != scope.digest or not isinstance(
            revision.bundle,
            ConfigurationBundle,
        ):
            raise ActivationControllerError(
                ActivationOutcomeCode.STALE_PLAN,
                "Activation target is not a resolved scoped configuration revision",
                retryable=False,
            )
        resolution = revision.bundle.tenant_resolution
        if resolution is None:
            raise ActivationControllerError(
                ActivationOutcomeCode.STALE_PLAN,
                "Managed activation target has no tenant resolution identity",
                retryable=False,
            )
        source_revision = self._configuration_store.read_revision(
            scope,
            resolution.tenant_configuration_revision_id,
        )
        policy = self._policy_source.read(scope)
        policy_digest = provenance_digest(policy.model_dump(mode="json"))
        try:
            validated = validate_tenant_authoring(
                source_revision,
                scope=scope,
                policy=policy,
                component_catalog_source=self._component_catalog_source,
                platform_resources=self._platform_resources,
                platform_services=self._platform_services,
                transport_registry=self._transport_registry,
                resource_registry=self._resource_registry,
                limits=registration.settings.limits.snapshot(),
                config_dir=self._config_dir,
            )
        except ConfigurationError as exc:
            raise ActivationControllerError(
                ActivationOutcomeCode.STALE_PLAN,
                "Published configuration is not valid under current host policy",
                retryable=False,
            ) from exc
        if validated.bundle != revision.bundle:
            raise ActivationControllerError(
                ActivationOutcomeCode.STALE_PLAN,
                "Published configuration no longer matches current host policy",
                retryable=False,
            )
        authored = build_definition_manifests(
            validated.bundle.workflows,
            validated.services,
            registration.settings.limits.snapshot(),
            resources=validated.resources,
        )
        catalog_state = registration.catalog_store.inspect()
        manifests = dict(catalog_state.catalog.manifests)
        for manifest in authored.values():
            key = (manifest.logical_name, manifest.definition_digest)
            existing = manifests.get(key)
            if existing is not None and existing != manifest:
                raise CatalogConflictError(
                    "Immutable definition content differs from its catalog identity"
                )
            manifests[key] = manifest
        desired_catalog = DefinitionCatalog(
            manifests.values(),
            {name: manifest.definition_digest for name, manifest in authored.items()},
        )
        router = WorkerDeploymentRouter.for_deployment(registration.worker_deployment)
        execution_configuration = _execution_configuration(scope, revision)
        snapshots = build_execution_environment_snapshots(
            registration.settings,
            desired_catalog,
            router,
            registration.catalog_store.backend_identity,
            execution_configuration,
        )
        prepared = prepare_definitions(
            validated.bundle.workflows,
            validated.services,
            registration.settings.limits.snapshot(),
            desired_catalog,
            router,
            {name: snapshot.snapshot_digest for name, snapshot in snapshots.items()},
            resources=validated.resources,
            runtime_scope=scope,
            execution_configuration=execution_configuration,
        )
        schedules = prepare_schedules(
            validated.bundle.triggers,
            settings=registration.settings,
            catalog_store=registration.catalog_store,
            router=router,
            execution_configuration=execution_configuration,
            catalog=desired_catalog,
        )
        all_snapshots = {snapshot.snapshot_digest: snapshot for snapshot in snapshots.values()}
        all_snapshots.update(schedules.environment_snapshots)
        return PreparedActivation(
            scope=scope,
            revision=revision,
            policy_digest=policy_digest,
            artifact=registration.worker_deployment.artifact_identity,
            task_queue_identity_digest=worker_task_queue_identity_digest(
                scope,
                registration.settings,
            ),
            manifests=prepared.manifests,
            environment_snapshots=all_snapshots,
            start_targets=prepared.start_targets,
            desired_schedules=schedules.desired,
        )

    def prepare_worker_deployment(
        self,
        target: PreparedActivation,
    ) -> PreparedWorkerDeployment:
        self._registration(target.scope)
        assignments = [target]
        for registration in self._registrations.values():
            if registration.scope == target.scope:
                continue
            if registration.worker_deployment.artifact_identity != target.artifact:
                continue
            active = self._configuration_store.read_active(registration.scope)
            if active is None:
                continue
            revision = self._configuration_store.read_revision(
                registration.scope,
                active.revision_id,
            )
            assignments.append(self.prepare(registration.scope, revision))
        return PreparedWorkerDeployment(
            target=target,
            assignments=tuple(sorted(assignments, key=lambda value: value.scope.digest)),
        )

    def _registration(self, scope: RuntimeScope) -> ActivationRuntimeRegistration:
        registration = self._registrations.get(scope.digest)
        if registration is None or registration.scope != scope:
            raise ActivationControllerError(
                ActivationOutcomeCode.INTERNAL_ERROR,
                "Activation runtime is not registered for the trusted scope",
                retryable=False,
            )
        return registration


def worker_task_queue_identity_digest(scope: RuntimeScope, settings: Settings) -> str:
    if settings.runtime.scope != scope:
        raise ValueError("Worker task-queue settings belong to another runtime scope")
    return provenance_digest(
        {
            "namespace": settings.temporal.namespace,
            "scope_digest": scope.digest,
            "task_queue": settings.temporal.task_queue,
        }
    )


def _execution_configuration(
    scope: RuntimeScope,
    revision: RevisionRecord,
) -> ExecutionConfigurationIdentity:
    bundle = revision.bundle
    if not isinstance(bundle, ConfigurationBundle):
        raise TypeError("Activation revision is not a resolved configuration bundle")
    return ConfigurationSnapshot(
        scope_digest=scope.digest,
        revision_id=revision.revision_id,
        bundle=bundle,
    ).execution_identity
