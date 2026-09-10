"""Construction of secret-free execution environment snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal

from justflow.definitions.catalog import DefinitionCatalog
from justflow.definitions.manifest import DefinitionManifest
from justflow.definitions.routing import WorkerDeploymentRouter
from justflow.provenance import (
    BrokerProviderSnapshotIdentity,
    CatalogBackendIdentity,
    ExecutionConfigurationIdentity,
    ExecutionEnvironmentSnapshot,
    ProviderContractSnapshotIdentity,
    RuntimeProfile,
    SanitizedRuntimeConfigurationIdentity,
    WorkerArtifactIdentity,
    installed_engine_version,
    provenance_digest,
)
from justflow.scope import RuntimeScope

if TYPE_CHECKING:
    from justflow.config.settings import Settings


def build_execution_environment_snapshots(
    settings: Settings,
    catalog: DefinitionCatalog,
    router: WorkerDeploymentRouter,
    catalog_backend: CatalogBackendIdentity,
    execution_configuration: ExecutionConfigurationIdentity | None = None,
) -> dict[str, ExecutionEnvironmentSnapshot]:
    snapshots: dict[str, ExecutionEnvironmentSnapshot] = {}
    for logical_name in sorted(catalog.aliases):
        manifest = catalog.resolve(logical_name)
        deployment = router.select(manifest)
        snapshots[logical_name] = build_execution_environment_snapshot(
            manifest=manifest,
            artifact_identity=deployment.artifact_identity,
            catalog_backend=catalog_backend,
            runtime_profile=settings.runtime.profile,
            configuration=sanitized_runtime_configuration(
                temporal_namespace=settings.temporal.namespace,
                temporal_task_queue=settings.temporal.task_queue,
                payload_protection_mode=settings.temporal.payload_protection.mode,
                broker_providers={
                    name: declaration.provider for name, declaration in settings.brokers.items()
                },
                runtime_limits=manifest.deterministic_policy.runtime_limits,
            ),
            scope=settings.runtime.scope,
            execution_configuration=execution_configuration,
        )
    return snapshots


def build_execution_environment_snapshot(
    *,
    manifest: DefinitionManifest,
    artifact_identity: WorkerArtifactIdentity,
    catalog_backend: CatalogBackendIdentity,
    runtime_profile: RuntimeProfile,
    configuration: SanitizedRuntimeConfigurationIdentity,
    scope: RuntimeScope | None = None,
    execution_configuration: ExecutionConfigurationIdentity | None = None,
) -> ExecutionEnvironmentSnapshot:
    artifact_identity.validate_for_profile(runtime_profile)
    return ExecutionEnvironmentSnapshot.create(
        engine_version=installed_engine_version(),
        worker_artifact=artifact_identity,
        definition_digest=manifest.definition_digest,
        provider_contracts=tuple(
            ProviderContractSnapshotIdentity(name=contract.name, version=contract.version)
            for contract in manifest.provider_contracts
        ),
        catalog_backend=catalog_backend,
        runtime_profile=runtime_profile,
        configuration=configuration,
        execution_configuration=execution_configuration,
        scope_digest=scope.digest if scope is not None else None,
    )


def sanitized_runtime_configuration(
    *,
    temporal_namespace: str,
    temporal_task_queue: str,
    payload_protection_mode: Literal["plaintext", "codec"],
    broker_providers: Mapping[str, str],
    runtime_limits: Mapping[str, int],
) -> SanitizedRuntimeConfigurationIdentity:
    return SanitizedRuntimeConfigurationIdentity(
        temporal_namespace_identity=provenance_digest({"temporal_namespace": temporal_namespace}),
        temporal_task_queue_identity=provenance_digest(
            {"temporal_task_queue": temporal_task_queue}
        ),
        payload_protection_mode=payload_protection_mode,
        broker_providers=tuple(
            BrokerProviderSnapshotIdentity(
                binding_identity=provenance_digest({"broker_binding": name}),
                provider=provider,
            )
            for name, provider in sorted(broker_providers.items())
        ),
        runtime_limits_digest=provenance_digest(dict(sorted(runtime_limits.items()))),
    )
