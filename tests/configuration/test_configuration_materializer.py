from __future__ import annotations

import pytest

from justflow.config.models import ResourcesConfig, ServicesConfig
from justflow.config.settings import Settings
from justflow.config.triggers import TriggersConfig
from justflow.configuration import (
    PlatformComponentCatalog,
    SqliteConfigurationStore,
    StaticTenantAuthoringPolicySource,
    TemporalIsolationMode,
    TemporalIsolationPolicy,
    TenantAuthoringPolicy,
    TenantConfiguration,
    TenantWorkflowConfig,
    validate_tenant_authoring,
)
from justflow.configuration.activation_errors import (
    ActivationIntegrityError,
    ActivationLimitError,
)
from justflow.configuration.models import (
    ComponentCatalogRevision,
    RevisionIdentity,
    RevisionRecord,
)
from justflow.definitions.catalog import CatalogStore
from justflow.definitions.manifest import ENGINE_WORKFLOW_ABI
from justflow.definitions.routing import WorkerDeployment
from justflow.provenance import WorkerArtifactIdentity, provenance_digest
from justflow.resources.builtins import builtin_resource_registry
from justflow.runtime.configuration_activation import PreparedRuntimeIndex
from justflow.runtime.configuration_materializer import (
    ActivationRuntimeRegistration,
    ConfigurationActivationMaterializer,
    worker_task_queue_identity_digest,
)
from justflow.scope import RuntimeScope
from justflow.transports.builtins import builtin_transport_registry

SCOPE = RuntimeScope.create(
    tenant="tenant-a",
    application="orders",
    environment="production",
)
OTHER_SCOPE = RuntimeScope.create(
    tenant="tenant-b",
    application="orders",
    environment="production",
)
CATALOG = PlatformComponentCatalog.create()
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="justflow",
    build_id="release-1",
    artifact_digest=f"sha256:{'a' * 64}",
    package_version="0.1.0",
    source_revision="abc1234",
)


class StaticCatalogSource:
    def read(self, revision_id: ComponentCatalogRevision) -> PlatformComponentCatalog:
        if revision_id != CATALOG.revision_id:
            raise ValueError("Unexpected component catalog revision")
        return CATALOG


def policy(scope: RuntimeScope = SCOPE) -> TenantAuthoringPolicy:
    return TenantAuthoringPolicy(
        scope_digest=scope.digest,
        component_catalog_revision=CATALOG.revision_id,
        temporal=TemporalIsolationPolicy.for_scope(
            scope,
            mode=TemporalIsolationMode.SHARED,
            namespace="shared",
            task_queue="orders",
            worker_deployment="orders",
            storage_prefix="orders",
        ),
    )


def settings(config_dir: str, scope: RuntimeScope = SCOPE) -> Settings:
    return Settings.model_validate(
        {
            "paths": {"config_dir": config_dir},
            "runtime": {"scope": scope.model_dump(mode="json")},
            "deployment": {
                "name": ARTIFACT.deployment_name,
                "build_id": ARTIFACT.build_id,
                "artifact_digest": ARTIFACT.artifact_digest,
                "package_version": ARTIFACT.package_version,
                "source_revision": ARTIFACT.source_revision,
            },
        }
    )


def tenant_configuration(description: str = "") -> TenantConfiguration:
    return TenantConfiguration(
        component_catalog_revision=CATALOG.revision_id,
        workflows={
            "orders": TenantWorkflowConfig.model_validate(
                {
                    "workflow": "orders",
                    "description": description,
                    "steps": {},
                    "flow": [{"name": "done", "terminal": True}],
                }
            )
        },
        triggers={},
    )


def publish_configuration(
    store: SqliteConfigurationStore,
    scope: RuntimeScope,
    configuration: TenantConfiguration,
    config_dir: str,
) -> RevisionRecord:
    source_revision = store.create_revision(
        scope,
        configuration,
        parent_revision_id=None,
    )
    validated = validate_tenant_authoring(
        source_revision,
        scope=scope,
        policy=policy(scope),
        component_catalog=CATALOG,
        platform_resources=ResourcesConfig(resources={}),
        platform_services=ServicesConfig(services={}),
        transport_registry=builtin_transport_registry(),
        resource_registry=builtin_resource_registry(),
        limits=settings(config_dir, scope).limits.snapshot(),
        config_dir=config_dir,
    )
    return store.create_revision(
        scope,
        validated.bundle,
        parent_revision_id=None,
    )


def test_materializer_revalidates_policy_and_prepares_pinned_runtime_targets(tmp_path) -> None:
    configuration_store = SqliteConfigurationStore(tmp_path / "configuration.sqlite3")
    catalog_store = CatalogStore(tmp_path)
    published = publish_configuration(
        configuration_store,
        SCOPE,
        tenant_configuration(),
        str(tmp_path),
    )
    runtime_settings = settings(str(tmp_path))
    deployment = WorkerDeployment(
        artifact_identity=ARTIFACT,
        compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
    )
    materializer = ConfigurationActivationMaterializer(
        configuration_store=configuration_store,
        policy_source=StaticTenantAuthoringPolicySource({SCOPE.digest: policy()}),
        component_catalog_source=StaticCatalogSource(),
        platform_resources=ResourcesConfig(resources={}),
        platform_services=ServicesConfig(services={}),
        transport_registry=builtin_transport_registry(),
        resource_registry=builtin_resource_registry(),
        registrations={
            SCOPE: ActivationRuntimeRegistration(
                scope=SCOPE,
                settings=runtime_settings,
                catalog_store=catalog_store,
                worker_deployment=deployment,
            )
        },
        config_dir=tmp_path,
    )
    try:
        prepared = materializer.prepare(SCOPE, published)

        assert prepared.revision == published
        assert prepared.policy_digest == provenance_digest(policy().model_dump(mode="json"))
        assert prepared.definition_digests == (prepared.manifests["orders"].definition_digest,)
        assert prepared.start_targets["orders"].scope_digest == SCOPE.digest
        assert prepared.start_targets["orders"].deployment == deployment
        assert prepared.task_queue_identity_digest == worker_task_queue_identity_digest(
            SCOPE,
            runtime_settings,
        )
        assert set(prepared.environment_snapshots) == {
            prepared.start_targets["orders"].environment_snapshot_digest
        }
        assert catalog_store.inspect().catalog.aliases == {}
    finally:
        configuration_store.close()


async def test_shared_worker_materialization_includes_every_active_assigned_scope(tmp_path) -> None:
    configuration_store = SqliteConfigurationStore(tmp_path / "configuration.sqlite3")
    target = publish_configuration(
        configuration_store,
        SCOPE,
        tenant_configuration("target"),
        str(tmp_path),
    )
    other = publish_configuration(
        configuration_store,
        OTHER_SCOPE,
        tenant_configuration("other"),
        str(tmp_path),
    )
    configuration_store.compare_and_swap_active(
        OTHER_SCOPE,
        other.revision_id,
        expected_revision_id=None,
    )
    target_settings = settings(str(tmp_path), SCOPE)
    other_settings = settings(str(tmp_path), OTHER_SCOPE)
    deployment = WorkerDeployment(
        artifact_identity=ARTIFACT,
        compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
    )
    materializer = ConfigurationActivationMaterializer(
        configuration_store=configuration_store,
        policy_source=StaticTenantAuthoringPolicySource(
            {
                SCOPE.digest: policy(SCOPE),
                OTHER_SCOPE.digest: policy(OTHER_SCOPE),
            }
        ),
        component_catalog_source=StaticCatalogSource(),
        platform_resources=ResourcesConfig(resources={}),
        platform_services=ServicesConfig(services={}),
        transport_registry=builtin_transport_registry(),
        resource_registry=builtin_resource_registry(),
        registrations={
            SCOPE: ActivationRuntimeRegistration(
                scope=SCOPE,
                settings=target_settings,
                catalog_store=CatalogStore(tmp_path / "target-catalog"),
                worker_deployment=deployment,
            ),
            OTHER_SCOPE: ActivationRuntimeRegistration(
                scope=OTHER_SCOPE,
                settings=other_settings,
                catalog_store=CatalogStore(tmp_path / "other-catalog"),
                worker_deployment=deployment,
            ),
        },
        config_dir=tmp_path,
    )
    try:
        prepared = materializer.prepare(SCOPE, target)
        worker_deployment = materializer.prepare_worker_deployment(prepared)
        runtime_index = PreparedRuntimeIndex.from_settings(target_settings.activation)
        runtime_index.replace(
            SCOPE,
            revision_id=prepared.revision.revision_id,
            policy_digest=prepared.policy_digest,
            artifact=prepared.artifact,
            targets=prepared.start_targets,
            triggers=prepared.bundle.triggers,
        )

        assert tuple(value.scope for value in worker_deployment.assignments) == tuple(
            sorted((SCOPE, OTHER_SCOPE), key=lambda value: value.digest)
        )
        assert len(worker_deployment.definition_digests) == 2
        assert (
            worker_deployment.registration_digest
            == materializer.prepare_worker_deployment(prepared).registration_digest
        )
        registration = await runtime_index.resolve(SCOPE, "orders")
        assert registration is not None
        assert registration.target == prepared.start_targets["orders"]
        assert runtime_index.snapshot(OTHER_SCOPE) is None
        assert await runtime_index.resolve(OTHER_SCOPE, "orders") is None
        with pytest.raises(ActivationIntegrityError, match="trusted scope"):
            runtime_index.replace(
                OTHER_SCOPE,
                revision_id=prepared.revision.revision_id,
                policy_digest=prepared.policy_digest,
                artifact=prepared.artifact,
                targets=prepared.start_targets,
                triggers=prepared.bundle.triggers,
            )
        with pytest.raises(ActivationIntegrityError, match="artifact"):
            runtime_index.replace(
                SCOPE,
                revision_id=prepared.revision.revision_id,
                policy_digest=prepared.policy_digest,
                artifact=prepared.artifact.model_copy(update={"build_id": "release-2"}),
                targets=prepared.start_targets,
                triggers=prepared.bundle.triggers,
            )
    finally:
        configuration_store.close()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        pytest.param({"max_scopes": 0}, "scope limit", id="scope-limit"),
        pytest.param(
            {"max_targets_per_scope": 0},
            "target limit",
            id="target-limit",
        ),
    ],
)
def test_runtime_index_rejects_invalid_bounds(
    kwargs: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        PreparedRuntimeIndex(**kwargs)


def test_runtime_index_enforces_scope_and_target_collection_bounds() -> None:
    scope_bounded = PreparedRuntimeIndex(max_scopes=1)
    scope_bounded.replace(
        SCOPE,
        revision_id=RevisionIdentity("a" * 64),
        policy_digest=provenance_digest({"policy": "current"}),
        artifact=ARTIFACT,
        targets={},
        triggers=TriggersConfig(triggers={}),
    )
    with pytest.raises(ActivationLimitError, match="scope collection"):
        scope_bounded.replace(
            OTHER_SCOPE,
            revision_id=RevisionIdentity("b" * 64),
            policy_digest=provenance_digest({"policy": "current"}),
            artifact=ARTIFACT,
            targets={},
            triggers=TriggersConfig(triggers={}),
        )
