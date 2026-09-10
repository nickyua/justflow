from __future__ import annotations

from datetime import UTC, datetime

import pytest

from justflow.config.models import FlowStep, ResourcesConfig, ServicesConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.configuration import (
    ConfigurationDiffOperation,
    ConfigurationPublicationService,
    ConfigurationValidationCategory,
    ConfigurationValidationSeverity,
    PlatformComponentCatalog,
    PublicationErrorCode,
    PublicationOperationError,
    PublicationState,
    RevisionIdentity,
    SqliteActivationStore,
    SqliteConfigurationStore,
    StaticTenantAuthoringPolicySource,
    TemporalIsolationMode,
    TemporalIsolationPolicy,
    TenantAuthoringPolicy,
    TenantConfiguration,
    TenantWorkflowConfig,
)
from justflow.configuration.errors import ConfigurationConflictError, ConfigurationNotFoundError
from justflow.configuration.lifecycle import (
    ConfigurationDiscardState,
    ConfigurationRelationshipState,
    discard_identity,
)
from justflow.provenance import provenance_digest
from justflow.resources.builtins import builtin_resource_registry
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
NOW = datetime(2026, 8, 4, 12, tzinfo=UTC)


class StaticCatalogSource:
    def read(self, revision_id: str) -> PlatformComponentCatalog:
        if revision_id != CATALOG.revision_id:
            raise ConfigurationNotFoundError("Component catalog was not found")
        return CATALOG


def configuration(
    *,
    catalog_revision: str = CATALOG.revision_id,
    workflows: dict[str, str] | None = None,
) -> TenantConfiguration:
    return TenantConfiguration(
        component_catalog_revision=catalog_revision,
        workflows={
            name: TenantWorkflowConfig(
                workflow=name,
                description=description,
                steps={},
                flow=[FlowStep(name="done", terminal=True)],
            )
            for name, description in (workflows or {}).items()
        },
        triggers={},
    )


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


def activate_draft(service, store, draft, *, idempotency_key: str) -> RevisionIdentity:
    publication = service.publish(
        SCOPE,
        expected_draft_version=draft.version,
        idempotency_key=idempotency_key,
        actor_identity="operator",
        correlation_identity=idempotency_key,
    )
    assert publication.published_revision_id is not None
    store.compare_and_swap_active(
        SCOPE,
        publication.published_revision_id,
        expected_revision_id=None,
    )
    return publication.published_revision_id


@pytest.fixture
def publication_service(tmp_path):
    configuration_store = SqliteConfigurationStore(tmp_path / "configuration.sqlite3")
    activation_store = SqliteActivationStore(tmp_path / "activation.sqlite3")
    service = ConfigurationPublicationService(
        configuration_store=configuration_store,
        activation_store=activation_store,
        policy_source=StaticTenantAuthoringPolicySource(
            {SCOPE.digest: policy(), OTHER_SCOPE.digest: policy(OTHER_SCOPE)}
        ),
        component_catalog_source=StaticCatalogSource(),
        platform_resources=ResourcesConfig(resources={}),
        platform_services=ServicesConfig(services={}),
        transport_registry=builtin_transport_registry(),
        resource_registry=builtin_resource_registry(),
        limits=DEFAULT_RUNTIME_LIMITS,
        config_dir=tmp_path,
        clock=lambda: NOW,
    )
    try:
        yield service, configuration_store
    finally:
        activation_store.close()
        configuration_store.close()


def test_publication_creates_immutable_revisions_without_activation(publication_service) -> None:
    service, store = publication_service
    draft = service.create_draft(SCOPE, configuration())

    published = service.publish(
        SCOPE,
        expected_draft_version=draft.version,
        idempotency_key="publish-orders",
        actor_identity="operator@example.invalid",
        correlation_identity="request-1",
    )
    repeated = service.publish(
        SCOPE,
        expected_draft_version=draft.version,
        idempotency_key="publish-orders",
        actor_identity="operator@example.invalid",
        correlation_identity="request-1",
    )

    assert published == repeated
    assert published.state is PublicationState.APPLIED
    assert published.source_revision_id is not None
    assert published.published_revision_id is not None
    assert store.read_active(SCOPE) is None
    source = store.read_revision(SCOPE, published.source_revision_id)
    resolved = store.read_revision(SCOPE, published.published_revision_id)
    assert isinstance(source.bundle, TenantConfiguration)
    assert resolved.bundle.tenant_resolution is not None
    assert (
        resolved.bundle.tenant_resolution.tenant_configuration_revision_id
        == published.source_revision_id
    )
    assert "operator@example.invalid" not in published.model_dump_json()


def test_publication_rejects_changed_request_for_idempotency_key(publication_service) -> None:
    service, _ = publication_service
    draft = service.create_draft(SCOPE, configuration())
    service.publish(
        SCOPE,
        expected_draft_version=draft.version,
        idempotency_key="publish-orders",
        actor_identity="operator",
        correlation_identity="request-1",
    )
    updated = service.update_draft(
        SCOPE,
        configuration(),
        expected_version=draft.version,
    )

    with pytest.raises(PublicationOperationError) as error:
        service.publish(
            SCOPE,
            expected_draft_version=updated.version,
            idempotency_key="publish-orders",
            actor_identity="operator",
            correlation_identity="request-2",
        )

    assert error.value.code is PublicationErrorCode.CONFLICT
    assert error.value.retryable is False


def test_publication_requires_matching_draft_version(publication_service) -> None:
    service, _ = publication_service
    service.create_draft(SCOPE, configuration())

    with pytest.raises(ConfigurationConflictError):
        service.publish(
            SCOPE,
            expected_draft_version=2,
            idempotency_key="publish-orders",
            actor_identity="operator",
            correlation_identity="request-1",
        )


@pytest.mark.parametrize("edit_draft", [False, True], ids=["activation", "draft-edit"])
def test_publication_retry_recovers_original_result_after_state_changes(
    publication_service, edit_draft: bool
) -> None:
    service, store = publication_service
    draft = service.create_draft(SCOPE, configuration())
    args = {
        "expected_draft_version": draft.version,
        "idempotency_key": "retry-publication",
        "actor_identity": "operator",
        "correlation_identity": "request",
    }
    original = service.publish(SCOPE, **args)
    if edit_draft:
        service.update_draft(
            SCOPE, configuration(workflows={"later": "later edit"}), expected_version=draft.version
        )
    else:
        store.compare_and_swap_active(
            SCOPE, original.published_revision_id, expected_revision_id=None
        )
    assert service.publish(SCOPE, **args) == original


def test_validation_and_publication_report_unavailable_catalog(publication_service) -> None:
    service, _ = publication_service
    unknown_catalog = "f" * 64
    draft = service.create_draft(SCOPE, configuration(catalog_revision=unknown_catalog))

    report = service.validate_draft(SCOPE)

    assert report.valid is False
    assert report.issues[0].category is ConfigurationValidationCategory.POLICY
    with pytest.raises(PublicationOperationError) as error:
        service.publish(
            SCOPE,
            expected_draft_version=draft.version,
            idempotency_key="publish-orders",
            actor_identity="operator",
            correlation_identity="request-1",
        )
    assert error.value.code is PublicationErrorCode.INVALID_CONFIGURATION
    assert error.value.retryable is False


def test_validation_preserves_nonfatal_unreachable_workflow_warning(
    publication_service,
) -> None:
    service, _ = publication_service
    service.create_draft(SCOPE, configuration(workflows={"orders": "active"}))

    report = service.validate_draft(SCOPE)

    assert report.valid is True
    assert len(report.issues) == 1
    assert report.issues[0].severity is ConfigurationValidationSeverity.WARNING
    assert report.issues[0].category is ConfigurationValidationCategory.SEMANTIC
    assert report.issues[0].location == ("workflows", "orders")
    assert "internal-only" in report.issues[0].message


def test_draft_yaml_round_trip_and_scope_isolation(publication_service) -> None:
    service, _ = publication_service
    draft = service.create_draft(SCOPE, configuration())
    payload = service.export_draft_yaml(SCOPE)
    imported = service.import_draft_yaml(
        SCOPE,
        payload,
        expected_version=draft.version,
    )

    assert imported.bundle == draft.bundle
    with pytest.raises(ConfigurationNotFoundError):
        service.export_draft_yaml(OTHER_SCOPE)


def test_revision_diff_omits_values(publication_service) -> None:
    service, store = publication_service
    source = store.create_revision(SCOPE, configuration(), parent_revision_id=None)
    target_document = configuration().model_copy(update={"component_catalog_revision": "f" * 64})
    target = store.create_revision(
        SCOPE,
        target_document,
        parent_revision_id=source.revision_id,
    )

    difference = service.compare_revisions(SCOPE, source.revision_id, target.revision_id)

    assert [(item.operation, item.path) for item in difference.items] == [
        (ConfigurationDiffOperation.UPDATE, ("component_catalog_revision",))
    ]
    assert (
        provenance_digest(target_document.model_dump(mode="json"))
        not in difference.model_dump_json()
    )


def test_relationships_and_discard_restore_the_active_authoring_revision(
    publication_service,
) -> None:
    service, store = publication_service
    active_configuration = configuration(workflows={"legacy": "active", "orders": "active"})
    initial = service.create_draft(SCOPE, active_configuration)
    active_revision = activate_draft(
        service,
        store,
        initial,
        idempotency_key="publish-active",
    )
    changed = service.update_draft(
        SCOPE,
        configuration(workflows={"billing": "new", "orders": "modified"}),
        expected_version=initial.version,
    )

    relationships = service.relationships(SCOPE)
    assert relationships.active_identity == str(active_revision)
    assert {
        (relationship.name, relationship.state) for relationship in relationships.relationships
    } == {
        ("billing", ConfigurationRelationshipState.NEW_PENDING_APPLY),
        ("legacy", ConfigurationRelationshipState.REMOVED_PENDING_APPLY),
        ("orders", ConfigurationRelationshipState.MODIFIED),
    }

    discarded = service.discard(
        SCOPE,
        expected_draft_version=changed.version,
        expected_active_identity=active_revision,
        idempotency_key="discard-managed",
        actor_identity="operator@example.invalid",
        correlation_identity="request-2",
    )
    repeated = service.discard(
        SCOPE,
        expected_draft_version=changed.version,
        expected_active_identity=active_revision,
        idempotency_key="discard-managed",
        actor_identity="operator@example.invalid",
        correlation_identity="request-2",
    )

    assert discarded == repeated
    assert discarded.running_process_changed is False
    assert service.read_draft(SCOPE).bundle == active_configuration
    assert all(
        relationship.state is ConfigurationRelationshipState.ACTIVE
        for relationship in service.relationships(SCOPE).relationships
    )
    with pytest.raises(ConfigurationNotFoundError):
        service.read_discard(OTHER_SCOPE, discarded.discard_id)


def test_discard_rejects_stale_active_identity_without_changing_the_draft(
    publication_service,
) -> None:
    service, store = publication_service
    draft = service.create_draft(SCOPE, configuration())
    activate_draft(
        service,
        store,
        draft,
        idempotency_key="publish-before-stale-discard",
    )

    with pytest.raises(ConfigurationConflictError):
        service.discard(
            SCOPE,
            expected_draft_version=draft.version,
            expected_active_identity=RevisionIdentity("f" * 64),
            idempotency_key="discard-stale-active",
            actor_identity="operator",
            correlation_identity="request",
        )

    assert service.read_draft(SCOPE) == draft


def test_discard_requires_an_active_authoring_revision(publication_service) -> None:
    service, _ = publication_service
    draft = service.create_draft(SCOPE, configuration())

    with pytest.raises(ConfigurationNotFoundError):
        service.discard(
            SCOPE,
            expected_draft_version=draft.version,
            expected_active_identity=RevisionIdentity("f" * 64),
            idempotency_key="discard-without-active",
            actor_identity="operator",
            correlation_identity="request",
        )


def test_equivalent_discards_converge_but_idempotency_key_reuse_conflicts(
    publication_service,
) -> None:
    service, store = publication_service
    initial = service.create_draft(SCOPE, configuration())
    active_revision = activate_draft(
        service,
        store,
        initial,
        idempotency_key="publish-convergent-discard",
    )
    changed = service.update_draft(
        SCOPE,
        configuration(workflows={"orders": "changed"}),
        expected_version=initial.version,
    )
    first = service.discard(
        SCOPE,
        expected_draft_version=changed.version,
        expected_active_identity=active_revision,
        idempotency_key="discard-first",
        actor_identity="operator",
        correlation_identity="request-1",
    )
    concurrent_equivalent = service.discard(
        SCOPE,
        expected_draft_version=changed.version,
        expected_active_identity=active_revision,
        idempotency_key="discard-second",
        actor_identity="operator",
        correlation_identity="request-2",
    )
    changed_again = service.update_draft(
        SCOPE,
        configuration(workflows={"billing": "changed again"}),
        expected_version=first.working_version,
    )

    assert concurrent_equivalent.working_version == first.working_version
    with pytest.raises(ConfigurationConflictError):
        service.discard(
            SCOPE,
            expected_draft_version=changed_again.version,
            expected_active_identity=active_revision,
            idempotency_key="discard-first",
            actor_identity="operator",
            correlation_identity="request-3",
        )


def test_stale_discard_is_audited_as_failed(publication_service) -> None:
    service, store = publication_service
    initial = service.create_draft(SCOPE, configuration())
    active_revision = activate_draft(
        service,
        store,
        initial,
        idempotency_key="publish-before-failed-discard",
    )
    changed = service.update_draft(
        SCOPE,
        configuration(workflows={"orders": "changed"}),
        expected_version=initial.version,
    )
    service.update_draft(
        SCOPE,
        configuration(workflows={"orders": "changed again"}),
        expected_version=changed.version,
    )

    with pytest.raises(ConfigurationConflictError):
        service.discard(
            SCOPE,
            expected_draft_version=changed.version,
            expected_active_identity=active_revision,
            idempotency_key="discard-stale-draft",
            actor_identity="operator",
            correlation_identity="request",
        )

    record = service.read_discard(
        SCOPE,
        discard_identity(SCOPE.digest, "discard-stale-draft"),
    )
    assert record.state is ConfigurationDiscardState.FAILED
    assert "operator" not in record.model_dump_json()


def test_export_rejects_version_from_before_a_concurrent_edit(publication_service) -> None:
    service, _ = publication_service
    initial = service.create_draft(SCOPE, configuration())
    current = service.update_draft(
        SCOPE, configuration(workflows={"new": "concurrent edit"}), expected_version=initial.version
    )
    with pytest.raises(ConfigurationConflictError):
        service.export_draft_yaml(SCOPE, expected_version=initial.version)
    assert b"concurrent edit" in service.export_draft_yaml(SCOPE, expected_version=current.version)
