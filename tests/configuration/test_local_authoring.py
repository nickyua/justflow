from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.config.schedules import IntervalScheduleSpec
from justflow.config.triggers import ScheduleTriggerDeclaration
from justflow.configuration.errors import (
    ConfigurationConflictError,
    ConfigurationError,
    ConfigurationNotFoundError,
    ConfigurationScopeError,
    ConfigurationUnavailableError,
)
from justflow.configuration.lifecycle import (
    ConfigurationApplyStageKind,
    ConfigurationApplyStageState,
    ConfigurationRelationshipState,
)
from justflow.configuration.local_authoring import (
    LocalAuthoringConfigurationSource,
    LocalAuthoringDocument,
)
from justflow.configuration.models import RevisionIdentity
from justflow.definitions.catalog import CatalogError, CatalogStore
from justflow.resources.builtins import builtin_resource_registry
from justflow.scope import RuntimeScope
from justflow.transports.builtins import builtin_transport_registry

SCOPE = RuntimeScope.create(
    tenant="tenant-a",
    application="orders",
    environment="local",
)
OTHER_SCOPE = RuntimeScope.create(
    tenant="tenant-b",
    application="orders",
    environment="local",
)


def workflow(name: str) -> WorkflowConfig:
    return WorkflowConfig(
        workflow=name,
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )


def document(name: str, *, with_trigger: bool = False) -> LocalAuthoringDocument:
    triggers = (
        {
            "hourly": ScheduleTriggerDeclaration(
                workflow=name,
                spec=IntervalScheduleSpec(every_seconds=3_600),
            )
        }
        if with_trigger
        else {}
    )
    return LocalAuthoringDocument(
        workflows={name: workflow(name)},
        triggers=triggers,
    )


def source(
    config_dir: Path,
    *,
    definition_catalog_store: CatalogStore | None = None,
) -> LocalAuthoringConfigurationSource:
    return LocalAuthoringConfigurationSource(
        config_dir,
        scope=SCOPE,
        transport_registry=builtin_transport_registry(),
        resource_registry=builtin_resource_registry(),
        limits=RuntimeLimits(),
        definition_catalog_store=definition_catalog_store,
    )


@pytest.fixture
def config_dir(tmp_path: Path) -> Path:
    root = tmp_path / "configs"
    root.mkdir()
    (root / "resources.yaml").write_text("resources: {}\n")
    (root / "services.yaml").write_text("services: {}\n")
    (root / "triggers.yaml").write_text("triggers: {}\n")
    workflows = root / "workflows"
    workflows.mkdir()
    (workflows / "example.yaml").write_text(
        "workflow: example\nsteps: {}\nflow:\n  - name: done\n    terminal: true\n"
    )
    return root


def test_saves_write_the_configuration_files_and_flag_restart(config_dir: Path) -> None:
    running_source = source(config_dir)
    initial = running_source.read_draft(SCOPE)

    saved = running_source.update_draft(
        SCOPE,
        document("replacement", with_trigger=True),
        expected_version=initial.version,
    )

    assert saved.restart_required is True
    workflow_file = config_dir / "workflows" / "replacement.yaml"
    assert workflow_file.exists()
    assert yaml.safe_load(workflow_file.read_text())["workflow"] == "replacement"
    assert not (config_dir / "workflows" / "example.yaml").exists()
    assert "hourly" in yaml.safe_load((config_dir / "triggers.yaml").read_text())["triggers"]
    # The files are the truth immediately; only the startup snapshot lags.
    assert tuple(running_source.read(SCOPE).bundle.workflows) == ("replacement",)

    restarted_source = source(config_dir)
    assert restarted_source.read_draft(SCOPE).restart_required is False


def test_definition_catalog_failure_is_reported_as_unpublished(
    config_dir: Path, tmp_path: Path
) -> None:
    catalog = CatalogStore(tmp_path / "definitions")
    configuration_source = source(config_dir, definition_catalog_store=catalog)
    initial = configuration_source.read_draft(SCOPE)

    with patch.object(catalog, "publish", side_effect=CatalogError("catalog unavailable")):
        saved = configuration_source.update_draft(
            SCOPE,
            document("replacement"),
            expected_version=initial.version,
        )

    assert saved.definitions_published is False


def test_unexpected_definition_publication_value_error_is_not_downgraded(
    config_dir: Path,
    tmp_path: Path,
) -> None:
    catalog = CatalogStore(tmp_path / "definitions")
    configuration_source = source(config_dir, definition_catalog_store=catalog)
    initial = configuration_source.read_draft(SCOPE)

    with (
        patch.object(catalog, "publish", side_effect=ValueError("programmer defect")),
        pytest.raises(ValueError, match="programmer defect"),
    ):
        configuration_source.update_draft(
            SCOPE,
            document("replacement"),
            expected_version=initial.version,
        )


def test_workflow_fragment_save_touches_only_that_workflow_file(config_dir: Path) -> None:
    running_source = source(config_dir)
    version = running_source.read_draft(SCOPE).version
    fragment = b"workflow: added\nsteps: {}\nflow:\n- name: done\n  terminal: true\n"

    saved = running_source.update_workflow_fragment(
        SCOPE, "added", fragment, expected_version=version
    )

    assert (config_dir / "workflows" / "added.yaml").exists()
    assert (config_dir / "workflows" / "example.yaml").exists()
    assert set(saved.bundle.workflows) == {"added", "example"}


def test_local_draft_compare_and_swap_allows_only_one_concurrent_update(
    config_dir: Path,
) -> None:
    configuration_source = source(config_dir)
    expected_version = configuration_source.read_draft(SCOPE).version

    def update(name: str) -> str:
        try:
            configuration_source.update_draft(
                SCOPE,
                document(name),
                expected_version=expected_version,
            )
        except ConfigurationConflictError:
            return "conflict"
        return "saved"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(update, ("first", "second")))

    assert sorted(outcomes) == ["conflict", "saved"]


def test_local_authoring_is_scope_bound_and_excludes_host_configuration(
    config_dir: Path,
) -> None:
    configuration_source = source(config_dir)

    with pytest.raises(ConfigurationScopeError):
        configuration_source.read_draft(OTHER_SCOPE)
    with pytest.raises(ConfigurationError, match="invalid"):
        configuration_source.parse_yaml(
            b"workflows: {}\ntriggers: {}\nservices:\n  leaked:\n    transport: http\n"
        )


def test_local_authoring_yaml_round_trips_declarations_only(config_dir: Path) -> None:
    configuration_source = source(config_dir)
    draft = configuration_source.read_draft(SCOPE)
    configuration_source.update_draft(
        SCOPE,
        document("example", with_trigger=True),
        expected_version=draft.version,
    )

    payload = configuration_source.export_draft_yaml(SCOPE)
    parsed = configuration_source.parse_yaml(payload)

    assert parsed == document("example", with_trigger=True)
    assert b"resources" not in payload
    assert b"services" not in payload


def test_local_relationships_apply_and_discard_preserve_process_truth(
    config_dir: Path,
    tmp_path: Path,
) -> None:
    catalog = CatalogStore(tmp_path / "definitions")
    configuration_source = source(
        config_dir,
        definition_catalog_store=catalog,
    )
    active = configuration_source.relationships(SCOPE)
    assert active.active_identity is not None
    initial = configuration_source.read_draft(SCOPE)
    saved = configuration_source.update_draft(
        SCOPE,
        document("replacement", with_trigger=True),
        expected_version=initial.version,
    )

    relationships = configuration_source.relationships(SCOPE)
    assert relationships.restart_required is True
    assert {
        (relationship.name, relationship.state) for relationship in relationships.relationships
    } == {
        ("example", ConfigurationRelationshipState.REMOVED_PENDING_APPLY),
        ("hourly", ConfigurationRelationshipState.NEW_PENDING_APPLY),
        ("replacement", ConfigurationRelationshipState.NEW_PENDING_APPLY),
    }

    applied = configuration_source.apply(
        SCOPE,
        expected_version=saved.version,
        actor_identity="operator@example.invalid",
        correlation_identity="request-1",
    )

    assert applied.definitions_published is True
    assert applied.restart_required is True
    assert applied.running_process_changed is False
    assert tuple((stage.kind, stage.state) for stage in applied.stages) == (
        (
            ConfigurationApplyStageKind.VALIDATION,
            ConfigurationApplyStageState.COMPLETED,
        ),
        (
            ConfigurationApplyStageKind.DEFINITION_PUBLICATION,
            ConfigurationApplyStageState.COMPLETED,
        ),
        (
            ConfigurationApplyStageKind.PROCESS_RESTART,
            ConfigurationApplyStageState.RESTART_REQUIRED,
        ),
    )
    assert catalog.load().resolve("replacement").logical_name == "replacement"
    assert configuration_source.relationships(SCOPE).active_identity == active.active_identity

    discarded = configuration_source.discard(
        SCOPE,
        expected_version=saved.version,
        expected_active_identity=RevisionIdentity(active.active_identity),
        idempotency_key="discard-local",
        actor_identity="operator@example.invalid",
        correlation_identity="request-2",
    )
    repeated = configuration_source.discard(
        SCOPE,
        expected_version=saved.version,
        expected_active_identity=RevisionIdentity(active.active_identity),
        idempotency_key="discard-local",
        actor_identity="operator@example.invalid",
        correlation_identity="request-2",
    )

    assert discarded == repeated
    assert discarded.running_process_changed is False
    assert discarded.restart_required is False
    assert configuration_source.read_draft(SCOPE).bundle == initial.bundle
    assert all(
        relationship.state is ConfigurationRelationshipState.ACTIVE
        for relationship in configuration_source.relationships(SCOPE).relationships
    )


def test_local_discard_compare_and_swap_rejects_stale_working_version(
    config_dir: Path,
) -> None:
    configuration_source = source(config_dir)
    relationships = configuration_source.relationships(SCOPE)
    assert relationships.active_identity is not None
    initial = configuration_source.read_draft(SCOPE)
    saved = configuration_source.update_draft(
        SCOPE,
        document("replacement"),
        expected_version=initial.version,
    )

    with pytest.raises(ConfigurationConflictError):
        configuration_source.discard(
            SCOPE,
            expected_version=initial.version,
            expected_active_identity=RevisionIdentity(relationships.active_identity),
            idempotency_key="discard-stale",
            actor_identity="operator",
            correlation_identity="request",
        )

    assert configuration_source.read_draft(SCOPE).version == saved.version


def test_local_discard_allows_only_one_concurrent_request(config_dir: Path) -> None:
    configuration_source = source(config_dir)
    relationships = configuration_source.relationships(SCOPE)
    assert relationships.active_identity is not None
    initial = configuration_source.read_draft(SCOPE)
    saved = configuration_source.update_draft(
        SCOPE,
        document("replacement"),
        expected_version=initial.version,
    )

    def discard(key: str) -> str:
        try:
            configuration_source.discard(
                SCOPE,
                expected_version=saved.version,
                expected_active_identity=RevisionIdentity(relationships.active_identity),
                idempotency_key=key,
                actor_identity="operator",
                correlation_identity=key,
            )
        except ConfigurationConflictError:
            return "conflict"
        return "discarded"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(discard, ("discard-1", "discard-2")))

    assert sorted(outcomes) == ["conflict", "discarded"]


def test_local_apply_requires_exact_version_and_definition_store(config_dir: Path) -> None:
    configuration_source = source(config_dir)
    version = configuration_source.read_draft(SCOPE).version

    with pytest.raises(ConfigurationConflictError):
        configuration_source.apply(
            SCOPE,
            expected_version=version + 1,
            actor_identity="operator",
            correlation_identity="request-1",
        )
    with pytest.raises(ConfigurationUnavailableError, match="publication is unavailable"):
        configuration_source.apply(
            SCOPE,
            expected_version=version,
            actor_identity="operator",
            correlation_identity="request-2",
        )


def test_local_apply_translates_catalog_failure(
    config_dir: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = CatalogStore(tmp_path / "definitions")
    configuration_source = source(config_dir, definition_catalog_store=catalog)
    version = configuration_source.read_draft(SCOPE).version

    def reject_publication(_manifests) -> None:
        raise CatalogError("private catalog failure")

    monkeypatch.setattr(catalog, "publish", reject_publication)

    with pytest.raises(ConfigurationUnavailableError, match="could not be published"):
        configuration_source.apply(
            SCOPE,
            expected_version=version,
            actor_identity="operator",
            correlation_identity="request",
        )


def test_local_discard_rejects_active_mismatch_key_reuse_and_missing_audit(
    config_dir: Path,
) -> None:
    configuration_source = source(config_dir)
    relationships = configuration_source.relationships(SCOPE)
    assert relationships.active_identity is not None
    initial = configuration_source.read_draft(SCOPE)
    saved = configuration_source.update_draft(
        SCOPE,
        document("replacement"),
        expected_version=initial.version,
    )

    with pytest.raises(ConfigurationConflictError):
        configuration_source.discard(
            SCOPE,
            expected_version=saved.version,
            expected_active_identity=RevisionIdentity("f" * 64),
            idempotency_key="discard-local",
            actor_identity="operator",
            correlation_identity="request-1",
        )
    configuration_source.discard(
        SCOPE,
        expected_version=saved.version,
        expected_active_identity=RevisionIdentity(relationships.active_identity),
        idempotency_key="discard-local",
        actor_identity="operator",
        correlation_identity="request-2",
    )
    with pytest.raises(ConfigurationConflictError):
        configuration_source.discard(
            SCOPE,
            expected_version=initial.version,
            expected_active_identity=RevisionIdentity(relationships.active_identity),
            idempotency_key="discard-local",
            actor_identity="operator",
            correlation_identity="request-3",
        )
    with pytest.raises(ConfigurationNotFoundError):
        configuration_source.read_discard(SCOPE, "f" * 64)


def test_export_rejects_version_from_before_a_concurrent_edit(config_dir: Path) -> None:
    configuration_source = source(config_dir)
    initial = configuration_source.read_draft(SCOPE)
    current = configuration_source.update_draft(
        SCOPE, document("new"), expected_version=initial.version
    )
    with pytest.raises(ConfigurationConflictError):
        configuration_source.export_draft_yaml(SCOPE, expected_version=initial.version)
    assert b"new" in configuration_source.export_draft_yaml(SCOPE, expected_version=current.version)
