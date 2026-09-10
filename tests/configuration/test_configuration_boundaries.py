from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.settings import (
    AwsConfigurationSettings,
    FileConfigurationSettings,
    SqliteConfigurationSettings,
)
from justflow.config.triggers import TriggersConfig
from justflow.configuration import (
    ActivePointer,
    ConfigurationBundle,
    ConfigurationError,
    ConfigurationIntegrityError,
    ConfigurationNotFoundError,
    ConfigurationScopeError,
    FileConfigurationSource,
    RevisionRecord,
    SqliteConfigurationStore,
    StoredConfigurationSource,
    TenantConfiguration,
    TenantWorkflowConfig,
    configured_configuration_source,
    configured_configuration_store,
    render_configuration_yaml,
)
from justflow.configuration.models import configuration_revision_identity
from justflow.scope import RuntimeScope

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


@pytest.fixture
def sqlite_store(tmp_path: Path) -> Iterator[SqliteConfigurationStore]:
    store = SqliteConfigurationStore(tmp_path / "configuration.sqlite3")
    try:
        yield store
    finally:
        store.close()


def bundle() -> ConfigurationBundle:
    workflow = WorkflowConfig(
        workflow="example",
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    return ConfigurationBundle(
        workflows={"example": workflow},
        triggers=TriggersConfig(triggers={}),
    )


def tenant_document() -> TenantConfiguration:
    workflow = TenantWorkflowConfig(
        workflow="example",
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    return TenantConfiguration(
        component_catalog_revision="a" * 64,
        workflows={"example": workflow},
        triggers={},
    )


def test_configuration_bundle_canonical_round_trip_and_integrity_checks() -> None:
    payload = bundle().canonical_bytes()

    assert ConfigurationBundle.from_bytes(payload) == bundle()
    with pytest.raises(ValueError, match="not canonical"):
        ConfigurationBundle.from_bytes(json.dumps(json.loads(payload), indent=2).encode())
    with pytest.raises(ValueError, match="duplicate"):
        ConfigurationBundle.from_bytes(b'{"workflows":{},"workflows":{}}')
    with pytest.raises(ValueError, match="empty"):
        ConfigurationBundle.from_bytes(b"")
    with pytest.raises(ValueError, match="Non-finite"):
        ConfigurationBundle.from_bytes(b"NaN")


def test_tenant_configuration_canonical_round_trip() -> None:
    payload = tenant_document().canonical_bytes()

    assert TenantConfiguration.from_bytes(payload) == tenant_document()


def test_file_source_is_complete_and_single_scope(tmp_path: Path) -> None:
    (tmp_path / "resources.yaml").write_text("resources: {}\n")
    (tmp_path / "services.yaml").write_text("services: {}\n")
    (tmp_path / "triggers.yaml").write_text("triggers: {}\n")
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows" / "example.yaml").write_text(
        "workflow: example\nsteps: {}\nflow:\n  - name: done\n    terminal: true\n"
    )
    source = FileConfigurationSource(tmp_path, scope=SCOPE)

    snapshot = source.read(SCOPE)
    assert tuple(snapshot.bundle.workflows) == ("example",)
    assert source.read_triggers(SCOPE).bundle.triggers.triggers == {}
    assert snapshot.execution_identity.configuration_revision_id == str(snapshot.revision_id)
    assert source.workflow_sources["example"].name == "example.yaml"
    with pytest.raises(ConfigurationScopeError):
        source.read(OTHER_SCOPE)
    with pytest.raises(ConfigurationScopeError):
        source.read_triggers(OTHER_SCOPE)


def test_safe_yaml_export_round_trips_through_the_file_source(tmp_path: Path) -> None:
    for relative_path, payload in render_configuration_yaml(bundle()).items():
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)

    assert FileConfigurationSource(tmp_path, scope=SCOPE).read(SCOPE).bundle == bundle()


def test_stored_source_never_falls_back_when_active_revision_is_missing(
    sqlite_store: SqliteConfigurationStore,
) -> None:
    source = StoredConfigurationSource(sqlite_store)

    with pytest.raises(ConfigurationNotFoundError):
        source.read(SCOPE)

    revision = sqlite_store.create_revision(SCOPE, bundle(), parent_revision_id=None)
    sqlite_store.compare_and_swap_active(
        SCOPE,
        revision.revision_id,
        expected_revision_id=None,
    )
    snapshot = source.read(SCOPE)
    assert snapshot.bundle == bundle()
    assert snapshot.revision_id == revision.revision_id
    assert source.read_triggers(SCOPE).bundle.triggers.triggers == {}


def test_stored_source_rejects_inconsistent_active_revision_identity() -> None:
    revision_id = configuration_revision_identity(OTHER_SCOPE.digest, bundle(), None)
    store = MagicMock()
    store.read_active.return_value = ActivePointer(
        scope_digest=SCOPE.digest,
        revision_id=revision_id,
        version=1,
    )
    store.read_revision.return_value = RevisionRecord(
        scope_digest=SCOPE.digest,
        revision_id=revision_id,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        bundle=bundle(),
    )

    with pytest.raises(ConfigurationIntegrityError, match="identity is inconsistent"):
        StoredConfigurationSource(store).read(SCOPE)


def test_store_retains_tenant_documents_without_executing_unpublished_content(
    sqlite_store: SqliteConfigurationStore,
) -> None:
    document = tenant_document()

    draft = sqlite_store.compare_and_swap_draft(SCOPE, document, expected_version=None)
    revision = sqlite_store.create_revision(SCOPE, document, parent_revision_id=None)
    sqlite_store.compare_and_swap_active(
        SCOPE,
        revision.revision_id,
        expected_revision_id=None,
    )

    assert draft.bundle == document
    assert sqlite_store.read_revision(SCOPE, revision.revision_id).bundle == document
    with pytest.raises(ConfigurationError, match="requires publication"):
        StoredConfigurationSource(sqlite_store).read(SCOPE)


def test_configuration_factories_keep_file_and_writable_profiles_explicit(
    tmp_path: Path,
) -> None:
    sqlite_settings = SqliteConfigurationSettings(path=str(tmp_path / "store.sqlite3"))
    store = configured_configuration_store(sqlite_settings)

    assert isinstance(store, SqliteConfigurationStore)
    assert isinstance(
        configured_configuration_source(
            FileConfigurationSettings(),
            scope=SCOPE,
            config_dir=tmp_path,
        ),
        FileConfigurationSource,
    )
    assert isinstance(
        configured_configuration_source(
            sqlite_settings,
            scope=SCOPE,
            config_dir=tmp_path,
            store=store,
        ),
        StoredConfigurationSource,
    )
    with pytest.raises(TypeError, match="no writable store"):
        configured_configuration_store(FileConfigurationSettings())
    with pytest.raises(ValueError, match="cannot use"):
        configured_configuration_source(
            FileConfigurationSettings(),
            scope=SCOPE,
            config_dir=tmp_path,
            store=store,
        )
    assert isinstance(store, SqliteConfigurationStore)
    store.close()


def test_aws_configuration_settings_validate_security_boundaries() -> None:
    settings = AwsConfigurationSettings(
        bucket="configuration-bucket",
        table_name="configuration-table",
        revision_index_name="scope-revisions",
    )

    assert settings.prefix == "justflow/configuration/"
    with pytest.raises(ValidationError, match="KMS key"):
        AwsConfigurationSettings(
            bucket="configuration-bucket",
            table_name="configuration-table",
            revision_index_name="scope-revisions",
            server_side_encryption="aws:kms",
        )
    with pytest.raises(ValidationError, match="without credentials"):
        AwsConfigurationSettings(
            bucket="configuration-bucket",
            table_name="configuration-table",
            revision_index_name="scope-revisions",
            endpoint_url="https://user:password@example.invalid",
        )
