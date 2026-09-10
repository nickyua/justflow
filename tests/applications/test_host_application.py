from __future__ import annotations

import pytest
from host_application import (
    compose_application,
    create_application,
    initialize_local_configuration_database,
)
from host_application.configuration import (
    HostConfigurationConflictError,
    activate_local_configuration,
    publish_local_configuration,
)
from host_application.resources import (
    PROVIDER_CONTRACT_VERSION,
    PROVIDER_NAME,
    TENANT_SETTINGS_PROVIDER,
    TenantSettings,
    TenantSettingsUnavailableError,
    create_resource_registry,
)

from justflow.config.models import ResourceConfig
from justflow.config.settings import Settings, SqliteConfigurationSettings
from justflow.configuration import ConfigurationNotFoundError, SqliteConfigurationStore
from justflow.resources import ResourceCapability, ResourceConfigError, ResourceFactoryContext
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope
from tests.settings import PRODUCTION_RUNTIME

CONFIG_DIR = "examples/host_application/src/host_application/configs"
FIRST_SCOPE = RuntimeScope.create(
    tenant="tenant-one",
    application="host-example",
    environment="test",
)
SECOND_SCOPE = RuntimeScope.create(
    tenant="tenant-two",
    application="host-example",
    environment="test",
)


def test_host_example_bootstraps_an_idempotent_sqlite_configuration(tmp_path) -> None:
    database = tmp_path / "configuration.sqlite3"

    first = initialize_local_configuration_database(database)
    second = initialize_local_configuration_database(database)
    application = create_application(configuration_database=database)
    store = SqliteConfigurationStore(database)
    try:
        active = store.read_active(LOCAL_RUNTIME_SCOPE)
    finally:
        store.close()

    assert first == second
    assert active is not None
    assert active.revision_id == first
    assert application.settings.configuration == SqliteConfigurationSettings(path=str(database))


@pytest.mark.parametrize(
    ("scope"),
    [
        pytest.param(FIRST_SCOPE, id="first-scope"),
        pytest.param(SECOND_SCOPE, id="second-scope"),
    ],
)
def test_host_example_keeps_configuration_revisions_scope_isolated(
    tmp_path,
    scope: RuntimeScope,
) -> None:
    database = tmp_path / "configuration.sqlite3"
    revision_id = initialize_local_configuration_database(database, scope=scope)
    store = SqliteConfigurationStore(database)
    other_scope = SECOND_SCOPE if scope == FIRST_SCOPE else FIRST_SCOPE
    try:
        active = store.read_active(scope)
        with pytest.raises(ConfigurationNotFoundError):
            store.read_revision(other_scope, revision_id)
    finally:
        store.close()

    assert active is not None
    assert active.revision_id == revision_id
    assert create_application(scope=scope).settings.runtime.scope == scope


def test_host_example_publication_and_activation_enforce_concurrency(tmp_path) -> None:
    database = tmp_path / "configuration.sqlite3"
    active_revision = initialize_local_configuration_database(database)
    next_revision = publish_local_configuration(
        database,
        CONFIG_DIR,
        expected_active_revision=active_revision,
    )

    activated = activate_local_configuration(
        database,
        next_revision,
        expected_active_revision=active_revision,
    )
    repeated = activate_local_configuration(
        database,
        next_revision,
        expected_active_revision=active_revision,
    )

    assert activated == repeated == next_revision
    with pytest.raises(HostConfigurationConflictError, match="changed before publication"):
        publish_local_configuration(
            database,
            CONFIG_DIR,
            expected_active_revision=active_revision,
        )


async def test_custom_provider_exposes_strict_schema_capability_and_lifecycle() -> None:
    registry = create_resource_registry()
    declaration = ResourceConfig(
        provider=PROVIDER_NAME,
        config={"values": {"greeting_prefix": "Welcome"}},
    )
    resolved = registry.resolve_resource("tenant_settings", declaration)
    resource = registry.build(resolved, ResourceFactoryContext())

    assert isinstance(resource, TenantSettings)
    with pytest.raises(TenantSettingsUnavailableError, match="unavailable"):
        resource.get("greeting_prefix")
    await resource.initialize()
    assert resource.get("greeting_prefix") == "Welcome"
    await resource.close()
    with pytest.raises(TenantSettingsUnavailableError, match="unavailable"):
        resource.get("greeting_prefix")

    assert resolved.provider_contract_version == PROVIDER_CONTRACT_VERSION
    assert resolved.capabilities == frozenset({ResourceCapability.CONFIG})
    schema = TENANT_SETTINGS_PROVIDER.config_model.model_json_schema()
    assert schema["additionalProperties"] is False


def test_custom_provider_translates_invalid_configuration() -> None:
    registry = create_resource_registry()

    with pytest.raises(ResourceConfigError, match="invalid config.*unexpected"):
        registry.resolve_resource(
            "tenant_settings",
            ResourceConfig(
                provider=PROVIDER_NAME,
                config={"values": {}, "unexpected": "not-allowed"},
            ),
        )


def test_host_composition_registers_custom_provider() -> None:
    application = create_application()
    recomposed = compose_application(application.settings)

    assert PROVIDER_NAME in application.resource_registry.providers
    assert PROVIDER_NAME in recomposed.resource_registry.providers


def test_demo_host_authentication_cannot_be_selected_for_production() -> None:
    with pytest.raises(ValueError, match="Local host authentication requires the local profile"):
        compose_application(Settings(runtime=PRODUCTION_RUNTIME))
