"""Tests for the typed Settings object."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from justflow.config.runtime_limits import DEFAULT_FANOUT_ITEMS
from justflow.config.settings import (
    MAX_ACTIVATION_LEASE_SECONDS,
    MAX_CONTROL_REQUEST_BYTES,
    CodecPayloadProtection,
    FileConfigurationSettings,
    PlaintextPayloadProtection,
    S3CatalogSettings,
    Settings,
    SqliteConfigurationSettings,
    TlsTemporalConnectionSettings,
    load_settings,
)
from justflow.definitions.manifest import ENGINE_WORKFLOW_ABI
from justflow.provenance import RuntimeProfile, installed_engine_version
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope
from tests.settings import PRODUCTION_RUNTIME

CUSTOM_FANOUT_ITEMS = 25
CUSTOM_ACTIVATION_LEASE_SECONDS = 45
CUSTOM_CATALOG_DIR = "/var/lib/justflow/catalog"
CUSTOM_SHUTDOWN_GRACE_SECONDS = 45


class TestSettings:
    def test_defaults(self):
        settings = Settings(runtime=PRODUCTION_RUNTIME)
        assert settings.temporal.address == "localhost:7233"
        assert settings.temporal.task_queue == "gateway-workflows"
        assert settings.temporal.namespace == "default"
        assert settings.temporal.deployment_registration.attempts == 50
        assert settings.temporal.deployment_registration.interval_seconds == 0.1
        assert settings.temporal.connection == TlsTemporalConnectionSettings(
            server_name="localhost"
        )
        assert isinstance(settings.temporal.payload_protection, PlaintextPayloadProtection)
        assert settings.brokers == {}
        assert settings.messaging.trigger is None
        assert settings.messaging.response is None
        assert settings.messaging.reconnect.initial_delay_seconds == 0.1
        assert settings.messaging.reconnect.max_delay_seconds == 30
        assert settings.transport_security.http.allowed_origins == frozenset()
        assert settings.transport_security.grpc_tls_profiles == {}
        assert settings.paths.config_dir == "configs"
        assert settings.paths.catalog_dir is None
        assert settings.paths.definition_catalog_dir == "configs"
        assert settings.control.shutdown_grace_seconds == 30
        assert settings.logging.level == "INFO"
        assert settings.runtime.profile is RuntimeProfile.PRODUCTION
        assert settings.runtime.scope == PRODUCTION_RUNTIME.scope
        assert settings.runtime.pinned_start_retry.attempts == 50
        assert settings.runtime.pinned_start_retry.interval_seconds == 0.1
        assert settings.runtime.worker_ready_timeout_seconds == 60
        assert isinstance(settings.configuration, FileConfigurationSettings)
        assert settings.schedules.max_schedules == 1_000
        assert settings.schedules.page_size == 100
        assert settings.scheduled_starts.max_horizon_seconds == 31_536_000
        assert settings.scheduled_starts.max_pending_per_scope == 1_000
        assert settings.scheduled_starts.default_list_limit == 50
        assert settings.scheduled_starts.describe_concurrency == 10
        assert settings.scheduled_starts.terminal_retention_seconds == 604_800
        assert settings.operations.admin_panel_enabled is False
        assert settings.operations.indexed_search_attributes_enabled is False
        assert settings.operations.local_source_authoring_enabled is False
        assert settings.operations.metrics_links == ()
        assert settings.activation.lease_seconds == 30
        assert settings.activation.max_scopes == 1_000
        assert settings.activation.max_targets_per_scope == 1_000
        assert settings.deployment.name == "justflow"
        assert settings.deployment.build_id is None
        assert settings.deployment.artifact_digest is None
        assert settings.deployment.package_version == installed_engine_version()
        assert settings.deployment.source_revision is None
        assert settings.deployment.compatible_engine_workflow_abis == frozenset(
            {ENGINE_WORKFLOW_ABI}
        )
        assert settings.limits.snapshot().fanout_items == DEFAULT_FANOUT_ITEMS

    def test_env_overrides_nested_groups(self, monkeypatch):
        monkeypatch.setenv("JUSTFLOW_RUNTIME__SCOPE", PRODUCTION_RUNTIME.scope.model_dump_json())
        monkeypatch.setenv("JUSTFLOW_TEMPORAL__ADDRESS", "temporal.prod:7233")
        monkeypatch.setenv(
            "JUSTFLOW_TEMPORAL__CONNECTION",
            '{"mode":"tls","server_name":"temporal.prod"}',
        )
        monkeypatch.setenv(
            "JUSTFLOW_BROKERS",
            '{"main":{"provider":"sqs","config":{"destinations":{"triggers":"q1"}}}}',
        )
        monkeypatch.setenv(
            "JUSTFLOW_MESSAGING__TRIGGER",
            '{"broker":"main","destination":"triggers","dead_letter_destination":"dead"}',
        )
        monkeypatch.setenv("JUSTFLOW_LOGGING__LEVEL", "DEBUG")
        monkeypatch.setenv("JUSTFLOW_PATHS__CATALOG_DIR", CUSTOM_CATALOG_DIR)
        monkeypatch.setenv(
            "JUSTFLOW_CONTROL__SHUTDOWN_GRACE_SECONDS",
            str(CUSTOM_SHUTDOWN_GRACE_SECONDS),
        )
        monkeypatch.setenv("JUSTFLOW_CATALOG__BACKEND", "s3")
        monkeypatch.setenv("JUSTFLOW_CATALOG__BUCKET", "workflow-definition-catalog")
        monkeypatch.setenv("JUSTFLOW_DEPLOYMENT__BUILD_ID", "release-1")
        monkeypatch.setenv(
            "JUSTFLOW_DEPLOYMENT__ARTIFACT_DIGEST",
            f"sha256:{'a' * 64}",
        )
        monkeypatch.setenv("JUSTFLOW_DEPLOYMENT__PACKAGE_VERSION", "1.2.3")
        monkeypatch.setenv("JUSTFLOW_DEPLOYMENT__SOURCE_REVISION", "abc1234")
        monkeypatch.setenv(
            "JUSTFLOW_DEPLOYMENT__COMPATIBLE_ENGINE_WORKFLOW_ABIS",
            '["justflow.workflow.v1", "justflow.workflow.v0"]',
        )
        monkeypatch.setenv("JUSTFLOW_LIMITS__FANOUT_ITEMS", str(CUSTOM_FANOUT_ITEMS))
        monkeypatch.setenv("JUSTFLOW_LIMITS__FANOUT_CHUNK_ITEMS", str(CUSTOM_FANOUT_ITEMS))
        monkeypatch.setenv(
            "JUSTFLOW_TRANSPORT_SECURITY__HTTP__ALLOWED_ORIGINS",
            '["https://api.example:443"]',
        )
        monkeypatch.setenv(
            "JUSTFLOW_TRANSPORT_SECURITY__GRPC_TLS_PROFILES",
            '{"internal":{"server_name":"service.internal",'
            '"client_certificate_path":"/runtime/client.crt",'
            '"client_private_key_path":"/runtime/client.key"}}',
        )
        monkeypatch.setenv(
            "JUSTFLOW_RESOURCE_CONNECTIONS__POSTGRES_DSNS",
            '{"primary":"postgresql://localhost/synthetic"}',
        )
        monkeypatch.setenv(
            "JUSTFLOW_RESOURCE_CONNECTIONS__REDIS_URLS",
            '{"primary":"rediss://cache.local/0"}',
        )
        monkeypatch.setenv(
            "JUSTFLOW_ACTIVATION__LEASE_SECONDS",
            str(CUSTOM_ACTIVATION_LEASE_SECONDS),
        )
        monkeypatch.setenv("JUSTFLOW_OPERATIONS__ADMIN_PANEL_ENABLED", "true")
        monkeypatch.setenv(
            "JUSTFLOW_OPERATIONS__METRICS_LINKS",
            '[{"label":"Metrics","url":"https://metrics.example.invalid/justflow"}]',
        )

        settings = load_settings()

        assert settings.temporal.address == "temporal.prod:7233"
        assert settings.temporal.connection == TlsTemporalConnectionSettings(
            server_name="temporal.prod"
        )
        assert settings.brokers["main"].provider == "sqs"
        assert settings.messaging.trigger is not None
        assert settings.messaging.trigger.destination == "triggers"
        assert settings.logging.level == "DEBUG"
        assert settings.paths.catalog_dir == CUSTOM_CATALOG_DIR
        assert settings.paths.definition_catalog_dir == CUSTOM_CATALOG_DIR
        assert settings.control.shutdown_grace_seconds == CUSTOM_SHUTDOWN_GRACE_SECONDS
        assert isinstance(settings.catalog, S3CatalogSettings)
        assert settings.catalog.bucket == "workflow-definition-catalog"
        assert settings.deployment.build_id == "release-1"
        assert settings.deployment.artifact_digest == f"sha256:{'a' * 64}"
        assert settings.deployment.package_version == "1.2.3"
        assert settings.deployment.source_revision == "abc1234"
        assert settings.deployment.compatible_engine_workflow_abis == frozenset(
            {"justflow.workflow.v1", "justflow.workflow.v0"}
        )
        assert settings.limits.snapshot().fanout_items == CUSTOM_FANOUT_ITEMS
        assert settings.temporal.task_queue == "gateway-workflows"
        assert settings.transport_security.http.allowed_origins == frozenset(
            {"https://api.example"}
        )
        assert (
            settings.transport_security.grpc_tls_profiles["internal"].server_name
            == "service.internal"
        )
        assert (
            settings.resource_connections.postgres_dsns["primary"].get_secret_value()
            == "postgresql://localhost/synthetic"
        )
        assert (
            settings.resource_connections.redis_urls["primary"].get_secret_value()
            == "rediss://cache.local/0"
        )
        assert settings.activation.lease_seconds == CUSTOM_ACTIVATION_LEASE_SECONDS
        assert settings.operations.admin_panel_enabled is True
        assert settings.operations.metrics_links[0].label == "Metrics"

    @pytest.mark.parametrize(
        "url",
        [
            pytest.param("/relative", id="relative"),
            pytest.param("file:///tmp/metrics", id="unsupported-scheme"),
            pytest.param(
                "https://operator:secret@metrics.example.invalid",
                id="embedded-credentials",
            ),
            pytest.param(
                "https://metrics.example.invalid/dashboard?token=secret",
                id="query",
            ),
            pytest.param(
                "https://metrics.example.invalid/dashboard#private",
                id="fragment",
            ),
        ],
    )
    def test_operations_metrics_links_require_credential_free_http_urls(
        self,
        url: str,
    ) -> None:
        with pytest.raises(ValidationError, match="metrics links"):
            Settings.model_validate(
                {"operations": {"metrics_links": [{"label": "Metrics", "url": url}]}}
            )

    def test_local_source_authoring_requires_explicit_local_file_mode(self) -> None:
        settings = Settings.model_validate(
            {
                "runtime": {"profile": "local"},
                "operations": {
                    "admin_panel_enabled": True,
                    "local_source_authoring_enabled": True,
                },
            }
        )

        assert settings.operations.local_source_authoring_enabled is True

    @pytest.mark.parametrize(
        "payload",
        [
            pytest.param(
                {
                    "operations": {
                        "admin_panel_enabled": True,
                        "local_source_authoring_enabled": True,
                    }
                },
                id="production-profile",
            ),
        ],
    )
    def test_local_source_authoring_rejects_incomplete_opt_in(
        self,
        payload: dict[str, object],
    ) -> None:
        with pytest.raises(ValidationError, match="Local source authoring"):
            Settings.model_validate(payload)

    def test_activation_lease_bound_is_enforced(self) -> None:
        with pytest.raises(ValidationError, match="less than or equal"):
            Settings.model_validate(
                {"activation": {"lease_seconds": MAX_ACTIVATION_LEASE_SECONDS + 1}}
            )

    def test_unknown_setting_is_rejected(self):
        with pytest.raises(ValidationError, match="unexpected"):
            Settings.model_validate({"unexpected": True})

    def test_scope_and_configuration_backend_load_from_typed_environment(
        self,
        monkeypatch,
    ) -> None:
        monkeypatch.setenv(
            "JUSTFLOW_RUNTIME__SCOPE",
            '{"tenant":"tenant-a","application":"orders","environment":"production"}',
        )
        monkeypatch.setenv("JUSTFLOW_CONFIGURATION__BACKEND", "sqlite")
        monkeypatch.setenv(
            "JUSTFLOW_CONFIGURATION__PATH",
            ".justflow/test-configuration.sqlite3",
        )

        settings = load_settings()

        assert settings.runtime.scope == RuntimeScope.create(
            tenant="tenant-a",
            application="orders",
            environment="production",
        )
        assert settings.configuration == SqliteConfigurationSettings(
            path=".justflow/test-configuration.sqlite3"
        )

    def test_message_concurrency_bound_is_enforced(self):
        with pytest.raises(ValidationError, match="less than or equal to 10"):
            Settings.model_validate({"messaging": {"message_concurrency": 11}})

    def test_consumer_reconnect_delay_bounds_are_enforced(self):
        with pytest.raises(ValidationError, match="initial delay cannot exceed"):
            Settings.model_validate(
                {
                    "messaging": {
                        "reconnect": {
                            "initial_delay_seconds": 2,
                            "max_delay_seconds": 1,
                        }
                    }
                }
            )

    def test_schedule_dispatch_queue_must_be_separate_from_business_queue(self) -> None:
        with pytest.raises(ValidationError, match="task queues must be different"):
            Settings.model_validate(
                {
                    "temporal": {"task_queue": "shared-queue"},
                    "schedules": {"task_queue": "shared-queue"},
                }
            )

    def test_scheduled_start_input_bound_cannot_exceed_runtime_limit(self) -> None:
        with pytest.raises(ValidationError, match="Scheduled-start input bound"):
            Settings.model_validate(
                {
                    "scheduled_starts": {"max_input_bytes": 2},
                    "limits": {"trigger_payload_bytes": 1},
                }
            )

    def test_scheduled_start_pending_quota_must_be_observable(self) -> None:
        with pytest.raises(ValidationError, match="pending quota cannot exceed"):
            Settings.model_validate(
                {
                    "scheduled_starts": {
                        "max_pending_per_scope": 101,
                        "max_schedules": 100,
                    }
                }
            )

    @pytest.mark.parametrize(
        "workload_policy",
        [
            pytest.param({"allowed_classes": []}, id="empty-grant"),
            pytest.param(
                {
                    "interactive_priority_key": 10,
                    "standard_priority_key": 5,
                    "batch_priority_key": 1,
                },
                id="inverted-priority",
            ),
        ],
    )
    def test_scheduled_start_workload_policy_fails_closed(
        self,
        workload_policy: dict[str, object],
    ) -> None:
        with pytest.raises(ValidationError, match="Scheduled-start"):
            Settings.model_validate({"scheduled_starts": {"workload_policy": workload_policy}})

    def test_control_request_bound_has_a_finite_maximum(self) -> None:
        with pytest.raises(ValidationError, match="less than or equal"):
            Settings.model_validate(
                {"control": {"max_request_body_bytes": MAX_CONTROL_REQUEST_BYTES + 1}}
            )

    def test_runtime_limits_must_be_positive(self):
        with pytest.raises(ValidationError, match="greater than or equal to 1"):
            Settings.model_validate({"limits": {"loop_attempts": 0}})

    def test_fanout_chunk_cannot_exceed_fanout_limit(self):
        with pytest.raises(ValidationError, match="fanout_chunk_items cannot exceed fanout_items"):
            Settings.model_validate({"limits": {"fanout_items": 10, "fanout_chunk_items": 11}})

    def test_codec_key_rotation_settings_load_from_env(self, monkeypatch):
        monkeypatch.setenv("JUSTFLOW_RUNTIME__SCOPE", PRODUCTION_RUNTIME.scope.model_dump_json())
        monkeypatch.setenv(
            "JUSTFLOW_TEMPORAL__PAYLOAD_PROTECTION",
            '{"mode":"codec","active_key_id":"new","readable_key_ids":["old","new"]}',
        )

        settings = load_settings()

        assert settings.temporal.payload_protection == CodecPayloadProtection(
            active_key_id="new",
            readable_key_ids=frozenset({"old", "new"}),
        )

    def test_active_payload_key_must_be_readable(self):
        with pytest.raises(ValidationError, match="active payload key must also be readable"):
            Settings.model_validate(
                {
                    "temporal": {
                        "payload_protection": {
                            "mode": "codec",
                            "active_key_id": "new",
                            "readable_key_ids": ["old"],
                        }
                    }
                }
            )


@pytest.mark.parametrize("scope", [None, LOCAL_RUNTIME_SCOPE], ids=["omitted", "local-default"])
def test_production_settings_require_an_explicit_nonlocal_scope(scope: RuntimeScope | None) -> None:
    runtime: dict[str, object] = {"profile": "production"}
    if scope is not None:
        runtime["scope"] = scope
    with pytest.raises(ValidationError, match="explicit non-local runtime scope"):
        Settings.model_validate({"runtime": runtime})
