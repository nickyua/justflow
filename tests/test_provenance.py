"""Tests for immutable worker artifact provenance."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

import pytest
from pydantic import ValidationError

from justflow.config.models import FlowStep, ServiceConfig, StepDefinition, WorkflowConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.config.settings import Settings
from justflow.definitions.catalog import CatalogStore, DefinitionCatalog
from justflow.definitions.environment import build_execution_environment_snapshots
from justflow.definitions.manifest import ENGINE_WORKFLOW_ABI, build_definition_manifests
from justflow.definitions.routing import WorkerDeployment, WorkerDeploymentRouter
from justflow.provenance import (
    LOCAL_ARTIFACT_DIGEST,
    CatalogBackendIdentity,
    ExecutionEnvironmentSnapshot,
    ProvenanceError,
    RuntimeProfile,
    WorkerArtifactIdentity,
    installed_engine_version,
    provenance_digest,
)
from justflow.transports.builtins import builtin_transport_registry
from tests.settings import PRODUCTION_RUNTIME

IMMUTABLE_ARTIFACT_DIGEST = f"sha256:{'a' * 64}"
SECRET_SENTINEL = "synthetic-access-token"
PAYLOAD_SENTINEL = "synthetic-customer-payload"
PII_SENTINEL = "synthetic-customer-namespace"
DIRECT_TIMEOUT_SECONDS = 10
NO_RETRIES = 0


@dataclass(frozen=True, kw_only=True)
class Returns:
    artifact_digest: str


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


ProfileOutcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class ProfileCase:
    id: str
    profile: RuntimeProfile
    artifact_digest: str
    outcome: ProfileOutcome


PROFILE_CASES = [
    ProfileCase(
        id="production-immutable",
        profile=RuntimeProfile.PRODUCTION,
        artifact_digest=IMMUTABLE_ARTIFACT_DIGEST,
        outcome=Returns(artifact_digest=IMMUTABLE_ARTIFACT_DIGEST),
    ),
    ProfileCase(
        id="production-local-rejected",
        profile=RuntimeProfile.PRODUCTION,
        artifact_digest=LOCAL_ARTIFACT_DIGEST,
        outcome=Raises(exc=ProvenanceError, match="production.*immutable SHA-256"),
    ),
    ProfileCase(
        id="local-explicit",
        profile=RuntimeProfile.LOCAL,
        artifact_digest=LOCAL_ARTIFACT_DIGEST,
        outcome=Returns(artifact_digest=LOCAL_ARTIFACT_DIGEST),
    ),
    ProfileCase(
        id="local-production-rejected",
        profile=RuntimeProfile.LOCAL,
        artifact_digest=IMMUTABLE_ARTIFACT_DIGEST,
        outcome=Raises(exc=ProvenanceError, match="local runtime profile"),
    ),
]


@pytest.mark.parametrize("case", PROFILE_CASES, ids=lambda case: case.id)
def test_artifact_identity_is_validated_for_runtime_profile(case: ProfileCase) -> None:
    identity = WorkerArtifactIdentity(
        deployment_name="commerce",
        build_id="build-1",
        artifact_digest=case.artifact_digest,
        package_version="1.2.3",
        source_revision="abc1234",
    )

    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            identity.validate_for_profile(case.profile)
        return

    identity.validate_for_profile(case.profile)
    assert identity.artifact_digest == case.outcome.artifact_digest


@pytest.mark.parametrize(
    "artifact_digest",
    [
        pytest.param("a" * 64, id="missing-algorithm"),
        pytest.param(f"sha256:{'A' * 64}", id="uppercase"),
        pytest.param("sha256:short", id="truncated"),
    ],
)
def test_artifact_identity_rejects_malformed_digest(artifact_digest: str) -> None:
    with pytest.raises(ValidationError, match="full lowercase SHA-256"):
        WorkerArtifactIdentity(
            deployment_name="commerce",
            build_id="build-1",
            artifact_digest=artifact_digest,
            package_version="1.2.3",
        )


def test_environment_snapshot_is_canonical_secret_free_and_content_addressed(tmp_path) -> None:
    settings = Settings.model_validate(
        {
            "runtime": PRODUCTION_RUNTIME,
            "brokers": {
                PII_SENTINEL: {
                    "provider": "sqs",
                    "config": {
                        "access_token": SECRET_SENTINEL,
                        "example_payload": {"customer": PAYLOAD_SENTINEL},
                    },
                },
            },
            "temporal": {
                "address": SECRET_SENTINEL,
                "namespace": PII_SENTINEL,
                "task_queue": PII_SENTINEL,
                "connection": {
                    "mode": "tls",
                    "server_name": PII_SENTINEL,
                    "root_ca_path": f"/runtime/{SECRET_SENTINEL}-ca.pem",
                    "client_certificate_path": f"/runtime/{SECRET_SENTINEL}-client.pem",
                    "client_private_key_path": f"/runtime/{SECRET_SENTINEL}-client.key",
                    "api_key": SECRET_SENTINEL,
                },
                "payload_protection": {
                    "mode": "codec",
                    "active_key_id": "current",
                    "readable_key_ids": ["current", "previous"],
                },
            },
            "resource_connections": {
                "postgres_dsns": {"primary": SECRET_SENTINEL},
                "redis_urls": {"primary": SECRET_SENTINEL},
            },
        }
    )
    manifests = _versioned_manifests()
    catalog_store = CatalogStore(tmp_path)
    catalog = catalog_store.publish(manifests)
    deployment = _deployment()

    snapshots = build_execution_environment_snapshots(
        settings,
        catalog,
        WorkerDeploymentRouter.for_deployment(deployment),
        catalog_store.backend_identity,
    )
    snapshot = snapshots["example"]
    payload = snapshot.canonical_bytes()
    catalog_store.store_environment_snapshot(snapshot)

    assert SECRET_SENTINEL.encode() not in payload
    assert PAYLOAD_SENTINEL.encode() not in payload
    assert PII_SENTINEL.encode() not in payload
    assert snapshot.engine_version == installed_engine_version()
    assert snapshot.configuration.payload_protection_mode == "codec"
    assert snapshot.configuration.broker_providers[0].provider == "sqs"
    assert snapshot.provider_contracts[0].name == "direct"
    assert ExecutionEnvironmentSnapshot.model_validate_json(payload) == snapshot
    assert catalog_store.load_environment_snapshot(snapshot.snapshot_digest) == snapshot
    assert catalog_store.store_environment_snapshot(snapshot).payload == payload


def test_environment_snapshot_rejects_a_changed_content_digest() -> None:
    settings = Settings(runtime=PRODUCTION_RUNTIME)
    manifests = _versioned_manifests()
    catalog = DefinitionCatalog.from_manifests(manifests)
    deployment = _deployment()
    snapshot = build_execution_environment_snapshots(
        settings,
        catalog,
        WorkerDeploymentRouter.for_deployment(deployment),
        CatalogBackendIdentity(
            provider="test",
            configuration_digest=provenance_digest({"name": "test"}),
        ),
    )["example"]
    changed = snapshot.model_dump(mode="json")
    changed["definition_digest"] = "0" * 64

    with pytest.raises(ValidationError, match="snapshot digest mismatch"):
        ExecutionEnvironmentSnapshot.model_validate(changed)


def _deployment() -> WorkerDeployment:
    return WorkerDeployment(
        artifact_identity=WorkerArtifactIdentity(
            deployment_name="commerce",
            build_id="build-1",
            artifact_digest=IMMUTABLE_ARTIFACT_DIGEST,
            package_version="1.2.3",
        ),
        compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
    )


def _versioned_manifests():
    services = builtin_transport_registry().resolve_services(
        {
            "processor": ServiceConfig(
                transport="direct",
                transport_config={"class": "tests.replay_cases.ReplayActions"},
                dispatch_timeout_sec=DIRECT_TIMEOUT_SECONDS,
                retries=NO_RETRIES,
            )
        }
    )
    workflow = WorkflowConfig(
        workflow="example",
        steps={"process": StepDefinition(service="processor", action="run")},
        flow=[
            FlowStep(name="process", op="process", then="done"),
            FlowStep(name="done", terminal=True),
        ],
    )
    return build_definition_manifests({"example": workflow}, services, RuntimeLimits())
