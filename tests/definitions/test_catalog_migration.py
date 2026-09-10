"""Tests for canonical catalog export, import, and migration planning."""

from __future__ import annotations

import json

import pytest

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.config.settings import Settings
from justflow.definitions.catalog import CatalogConflictError, CatalogStore
from justflow.definitions.configuration import configured_catalog_store
from justflow.definitions.environment import build_execution_environment_snapshots
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    DefinitionManifest,
    build_definition_manifests,
)
from justflow.definitions.migration import (
    CatalogBundle,
    CatalogMigrationError,
    UnscopedCatalogDecision,
    export_catalog,
    import_catalog,
    migrate_unscoped_catalog,
    plan_catalog_import,
    plan_unscoped_catalog_migration,
)
from justflow.definitions.routing import WorkerDeployment, WorkerDeploymentRouter
from justflow.provenance import WorkerArtifactIdentity
from justflow.scope import LOCAL_RUNTIME_SCOPE
from tests.settings import PRODUCTION_RUNTIME

ARTIFACT_DIGEST = f"sha256:{'a' * 64}"


def test_catalog_export_import_round_trip_is_byte_identical_and_retry_safe(tmp_path) -> None:
    source = CatalogStore(tmp_path / "source")
    original = _manifests()
    changed = _manifests(description="updated")
    source.publish(original)
    catalog = source.publish(changed)
    snapshot = build_execution_environment_snapshots(
        Settings(runtime=PRODUCTION_RUNTIME),
        catalog,
        WorkerDeploymentRouter.for_deployment(_deployment()),
        source.backend_identity,
    )["example"]
    source.store_environment_snapshot(snapshot)
    exported = export_catalog(source)
    payload = exported.canonical_bytes()

    parsed = CatalogBundle.from_bytes(payload)
    destination = CatalogStore(tmp_path / "destination")
    plan = plan_catalog_import(destination, parsed)
    applied = import_catalog(destination, parsed)

    assert len(plan.manifests_to_create) == 2
    assert plan.environment_snapshots_to_create == (snapshot.snapshot_digest,)
    assert plan.aliases_to_create == ("example",)
    assert applied == plan
    assert export_catalog(destination).canonical_bytes() == payload

    retry_plan = import_catalog(destination, parsed)
    assert retry_plan.has_changes is False
    assert retry_plan.conflicts == ()


def test_catalog_import_reports_alias_conflict_before_mutating(tmp_path) -> None:
    source = CatalogStore(tmp_path / "source")
    source.publish(_manifests())
    bundle = export_catalog(source)
    destination = CatalogStore(tmp_path / "destination")
    destination_catalog = destination.publish(_manifests(description="destination"))
    destination_manifest_count = len(destination_catalog.manifests)

    plan = plan_catalog_import(destination, bundle)

    assert [conflict.kind for conflict in plan.conflicts] == ["alias"]
    with pytest.raises(CatalogConflictError, match="unresolved conflict"):
        import_catalog(destination, bundle)
    assert len(destination.load().manifests) == destination_manifest_count
    assert destination.load().resolve("example").definition_digest == (
        destination_catalog.resolve("example").definition_digest
    )


def test_unscoped_catalog_migration_requires_a_decision_and_is_retry_safe(tmp_path) -> None:
    unscoped = CatalogStore(tmp_path)
    unscoped.publish(_manifests())
    scoped = configured_catalog_store(
        Settings(runtime=PRODUCTION_RUNTIME).catalog,
        tmp_path,
        scope=LOCAL_RUNTIME_SCOPE,
    )

    plan = plan_unscoped_catalog_migration(unscoped, scoped)
    migrated = migrate_unscoped_catalog(
        unscoped,
        scoped,
        decision=UnscopedCatalogDecision.MIGRATE,
    )

    assert plan.requires_decision
    assert migrated == plan
    assert scoped.load().aliases == unscoped.load().aliases
    assert migrate_unscoped_catalog(
        unscoped,
        scoped,
        decision=UnscopedCatalogDecision.MIGRATE,
    ).scoped_aliases == ("example",)
    retained = migrate_unscoped_catalog(
        unscoped,
        scoped,
        decision=UnscopedCatalogDecision.KEEP_SCOPED,
    )
    assert retained.scoped_aliases == ("example",)


def test_unscoped_catalog_migration_rejects_divergent_scoped_state(tmp_path) -> None:
    unscoped = CatalogStore(tmp_path)
    unscoped.publish(_manifests())
    scoped = configured_catalog_store(
        Settings(runtime=PRODUCTION_RUNTIME).catalog,
        tmp_path,
        scope=LOCAL_RUNTIME_SCOPE,
    )
    scoped.publish(_manifests(description="scoped"))

    plan = plan_unscoped_catalog_migration(unscoped, scoped)

    assert plan.collisions == ("example",)
    with pytest.raises(CatalogConflictError, match="choose which state to retain"):
        migrate_unscoped_catalog(
            unscoped,
            scoped,
            decision=UnscopedCatalogDecision.MIGRATE,
        )


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(
            b'{"aliases":{},"aliases":{},"environment_snapshots":[],"format_version":1,'
            b'"manifests":[]}',
            id="duplicate-key",
        ),
        pytest.param(
            json.dumps(
                {
                    "aliases": {},
                    "environment_snapshots": [],
                    "format_version": 1,
                    "manifests": [],
                },
                indent=2,
            ).encode(),
            id="noncanonical-layout",
        ),
    ],
)
def test_catalog_bundle_rejects_noncanonical_input(payload: bytes) -> None:
    with pytest.raises(CatalogMigrationError):
        CatalogBundle.from_bytes(payload)


def _manifests(*, description: str = "") -> dict[str, DefinitionManifest]:
    workflow = WorkflowConfig(
        workflow="example",
        description=description,
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    return build_definition_manifests({"example": workflow}, {}, RuntimeLimits())


def _deployment() -> WorkerDeployment:
    return WorkerDeployment(
        artifact_identity=WorkerArtifactIdentity(
            deployment_name="migration-test",
            build_id="build-1",
            artifact_digest=ARTIFACT_DIGEST,
            package_version="1.2.3",
        ),
        compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
    )
