"""Canonical catalog bundles and conflict-aware migration operations."""

from __future__ import annotations

import json
from collections.abc import Mapping
from enum import Enum
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from justflow.definitions.catalog import (
    CatalogConflictError,
    CatalogError,
    DefinitionCatalog,
    DefinitionCatalogStore,
)
from justflow.definitions.manifest import DefinitionManifest
from justflow.provenance import (
    ExecutionEnvironmentSnapshot,
    ProvenanceError,
    canonical_provenance_bytes,
)

CATALOG_BUNDLE_FORMAT_VERSION = 1
MAX_CATALOG_BUNDLE_BYTES = 64 * 1024 * 1024


class CatalogMigrationError(Exception):
    """A catalog bundle or migration operation is invalid."""


class FrozenMigrationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CatalogBundle(FrozenMigrationModel):
    format_version: int = CATALOG_BUNDLE_FORMAT_VERSION
    aliases: dict[str, str]
    manifests: tuple[DefinitionManifest, ...]
    environment_snapshots: tuple[ExecutionEnvironmentSnapshot, ...]

    @model_validator(mode="after")
    def validate_bundle(self) -> Self:
        if self.format_version != CATALOG_BUNDLE_FORMAT_VERSION:
            raise ValueError(f"Unsupported catalog bundle format version {self.format_version}")
        try:
            DefinitionCatalog(self.manifests, self.aliases)
        except CatalogError as exc:
            raise ValueError(str(exc)) from exc
        snapshot_digests = [snapshot.snapshot_digest for snapshot in self.environment_snapshots]
        if len(snapshot_digests) != len(set(snapshot_digests)):
            raise ValueError("Catalog bundle contains duplicate execution environment snapshots")
        return self

    @classmethod
    def create(
        cls,
        *,
        aliases: Mapping[str, str],
        manifests: tuple[DefinitionManifest, ...],
        environment_snapshots: tuple[ExecutionEnvironmentSnapshot, ...],
    ) -> CatalogBundle:
        return cls(
            aliases=dict(sorted(aliases.items())),
            manifests=tuple(
                sorted(
                    manifests,
                    key=lambda manifest: (manifest.logical_name, manifest.definition_digest),
                )
            ),
            environment_snapshots=tuple(
                sorted(
                    environment_snapshots,
                    key=lambda snapshot: snapshot.snapshot_digest,
                )
            ),
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> CatalogBundle:
        if len(payload) > MAX_CATALOG_BUNDLE_BYTES:
            raise CatalogMigrationError(f"Catalog bundle exceeds {MAX_CATALOG_BUNDLE_BYTES} bytes")
        try:
            value = json.loads(
                payload,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_non_finite_constant,
            )
            bundle = cls.model_validate(value)
        except (
            json.JSONDecodeError,
            ValidationError,
            TypeError,
            ValueError,
            UnicodeError,
            CatalogError,
        ) as exc:
            raise CatalogMigrationError(f"Catalog bundle is invalid: {exc}") from exc
        if payload != bundle.canonical_bytes():
            raise CatalogMigrationError("Catalog bundle is not canonical JSON")
        return bundle

    def canonical_bytes(self) -> bytes:
        try:
            return canonical_provenance_bytes(self.model_dump(mode="json"))
        except (ProvenanceError, TypeError, ValueError, UnicodeError) as exc:
            raise CatalogMigrationError(f"Catalog bundle is not canonical JSON: {exc}") from exc


class CatalogImportConflict(FrozenMigrationModel):
    kind: Literal["manifest", "environment_snapshot", "alias", "destination_alias"]
    identity: str
    destination: str | None = None
    incoming: str | None = None


class CatalogImportPlan(FrozenMigrationModel):
    expected_alias_version: str | None
    manifests_to_create: tuple[str, ...]
    environment_snapshots_to_create: tuple[str, ...]
    aliases_to_create: tuple[str, ...]
    unchanged_aliases: tuple[str, ...]
    conflicts: tuple[CatalogImportConflict, ...]

    @property
    def has_changes(self) -> bool:
        return bool(
            self.manifests_to_create
            or self.environment_snapshots_to_create
            or self.aliases_to_create
        )


class UnscopedCatalogDecision(str, Enum):
    MIGRATE = "migrate"
    KEEP_SCOPED = "keep_scoped"


class UnscopedCatalogMigrationPlan(FrozenMigrationModel):
    unscoped_aliases: tuple[str, ...]
    scoped_aliases: tuple[str, ...]
    collisions: tuple[str, ...]

    @property
    def requires_decision(self) -> bool:
        return bool(self.unscoped_aliases)


def plan_unscoped_catalog_migration(
    unscoped_store: DefinitionCatalogStore,
    scoped_store: DefinitionCatalogStore,
) -> UnscopedCatalogMigrationPlan:
    unscoped = unscoped_store.inspect().catalog.aliases
    scoped = scoped_store.inspect().catalog.aliases
    collisions = tuple(
        name
        for name in sorted(set(unscoped).intersection(scoped))
        if unscoped[name] != scoped[name]
    )
    return UnscopedCatalogMigrationPlan(
        unscoped_aliases=tuple(sorted(unscoped)),
        scoped_aliases=tuple(sorted(scoped)),
        collisions=collisions,
    )


def migrate_unscoped_catalog(
    unscoped_store: DefinitionCatalogStore,
    scoped_store: DefinitionCatalogStore,
    *,
    decision: UnscopedCatalogDecision,
) -> UnscopedCatalogMigrationPlan:
    plan = plan_unscoped_catalog_migration(unscoped_store, scoped_store)
    if decision is UnscopedCatalogDecision.KEEP_SCOPED or not plan.unscoped_aliases:
        return plan
    if plan.collisions or (plan.scoped_aliases and plan.scoped_aliases != plan.unscoped_aliases):
        raise CatalogConflictError(
            "Unscoped and scoped catalogs both contain aliases; choose which state to retain"
        )
    import_catalog(scoped_store, export_catalog(unscoped_store))
    return plan


def export_catalog(store: DefinitionCatalogStore) -> CatalogBundle:
    catalog = store.load()
    return CatalogBundle.create(
        aliases=catalog.aliases,
        manifests=tuple(catalog.manifests.values()),
        environment_snapshots=store.list_environment_snapshots(),
    )


def plan_catalog_import(
    store: DefinitionCatalogStore,
    bundle: CatalogBundle,
) -> CatalogImportPlan:
    state = store.inspect()
    current_manifests = state.catalog.manifests
    current_snapshots = {
        snapshot.snapshot_digest: snapshot for snapshot in store.list_environment_snapshots()
    }
    manifests_to_create: list[str] = []
    snapshots_to_create: list[str] = []
    conflicts: list[CatalogImportConflict] = []

    for manifest in bundle.manifests:
        identity = (manifest.logical_name, manifest.definition_digest)
        current = current_manifests.get(identity)
        display_identity = f"{manifest.logical_name}@{manifest.definition_digest}"
        if current is None:
            manifests_to_create.append(display_identity)
        elif current != manifest:
            conflicts.append(CatalogImportConflict(kind="manifest", identity=display_identity))

    for snapshot in bundle.environment_snapshots:
        current_snapshot = current_snapshots.get(snapshot.snapshot_digest)
        if current_snapshot is None:
            snapshots_to_create.append(snapshot.snapshot_digest)
        elif current_snapshot != snapshot:
            conflicts.append(
                CatalogImportConflict(
                    kind="environment_snapshot",
                    identity=snapshot.snapshot_digest,
                )
            )

    aliases_to_create: list[str] = []
    unchanged_aliases: list[str] = []
    for logical_name, incoming_digest in sorted(bundle.aliases.items()):
        destination_digest = state.catalog.aliases.get(logical_name)
        if destination_digest is None:
            aliases_to_create.append(logical_name)
        elif destination_digest == incoming_digest:
            unchanged_aliases.append(logical_name)
        else:
            conflicts.append(
                CatalogImportConflict(
                    kind="alias",
                    identity=logical_name,
                    destination=destination_digest,
                    incoming=incoming_digest,
                )
            )
    for logical_name in sorted(set(state.catalog.aliases).difference(bundle.aliases)):
        conflicts.append(
            CatalogImportConflict(
                kind="destination_alias",
                identity=logical_name,
                destination=state.catalog.aliases[logical_name],
            )
        )

    return CatalogImportPlan(
        expected_alias_version=state.alias_version,
        manifests_to_create=tuple(manifests_to_create),
        environment_snapshots_to_create=tuple(snapshots_to_create),
        aliases_to_create=tuple(aliases_to_create),
        unchanged_aliases=tuple(unchanged_aliases),
        conflicts=tuple(conflicts),
    )


def import_catalog(
    store: DefinitionCatalogStore,
    bundle: CatalogBundle,
) -> CatalogImportPlan:
    plan = plan_catalog_import(store, bundle)
    if plan.conflicts:
        raise CatalogConflictError(
            f"Catalog import has {len(plan.conflicts)} unresolved conflict(s)"
        )

    manifests = {
        f"{manifest.logical_name}@{manifest.definition_digest}": manifest
        for manifest in bundle.manifests
    }
    snapshots = {snapshot.snapshot_digest: snapshot for snapshot in bundle.environment_snapshots}
    for identity in plan.manifests_to_create:
        store.store_definition_manifest(manifests[identity])
    for digest in plan.environment_snapshots_to_create:
        store.store_environment_snapshot(snapshots[digest])
    if plan.aliases_to_create:
        store.replace_aliases(
            bundle.aliases,
            expected_version=plan.expected_alias_version,
        )
    return plan


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate object key '{key}'")
        value[key] = item
    return value


def _reject_non_finite_constant(value: str) -> None:
    raise ValueError(f"non-finite number '{value}'")
