"""Tests for immutable definition catalog persistence and drift detection."""

from __future__ import annotations

import hashlib
import json

import pytest

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.definitions.catalog import (
    CatalogConflictError,
    CatalogDriftError,
    CatalogError,
    CatalogStore,
    DefinitionCatalogStore,
    ScopedCatalogBackend,
    StoredCatalogObject,
)
from justflow.definitions.manifest import build_definition_manifests
from justflow.provenance import CatalogBackendIdentity, provenance_digest
from justflow.scope import RuntimeScope


class MemoryCatalogBackend:
    def __init__(self) -> None:
        self.identity = CatalogBackendIdentity(
            provider="memory-test",
            configuration_digest=provenance_digest({"name": "memory-test"}),
        )
        self.objects: dict[str, bytes] = {}
        self.reject_alias_update = False

    def read_object(self, key: str) -> StoredCatalogObject | None:
        payload = self.objects.get(key)
        return self._stored(key, payload) if payload is not None else None

    def list_objects(self, prefix: str) -> tuple[StoredCatalogObject, ...]:
        return tuple(
            self._stored(key, payload)
            for key, payload in sorted(self.objects.items())
            if key.startswith(prefix)
        )

    def create_immutable(self, key: str, payload: bytes) -> StoredCatalogObject:
        current = self.objects.get(key)
        if current is not None and current != payload:
            raise CatalogConflictError(f"Immutable object '{key}' differs")
        self.objects[key] = payload
        return self._stored(key, payload)

    def compare_and_swap(
        self,
        key: str,
        payload: bytes,
        *,
        expected_version: str | None,
    ) -> StoredCatalogObject:
        if self.reject_alias_update:
            raise CatalogConflictError(f"Alias object '{key}' changed")
        current = self.read_object(key)
        current_version = current.version if current is not None else None
        if current_version != expected_version and (current is None or current.payload != payload):
            raise CatalogConflictError(f"Alias object '{key}' changed")
        self.objects[key] = payload
        return self._stored(key, payload)

    @staticmethod
    def _stored(key: str, payload: bytes) -> StoredCatalogObject:
        return StoredCatalogObject(
            key=key,
            payload=payload,
            version=hashlib.sha256(payload).hexdigest(),
        )


def _manifests(description: str = ""):
    workflow = WorkflowConfig(
        workflow="example",
        description=description,
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    return build_definition_manifests({"example": workflow}, {}, RuntimeLimits())


def test_publish_load_and_resolve_round_trip(tmp_path) -> None:
    store = CatalogStore(tmp_path)
    manifests = _manifests()

    published = store.publish(manifests)
    loaded = store.load()

    expected = manifests["example"]
    assert published.resolve("example") == expected
    assert loaded.resolve("example") == expected
    assert store.manifest_path("example", expected.definition_digest).is_file()


def test_facade_accepts_explicit_custom_backend() -> None:
    backend = MemoryCatalogBackend()
    store = DefinitionCatalogStore(backend)

    published = store.publish(_manifests())

    loaded = store.load()
    assert loaded.manifests == published.manifests
    assert loaded.aliases == published.aliases
    assert store.backend_identity == backend.identity


def test_scoped_catalog_aliases_and_objects_are_cross_tenant_isolated() -> None:
    backend = MemoryCatalogBackend()
    scope_a = RuntimeScope.create(
        tenant="tenant-a",
        application="orders",
        environment="production",
    )
    scope_b = RuntimeScope.create(
        tenant="tenant-b",
        application="orders",
        environment="production",
    )
    store_a = DefinitionCatalogStore(ScopedCatalogBackend(backend, scope_a))
    store_b = DefinitionCatalogStore(ScopedCatalogBackend(backend, scope_b))

    published_a = store_a.publish(_manifests("tenant-a"))
    published_b = store_b.publish(_manifests("tenant-b"))

    assert store_a.load().resolve("example") == published_a.resolve("example")
    assert store_b.load().resolve("example") == published_b.resolve("example")
    assert store_a.backend_identity != store_b.backend_identity
    assert len([key for key in backend.objects if key.endswith("aliases.json")]) == 2


def test_facade_does_not_hide_alias_update_conflict() -> None:
    backend = MemoryCatalogBackend()
    store = DefinitionCatalogStore(backend)
    store.publish(_manifests())
    backend.reject_alias_update = True

    with pytest.raises(CatalogConflictError, match="changed"):
        store.publish(_manifests("changed"))


def test_publish_preserves_old_manifest_and_moves_only_alias(tmp_path) -> None:
    store = CatalogStore(tmp_path)
    original = _manifests()
    changed = _manifests("changed")

    store.publish(original)
    catalog = store.publish(changed)

    assert len(catalog.manifests) == 2
    assert catalog.resolve("example") == changed["example"]
    assert catalog.get("example", original["example"].definition_digest) == original["example"]


def test_verify_authored_rejects_unpublished_change(tmp_path) -> None:
    store = CatalogStore(tmp_path)
    catalog = store.publish(_manifests())

    with pytest.raises(CatalogDriftError, match="unpublished definition changes"):
        catalog.verify_authored(_manifests("changed"))


def test_load_rejects_catalog_path_disagreement(tmp_path) -> None:
    store = CatalogStore(tmp_path)
    manifests = _manifests()
    manifest = manifests["example"]
    store.publish(manifests)
    correct_path = store.manifest_path("example", manifest.definition_digest)
    wrong_path = correct_path.with_name(f"{'0' * 64}.json")
    correct_path.rename(wrong_path)

    with pytest.raises(CatalogError, match="path mismatch"):
        store.load()


def test_load_rejects_manifest_content_drift(tmp_path) -> None:
    store = CatalogStore(tmp_path)
    manifests = _manifests()
    manifest = manifests["example"]
    store.publish(manifests)
    path = store.manifest_path("example", manifest.definition_digest)
    raw = json.loads(path.read_text())
    raw["workflow"]["description"] = "tampered"
    path.write_text(json.dumps(raw))

    with pytest.raises(CatalogError, match="digest mismatch"):
        store.load()


def test_load_rejects_noncanonical_manifest_serialization(tmp_path) -> None:
    store = CatalogStore(tmp_path)
    manifests = _manifests()
    manifest = manifests["example"]
    store.publish(manifests)
    path = store.manifest_path("example", manifest.definition_digest)
    path.write_text(json.dumps(json.loads(path.read_text()), indent=2))

    with pytest.raises(CatalogError, match="not canonical JSON"):
        store.load()


def test_load_requires_published_alias_document(tmp_path) -> None:
    with pytest.raises(CatalogError, match="definitions publish"):
        CatalogStore(tmp_path).load()


@pytest.mark.parametrize(
    "logical_name",
    [".", "..", "../outside", "nested/workflow", "nested\\workflow"],
)
def test_manifest_path_rejects_unsafe_workflow_names(tmp_path, logical_name: str) -> None:
    with pytest.raises(CatalogError, match="workflow name"):
        CatalogStore(tmp_path).manifest_path(logical_name, "a" * 64)
