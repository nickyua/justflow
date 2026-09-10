"""Immutable definition catalog storage and active-alias selection."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict

from justflow.definitions.manifest import DefinitionManifest
from justflow.provenance import (
    CatalogBackendIdentity,
    ExecutionEnvironmentSnapshot,
    provenance_digest,
)
from justflow.scope import RuntimeScope

CATALOG_DIRECTORY = "definitions"
MANIFESTS_DIRECTORY = "manifests"
ENVIRONMENTS_DIRECTORY = "environments"
ALIASES_FILENAME = "aliases.json"
ALIASES_FORMAT_VERSION = 1
JSON_SUFFIX = ".json"
MANIFESTS_PREFIX = f"{MANIFESTS_DIRECTORY}/"
ENVIRONMENTS_PREFIX = f"{ENVIRONMENTS_DIRECTORY}/"
SCOPES_DIRECTORY = "scopes"


class CatalogError(Exception):
    """Definition catalog content or operation is invalid."""


class CatalogDriftError(CatalogError):
    """Authored definitions and the published catalog disagree."""


class CatalogStorageError(CatalogError):
    """Catalog storage could not complete an operation."""


class CatalogConflictError(CatalogError):
    """Catalog state changed concurrently or immutable content disagrees."""


@dataclass(frozen=True, kw_only=True)
class StoredCatalogObject:
    key: str
    payload: bytes
    version: str


@dataclass(frozen=True, kw_only=True)
class CatalogState:
    catalog: DefinitionCatalog
    alias_version: str | None


class CatalogBackend(Protocol):
    @property
    def identity(self) -> CatalogBackendIdentity: ...

    def read_object(self, key: str) -> StoredCatalogObject | None: ...

    def list_objects(self, prefix: str) -> Sequence[StoredCatalogObject]: ...

    def create_immutable(self, key: str, payload: bytes) -> StoredCatalogObject: ...

    def compare_and_swap(
        self,
        key: str,
        payload: bytes,
        *,
        expected_version: str | None,
    ) -> StoredCatalogObject: ...


class AliasDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    format_version: int = ALIASES_FORMAT_VERSION
    aliases: dict[str, str]


class DefinitionCatalog:
    def __init__(
        self,
        manifests: Iterable[DefinitionManifest],
        aliases: Mapping[str, str],
    ) -> None:
        indexed: dict[tuple[str, str], DefinitionManifest] = {}
        for manifest in manifests:
            key = (manifest.logical_name, manifest.definition_digest)
            if key in indexed:
                raise CatalogError(
                    f"Definition '{manifest.logical_name}@{manifest.definition_digest}' is duplicated"
                )
            indexed[key] = manifest
        normalized_aliases = dict(aliases)
        for logical_name, digest in normalized_aliases.items():
            if (logical_name, digest) not in indexed:
                raise CatalogError(
                    f"Active alias '{logical_name}' points to missing definition '{digest}'"
                )
        self._manifests = MappingProxyType(indexed)
        self._aliases = MappingProxyType(normalized_aliases)

    @classmethod
    def from_manifests(
        cls,
        manifests: Mapping[str, DefinitionManifest],
    ) -> DefinitionCatalog:
        return cls(
            manifests.values(),
            {name: manifest.definition_digest for name, manifest in manifests.items()},
        )

    @property
    def manifests(self) -> Mapping[tuple[str, str], DefinitionManifest]:
        return self._manifests

    @property
    def aliases(self) -> Mapping[str, str]:
        return self._aliases

    def get(self, logical_name: str, digest: str) -> DefinitionManifest:
        try:
            return self._manifests[(logical_name, digest)]
        except KeyError as exc:
            raise CatalogError(
                f"Definition '{logical_name}@{digest}' is not in the catalog"
            ) from exc

    def resolve(self, logical_name: str) -> DefinitionManifest:
        try:
            digest = self._aliases[logical_name]
        except KeyError as exc:
            raise CatalogError(f"Workflow '{logical_name}' has no active definition alias") from exc
        return self.get(logical_name, digest)

    def verify_authored(self, authored: Mapping[str, DefinitionManifest]) -> None:
        if set(authored) != set(self._aliases):
            raise CatalogDriftError(
                "Published aliases and authored workflows differ: "
                f"published={sorted(self._aliases)}, authored={sorted(authored)}"
            )
        for logical_name, manifest in authored.items():
            active = self.resolve(logical_name)
            if active != manifest:
                raise CatalogDriftError(
                    f"Workflow '{logical_name}' has unpublished definition changes: "
                    f"active={active.definition_digest}, authored={manifest.definition_digest}"
                )


class DefinitionCatalogStore:
    def __init__(self, backend: CatalogBackend) -> None:
        self.backend = backend

    @property
    def backend_identity(self) -> CatalogBackendIdentity:
        return self.backend.identity

    def load(self) -> DefinitionCatalog:
        aliases_object, catalog = self._load_state(require_aliases=True)
        if aliases_object is None:
            raise CatalogError("Published catalog aliases are unavailable")
        return catalog

    def inspect(self) -> CatalogState:
        aliases_object, catalog = self._load_state(require_aliases=False)
        return CatalogState(
            catalog=catalog,
            alias_version=aliases_object.version if aliases_object is not None else None,
        )

    def publish(self, manifests: Mapping[str, DefinitionManifest]) -> DefinitionCatalog:
        aliases_object, existing_catalog = self._load_state(require_aliases=False)
        all_manifests = dict(existing_catalog.manifests)
        for manifest in manifests.values():
            key = (manifest.logical_name, manifest.definition_digest)
            prior = all_manifests.get(key)
            if prior is not None and prior != manifest:
                raise CatalogConflictError(
                    f"Immutable definition '{manifest.logical_name}@{manifest.definition_digest}' "
                    "cannot be replaced"
                )
            all_manifests[key] = manifest
            self.backend.create_immutable(_manifest_key(manifest), manifest.canonical_bytes())

        aliases = {name: manifest.definition_digest for name, manifest in manifests.items()}
        catalog = DefinitionCatalog(all_manifests.values(), aliases)
        self.backend.compare_and_swap(
            ALIASES_FILENAME,
            _alias_payload(aliases),
            expected_version=(aliases_object.version if aliases_object is not None else None),
        )
        return catalog

    def store_environment_snapshot(
        self,
        snapshot: ExecutionEnvironmentSnapshot,
    ) -> StoredCatalogObject:
        return self.backend.create_immutable(
            _environment_snapshot_key(snapshot.snapshot_digest),
            snapshot.canonical_bytes(),
        )

    def store_definition_manifest(self, manifest: DefinitionManifest) -> StoredCatalogObject:
        return self.backend.create_immutable(_manifest_key(manifest), manifest.canonical_bytes())

    def replace_aliases(
        self,
        aliases: Mapping[str, str],
        *,
        expected_version: str | None,
    ) -> StoredCatalogObject:
        _, current_catalog = self._load_state(require_aliases=False)
        DefinitionCatalog(current_catalog.manifests.values(), aliases)
        return self.backend.compare_and_swap(
            ALIASES_FILENAME,
            _alias_payload(aliases),
            expected_version=expected_version,
        )

    def load_environment_snapshot(self, digest: str) -> ExecutionEnvironmentSnapshot:
        key = _environment_snapshot_key(digest)
        stored = self.backend.read_object(key)
        if stored is None:
            raise CatalogError(f"Execution environment snapshot '{digest}' is not in the catalog")
        return _parse_environment_snapshot(stored)

    def list_environment_snapshots(self) -> tuple[ExecutionEnvironmentSnapshot, ...]:
        return tuple(
            _parse_environment_snapshot(stored)
            for stored in self.backend.list_objects(ENVIRONMENTS_PREFIX)
        )

    def _load_state(
        self,
        *,
        require_aliases: bool,
    ) -> tuple[StoredCatalogObject | None, DefinitionCatalog]:
        aliases_object = self.backend.read_object(ALIASES_FILENAME)
        if aliases_object is None and require_aliases:
            raise CatalogError(
                f"Definition catalog is not published in '{self.backend.identity.provider}'; "
                "run 'justflow definitions publish' first"
            )
        aliases = _parse_aliases(aliases_object) if aliases_object is not None else {}
        manifests = [
            _parse_manifest(stored) for stored in self.backend.list_objects(MANIFESTS_PREFIX)
        ]
        return aliases_object, DefinitionCatalog(manifests, aliases)


class ScopedCatalogBackend:
    """Namespace every catalog object by an opaque runtime-scope digest."""

    def __init__(self, backend: CatalogBackend, scope: RuntimeScope) -> None:
        self._backend = backend
        self._prefix = f"{SCOPES_DIRECTORY}/{scope.digest}/"
        self._identity = CatalogBackendIdentity(
            provider=backend.identity.provider,
            configuration_digest=provenance_digest(
                {
                    "backend": backend.identity.configuration_digest,
                    "scope_digest": scope.digest,
                }
            ),
        )

    @property
    def identity(self) -> CatalogBackendIdentity:
        return self._identity

    def read_object(self, key: str) -> StoredCatalogObject | None:
        stored = self._backend.read_object(self._scoped_key(key))
        return self._unscoped(stored)

    def list_objects(self, prefix: str) -> Sequence[StoredCatalogObject]:
        return tuple(
            self._unscoped_required(stored)
            for stored in self._backend.list_objects(self._scoped_key(prefix))
        )

    def create_immutable(self, key: str, payload: bytes) -> StoredCatalogObject:
        return self._unscoped_required(
            self._backend.create_immutable(self._scoped_key(key), payload)
        )

    def compare_and_swap(
        self,
        key: str,
        payload: bytes,
        *,
        expected_version: str | None,
    ) -> StoredCatalogObject:
        return self._unscoped_required(
            self._backend.compare_and_swap(
                self._scoped_key(key),
                payload,
                expected_version=expected_version,
            )
        )

    def _scoped_key(self, key: str) -> str:
        _catalog_key_parts(key, allow_trailing_separator=key.endswith("/"))
        return f"{self._prefix}{key}"

    def _unscoped(self, stored: StoredCatalogObject | None) -> StoredCatalogObject | None:
        if stored is None:
            return None
        if not stored.key.startswith(self._prefix):
            raise CatalogStorageError("Catalog backend returned an object outside its scope")
        return StoredCatalogObject(
            key=stored.key[len(self._prefix) :],
            payload=stored.payload,
            version=stored.version,
        )

    def _unscoped_required(self, stored: StoredCatalogObject) -> StoredCatalogObject:
        result = self._unscoped(stored)
        if result is None:
            raise CatalogStorageError("Catalog backend omitted a written object")
        return result


class LocalCatalogBackend:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self._identity = CatalogBackendIdentity(
            provider="local",
            configuration_digest=provenance_digest({"root": str(self.root.resolve(strict=False))}),
        )

    @property
    def identity(self) -> CatalogBackendIdentity:
        return self._identity

    @property
    def aliases_path(self) -> Path:
        return self.root / ALIASES_FILENAME

    def manifest_path(self, logical_name: str, digest: str) -> Path:
        _validate_path_segment(logical_name, kind="workflow name")
        _validate_path_segment(digest, kind="definition digest")
        return self.root / MANIFESTS_DIRECTORY / logical_name / f"{digest}{JSON_SUFFIX}"

    def read_object(self, key: str) -> StoredCatalogObject | None:
        path = self._path_for_key(key)
        if not path.exists():
            return None
        if not path.is_file():
            raise CatalogStorageError(f"Catalog object '{path}' is not a regular file")
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise CatalogStorageError(f"Cannot read catalog object '{path}': {exc}") from exc
        return _stored_object(key, payload)

    def list_objects(self, prefix: str) -> Sequence[StoredCatalogObject]:
        prefix_path = self._path_for_prefix(prefix)
        if not prefix_path.exists():
            return ()
        try:
            paths = sorted(path for path in prefix_path.rglob(f"*{JSON_SUFFIX}") if path.is_file())
            return tuple(
                _stored_object(path.relative_to(self.root).as_posix(), path.read_bytes())
                for path in paths
            )
        except OSError as exc:
            raise CatalogStorageError(
                f"Cannot list catalog objects below '{prefix_path}': {exc}"
            ) from exc

    def create_immutable(self, key: str, payload: bytes) -> StoredCatalogObject:
        path = self._path_for_key(key)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise CatalogStorageError(
                f"Cannot create catalog directory '{path.parent}': {exc}"
            ) from exc
        if path.exists():
            current = self.read_object(key)
            if current is None or current.payload != payload:
                raise CatalogConflictError(f"Immutable catalog object '{path}' has changed")
            return current
        try:
            with path.open("xb") as stream:
                stream.write(payload)
        except FileExistsError:
            current = self.read_object(key)
            if current is None or current.payload != payload:
                raise CatalogConflictError(
                    f"Immutable catalog object '{path}' was created concurrently"
                ) from None
            return current
        except OSError as exc:
            raise CatalogStorageError(f"Cannot create catalog object '{path}': {exc}") from exc
        return _stored_object(key, payload)

    def compare_and_swap(
        self,
        key: str,
        payload: bytes,
        *,
        expected_version: str | None,
    ) -> StoredCatalogObject:
        path = self._path_for_key(key)
        try:
            self.root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise CatalogStorageError(
                f"Cannot create catalog directory '{self.root}': {exc}"
            ) from exc
        current = self.read_object(key)
        if current is not None and current.payload == payload:
            return current
        current_version = current.version if current is not None else None
        if current_version != expected_version:
            raise CatalogConflictError(
                f"Catalog object '{path}' changed while it was being updated"
            )
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                dir=self.root,
                prefix=f".{path.name}.",
            )
        except OSError as exc:
            raise CatalogStorageError(
                f"Cannot create a temporary catalog object in '{self.root}': {exc}"
            ) from exc
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            Path(temporary_name).replace(path)
        except OSError as exc:
            Path(temporary_name).unlink(missing_ok=True)
            raise CatalogStorageError(f"Cannot update catalog object '{path}': {exc}") from exc
        except BaseException:
            Path(temporary_name).unlink(missing_ok=True)
            raise
        return _stored_object(key, payload)

    def _path_for_key(self, key: str) -> Path:
        parts = _catalog_key_parts(key, allow_trailing_separator=False)
        return self.root.joinpath(*parts)

    def _path_for_prefix(self, prefix: str) -> Path:
        parts = _catalog_key_parts(prefix, allow_trailing_separator=True)
        return self.root.joinpath(*parts)


class CatalogStore(DefinitionCatalogStore):
    def __init__(self, config_dir: str | Path) -> None:
        self.local_backend = LocalCatalogBackend(Path(config_dir) / CATALOG_DIRECTORY)
        super().__init__(self.local_backend)

    @property
    def root(self) -> Path:
        return self.local_backend.root

    @property
    def aliases_path(self) -> Path:
        return self.local_backend.aliases_path

    def manifest_path(self, logical_name: str, digest: str) -> Path:
        return self.local_backend.manifest_path(logical_name, digest)


def _parse_json(payload: bytes, *, object_key: str) -> Any:
    try:
        return json.loads(
            payload,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_non_finite_constant,
        )
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
        raise CatalogError(f"Cannot read definition catalog object '{object_key}': {exc}") from exc


def _parse_aliases(stored: StoredCatalogObject) -> dict[str, str]:
    try:
        document = AliasDocument.model_validate(_parse_json(stored.payload, object_key=stored.key))
    except (ValueError, TypeError) as exc:
        raise CatalogError(f"Invalid alias document '{stored.key}': {exc}") from exc
    if document.format_version != ALIASES_FORMAT_VERSION:
        raise CatalogError(f"Unsupported alias document format version {document.format_version}")
    if stored.payload != _alias_payload(document.aliases):
        raise CatalogError(f"Definition alias document '{stored.key}' is not canonical JSON")
    return document.aliases


def _parse_manifest(stored: StoredCatalogObject) -> DefinitionManifest:
    try:
        manifest = DefinitionManifest.model_validate(
            _parse_json(stored.payload, object_key=stored.key)
        )
    except (ValueError, TypeError) as exc:
        raise CatalogError(f"Invalid definition manifest '{stored.key}': {exc}") from exc
    expected_key = _manifest_key(manifest)
    if stored.key != expected_key:
        raise CatalogError(
            f"Definition catalog path mismatch: '{stored.key}' should be '{expected_key}'"
        )
    if stored.payload != manifest.canonical_bytes():
        raise CatalogError(f"Definition manifest '{stored.key}' is not canonical JSON")
    return manifest


def _parse_environment_snapshot(stored: StoredCatalogObject) -> ExecutionEnvironmentSnapshot:
    try:
        snapshot = ExecutionEnvironmentSnapshot.model_validate(
            _parse_json(stored.payload, object_key=stored.key)
        )
    except (ValueError, TypeError) as exc:
        raise CatalogError(f"Invalid execution environment snapshot '{stored.key}': {exc}") from exc
    expected_key = _environment_snapshot_key(snapshot.snapshot_digest)
    if stored.key != expected_key:
        raise CatalogError(
            f"Execution environment snapshot path mismatch: '{stored.key}' should be '{expected_key}'"
        )
    if stored.payload != snapshot.canonical_bytes():
        raise CatalogError(f"Execution environment snapshot '{stored.key}' is not canonical JSON")
    return snapshot


def _alias_payload(aliases: Mapping[str, str]) -> bytes:
    document = AliasDocument(aliases=dict(aliases))
    return json.dumps(
        document.model_dump(mode="json"),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _manifest_key(manifest: DefinitionManifest) -> str:
    _validate_path_segment(manifest.logical_name, kind="workflow name")
    _validate_path_segment(manifest.definition_digest, kind="definition digest")
    return (
        f"{MANIFESTS_DIRECTORY}/{manifest.logical_name}/{manifest.definition_digest}{JSON_SUFFIX}"
    )


def _environment_snapshot_key(digest: str) -> str:
    _validate_path_segment(digest, kind="execution environment snapshot digest")
    return f"{ENVIRONMENTS_DIRECTORY}/{digest}{JSON_SUFFIX}"


def _stored_object(key: str, payload: bytes) -> StoredCatalogObject:
    return StoredCatalogObject(
        key=key,
        payload=payload,
        version=hashlib.sha256(payload).hexdigest(),
    )


def _catalog_key_parts(
    key: str,
    *,
    allow_trailing_separator: bool,
) -> tuple[str, ...]:
    normalized = key[:-1] if allow_trailing_separator and key.endswith("/") else key
    parts = tuple(normalized.split("/"))
    if (
        not normalized
        or normalized.startswith("/")
        or "\\" in normalized
        or "\0" in normalized
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise CatalogStorageError(f"Invalid catalog object key '{key}'")
    return parts


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate object key '{key}'")
        value[key] = item
    return value


def _reject_non_finite_constant(value: str) -> None:
    raise ValueError(f"non-finite number '{value}'")


def _validate_path_segment(value: str, *, kind: str) -> None:
    if not value or value in {".", ".."} or "/" in value or "\\" in value or "\0" in value:
        raise CatalogError(f"Invalid catalog {kind} '{value}'")
