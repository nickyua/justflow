"""Local/Git and stored configuration read sources."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

from justflow.config.loader import ConfigLoader
from justflow.configuration.errors import (
    ConfigurationError,
    ConfigurationIntegrityError,
    ConfigurationNotFoundError,
    ConfigurationScopeError,
)
from justflow.configuration.models import (
    ConfigurationBundle,
    ConfigurationSnapshot,
    configuration_revision_identity,
)
from justflow.configuration.ports import ConfigurationStore
from justflow.scope import RuntimeScope

LOCAL_CONFIGURATION_ROOTS = ("resources.yaml", "services.yaml", "workflows")


class FileConfigurationSource:
    """Immutable local/Git-managed YAML bound to exactly one runtime scope."""

    def __init__(self, config_dir: str | Path, *, scope: RuntimeScope) -> None:
        self._config_dir = Path(config_dir)
        self._scope = scope
        self._workflow_sources: Mapping[str, Path] = MappingProxyType({})

    @property
    def workflow_sources(self) -> Mapping[str, Path]:
        return self._workflow_sources

    def read(self, scope: RuntimeScope) -> ConfigurationSnapshot:
        return self._read(scope)

    def read_triggers(self, scope: RuntimeScope) -> ConfigurationSnapshot:
        if scope != self._scope:
            raise ConfigurationScopeError("Local configuration is bound to another runtime scope")
        if any((self._config_dir / name).exists() for name in LOCAL_CONFIGURATION_ROOTS):
            return self._read(scope)
        triggers = ConfigLoader(self._config_dir).load_triggers()
        bundle = ConfigurationBundle(workflows={}, triggers=triggers)
        return ConfigurationSnapshot(
            scope_digest=scope.digest,
            revision_id=configuration_revision_identity(scope.digest, bundle, None),
            bundle=bundle,
        )

    def _read(
        self,
        scope: RuntimeScope,
    ) -> ConfigurationSnapshot:
        if scope != self._scope:
            raise ConfigurationScopeError("Local configuration is bound to another runtime scope")
        loader = ConfigLoader(self._config_dir)
        resources, services, workflows = loader.load_all()
        self._workflow_sources = MappingProxyType(dict(loader.workflow_sources))
        triggers = loader.load_triggers()
        bundle = ConfigurationBundle(
            resources=resources,
            services=services,
            workflows=workflows,
            triggers=triggers,
        )
        return ConfigurationSnapshot(
            scope_digest=scope.digest,
            revision_id=configuration_revision_identity(
                scope.digest,
                bundle,
                None,
            ),
            bundle=bundle,
        )


class StoredConfigurationSource:
    def __init__(self, store: ConfigurationStore) -> None:
        self._store = store

    def read(self, scope: RuntimeScope) -> ConfigurationSnapshot:
        pointer = self._store.read_active(scope)
        if pointer is None:
            raise ConfigurationNotFoundError(
                "The runtime scope has no active configuration revision"
            )
        revision = self._store.read_revision(scope, pointer.revision_id)
        if (
            revision.scope_digest != scope.digest
            or revision.revision_id != pointer.revision_id
            or configuration_revision_identity(
                scope.digest,
                revision.bundle,
                revision.parent_revision_id,
            )
            != revision.revision_id
        ):
            raise ConfigurationIntegrityError(
                "Active configuration revision identity is inconsistent"
            )
        document = revision.bundle
        if not isinstance(document, ConfigurationBundle):
            raise ConfigurationError(
                "Active tenant configuration requires publication before runtime use"
            )
        return ConfigurationSnapshot(
            scope_digest=scope.digest,
            revision_id=revision.revision_id,
            bundle=document,
        )

    def read_triggers(self, scope: RuntimeScope) -> ConfigurationSnapshot:
        return self.read(scope)
