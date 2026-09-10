"""Local authoring that edits the configuration files directly.

The config directory IS the storage: workflow saves write
`<config_dir>/workflows/<name>.yaml`, trigger saves write
`<config_dir>/triggers.yaml`, and valid saves publish immutable definitions to
the catalog. The running process keeps its startup snapshot; `restart_required`
reports divergence between the files and that snapshot."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Self

import yaml
from pydantic import Field, ValidationError, model_validator

from justflow.config.authored_yaml import authored_dump, render_authored_yaml
from justflow.config.grammar import TriggerName
from justflow.config.loader import ConfigLoadError, load_bounded_yaml
from justflow.config.models import WorkflowConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.config.triggers import TriggerDeclaration, TriggersConfig
from justflow.config.validator import ConfigValidator, ValidationResult
from justflow.configuration.activation import control_identity_digest
from justflow.configuration.errors import (
    ConfigurationConflictError,
    ConfigurationError,
    ConfigurationLimitError,
    ConfigurationNotFoundError,
    ConfigurationScopeError,
    ConfigurationUnavailableError,
)
from justflow.configuration.lifecycle import (
    MAX_CONFIGURATION_RELATIONSHIPS,
    ConfigurationApplyStage,
    ConfigurationApplyStageKind,
    ConfigurationApplyStageState,
    ConfigurationDiscardErrorCode,
    ConfigurationDiscardRecord,
    ConfigurationDiscardResult,
    ConfigurationDiscardState,
    ConfigurationRelationships,
    LocalConfigurationApplyResult,
    build_configuration_relationships,
    complete_discard,
    configuration_declarations,
    discard_identity,
    fail_discard,
)
from justflow.configuration.models import (
    MAX_CONFIGURATION_BYTES,
    ConfigurationBundle,
    ConfigurationSnapshot,
    RevisionIdentity,
    StrictConfigurationModel,
)
from justflow.configuration.source import FileConfigurationSource
from justflow.definitions.catalog import CatalogError, DefinitionCatalogStore
from justflow.definitions.manifest import (
    DefinitionManifest,
    DefinitionManifestError,
    build_definition_manifests,
)
from justflow.provenance import provenance_digest
from justflow.resources.registry import ResourceRegistry
from justflow.scope import RuntimeScope
from justflow.transports.registry import TransportRegistry

LOCAL_AUTHORING_SOURCE = "local-authoring"
LEGACY_OVERLAY_PATH = ".justflow/admin-source-draft.yaml"
WORKFLOWS_SUBDIRECTORY = "workflows"
TRIGGERS_FILENAME = "triggers.yaml"
LOCAL_AUTHORING_VERSION_BYTES = 6
MAX_LOCAL_DISCARD_RECORDS = MAX_CONFIGURATION_RELATIONSHIPS

logger = logging.getLogger(__name__)
MAX_LOCAL_AUTHORING_YAML_BYTES = MAX_CONFIGURATION_BYTES * 2


class WorkflowFragmentError(ConfigurationError):
    """A submitted workflow fragment cannot join the draft."""


class WorkflowFragmentDependencyError(ConfigurationError):
    """A workflow cannot leave the draft while triggers still reference it."""

    def __init__(self, workflow: str, triggers: tuple[str, ...]) -> None:
        self.triggers = triggers
        super().__init__(f"Remove dependent trigger declarations first: {', '.join(triggers)}")
        self.workflow = workflow


class LocalAuthoringDocument(StrictConfigurationModel):
    workflows: dict[str, WorkflowConfig]
    triggers: dict[TriggerName, TriggerDeclaration]

    @model_validator(mode="after")
    def validate_references(self) -> Self:
        for name, workflow in self.workflows.items():
            if workflow.workflow != name:
                raise ValueError("Workflow mapping key does not match its declaration")
        for trigger in self.triggers.values():
            if trigger.workflow not in self.workflows:
                raise ValueError("Trigger references an unknown workflow")
        if len(self.canonical_bytes()) > MAX_CONFIGURATION_BYTES:
            raise ValueError("Local authoring document exceeds its byte limit")
        return self

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json", by_alias=True, exclude_none=True),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")


class LocalAuthoringDraft(StrictConfigurationModel):
    scope_digest: str
    version: int = Field(ge=1)
    bundle: LocalAuthoringDocument = Field(repr=False)
    restart_required: bool
    definitions_published: bool = False


class LocalAuthoringConfigurationSource(FileConfigurationSource):
    def __init__(
        self,
        config_dir: str | Path,
        *,
        scope: RuntimeScope,
        transport_registry: TransportRegistry,
        resource_registry: ResourceRegistry,
        limits: RuntimeLimits,
        definition_catalog_store: DefinitionCatalogStore | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        super().__init__(config_dir, scope=scope)
        self._config_dir = Path(config_dir)
        self._scope = scope
        self._transport_registry = transport_registry
        self._resource_registry = resource_registry
        self._limits = limits
        self._definition_catalog_store = definition_catalog_store
        self._clock = clock
        self._draft_lock = Lock()
        self._discard_records: dict[str, ConfigurationDiscardRecord] = {}
        self._startup_document = self._document(super().read(scope).bundle)
        legacy_overlay = Path(LEGACY_OVERLAY_PATH)
        if legacy_overlay.exists():
            logger.warning(
                "Legacy authoring overlay %s is no longer used; local authoring "
                "now edits the configuration files directly. Merge or delete it.",
                legacy_overlay,
            )

    def read_triggers(self, scope: RuntimeScope) -> ConfigurationSnapshot:
        return self.read(scope)

    def read_draft(self, scope: RuntimeScope) -> LocalAuthoringDraft:
        document = self._document(self._base(scope).bundle)
        return LocalAuthoringDraft(
            scope_digest=scope.digest,
            version=_document_version(document),
            bundle=document,
            restart_required=document != self._startup_document,
        )

    def create_draft(
        self,
        scope: RuntimeScope,
        document: LocalAuthoringDocument,
    ) -> LocalAuthoringDraft:
        self._require_scope(scope)
        with self._draft_lock:
            return self._save(scope, document)

    def update_draft(
        self,
        scope: RuntimeScope,
        document: LocalAuthoringDocument,
        *,
        expected_version: int,
    ) -> LocalAuthoringDraft:
        with self._draft_lock:
            current = self.read_draft(scope)
            if current.version != expected_version:
                raise ConfigurationConflictError(scope.digest)
            return self._save(scope, document)

    def read_workflow_fragment(self, scope: RuntimeScope, name: str) -> tuple[bytes, int]:
        draft = self.read_draft(scope)
        workflow = draft.bundle.workflows.get(name)
        if workflow is None:
            raise ConfigurationNotFoundError("Workflow is not present in the draft")
        return _render_yaml(authored_dump(workflow)), draft.version

    def read_triggers_fragment(self, scope: RuntimeScope) -> tuple[bytes, int]:
        draft = self.read_draft(scope)
        payload = {
            "triggers": {
                name: authored_dump(trigger)
                for name, trigger in sorted(draft.bundle.triggers.items())
            }
        }
        return _render_yaml(payload), draft.version

    def update_triggers_fragment(
        self,
        scope: RuntimeScope,
        payload: bytes,
        *,
        expected_version: int,
    ) -> LocalAuthoringDraft:
        if len(payload) > MAX_LOCAL_AUTHORING_YAML_BYTES:
            raise ConfigurationLimitError("Triggers fragment exceeds its byte limit")
        try:
            parsed = TriggersConfig.model_validate(
                load_bounded_yaml(payload, source=LOCAL_AUTHORING_SOURCE)
            )
        except (ConfigLoadError, ValidationError) as exc:
            raise WorkflowFragmentError("Triggers fragment is invalid") from exc
        with self._draft_lock:
            current = self.read_draft(scope)
            if current.version != expected_version:
                raise ConfigurationConflictError(scope.digest)
            document = LocalAuthoringDocument(
                workflows=dict(current.bundle.workflows),
                triggers=dict(parsed.triggers),
            )
            return self._save(scope, document)

    def update_workflow_fragment(
        self,
        scope: RuntimeScope,
        name: str,
        payload: bytes,
        *,
        expected_version: int,
    ) -> LocalAuthoringDraft:
        workflow = self.parse_workflow_fragment(payload)
        if workflow.workflow != name:
            raise WorkflowFragmentError(
                "Workflow fragment declaration name does not match the requested workflow"
            )
        with self._draft_lock:
            current = self.read_draft(scope)
            if current.version != expected_version:
                raise ConfigurationConflictError(scope.digest)
            document = LocalAuthoringDocument(
                workflows={**current.bundle.workflows, name: workflow},
                triggers=dict(current.bundle.triggers),
            )
            return self._save(scope, document)

    def delete_workflow_fragment(
        self,
        scope: RuntimeScope,
        name: str,
        *,
        expected_version: int,
    ) -> LocalAuthoringDraft:
        with self._draft_lock:
            current = self.read_draft(scope)
            if current.version != expected_version:
                raise ConfigurationConflictError(scope.digest)
            if name not in current.bundle.workflows:
                raise ConfigurationNotFoundError("Workflow is not present in the draft")
            dependents = tuple(
                sorted(
                    trigger_name
                    for trigger_name, trigger in current.bundle.triggers.items()
                    if trigger.workflow == name
                )
            )
            if dependents:
                raise WorkflowFragmentDependencyError(name, dependents)
            workflows = dict(current.bundle.workflows)
            del workflows[name]
            document = LocalAuthoringDocument(
                workflows=workflows,
                triggers=dict(current.bundle.triggers),
            )
            return self._save(scope, document)

    @staticmethod
    def parse_workflow_fragment(payload: bytes) -> WorkflowConfig:
        if len(payload) > MAX_LOCAL_AUTHORING_YAML_BYTES:
            raise ConfigurationLimitError("Workflow fragment exceeds its byte limit")
        try:
            return WorkflowConfig.model_validate(
                load_bounded_yaml(payload, source=LOCAL_AUTHORING_SOURCE)
            )
        except (ConfigLoadError, ValidationError) as exc:
            raise WorkflowFragmentError("Workflow fragment is invalid") from exc

    def validate_draft(self, scope: RuntimeScope) -> ValidationResult:
        document = self.read_draft(scope).bundle
        return self._merged_validator(document).validate()

    def relationships(self, scope: RuntimeScope) -> ConfigurationRelationships:
        draft = self.read_draft(scope)
        return build_configuration_relationships(
            working_version=draft.version,
            active_identity=_document_identity(self._startup_document),
            working=configuration_declarations(
                workflows=draft.bundle.workflows,
                triggers=draft.bundle.triggers,
            ),
            active=configuration_declarations(
                workflows=self._startup_document.workflows,
                triggers=self._startup_document.triggers,
            ),
            restart_required=draft.restart_required,
        )

    def apply(
        self,
        scope: RuntimeScope,
        *,
        expected_version: int,
        actor_identity: str,
        correlation_identity: str,
    ) -> LocalConfigurationApplyResult:
        with self._draft_lock:
            draft = self.read_draft(scope)
            if draft.version != expected_version:
                raise ConfigurationConflictError(scope.digest)
            store = self._definition_catalog_store
            if store is None:
                raise ConfigurationUnavailableError(
                    "Immutable definition publication is unavailable"
                )
            try:
                store.publish(self._definition_manifests(draft.bundle))
            except (CatalogError, DefinitionManifestError) as exc:
                raise ConfigurationUnavailableError(
                    "Immutable definitions could not be published"
                ) from exc
            logger.info(
                "Local authoring apply completed",
                extra={
                    "actor_digest": control_identity_digest(
                        "local-apply-actor",
                        actor_identity,
                    ),
                    "correlation_digest": control_identity_digest(
                        "local-apply-correlation",
                        correlation_identity,
                    ),
                    "restart_required": draft.restart_required,
                    "working_version": draft.version,
                },
            )
            return LocalConfigurationApplyResult(
                working_version=draft.version,
                stages=(
                    ConfigurationApplyStage(
                        kind=ConfigurationApplyStageKind.VALIDATION,
                        state=ConfigurationApplyStageState.COMPLETED,
                    ),
                    ConfigurationApplyStage(
                        kind=ConfigurationApplyStageKind.DEFINITION_PUBLICATION,
                        state=ConfigurationApplyStageState.COMPLETED,
                    ),
                    ConfigurationApplyStage(
                        kind=ConfigurationApplyStageKind.PROCESS_RESTART,
                        state=(
                            ConfigurationApplyStageState.RESTART_REQUIRED
                            if draft.restart_required
                            else ConfigurationApplyStageState.NOT_APPLICABLE
                        ),
                    ),
                ),
                definitions_published=True,
                restart_required=draft.restart_required,
            )

    def discard(
        self,
        scope: RuntimeScope,
        *,
        expected_version: int,
        expected_active_identity: RevisionIdentity,
        idempotency_key: str,
        actor_identity: str,
        correlation_identity: str,
    ) -> ConfigurationDiscardResult:
        with self._draft_lock:
            self._require_scope(scope)
            active_identity = _document_identity(self._startup_document)
            if str(expected_active_identity) != active_identity:
                raise ConfigurationConflictError(scope.digest)
            request_digest = provenance_digest(
                {
                    "active_identity": active_identity,
                    "draft_version": expected_version,
                    "scope_digest": scope.digest,
                }
            )
            record_id = discard_identity(scope.digest, idempotency_key)
            existing = self._discard_records.get(record_id)
            if existing is not None:
                if existing.request_digest != request_digest:
                    raise ConfigurationConflictError(scope.digest)
                if (
                    existing.state is ConfigurationDiscardState.APPLIED
                    and existing.result_draft_version is not None
                ):
                    return ConfigurationDiscardResult(
                        discard_id=existing.discard_id,
                        working_version=existing.result_draft_version,
                        active_identity=existing.expected_active_identity,
                        definitions_published=existing.definitions_published,
                    )
                raise ConfigurationConflictError(scope.digest)
            if len(self._discard_records) >= MAX_LOCAL_DISCARD_RECORDS:
                raise ConfigurationLimitError("Local discard audit exceeds its item bound")
            now = self._now()
            pending = ConfigurationDiscardRecord(
                discard_id=record_id,
                scope_digest=scope.digest,
                idempotency_key_digest=control_identity_digest(
                    "local-discard-key",
                    idempotency_key,
                ),
                request_digest=request_digest,
                actor_digest=control_identity_digest(
                    "local-discard-actor",
                    actor_identity,
                ),
                correlation_digest=control_identity_digest(
                    "local-discard-correlation",
                    correlation_identity,
                ),
                expected_draft_version=expected_version,
                expected_active_identity=expected_active_identity,
                active_document_digest=provenance_digest(
                    {"configuration": self._startup_document.model_dump(mode="json")}
                ),
                created_at=now,
                updated_at=now,
            )
            self._discard_records[record_id] = pending
            current = self.read_draft(scope)
            if current.version != expected_version:
                self._discard_records[record_id] = fail_discard(
                    pending,
                    error_code=ConfigurationDiscardErrorCode.CONFLICT,
                    occurred_at=self._now(),
                )
                raise ConfigurationConflictError(scope.digest)
            saved = self._save(scope, self._startup_document)
            completed = complete_discard(
                pending,
                result_draft_version=saved.version,
                occurred_at=self._now(),
                definitions_published=saved.definitions_published,
            )
            self._discard_records[record_id] = completed
            logger.info(
                "Local authoring discard completed",
                extra={
                    "actor_digest": completed.actor_digest,
                    "correlation_digest": completed.correlation_digest,
                    "discard_id": completed.discard_id,
                    "working_version": saved.version,
                },
            )
            return ConfigurationDiscardResult(
                discard_id=completed.discard_id,
                working_version=saved.version,
                active_identity=expected_active_identity,
                definitions_published=saved.definitions_published,
            )

    def read_discard(
        self,
        scope: RuntimeScope,
        discard_id: str,
    ) -> ConfigurationDiscardRecord:
        self._require_scope(scope)
        record = self._discard_records.get(discard_id)
        if record is None:
            raise ConfigurationNotFoundError("Configuration discard was not found")
        return record

    def _merged_validator(self, document: LocalAuthoringDocument) -> ConfigValidator:
        bundle = self._merge(super().read(self._scope).bundle, document)
        return ConfigValidator(
            bundle.resources,
            bundle.services,
            bundle.workflows,
            transport_registry=self._transport_registry,
            resource_registry=self._resource_registry,
            limits=self._limits,
            config_dir=self._config_dir,
            workflow_sources=self.workflow_sources,
            triggers=bundle.triggers,
        )

    def _publish_definitions(self, document: LocalAuthoringDocument) -> bool:
        """Best-effort immutable-definition publication on save: a valid saved
        workflow is immediately in the catalog, so a restart can activate it
        without a separate publish step. Invalid drafts save without publishing."""
        store = self._definition_catalog_store
        if store is None:
            return False
        try:
            store.publish(self._definition_manifests(document))
        except (CatalogError, ConfigurationError, DefinitionManifestError) as exc:
            logger.warning("Definitions were not published for the saved draft: %s", exc)
            return False
        return True

    def _definition_manifests(
        self,
        document: LocalAuthoringDocument,
    ) -> Mapping[str, DefinitionManifest]:
        validator = self._merged_validator(document)
        validation = validator.validate()
        if not validation.is_valid:
            raise ConfigurationError("Local authoring validation failed")
        return build_definition_manifests(
            document.workflows,
            dict(validator.resolved_services),
            self._limits,
            resources=dict(validator.resolved_resources),
        )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ConfigurationUnavailableError("Local authoring clock must be timezone-aware")
        return value.astimezone(UTC)

    def export_draft_yaml(
        self, scope: RuntimeScope, *, expected_version: int | None = None
    ) -> bytes:
        draft = self.read_draft(scope)
        if expected_version is not None and draft.version != expected_version:
            raise ConfigurationConflictError(scope.digest)
        return _render_yaml(_authored_document_payload(draft.bundle))

    @staticmethod
    def parse_yaml(payload: bytes) -> LocalAuthoringDocument:
        if len(payload) > MAX_LOCAL_AUTHORING_YAML_BYTES:
            raise ConfigurationLimitError("Local authoring YAML exceeds its byte limit")
        try:
            return LocalAuthoringDocument.model_validate(
                load_bounded_yaml(payload, source=LOCAL_AUTHORING_SOURCE)
            )
        except (ConfigLoadError, ValidationError) as exc:
            raise ConfigurationError("Local authoring YAML is invalid") from exc

    def _base(self, scope: RuntimeScope) -> ConfigurationSnapshot:
        self._require_scope(scope)
        return super().read(scope)

    def _save(
        self,
        scope: RuntimeScope,
        document: LocalAuthoringDocument,
    ) -> LocalAuthoringDraft:
        self._require_scope(scope)
        current = self._document(self._base(scope).bundle)
        self._persist_files(current, document)
        return LocalAuthoringDraft(
            scope_digest=scope.digest,
            version=_document_version(document),
            bundle=document,
            restart_required=document != self._startup_document,
            definitions_published=self._publish_definitions(document),
        )

    def _persist_files(
        self,
        current: LocalAuthoringDocument,
        document: LocalAuthoringDocument,
    ) -> None:
        """Write the changed declarations into the config directory itself."""
        for name, workflow in sorted(document.workflows.items()):
            if current.workflows.get(name) == workflow:
                continue
            self._write_atomic(self._workflow_path(name), _render_yaml(authored_dump(workflow)))
        for name in sorted(set(current.workflows) - set(document.workflows)):
            path = self._workflow_path(name)
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                raise ConfigurationUnavailableError(
                    "Workflow declaration file could not be removed"
                ) from exc
        if current.triggers != document.triggers:
            payload = {
                "triggers": {
                    name: authored_dump(trigger)
                    for name, trigger in sorted(document.triggers.items())
                }
            }
            self._write_atomic(self._config_dir / TRIGGERS_FILENAME, _render_yaml(payload))

    def _workflow_path(self, name: str) -> Path:
        """The declaration file for one workflow — the one it was loaded from,
        as long as that file holds only this workflow (the panel cannot safely
        rewrite a shared multi-workflow file)."""
        source = self.workflow_sources.get(name)
        if source is None:
            return self._config_dir / WORKFLOWS_SUBDIRECTORY / f"{name}.yaml"
        sharing = [
            other
            for other, path in self.workflow_sources.items()
            if path == source and other != name
        ]
        if sharing:
            raise WorkflowFragmentError(
                f"Workflow '{name}' is declared in {source.name} together with "
                f"{', '.join(sorted(sharing))}; split it into one file per workflow to edit it here"
            )
        return source

    @staticmethod
    def _write_atomic(path: Path, payload: bytes) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, temporary_name = tempfile.mkstemp(
                dir=path.parent,
                prefix=f".{path.name}.",
            )
            temporary_path = Path(temporary_name)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary_path, path)
            finally:
                temporary_path.unlink(missing_ok=True)
        except OSError as exc:
            raise ConfigurationUnavailableError(
                "Configuration file could not be persisted"
            ) from exc

    @staticmethod
    def _document(bundle: ConfigurationBundle) -> LocalAuthoringDocument:
        return LocalAuthoringDocument(
            workflows=dict(bundle.workflows),
            triggers=dict(bundle.triggers.triggers),
        )

    @staticmethod
    def _merge(
        base: ConfigurationBundle,
        document: LocalAuthoringDocument,
    ) -> ConfigurationBundle:
        return ConfigurationBundle(
            resources=base.resources,
            services=base.services,
            workflows=document.workflows,
            triggers=TriggersConfig(triggers=document.triggers),
        )

    def _require_scope(self, scope: RuntimeScope) -> None:
        if scope != self._scope:
            raise ConfigurationScopeError("Local authoring is bound to another runtime scope")


def _document_version(document: LocalAuthoringDocument) -> int:
    digest = hashlib.sha256(document.canonical_bytes()).digest()
    return int.from_bytes(digest[:LOCAL_AUTHORING_VERSION_BYTES], "big") + 1


def _document_identity(document: LocalAuthoringDocument) -> str:
    return hashlib.sha256(document.canonical_bytes()).hexdigest()


def _authored_document_payload(document: LocalAuthoringDocument) -> dict[str, object]:
    """Compose per declaration so one default-dependent entry cannot force the
    whole document into the verbose fallback rendering."""
    payload: dict[str, object] = {
        "workflows": {
            name: authored_dump(workflow) for name, workflow in sorted(document.workflows.items())
        }
    }
    if document.triggers:
        payload["triggers"] = {
            name: authored_dump(trigger) for name, trigger in sorted(document.triggers.items())
        }
    return payload


def _render_yaml(payload: object) -> bytes:
    try:
        rendered = render_authored_yaml(payload).encode("utf-8")
    except (TypeError, ValueError, yaml.YAMLError) as exc:
        raise ConfigurationError("Local authoring document cannot be rendered") from exc
    if len(rendered) > MAX_LOCAL_AUTHORING_YAML_BYTES:
        raise ConfigurationLimitError("Local authoring YAML exceeds its byte limit")
    return rendered
