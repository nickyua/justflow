"""Bounded canonical configuration documents and store records."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Annotated, Literal, Self, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    StrictBool,
    StrictStr,
    model_validator,
)

from justflow.config.grammar import (
    Identifier,
    OperationName,
    ReferencePath,
    ResourceName,
    ServiceName,
    WorkflowName,
)
from justflow.config.models import (
    MAX_DESCRIPTION_LENGTH,
    CacheConfig,
    FlowStep,
    OnCompleteConfig,
    OnErrorConfig,
    ResourcesConfig,
    ServicesConfig,
    WorkflowConfig,
)
from justflow.config.triggers import TriggerKind, TriggersConfig
from justflow.provenance import ExecutionConfigurationIdentity, provenance_digest
from justflow.resources.base import ResourceCapability
from justflow.scope import SCOPE_DIGEST_LENGTH

CONFIGURATION_ENVELOPE_VERSION: Literal[1] = 1
MAX_CONFIGURATION_BYTES = 1_048_576
MAX_CONFIGURATION_ENVELOPE_BYTES = MAX_CONFIGURATION_BYTES + 4_096
MAX_CONFIGURATION_COLLECTION_ITEMS = 10_000
MAX_CONFIGURATION_DEPTH = 32
MAX_REVISION_PAGE_SIZE = 100
MAX_RETENTION_DELETE_COUNT = 100
REVISION_DIGEST_LENGTH = 64
MAX_COMPONENTS = 10_000
MAX_COMPONENT_NAME_LENGTH = 128
MAX_COMPONENT_VERSION_LENGTH = 64
MAX_COMPONENT_BINDINGS = 100
COMPONENT_NAME_PATTERN = r"^[A-Za-z][A-Za-z0-9._-]*$"
COMPONENT_VERSION_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]*$"
SECRET_FIELD_FRAGMENTS = frozenset(
    {"api_key", "credential", "password", "private_key", "secret", "token"}
)
SECRET_ALIAS_SUFFIXES = ("_alias", "_aliases")


class StrictConfigurationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class RevisionIdentity(RootModel[str]):
    model_config = ConfigDict(frozen=True)
    root: str = Field(
        min_length=REVISION_DIGEST_LENGTH,
        max_length=REVISION_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{REVISION_DIGEST_LENGTH}}}$",
    )

    def __str__(self) -> str:
        return self.root


ResolvedComponentIdentity = Annotated[
    StrictStr,
    Field(
        min_length=3,
        max_length=MAX_COMPONENT_NAME_LENGTH + MAX_COMPONENT_VERSION_LENGTH + 1,
        pattern=r"^[A-Za-z][A-Za-z0-9._-]*@[A-Za-z0-9][A-Za-z0-9._-]*$",
    ),
]


class TenantConfigurationResolution(StrictConfigurationModel):
    tenant_configuration_revision_id: RevisionIdentity
    component_catalog_revision: str = Field(
        min_length=REVISION_DIGEST_LENGTH,
        max_length=REVISION_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{REVISION_DIGEST_LENGTH}}}$",
    )
    component_identities: tuple[ResolvedComponentIdentity, ...] = Field(
        max_length=MAX_COMPONENTS,
    )
    resolution_digest: str = Field(
        min_length=len("sha256:") + REVISION_DIGEST_LENGTH,
        max_length=len("sha256:") + REVISION_DIGEST_LENGTH,
        pattern=rf"^sha256:[0-9a-f]{{{REVISION_DIGEST_LENGTH}}}$",
    )


class ConfigurationBundle(StrictConfigurationModel):
    resources: ResourcesConfig = Field(default_factory=lambda: ResourcesConfig(resources={}))
    services: ServicesConfig = Field(default_factory=lambda: ServicesConfig(services={}))
    workflows: dict[WorkflowName, WorkflowConfig]
    triggers: TriggersConfig
    tenant_resolution: TenantConfigurationResolution | None = None

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        value = _configuration_bundle_value(self)
        _validate_structure(value)
        _reject_secret_values(value)
        if len(_canonical_json(value)) > MAX_CONFIGURATION_BYTES:
            raise ValueError("Configuration bundle exceeds its byte limit")
        for name, workflow in self.workflows.items():
            if workflow.workflow != name:
                raise ValueError("Workflow mapping key does not match its declaration")
        return self

    def canonical_bytes(self) -> bytes:
        return _canonical_json(_configuration_bundle_value(self))

    @classmethod
    def from_bytes(cls, payload: bytes) -> ConfigurationBundle:
        value = _parse_configuration_json(payload)
        bundle = cls.model_validate(value)
        if payload != bundle.canonical_bytes():
            raise ValueError("Configuration bundle is not canonical JSON")
        return bundle


ComponentName = Annotated[
    StrictStr,
    Field(
        min_length=1,
        max_length=MAX_COMPONENT_NAME_LENGTH,
        pattern=COMPONENT_NAME_PATTERN,
    ),
]
ComponentVersion = Annotated[
    StrictStr,
    Field(
        min_length=1,
        max_length=MAX_COMPONENT_VERSION_LENGTH,
        pattern=COMPONENT_VERSION_PATTERN,
    ),
]
ComponentCatalogRevision = Annotated[
    StrictStr,
    Field(
        min_length=REVISION_DIGEST_LENGTH,
        max_length=REVISION_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{REVISION_DIGEST_LENGTH}}}$",
    ),
]


class ComponentReference(StrictConfigurationModel):
    name: ComponentName
    version: ComponentVersion

    @property
    def identity(self) -> str:
        return f"{self.name}@{self.version}"


class ComponentRuntimeIdentity(StrictConfigurationModel):
    """Host-owned runtime implementation selected by a component revision."""

    implementation: ComponentName
    version: ComponentVersion
    artifact_digest: str = Field(
        min_length=len("sha256:") + REVISION_DIGEST_LENGTH,
        max_length=len("sha256:") + REVISION_DIGEST_LENGTH,
        pattern=rf"^sha256:[0-9a-f]{{{REVISION_DIGEST_LENGTH}}}$",
    )


class ComponentReplayContract(StrictConfigurationModel):
    """Determinism and retirement metadata retained with the exact component."""

    deterministic: StrictBool
    compatible_versions: tuple[ComponentReference, ...] = Field(
        default_factory=tuple,
        max_length=MAX_COMPONENTS,
    )
    retirement_identity: str = Field(
        min_length=len("sha256:") + REVISION_DIGEST_LENGTH,
        max_length=len("sha256:") + REVISION_DIGEST_LENGTH,
        pattern=rf"^sha256:[0-9a-f]{{{REVISION_DIGEST_LENGTH}}}$",
    )


class PlatformStepComponent(StrictConfigurationModel):
    reference: ComponentReference
    description: str | None = Field(default=None, max_length=MAX_DESCRIPTION_LENGTH)
    runtime_implementation: ComponentRuntimeIdentity
    replay: ComponentReplayContract
    transport: str = Field(min_length=1, max_length=MAX_COMPONENT_NAME_LENGTH)
    action: str = Field(min_length=1, max_length=MAX_COMPONENT_NAME_LENGTH)
    parameter_schema: dict[str, object]
    input_schema: dict[str, object]
    output_schema: dict[str, object]
    resource_slots: dict[Identifier, ResourceCapability] = Field(
        default_factory=dict,
        max_length=MAX_COMPONENT_BINDINGS,
    )
    approved_alias_parameters: frozenset[Identifier] = Field(
        default_factory=frozenset,
        max_length=MAX_COMPONENT_BINDINGS,
    )
    allow_cache: bool = False

    @model_validator(mode="after")
    def validate_replay_contract(self) -> Self:
        _validate_replay_compatibility(self.reference, self.replay)
        return self


class PlatformTriggerComponent(StrictConfigurationModel):
    reference: ComponentReference
    description: str | None = Field(default=None, max_length=MAX_DESCRIPTION_LENGTH)
    runtime_implementation: ComponentRuntimeIdentity
    replay: ComponentReplayContract
    kind: TriggerKind
    parameter_schema: dict[str, object]
    output_schema: dict[str, object]
    resource_slots: dict[Identifier, ResourceCapability] = Field(
        default_factory=dict,
        max_length=MAX_COMPONENT_BINDINGS,
    )
    approved_alias_parameters: frozenset[Identifier] = Field(
        default_factory=frozenset,
        max_length=MAX_COMPONENT_BINDINGS,
    )

    @model_validator(mode="after")
    def validate_replay_contract(self) -> Self:
        _validate_replay_compatibility(self.reference, self.replay)
        return self


def _validate_replay_compatibility(
    reference: ComponentReference,
    replay: ComponentReplayContract,
) -> None:
    if len(set(replay.compatible_versions)) != len(replay.compatible_versions):
        raise ValueError("Component replay-compatible versions must be unique")
    if any(candidate.name != reference.name for candidate in replay.compatible_versions):
        raise ValueError("Replay-compatible versions must reference the same component name")


class PlatformComponentCatalog(StrictConfigurationModel):
    revision_id: ComponentCatalogRevision
    steps: dict[str, PlatformStepComponent] = Field(
        default_factory=dict,
        max_length=MAX_COMPONENTS,
    )
    triggers: dict[str, PlatformTriggerComponent] = Field(
        default_factory=dict,
        max_length=MAX_COMPONENTS,
    )

    @classmethod
    def create(
        cls,
        *,
        steps: dict[str, PlatformStepComponent] | None = None,
        triggers: dict[str, PlatformTriggerComponent] | None = None,
    ) -> Self:
        component_steps = steps or {}
        component_triggers = triggers or {}
        revision_id = _component_catalog_revision(component_steps, component_triggers)
        return cls(
            revision_id=revision_id,
            steps=component_steps,
            triggers=component_triggers,
        )

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        for key, component in self.steps.items():
            if key != component.reference.identity:
                raise ValueError("Component catalog key does not match its reference")
        for key, trigger_component in self.triggers.items():
            if key != trigger_component.reference.identity:
                raise ValueError("Component catalog key does not match its reference")
        value = self.model_dump(mode="json", by_alias=True, exclude_none=True)
        _validate_structure(value)
        _reject_secret_values(value)
        if len(_canonical_json(value)) > MAX_CONFIGURATION_BYTES:
            raise ValueError("Component catalog exceeds its byte limit")
        expected = _component_catalog_revision(self.steps, self.triggers)
        if self.revision_id != expected:
            raise ValueError("Component catalog revision identity does not match its content")
        return self

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.model_dump(mode="json", by_alias=True, exclude_none=True))

    @classmethod
    def from_bytes(cls, payload: bytes) -> PlatformComponentCatalog:
        value = _parse_configuration_json(payload)
        catalog = cls.model_validate(value)
        if payload != catalog.canonical_bytes():
            raise ValueError("Platform component catalog is not canonical JSON")
        return catalog


class TenantComponentOperation(StrictConfigurationModel):
    kind: Literal["component"] = "component"
    component: ComponentReference
    service_binding: ServiceName
    resource_bindings: dict[Identifier, ResourceName] = Field(
        default_factory=dict,
        max_length=MAX_COMPONENT_BINDINGS,
    )
    parameters: dict[str, object] = Field(default_factory=dict)
    cache: CacheConfig | None = None


class TenantWorkflowOperation(StrictConfigurationModel):
    kind: Literal["workflow"] = "workflow"
    workflow: WorkflowName


TenantOperation: TypeAlias = Annotated[
    TenantComponentOperation | TenantWorkflowOperation,
    Field(discriminator="kind"),
]


class TenantWorkflowConfig(StrictConfigurationModel):
    workflow: WorkflowName
    description: str = Field(default="", max_length=MAX_DESCRIPTION_LENGTH)
    on_complete: OnCompleteConfig | None = None
    on_error: OnErrorConfig | None = None
    params: dict[Identifier, str] = Field(default_factory=dict)
    input_schema: dict[str, object] | None = None
    output_schema: dict[str, object] | None = None
    result: ReferencePath | None = None
    steps: dict[OperationName, TenantOperation]
    flow: list[FlowStep] = Field(min_length=1)


class TenantTriggerDeclaration(StrictConfigurationModel):
    component: ComponentReference
    kind: TriggerKind
    workflow: WorkflowName
    binding_alias: Identifier
    paused: StrictBool = False
    resource_bindings: dict[Identifier, ResourceName] = Field(
        default_factory=dict,
        max_length=MAX_COMPONENT_BINDINGS,
    )
    parameters: dict[str, object] = Field(default_factory=dict)


class TenantConfiguration(StrictConfigurationModel):
    component_catalog_revision: ComponentCatalogRevision
    workflows: dict[WorkflowName, TenantWorkflowConfig]
    triggers: dict[Identifier, TenantTriggerDeclaration]

    @model_validator(mode="after")
    def validate_bounds_and_references(self) -> Self:
        value = self.model_dump(mode="json", by_alias=True)
        _validate_structure(value)
        _reject_secret_values(value)
        if len(_canonical_json(value)) > MAX_CONFIGURATION_BYTES:
            raise ValueError("Tenant configuration exceeds its byte limit")
        for name, workflow in self.workflows.items():
            if workflow.workflow != name:
                raise ValueError("Tenant workflow mapping key does not match its declaration")
        for trigger in self.triggers.values():
            if trigger.workflow not in self.workflows:
                raise ValueError("Tenant trigger references an unknown workflow")
        return self

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.model_dump(mode="json", by_alias=True))

    @classmethod
    def from_bytes(cls, payload: bytes) -> TenantConfiguration:
        value = _parse_configuration_json(payload)
        configuration = cls.model_validate(value)
        if payload != configuration.canonical_bytes():
            raise ValueError("Tenant configuration is not canonical JSON")
        return configuration


ConfigurationDocument: TypeAlias = ConfigurationBundle | TenantConfiguration


class ConfigurationEnvelope(StrictConfigurationModel):
    format_version: Literal[1] = CONFIGURATION_ENVELOPE_VERSION
    scope_digest: str = Field(
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$",
    )
    bundle: ConfigurationDocument

    def canonical_bytes(self) -> bytes:
        value = self.model_dump(mode="json", by_alias=True)
        value["bundle"] = json.loads(self.bundle.canonical_bytes())
        payload = _canonical_json(value)
        if len(payload) > MAX_CONFIGURATION_ENVELOPE_BYTES:
            raise ValueError("Configuration envelope exceeds its byte limit")
        return payload


class ConfigurationSnapshot(StrictConfigurationModel):
    scope_digest: str = Field(
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$",
    )
    revision_id: RevisionIdentity
    bundle: ConfigurationBundle = Field(repr=False)

    @property
    def execution_identity(self) -> ExecutionConfigurationIdentity:
        resolution = self.bundle.tenant_resolution
        if resolution is None:
            return ExecutionConfigurationIdentity(
                configuration_revision_id=str(self.revision_id),
                resolution_digest=provenance_digest(
                    {
                        "configuration_revision_id": str(self.revision_id),
                        "scope_digest": self.scope_digest,
                    }
                ),
            )
        return ExecutionConfigurationIdentity(
            configuration_revision_id=str(self.revision_id),
            tenant_configuration_revision_id=str(resolution.tenant_configuration_revision_id),
            component_catalog_revision=resolution.component_catalog_revision,
            component_identity_digest=provenance_digest(list(resolution.component_identities)),
            resolution_digest=resolution.resolution_digest,
        )


class DraftRecord(StrictConfigurationModel):
    scope_digest: str
    version: int = Field(ge=1)
    bundle: ConfigurationDocument = Field(repr=False)


class RevisionRecord(StrictConfigurationModel):
    scope_digest: str
    revision_id: RevisionIdentity
    parent_revision_id: RevisionIdentity | None = None
    created_at: datetime
    bundle: ConfigurationDocument = Field(repr=False)


class RevisionSummary(StrictConfigurationModel):
    scope_digest: str
    revision_id: RevisionIdentity
    parent_revision_id: RevisionIdentity | None = None
    created_at: datetime


class RevisionPage(StrictConfigurationModel):
    revisions: tuple[RevisionSummary, ...]
    next_cursor: str | None = Field(default=None, repr=False)


class ActivePointer(StrictConfigurationModel):
    scope_digest: str
    revision_id: RevisionIdentity
    version: int = Field(ge=1)


class RetentionResult(StrictConfigurationModel):
    deleted_revision_ids: tuple[RevisionIdentity, ...]


def configuration_revision_identity(
    scope_digest: str,
    bundle: ConfigurationDocument,
    parent_revision_id: RevisionIdentity | None,
) -> RevisionIdentity:
    payload = _canonical_json(
        {
            "bundle": json.loads(bundle.canonical_bytes()),
            "parent_revision_id": (
                str(parent_revision_id) if parent_revision_id is not None else None
            ),
            "scope_digest": scope_digest,
        }
    )
    return RevisionIdentity(hashlib.sha256(payload).hexdigest())


def configuration_document_from_bytes(payload: bytes) -> ConfigurationDocument:
    value = _parse_configuration_json(payload)
    document: ConfigurationDocument
    if isinstance(value, dict) and "component_catalog_revision" in value:
        document = TenantConfiguration.model_validate(value)
    else:
        document = ConfigurationBundle.model_validate(value)
    if payload != document.canonical_bytes():
        raise ValueError("Configuration document is not canonical JSON")
    return document


def _component_catalog_revision(
    steps: dict[str, PlatformStepComponent],
    triggers: dict[str, PlatformTriggerComponent],
) -> str:
    return hashlib.sha256(
        _canonical_json(
            {
                "steps": {
                    key: value.model_dump(mode="json", exclude_none=True)
                    for key, value in steps.items()
                },
                "triggers": {
                    key: value.model_dump(mode="json", exclude_none=True)
                    for key, value in triggers.items()
                },
            }
        )
    ).hexdigest()


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _configuration_bundle_value(bundle: ConfigurationBundle) -> dict[str, object]:
    value = bundle.model_dump(mode="json", by_alias=True)
    if value["tenant_resolution"] is None:
        del value["tenant_resolution"]
    return value


def _parse_configuration_json(payload: bytes) -> object:
    if not payload or len(payload) > MAX_CONFIGURATION_BYTES:
        raise ValueError("Configuration document exceeds its byte limit or is empty")
    return json.loads(
        payload,
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_non_finite_constant,
    )


def _validate_structure(value: object, *, depth: int = 0) -> int:
    if depth > MAX_CONFIGURATION_DEPTH:
        raise ValueError("Configuration bundle exceeds its nesting limit")
    if isinstance(value, dict):
        count = len(value)
        for item in value.values():
            count += _validate_structure(item, depth=depth + 1)
    elif isinstance(value, list):
        count = len(value)
        for item in value:
            count += _validate_structure(item, depth=depth + 1)
    else:
        count = 0
    if count > MAX_CONFIGURATION_COLLECTION_ITEMS:
        raise ValueError("Configuration bundle exceeds its collection limit")
    return count


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Configuration bundle contains a duplicate object key")
        value[key] = item
    return value


def _reject_non_finite_constant(value: str) -> None:
    raise ValueError(f"Non-finite JSON number '{value}' is not allowed")


def _reject_secret_values(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            lowered = key.lower()
            if not lowered.endswith(SECRET_ALIAS_SUFFIXES) and any(
                fragment in lowered for fragment in SECRET_FIELD_FRAGMENTS
            ):
                raise ValueError("Configuration bundles may contain secret aliases only")
            _reject_secret_values(item)
    elif isinstance(value, list):
        for item in value:
            _reject_secret_values(item)
