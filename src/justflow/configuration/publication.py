"""Scoped draft validation, comparison, and immutable publication."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Protocol

from pydantic import Field

from justflow.config.diagnostics import MAX_DIAGNOSTIC_MESSAGE_LENGTH
from justflow.config.models import ResourcesConfig, ServicesConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.config.validator import ConfigValidationError
from justflow.configuration.activation import (
    PublicationErrorCode,
    PublicationRecord,
    PublicationState,
    control_identity_digest,
    publication_identity,
    publication_request_digest,
)
from justflow.configuration.activation_errors import (
    ActivationConflictError,
    ActivationNotFoundError,
    ActivationUnavailableError,
)
from justflow.configuration.activation_store import (
    ActivationStore,
    complete_publication,
    fail_publication,
)
from justflow.configuration.errors import (
    ConfigurationConflictError,
    ConfigurationError,
    ConfigurationNotFoundError,
    ConfigurationUnavailableError,
)
from justflow.configuration.lifecycle import (
    ConfigurationDiscardErrorCode,
    ConfigurationDiscardRecord,
    ConfigurationDiscardResult,
    ConfigurationDiscardState,
    ConfigurationRelationships,
    build_configuration_relationships,
    complete_discard,
    configuration_declarations,
    discard_identity,
    fail_discard,
)
from justflow.configuration.models import (
    MAX_CONFIGURATION_DEPTH,
    MAX_REVISION_PAGE_SIZE,
    ConfigurationBundle,
    ConfigurationDocument,
    DraftRecord,
    RevisionIdentity,
    RevisionPage,
    RevisionRecord,
    StrictConfigurationModel,
    TenantConfiguration,
    configuration_revision_identity,
)
from justflow.configuration.policy import (
    TenantAuthoringPolicy,
    TenantValidatedConfiguration,
    validate_tenant_authoring,
)
from justflow.configuration.ports import (
    ConfigurationStore,
    PlatformComponentCatalogSource,
)
from justflow.configuration.yaml import (
    parse_tenant_configuration_yaml,
    render_tenant_configuration_yaml,
)
from justflow.provenance import provenance_digest
from justflow.resources.registry import ResourceRegistry
from justflow.scope import RuntimeScope
from justflow.transports.registry import TransportRegistry

MAX_VALIDATION_ISSUES = 100
MAX_CONFIGURATION_DIFF_ITEMS = 1_000
MAX_CONFIGURATION_DIFF_PATH_SEGMENT_LENGTH = 128
VALIDATION_RECORD_TIME = datetime(1970, 1, 1, tzinfo=UTC)


class TenantAuthoringPolicySource(Protocol):
    def read(self, scope: RuntimeScope) -> TenantAuthoringPolicy: ...


class StaticTenantAuthoringPolicySource:
    def __init__(self, policies: Mapping[str, TenantAuthoringPolicy]) -> None:
        self._policies = dict(policies)

    def read(self, scope: RuntimeScope) -> TenantAuthoringPolicy:
        policy = self._policies.get(scope.digest)
        if policy is None or policy.scope_digest != scope.digest:
            raise ConfigurationNotFoundError(
                "Tenant authoring policy was not found for the runtime scope"
            )
        return policy


class ConfigurationValidationCategory(str, Enum):
    DECLARATION = "declaration"
    SEMANTIC = "semantic"
    POLICY = "policy"
    UNAVAILABLE = "unavailable"


class ConfigurationValidationSeverity(str, Enum):
    ERROR = "error"
    WARNING = "warning"


class ConfigurationValidationIssue(StrictConfigurationModel):
    severity: ConfigurationValidationSeverity
    category: ConfigurationValidationCategory
    location: tuple[str, ...] = Field(max_length=MAX_CONFIGURATION_DEPTH)
    message: str = Field(min_length=1, max_length=MAX_DIAGNOSTIC_MESSAGE_LENGTH)


class ConfigurationValidationReport(StrictConfigurationModel):
    valid: bool
    issues: tuple[ConfigurationValidationIssue, ...] = Field(max_length=MAX_VALIDATION_ISSUES)


class ConfigurationDiffOperation(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class ConfigurationDiffItem(StrictConfigurationModel):
    operation: ConfigurationDiffOperation
    path: tuple[str, ...] = Field(max_length=MAX_CONFIGURATION_DEPTH)


class ConfigurationDiff(StrictConfigurationModel):
    source_revision_id: RevisionIdentity
    target_revision_id: RevisionIdentity
    items: tuple[ConfigurationDiffItem, ...] = Field(max_length=MAX_CONFIGURATION_DIFF_ITEMS)


class PublicationOperationError(ConfigurationError):
    def __init__(
        self,
        code: PublicationErrorCode,
        message: str,
        *,
        retryable: bool,
    ) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(message)


@dataclass(frozen=True, kw_only=True)
class _ValidatedDraft:
    report: ConfigurationValidationReport
    resolved: TenantValidatedConfiguration | None
    source_revision: RevisionRecord
    policy_digest: str


class ConfigurationPublicationService:
    """Expose writable configuration primitives through one validated scope."""

    def __init__(
        self,
        *,
        configuration_store: ConfigurationStore,
        activation_store: ActivationStore,
        policy_source: TenantAuthoringPolicySource,
        component_catalog_source: PlatformComponentCatalogSource,
        platform_resources: ResourcesConfig,
        platform_services: ServicesConfig,
        transport_registry: TransportRegistry,
        resource_registry: ResourceRegistry,
        limits: RuntimeLimits,
        config_dir: str | Path,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._configuration_store = configuration_store
        self._activation_store = activation_store
        self._policy_source = policy_source
        self._component_catalog_source = component_catalog_source
        self._platform_resources = platform_resources
        self._platform_services = platform_services
        self._transport_registry = transport_registry
        self._resource_registry = resource_registry
        self._limits = limits
        self._config_dir = Path(config_dir)
        self._clock = clock

    def create_draft(
        self,
        scope: RuntimeScope,
        configuration: TenantConfiguration,
    ) -> DraftRecord:
        return self._configuration_store.compare_and_swap_draft(
            scope,
            configuration,
            expected_version=None,
        )

    def read_draft(self, scope: RuntimeScope) -> DraftRecord:
        draft = self._configuration_store.read_draft(scope)
        if draft is None:
            raise ConfigurationNotFoundError("Configuration draft was not found")
        if not isinstance(draft.bundle, TenantConfiguration):
            raise ConfigurationError("Managed configuration draft has an invalid document type")
        return draft

    def update_draft(
        self,
        scope: RuntimeScope,
        configuration: TenantConfiguration,
        *,
        expected_version: int,
    ) -> DraftRecord:
        return self._configuration_store.compare_and_swap_draft(
            scope,
            configuration,
            expected_version=expected_version,
        )

    def import_draft_yaml(
        self,
        scope: RuntimeScope,
        payload: bytes,
        *,
        expected_version: int | None,
    ) -> DraftRecord:
        configuration = parse_tenant_configuration_yaml(payload)
        return self._configuration_store.compare_and_swap_draft(
            scope,
            configuration,
            expected_version=expected_version,
        )

    def export_draft_yaml(
        self, scope: RuntimeScope, *, expected_version: int | None = None
    ) -> bytes:
        draft = self.read_draft(scope)
        if expected_version is not None and draft.version != expected_version:
            raise ConfigurationConflictError(scope.digest)
        configuration = draft.bundle
        if not isinstance(configuration, TenantConfiguration):
            raise ConfigurationError("Managed configuration draft has an invalid document type")
        return render_tenant_configuration_yaml(configuration)

    def validate_draft(self, scope: RuntimeScope) -> ConfigurationValidationReport:
        return self._validate(scope, self.read_draft(scope)).report

    def relationships(self, scope: RuntimeScope) -> ConfigurationRelationships:
        draft = self.read_draft(scope)
        configuration = draft.bundle
        if not isinstance(configuration, TenantConfiguration):
            raise ConfigurationError("Managed configuration draft has an invalid document type")
        active_identity, active_configuration = self._active_authoring_configuration(scope)
        active_declarations = (
            {}
            if active_configuration is None
            else configuration_declarations(
                workflows=active_configuration.workflows,
                triggers=active_configuration.triggers,
            )
        )
        return build_configuration_relationships(
            working_version=draft.version,
            active_identity=(str(active_identity) if active_identity is not None else None),
            working=configuration_declarations(
                workflows=configuration.workflows,
                triggers=configuration.triggers,
            ),
            active=active_declarations,
        )

    def discard(
        self,
        scope: RuntimeScope,
        *,
        expected_draft_version: int,
        expected_active_identity: RevisionIdentity,
        idempotency_key: str,
        actor_identity: str,
        correlation_identity: str,
    ) -> ConfigurationDiscardResult:
        active_identity, active_configuration = self._active_authoring_configuration(scope)
        if active_identity is None or active_configuration is None:
            raise ConfigurationNotFoundError("Active managed authoring configuration was not found")
        if active_identity != expected_active_identity:
            raise ConfigurationConflictError(scope.digest)
        active_document_digest = provenance_digest(
            {"configuration": active_configuration.model_dump(mode="json")}
        )
        request_digest = provenance_digest(
            {
                "active_document_digest": active_document_digest,
                "active_identity": str(active_identity),
                "draft_version": expected_draft_version,
                "scope_digest": scope.digest,
            }
        )
        now = self._now()
        pending = ConfigurationDiscardRecord(
            discard_id=discard_identity(scope.digest, idempotency_key),
            scope_digest=scope.digest,
            idempotency_key_digest=control_identity_digest(
                "discard-key",
                idempotency_key,
            ),
            request_digest=request_digest,
            actor_digest=control_identity_digest("discard-actor", actor_identity),
            correlation_digest=control_identity_digest(
                "discard-correlation",
                correlation_identity,
            ),
            expected_draft_version=expected_draft_version,
            expected_active_identity=active_identity,
            active_document_digest=active_document_digest,
            created_at=now,
            updated_at=now,
        )
        try:
            record = self._activation_store.create_discard(scope, pending)
        except ActivationConflictError as exc:
            raise ConfigurationConflictError(scope.digest) from exc
        if record.request_digest != request_digest:
            raise ConfigurationConflictError(scope.digest)
        if record.state is ConfigurationDiscardState.APPLIED:
            return _discard_result(record)
        if record.state is ConfigurationDiscardState.FAILED:
            raise ConfigurationConflictError(scope.digest)
        try:
            current = self.read_draft(scope)
            if current.version == expected_draft_version:
                current = self._configuration_store.compare_and_swap_draft(
                    scope,
                    active_configuration,
                    expected_version=expected_draft_version,
                )
            elif not (
                current.version == expected_draft_version + 1
                and current.bundle == active_configuration
            ):
                raise ConfigurationConflictError(scope.digest)
        except ConfigurationConflictError:
            self._record_discard_failure(scope, record)
            raise
        completed = complete_discard(
            record,
            result_draft_version=current.version,
            occurred_at=self._now(),
        )
        try:
            stored = self._activation_store.update_discard(
                scope,
                completed,
                expected_version=record.version,
            )
        except ActivationConflictError as exc:
            stored = self._activation_store.read_discard(scope, record.discard_id)
            if (
                stored.request_digest != record.request_digest
                or stored.state is not ConfigurationDiscardState.APPLIED
            ):
                raise ConfigurationConflictError(scope.digest) from exc
        return _discard_result(stored)

    def read_discard(
        self,
        scope: RuntimeScope,
        discard_id: str,
    ) -> ConfigurationDiscardRecord:
        try:
            return self._activation_store.read_discard(scope, discard_id)
        except ActivationNotFoundError as exc:
            raise ConfigurationNotFoundError("Configuration discard was not found") from exc

    def compare_revisions(
        self,
        scope: RuntimeScope,
        source_revision_id: RevisionIdentity,
        target_revision_id: RevisionIdentity,
    ) -> ConfigurationDiff:
        source = self._configuration_store.read_revision(scope, source_revision_id)
        target = self._configuration_store.read_revision(scope, target_revision_id)
        items = _configuration_diff(source.bundle, target.bundle)
        return ConfigurationDiff(
            source_revision_id=source_revision_id,
            target_revision_id=target_revision_id,
            items=items,
        )

    def list_history(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> RevisionPage:
        if not 1 <= limit <= MAX_REVISION_PAGE_SIZE:
            raise ValueError("Configuration history page size is invalid")
        return self._configuration_store.list_revisions(scope, limit=limit, cursor=cursor)

    def read_revision(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
    ) -> RevisionRecord:
        return self._configuration_store.read_revision(scope, revision_id)

    def read_publication(
        self,
        scope: RuntimeScope,
        publication_id: str,
    ) -> PublicationRecord:
        return self._activation_store.read_publication(scope, publication_id)

    def publish(
        self,
        scope: RuntimeScope,
        *,
        expected_draft_version: int,
        idempotency_key: str,
        actor_identity: str,
        correlation_identity: str,
    ) -> PublicationRecord:
        publication_id = publication_identity(scope.digest, idempotency_key)
        actor_digest = control_identity_digest("publication-actor", actor_identity)
        try:
            existing = self._activation_store.read_publication(scope, publication_id)
        except ActivationNotFoundError:
            existing = None
        if existing is not None:
            if existing.actor_digest != actor_digest:
                raise PublicationOperationError(
                    PublicationErrorCode.CONFLICT,
                    "Publication belongs to another submitted request",
                    retryable=False,
                )
            if existing.state is PublicationState.APPLIED:
                if existing.source_revision_id is None:
                    raise ConfigurationError("Applied publication has no source revision")
                source = self._configuration_store.read_revision(scope, existing.source_revision_id)
                submitted_digest = _publication_digest(
                    scope,
                    expected_draft_version,
                    source.bundle,
                    policy_digest=existing.policy_digest,
                    active_revision_id=existing.expected_active_revision_id,
                    source_parent_revision_id=existing.source_parent_revision_id,
                )
                if submitted_digest != existing.request_digest:
                    raise PublicationOperationError(
                        PublicationErrorCode.CONFLICT,
                        "Publication belongs to another submitted request",
                        retryable=False,
                    )
                return existing
        draft = self.read_draft(scope)
        if draft.version != expected_draft_version:
            raise ConfigurationConflictError(scope.digest)
        active = self._configuration_store.read_active(scope)
        active_revision = (
            self._configuration_store.read_revision(scope, active.revision_id)
            if active is not None
            else None
        )
        source_parent_revision_id = _source_revision_id(active_revision)
        policy = self._policy_source.read(scope)
        policy_digest = provenance_digest(policy.model_dump(mode="json"))
        request_digest = _publication_digest(
            scope,
            draft.version,
            draft.bundle,
            policy_digest=policy_digest,
            active_revision_id=active.revision_id if active is not None else None,
            source_parent_revision_id=source_parent_revision_id,
        )
        now = self._now()
        record = PublicationRecord(
            publication_id=publication_identity(scope.digest, idempotency_key),
            scope_digest=scope.digest,
            idempotency_key_digest=control_identity_digest(
                "publication-key",
                idempotency_key,
            ),
            request_digest=request_digest,
            actor_digest=control_identity_digest("publication-actor", actor_identity),
            correlation_digest=control_identity_digest(
                "publication-correlation",
                correlation_identity,
            ),
            policy_digest=policy_digest,
            source_parent_revision_id=source_parent_revision_id,
            expected_active_revision_id=(active.revision_id if active is not None else None),
            created_at=now,
            updated_at=now,
        )
        try:
            pending = self._activation_store.create_publication(scope, record)
        except ActivationConflictError as exc:
            raise PublicationOperationError(
                PublicationErrorCode.CONFLICT,
                "Publication idempotency key was already used for another request",
                retryable=False,
            ) from exc
        if pending.state is PublicationState.APPLIED:
            return pending
        if pending.request_digest != request_digest:
            raise PublicationOperationError(
                PublicationErrorCode.CONFLICT,
                "Publication request no longer matches its reserved identity",
                retryable=False,
            )
        if pending.state is PublicationState.FAILED:
            pending = PublicationRecord.model_validate(
                {
                    **pending.model_dump(),
                    "source_revision_id": None,
                    "published_revision_id": None,
                    "state": PublicationState.PENDING,
                    "error_code": None,
                    "version": pending.version + 1,
                    "updated_at": now,
                }
            )
            pending = self._activation_store.update_publication(
                scope,
                pending,
                expected_version=pending.version - 1,
            )
        try:
            validated = self._validate(
                scope,
                draft,
                policy=policy,
                parent_revision_id=pending.source_parent_revision_id,
            )
            if not validated.report.valid or validated.resolved is None:
                dependency_unavailable = any(
                    issue.category is ConfigurationValidationCategory.UNAVAILABLE
                    for issue in validated.report.issues
                )
                raise PublicationOperationError(
                    (
                        PublicationErrorCode.CATALOG_UNAVAILABLE
                        if dependency_unavailable
                        else PublicationErrorCode.INVALID_CONFIGURATION
                    ),
                    (
                        "Configuration publication dependency is unavailable"
                        if dependency_unavailable
                        else "Configuration publication validation failed"
                    ),
                    retryable=dependency_unavailable,
                )
            source_revision = self._configuration_store.create_revision(
                scope,
                draft.bundle,
                parent_revision_id=pending.source_parent_revision_id,
            )
            if source_revision.revision_id != validated.source_revision.revision_id:
                raise ConfigurationError("Published source revision identity changed")
            published_revision = self._configuration_store.create_revision(
                scope,
                validated.resolved.bundle,
                parent_revision_id=pending.expected_active_revision_id,
            )
            completed = complete_publication(
                pending,
                source_revision_id=source_revision.revision_id,
                published_revision_id=published_revision.revision_id,
                occurred_at=self._now(),
            )
            return self._update_publication(scope, pending, completed)
        except PublicationOperationError as exc:
            self._record_publication_failure(scope, pending, exc.code)
            raise
        except ConfigurationUnavailableError as exc:
            self._record_publication_failure(
                scope,
                pending,
                PublicationErrorCode.STORE_UNAVAILABLE,
            )
            raise PublicationOperationError(
                PublicationErrorCode.STORE_UNAVAILABLE,
                "Configuration publication storage is unavailable",
                retryable=True,
            ) from exc
        except ConfigurationError as exc:
            self._record_publication_failure(
                scope,
                pending,
                PublicationErrorCode.INVALID_CONFIGURATION,
            )
            raise PublicationOperationError(
                PublicationErrorCode.INVALID_CONFIGURATION,
                "Configuration publication failed validation",
                retryable=False,
            ) from exc

    def _validate(
        self,
        scope: RuntimeScope,
        draft: DraftRecord,
        *,
        policy: TenantAuthoringPolicy | None = None,
        parent_revision_id: RevisionIdentity | None = None,
    ) -> _ValidatedDraft:
        if not isinstance(draft.bundle, TenantConfiguration):
            issue = ConfigurationValidationIssue(
                severity=ConfigurationValidationSeverity.ERROR,
                category=ConfigurationValidationCategory.DECLARATION,
                location=(),
                message="Managed authoring requires a tenant configuration document",
            )
            return _ValidatedDraft(
                report=ConfigurationValidationReport(valid=False, issues=(issue,)),
                resolved=None,
                source_revision=_candidate_revision(
                    scope,
                    draft.bundle,
                    parent_revision_id,
                ),
                policy_digest=provenance_digest({"policy": "unavailable"}),
            )
        authoring_policy = policy or self._policy_source.read(scope)
        policy_digest = provenance_digest(authoring_policy.model_dump(mode="json"))
        candidate = _candidate_revision(scope, draft.bundle, parent_revision_id)
        try:
            resolved = validate_tenant_authoring(
                candidate,
                scope=scope,
                policy=authoring_policy,
                component_catalog_source=self._component_catalog_source,
                platform_resources=self._platform_resources,
                platform_services=self._platform_services,
                transport_registry=self._transport_registry,
                resource_registry=self._resource_registry,
                limits=self._limits,
                config_dir=self._config_dir,
            )
        except (ConfigurationNotFoundError, ConfigurationUnavailableError):
            issue = ConfigurationValidationIssue(
                severity=ConfigurationValidationSeverity.ERROR,
                category=ConfigurationValidationCategory.UNAVAILABLE,
                location=(),
                message="Configuration validation dependency is unavailable",
            )
            return _ValidatedDraft(
                report=ConfigurationValidationReport(valid=False, issues=(issue,)),
                resolved=None,
                source_revision=candidate,
                policy_digest=policy_digest,
            )
        except ConfigurationError as exc:
            issues = _validation_issues(exc)
            return _ValidatedDraft(
                report=ConfigurationValidationReport(valid=False, issues=issues),
                resolved=None,
                source_revision=candidate,
                policy_digest=policy_digest,
            )
        return _ValidatedDraft(
            report=ConfigurationValidationReport(
                valid=True,
                issues=tuple(
                    ConfigurationValidationIssue(
                        severity=ConfigurationValidationSeverity.WARNING,
                        category=ConfigurationValidationCategory.SEMANTIC,
                        location=tuple(str(component) for component in diagnostic.location),
                        message=diagnostic.message,
                    )
                    for diagnostic in resolved.diagnostics[:MAX_VALIDATION_ISSUES]
                ),
            ),
            resolved=resolved,
            source_revision=candidate,
            policy_digest=policy_digest,
        )

    def _record_publication_failure(
        self,
        scope: RuntimeScope,
        pending: PublicationRecord,
        code: PublicationErrorCode,
    ) -> None:
        failed = fail_publication(pending, error_code=code, occurred_at=self._now())
        try:
            self._activation_store.update_publication(
                scope,
                failed,
                expected_version=pending.version,
            )
        except ActivationConflictError:
            return

    def _record_discard_failure(
        self,
        scope: RuntimeScope,
        record: ConfigurationDiscardRecord,
    ) -> None:
        failed = fail_discard(
            record,
            error_code=ConfigurationDiscardErrorCode.CONFLICT,
            occurred_at=self._now(),
        )
        try:
            self._activation_store.update_discard(
                scope,
                failed,
                expected_version=record.version,
            )
        except ActivationConflictError:
            return

    def _active_authoring_configuration(
        self,
        scope: RuntimeScope,
    ) -> tuple[RevisionIdentity | None, TenantConfiguration | None]:
        active = self._configuration_store.read_active(scope)
        if active is None:
            return None, None
        resolved = self._configuration_store.read_revision(scope, active.revision_id)
        source_revision_id = _source_revision_id(resolved)
        if source_revision_id is None:
            raise ConfigurationError("Active configuration has no tenant authoring source revision")
        source = self._configuration_store.read_revision(scope, source_revision_id)
        if not isinstance(source.bundle, TenantConfiguration):
            raise ConfigurationError("Active tenant authoring source has an invalid document type")
        return active.revision_id, source.bundle

    def _update_publication(
        self,
        scope: RuntimeScope,
        pending: PublicationRecord,
        updated: PublicationRecord,
    ) -> PublicationRecord:
        try:
            return self._activation_store.update_publication(
                scope,
                updated,
                expected_version=pending.version,
            )
        except ActivationConflictError:
            current = self._activation_store.read_publication(scope, pending.publication_id)
            if (
                current.request_digest == pending.request_digest
                and current.state is PublicationState.APPLIED
            ):
                return current
            raise

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ActivationUnavailableError("Publication clock must be timezone-aware")
        return value.astimezone(UTC)


def _candidate_revision(
    scope: RuntimeScope,
    document: ConfigurationDocument,
    parent_revision_id: RevisionIdentity | None,
) -> RevisionRecord:
    return RevisionRecord(
        scope_digest=scope.digest,
        revision_id=configuration_revision_identity(
            scope.digest,
            document,
            parent_revision_id,
        ),
        parent_revision_id=parent_revision_id,
        created_at=VALIDATION_RECORD_TIME,
        bundle=document,
    )


def _source_revision_id(revision: RevisionRecord | None) -> RevisionIdentity | None:
    if revision is None or not isinstance(revision.bundle, ConfigurationBundle):
        return None
    resolution = revision.bundle.tenant_resolution
    return resolution.tenant_configuration_revision_id if resolution is not None else None


def _discard_result(record: ConfigurationDiscardRecord) -> ConfigurationDiscardResult:
    if record.state is not ConfigurationDiscardState.APPLIED or record.result_draft_version is None:
        raise ConfigurationError("Configuration discard has no applied outcome")
    return ConfigurationDiscardResult(
        discard_id=record.discard_id,
        working_version=record.result_draft_version,
        active_identity=record.expected_active_identity,
        definitions_published=record.definitions_published,
    )


def _validation_issues(error: ConfigurationError) -> tuple[ConfigurationValidationIssue, ...]:
    cause: BaseException | None = error
    while cause is not None and not isinstance(cause, ConfigValidationError):
        cause = cause.__cause__
    if not isinstance(cause, ConfigValidationError):
        return (
            ConfigurationValidationIssue(
                severity=ConfigurationValidationSeverity.ERROR,
                category=ConfigurationValidationCategory.POLICY,
                location=(),
                message="Tenant configuration violates host authoring policy",
            ),
        )
    issues = tuple(
        ConfigurationValidationIssue(
            severity=ConfigurationValidationSeverity.ERROR,
            category=ConfigurationValidationCategory.SEMANTIC,
            location=tuple(str(component) for component in diagnostic.location),
            message=diagnostic.message,
        )
        for diagnostic in cause.errors[:MAX_VALIDATION_ISSUES]
    )
    return issues or (
        ConfigurationValidationIssue(
            severity=ConfigurationValidationSeverity.ERROR,
            category=ConfigurationValidationCategory.SEMANTIC,
            location=(),
            message="Tenant configuration is invalid",
        ),
    )


def _configuration_diff(
    source: ConfigurationDocument,
    target: ConfigurationDocument,
) -> tuple[ConfigurationDiffItem, ...]:
    source_value = source.model_dump(mode="json", by_alias=True, exclude_none=True)
    target_value = target.model_dump(mode="json", by_alias=True, exclude_none=True)
    items: list[ConfigurationDiffItem] = []

    def append(item: ConfigurationDiffItem) -> None:
        if len(items) >= MAX_CONFIGURATION_DIFF_ITEMS:
            raise ValueError("Configuration difference exceeds its item limit")
        items.append(item)

    def compare(left: object, right: object, path: tuple[str, ...]) -> None:
        if isinstance(left, dict) and isinstance(right, dict):
            for key in sorted(set(left) | set(right)):
                segment = _diff_path_segment(str(key))
                child_path = (*path, segment)
                if key not in right:
                    append(
                        ConfigurationDiffItem(
                            operation=ConfigurationDiffOperation.DELETE,
                            path=child_path,
                        )
                    )
                elif key not in left:
                    append(
                        ConfigurationDiffItem(
                            operation=ConfigurationDiffOperation.CREATE,
                            path=child_path,
                        )
                    )
                else:
                    compare(left[key], right[key], child_path)
            return
        if left != right:
            append(
                ConfigurationDiffItem(
                    operation=ConfigurationDiffOperation.UPDATE,
                    path=path,
                )
            )

    compare(source_value, target_value, ())
    return tuple(items)


def _diff_path_segment(value: str) -> str:
    if not value or len(value) > MAX_CONFIGURATION_DIFF_PATH_SEGMENT_LENGTH:
        return control_identity_digest("difference-path", value)
    return value


def _publication_digest(
    scope: RuntimeScope,
    draft_version: int,
    document: ConfigurationDocument,
    *,
    policy_digest: str,
    active_revision_id: RevisionIdentity | None,
    source_parent_revision_id: RevisionIdentity | None,
) -> str:
    return provenance_digest(
        {
            "active_revision_id": str(active_revision_id)
            if active_revision_id is not None
            else None,
            "policy_digest": policy_digest,
            "publication": publication_request_digest(
                scope_digest=scope.digest,
                draft_version=draft_version,
                document_bytes=document.canonical_bytes(),
            ),
            "source_parent_revision_id": str(source_parent_revision_id)
            if source_parent_revision_id is not None
            else None,
        }
    )
