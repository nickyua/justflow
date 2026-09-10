"""Shared policy boundary for starting immutable workflow definitions."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Annotated, Literal, Protocol, Self, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, model_validator
from temporalio.client import Client
from temporalio.common import Priority, WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from justflow.config.grammar import ProviderName, TriggerName, WorkflowName
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.config.settings import PinnedStartRetrySettings
from justflow.config.triggers import (
    ApiTriggerDeclaration,
    BrokerTriggerDeclaration,
    EventTriggerDeclaration,
    HostTriggerDeclaration,
    ScheduleTriggerDeclaration,
    TriggerDeclaration,
    TriggerKind,
    TriggersConfig,
    WebhookTriggerDeclaration,
)
from justflow.definitions.routing import (
    MEMO_COMPONENT_CATALOG_REVISION,
    MEMO_COMPONENT_IDENTITY_DIGEST,
    MEMO_CONFIGURATION_RESOLUTION_DIGEST,
    MEMO_CONFIGURATION_REVISION,
    MEMO_DEFINITION_DIGEST,
    MEMO_ENVIRONMENT_SNAPSHOT_DIGEST,
    MEMO_LOGICAL_WORKFLOW,
    MEMO_SCOPE_DIGEST,
    MEMO_TENANT_CONFIGURATION_REVISION,
    MEMO_WORKER_ARTIFACT_DIGEST,
    MEMO_WORKER_BUILD_ID,
    MEMO_WORKER_DEPLOYMENT,
    MEMO_WORKER_PACKAGE_VERSION,
    MEMO_WORKER_SOURCE_REVISION,
    WorkflowStartTarget,
    retry_pinned_workflow_start,
)
from justflow.engine.contracts import (
    ContractViolation,
    SchemaDeclarationError,
    SchemaLoadError,
    validate_payload,
)
from justflow.engine.data import DataNormalizationError, JSONValue, normalize_json_object
from justflow.engine.limits import (
    LimitExceededError,
    PayloadSerializationError,
    enforce_payload_bytes,
)
from justflow.provenance import ExecutionConfigurationIdentity, WorkerArtifactIdentity
from justflow.runtime.metrics import MetricsRegistry, StartMetricOutcome
from justflow.runtime.visibility import execution_search_attributes
from justflow.scope import (
    LEGACY_LOCAL_UNSCOPED_POLICY,
    LOCAL_RUNTIME_SCOPE,
    SCOPE_DIGEST_LENGTH,
    RuntimeScope,
    ScopeBindingKind,
    ScopeResolutionError,
    TrustedScopeBinding,
    require_bound_scope,
)
from justflow.sdk.logging_context import identity_log_digest, logging_context
from justflow.sdk.message_contract import (
    DEFINITION_DIGEST_LENGTH,
    MAX_IDENTIFIER_LENGTH,
    WorkflowTrigger,
    make_workflow_id,
)

logger = logging.getLogger(__name__)

MEMO_TRIGGER_SOURCE = "justflow.trigger_source"
MEMO_SOURCE_IDENTITY_DIGEST = "justflow.source_identity_digest"
MEMO_CORRELATION_IDENTITY_DIGEST = "justflow.correlation_identity_digest"
MEMO_SOURCE_PROVIDER = "justflow.source_provider"
MEMO_TRIGGER_NAME = "justflow.trigger_name"
IDENTITY_DIGEST_LENGTH = 64
AUTHORITATIVE_START_REJECTION_STATUSES = frozenset(
    {
        RPCStatusCode.INVALID_ARGUMENT,
        RPCStatusCode.NOT_FOUND,
        RPCStatusCode.PERMISSION_DENIED,
        RPCStatusCode.RESOURCE_EXHAUSTED,
        RPCStatusCode.FAILED_PRECONDITION,
        RPCStatusCode.ABORTED,
        RPCStatusCode.OUT_OF_RANGE,
        RPCStatusCode.UNIMPLEMENTED,
        RPCStatusCode.UNAUTHENTICATED,
    }
)


class StrictRuntimeModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class TriggerSource(str, Enum):
    BROKER = "broker"
    CONTROL_API = "control_api"
    WEBHOOK = "webhook"
    HOST = "host"
    SCHEDULE = "schedule"
    CLOUD_EVENT = "cloud_event"


class BrokerSourceIdentity(StrictRuntimeModel):
    source: Literal[TriggerSource.BROKER] = TriggerSource.BROKER
    broker: ProviderName
    message_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH, repr=False)


class ControlApiSourceIdentity(StrictRuntimeModel):
    source: Literal[TriggerSource.CONTROL_API] = TriggerSource.CONTROL_API
    request_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH, repr=False)


class WebhookSourceIdentity(StrictRuntimeModel):
    source: Literal[TriggerSource.WEBHOOK] = TriggerSource.WEBHOOK
    provider: ProviderName
    event_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH, repr=False)


class HostSourceIdentity(StrictRuntimeModel):
    source: Literal[TriggerSource.HOST] = TriggerSource.HOST
    adapter: ProviderName
    event_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH, repr=False)


class ScheduleSourceIdentity(StrictRuntimeModel):
    source: Literal[TriggerSource.SCHEDULE] = TriggerSource.SCHEDULE
    schedule: TriggerName
    occurrence_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH, repr=False)


class CloudEventSourceIdentity(StrictRuntimeModel):
    source: Literal[TriggerSource.CLOUD_EVENT] = TriggerSource.CLOUD_EVENT
    mapping: ProviderName
    event_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH, repr=False)


SourceIdentity: TypeAlias = Annotated[
    BrokerSourceIdentity
    | ControlApiSourceIdentity
    | WebhookSourceIdentity
    | HostSourceIdentity
    | ScheduleSourceIdentity
    | CloudEventSourceIdentity,
    Field(discriminator="source"),
]


class StartWorkflowRequest(StrictRuntimeModel):
    workflow_name: WorkflowName
    business_request_id: str = Field(
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
        repr=False,
    )
    input: dict[str, object] = Field(default_factory=dict, repr=False)
    source: SourceIdentity
    definition_digest: str | None = Field(
        default=None,
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{DEFINITION_DIGEST_LENGTH}}}$",
    )
    correlation_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
        repr=False,
    )
    trace_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
        repr=False,
    )

    @model_validator(mode="after")
    def default_correlation_identity(self) -> Self:
        if self.correlation_id is None:
            object.__setattr__(self, "correlation_id", self.business_request_id)
        return self


class StartStatus(str, Enum):
    STARTED = "started"
    DUPLICATE = "duplicate"


class StartWorkflowResult(StrictRuntimeModel):
    workflow_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    run_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    workflow_name: WorkflowName
    definition_digest: str = Field(
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{DEFINITION_DIGEST_LENGTH}}}$",
    )
    artifact_identity: WorkerArtifactIdentity
    environment_snapshot_digest: str = Field(
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{DEFINITION_DIGEST_LENGTH}}}$",
    )
    execution_configuration: ExecutionConfigurationIdentity | None = None
    trigger_name: TriggerName
    source_identity_digest: str = Field(
        min_length=IDENTITY_DIGEST_LENGTH,
        max_length=IDENTITY_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{IDENTITY_DIGEST_LENGTH}}}$",
    )
    scope_digest: str = Field(
        default=LOCAL_RUNTIME_SCOPE.digest,
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$",
    )
    status: StartStatus


class StartErrorCode(str, Enum):
    INVALID_REQUEST = "invalid_request"
    UNKNOWN_WORKFLOW = "unknown_workflow"
    DEFINITION_UNAVAILABLE = "definition_unavailable"
    INCOMPATIBLE_WORKER = "incompatible_worker"
    INPUT_REJECTED = "input_rejected"
    CONFIGURATION_ERROR = "configuration_error"
    TRIGGER_PAUSED = "trigger_paused"
    TRIGGER_UNAVAILABLE = "trigger_unavailable"
    TEMPORAL_UNAVAILABLE = "temporal_unavailable"


class StartRequestCertainty(str, Enum):
    AUTHORITATIVE_REJECTION = "authoritative_rejection"
    AMBIGUOUS = "ambiguous"


class WorkflowStartError(Exception):
    def __init__(
        self,
        code: StartErrorCode,
        message: str,
        *,
        retryable: bool,
        request_certainty: StartRequestCertainty = (StartRequestCertainty.AUTHORITATIVE_REJECTION),
    ) -> None:
        self.code = code
        self.retryable = retryable
        self.request_certainty = request_certainty
        super().__init__(message)


@dataclass(frozen=True, kw_only=True)
class WorkflowStartRegistration:
    target: WorkflowStartTarget
    triggers: Mapping[TriggerName, TriggerDeclaration]

    def __post_init__(self) -> None:
        copied = dict(self.triggers)
        if any(
            declaration.workflow != self.target.manifest.logical_name
            for declaration in copied.values()
        ):
            raise ValueError("Workflow start registration contains a trigger for another workflow")
        object.__setattr__(self, "triggers", MappingProxyType(copied))


@dataclass(frozen=True, kw_only=True)
class MatchedTrigger:
    name: TriggerName
    kind: TriggerKind


@dataclass(frozen=True, kw_only=True)
class PreparedWorkflowStart:
    target: WorkflowStartTarget
    trigger: MatchedTrigger
    normalized_input: dict[str, JSONValue]


class WorkflowTargetResolver(Protocol):
    async def resolve(
        self,
        scope: RuntimeScope,
        workflow_name: str,
    ) -> WorkflowStartRegistration | None: ...


class ScopedWorkflowTargetResolver:
    """Immutable workflow-target mappings partitioned by runtime scope."""

    def __init__(
        self,
        targets_by_scope: Mapping[RuntimeScope, Mapping[str, WorkflowStartTarget]],
        triggers_by_scope: Mapping[RuntimeScope, TriggersConfig],
    ) -> None:
        if set(targets_by_scope) != set(triggers_by_scope):
            raise ValueError("Workflow targets and trigger declarations cover different scopes")
        indexed: dict[str, tuple[RuntimeScope, dict[str, WorkflowStartRegistration]]] = {}
        for scope, targets in targets_by_scope.items():
            if scope.digest in indexed:
                raise ValueError("Workflow target resolver contains a duplicate runtime scope")
            copied = dict(targets)
            for name, target in copied.items():
                if name != target.manifest.logical_name:
                    raise ValueError("Workflow target key does not match its logical name")
                if not LEGACY_LOCAL_UNSCOPED_POLICY.owns(target.scope_digest, scope):
                    raise ValueError("Workflow target does not belong to its runtime scope")
            declarations = triggers_by_scope[scope]
            indexed[scope.digest] = (
                scope,
                {
                    name: WorkflowStartRegistration(
                        target=target,
                        triggers=_triggers_for_workflow(declarations, name),
                    )
                    for name, target in copied.items()
                },
            )
        self._targets_by_scope = indexed

    @classmethod
    def single_scope(
        cls,
        scope: RuntimeScope,
        targets: Mapping[str, WorkflowStartTarget],
        triggers: TriggersConfig,
    ) -> Self:
        return cls({scope: targets}, {scope: triggers})

    async def resolve(
        self,
        scope: RuntimeScope,
        workflow_name: str,
    ) -> WorkflowStartRegistration | None:
        entry = self._targets_by_scope.get(scope.digest)
        if entry is None or entry[0] != scope:
            return None
        return entry[1].get(workflow_name)


class WorkflowStarter:
    """Resolve, validate, and start workflows with one idempotency policy."""

    def __init__(
        self,
        temporal_client: Client,
        task_queue: str,
        targets_by_workflow_name: Mapping[str, WorkflowStartTarget] | None = None,
        *,
        triggers: TriggersConfig | None = None,
        target_resolver: WorkflowTargetResolver | None = None,
        scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
        limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
        metrics: MetricsRegistry | None = None,
        indexed_search_attributes_enabled: bool = False,
        pinned_start_retry: PinnedStartRetrySettings | None = None,
    ) -> None:
        if not task_queue:
            raise ValueError("Workflow starter task queue must not be empty")
        if (targets_by_workflow_name is None) == (target_resolver is None):
            raise ValueError("Provide workflow targets or one scoped target resolver")
        if target_resolver is None and triggers is None:
            raise ValueError("Direct workflow targets require declared triggers")
        if target_resolver is not None and triggers is not None:
            raise ValueError("A scoped target resolver owns its declared triggers")
        self._client = temporal_client
        self._task_queue = task_queue
        self._target_resolver = target_resolver or ScopedWorkflowTargetResolver.single_scope(
            scope,
            targets_by_workflow_name or {},
            triggers or TriggersConfig(triggers={}),
        )
        self._limits = limits
        self._metrics = metrics
        self._indexed_search_attributes_enabled = indexed_search_attributes_enabled
        self._pinned_start_retry = pinned_start_retry or PinnedStartRetrySettings()

    async def start(
        self,
        request: StartWorkflowRequest,
        *,
        scope_binding: TrustedScopeBinding,
    ) -> StartWorkflowResult:
        return await self._start_with_metrics(
            request,
            scope_binding=scope_binding,
            target=None,
        )

    async def find_existing(
        self,
        workflow_name: WorkflowName,
        business_request_id: str,
        *,
        scope: RuntimeScope,
    ) -> StartWorkflowResult | None:
        workflow_id = make_workflow_id(
            workflow_name,
            business_request_id,
            scope=scope,
        )
        return await self._lookup_existing_result(
            workflow_id,
            run_id=None,
            scope=scope,
        )

    async def start_resolved(
        self,
        request: StartWorkflowRequest,
        target: WorkflowStartTarget,
        *,
        trigger_name: TriggerName,
        scope_binding: TrustedScopeBinding,
    ) -> StartWorkflowResult:
        """Start an already-resolved immutable target through the shared policy boundary."""
        if request.definition_digest is None:
            raise WorkflowStartError(
                StartErrorCode.DEFINITION_UNAVAILABLE,
                "A resolved workflow start requires an immutable definition identity",
                retryable=False,
            )
        if not isinstance(request.source, ScheduleSourceIdentity) or (
            request.source.schedule != trigger_name
        ):
            raise WorkflowStartError(
                StartErrorCode.INVALID_REQUEST,
                "Resolved schedule start does not match its immutable trigger identity",
                retryable=False,
            )
        return await self._start_with_metrics(
            request,
            scope_binding=scope_binding,
            target=target,
            matched_trigger=MatchedTrigger(name=trigger_name, kind=TriggerKind.SCHEDULE),
        )

    async def prepare(
        self,
        request: StartWorkflowRequest,
        *,
        scope_binding: TrustedScopeBinding,
    ) -> PreparedWorkflowStart:
        try:
            scope = require_bound_scope(
                scope_binding,
                expected_kind=_scope_binding_kind(request.source),
                required_scope=scope_binding.scope,
            )
        except ScopeResolutionError as exc:
            raise WorkflowStartError(
                StartErrorCode.INVALID_REQUEST,
                "Workflow start source does not match its trusted scope binding",
                retryable=False,
            ) from exc
        registration = await self._resolve_target(scope, request)
        trigger = _match_trigger(registration.triggers, request.source, scope_binding)
        self._validate_resolved_target(scope, request, registration.target)
        normalized_input = validate_workflow_start_input(
            request.workflow_name,
            request.input,
            registration.target,
            self._limits,
        )
        return PreparedWorkflowStart(
            target=registration.target,
            trigger=trigger,
            normalized_input=normalized_input,
        )

    async def resolve_active_target(
        self,
        scope: RuntimeScope,
        workflow_name: str,
    ) -> WorkflowStartTarget:
        registration = await self._target_resolver.resolve(scope, workflow_name)
        if registration is None:
            raise WorkflowStartError(
                StartErrorCode.UNKNOWN_WORKFLOW,
                "The requested workflow is not registered",
                retryable=False,
            )
        target = registration.target
        if not target.deployment.supports(target.manifest):
            raise WorkflowStartError(
                StartErrorCode.INCOMPATIBLE_WORKER,
                "The active workflow definition has no compatible worker",
                retryable=False,
            )
        return target

    def validate_resolved_input(
        self,
        workflow_name: str,
        input: Mapping[str, object],
        target: WorkflowStartTarget,
    ) -> dict[str, JSONValue]:
        return validate_workflow_start_input(
            workflow_name,
            input,
            target,
            self._limits,
        )

    async def start_accepted_api(
        self,
        request: StartWorkflowRequest,
        target: WorkflowStartTarget,
        *,
        trigger_name: TriggerName,
        scope_binding: TrustedScopeBinding,
        priority: Priority | None = None,
    ) -> StartWorkflowResult:
        if request.definition_digest is None:
            raise WorkflowStartError(
                StartErrorCode.DEFINITION_UNAVAILABLE,
                "A deferred workflow start requires an immutable definition identity",
                retryable=False,
            )
        if not isinstance(request.source, ControlApiSourceIdentity):
            raise WorkflowStartError(
                StartErrorCode.INVALID_REQUEST,
                "Deferred API start does not retain its accepted source identity",
                retryable=False,
            )
        return await self._start_with_metrics(
            request,
            scope_binding=scope_binding,
            target=target,
            matched_trigger=MatchedTrigger(name=trigger_name, kind=TriggerKind.API),
            priority=priority,
        )

    async def _start_with_metrics(
        self,
        request: StartWorkflowRequest,
        *,
        scope_binding: TrustedScopeBinding,
        target: WorkflowStartTarget | None,
        matched_trigger: MatchedTrigger | None = None,
        priority: Priority | None = None,
    ) -> StartWorkflowResult:
        try:
            result = await self._start(
                request,
                scope_binding=scope_binding,
                target=target,
                matched_trigger=matched_trigger,
                priority=priority,
            )
        except WorkflowStartError as exc:
            if self._metrics is not None:
                outcome = StartMetricOutcome.ERROR if exc.retryable else StartMetricOutcome.REJECTED
                self._metrics.record_start(request.source.source.value, outcome)
            raise
        if self._metrics is not None:
            outcome = (
                StartMetricOutcome.STARTED
                if result.status is StartStatus.STARTED
                else StartMetricOutcome.DUPLICATE
            )
            self._metrics.record_start(request.source.source.value, outcome)
        return result

    async def _start(
        self,
        request: StartWorkflowRequest,
        *,
        scope_binding: TrustedScopeBinding,
        target: WorkflowStartTarget | None,
        matched_trigger: MatchedTrigger | None,
        priority: Priority | None,
    ) -> StartWorkflowResult:
        try:
            scope = require_bound_scope(
                scope_binding,
                expected_kind=_scope_binding_kind(request.source),
                required_scope=scope_binding.scope,
            )
        except ScopeResolutionError as exc:
            raise WorkflowStartError(
                StartErrorCode.INVALID_REQUEST,
                "Workflow start source does not match its trusted scope binding",
                retryable=False,
            ) from exc
        correlation_id = request.correlation_id or request.business_request_id
        correlation_digest = identity_log_digest(correlation_id)
        with logging_context(request_id=correlation_digest, flow_name=request.workflow_name):
            if target is None:
                registration = await self._resolve_target(scope, request)
                target = registration.target
                matched_trigger = _match_trigger(
                    registration.triggers,
                    request.source,
                    scope_binding,
                )
            elif matched_trigger is None:
                raise RuntimeError("Resolved workflow start omitted its trigger identity")
            _validate_source_binding(request.source, scope_binding)
            self._validate_resolved_target(scope, request, target)
            normalized_input = validate_workflow_start_input(
                request.workflow_name,
                request.input,
                target,
                self._limits,
            )
            workflow_id = make_workflow_id(
                request.workflow_name,
                request.business_request_id,
                scope=scope,
            )
            source_digest = source_identity_digest(request.source, scope)
            memo = {
                **target.memo,
                MEMO_TRIGGER_SOURCE: request.source.source.value,
                MEMO_TRIGGER_NAME: matched_trigger.name,
                MEMO_SOURCE_IDENTITY_DIGEST: source_digest,
                MEMO_CORRELATION_IDENTITY_DIGEST: correlation_digest,
                MEMO_SCOPE_DIGEST: scope.digest,
            }
            source_provider = _source_provider(request.source)
            if source_provider is not None:
                memo[MEMO_SOURCE_PROVIDER] = source_provider
            trigger = WorkflowTrigger(
                request_id=request.business_request_id,
                globals=normalized_input,
                correlation_id=correlation_id,
                trace_id=request.trace_id,
                definition_digest=target.manifest.definition_digest,
                worker_deployment=target.deployment.name,
                worker_build_id=target.deployment.build_id,
                worker_artifact=target.deployment.artifact_identity,
                environment_snapshot_digest=target.environment_snapshot_digest,
                execution_configuration=target.execution_configuration,
                trigger_source=request.source.source.value,
                trigger_name=matched_trigger.name,
                source_identity_digest=source_digest,
                correlation_identity_digest=correlation_digest,
                scope_digest=scope.digest,
            )
            try:
                search_attributes = (
                    execution_search_attributes(
                        scope_digest=scope.digest,
                        logical_workflow=target.manifest.logical_name,
                        definition_digest=target.manifest.definition_digest,
                        trigger_source=request.source.source.value,
                        worker_artifact_digest=(
                            target.deployment.artifact_identity.artifact_digest
                        ),
                    )
                    if self._indexed_search_attributes_enabled
                    else None
                )
                handle = await retry_pinned_workflow_start(
                    lambda: self._client.start_workflow(
                        target.workflow_type,
                        trigger.model_dump(mode="json", exclude_none=True),
                        id=workflow_id,
                        task_queue=self._task_queue,
                        id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                        id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                        memo=memo,
                        search_attributes=search_attributes,
                        versioning_override=target.versioning_override,
                        priority=priority or Priority(),
                    ),
                    target.deployment,
                    self._task_queue,
                    attempts=self._pinned_start_retry.attempts,
                    interval_seconds=self._pinned_start_retry.interval_seconds,
                )
            except WorkflowAlreadyStartedError as exc:
                result = await self._existing_result(
                    workflow_id,
                    run_id=exc.run_id,
                    scope=scope,
                )
                logger.info("Duplicate start did not create another workflow")
                return result
            except RPCError as exc:
                certainty = (
                    StartRequestCertainty.AUTHORITATIVE_REJECTION
                    if exc.status in AUTHORITATIVE_START_REJECTION_STATUSES
                    else StartRequestCertainty.AMBIGUOUS
                )
                raise WorkflowStartError(
                    StartErrorCode.TEMPORAL_UNAVAILABLE,
                    "Temporal did not accept the workflow start",
                    retryable=True,
                    request_certainty=certainty,
                ) from exc
            except Exception as exc:
                raise WorkflowStartError(
                    StartErrorCode.TEMPORAL_UNAVAILABLE,
                    "Temporal did not accept the workflow start",
                    retryable=True,
                    request_certainty=StartRequestCertainty.AMBIGUOUS,
                ) from exc

            run_id = await self._resolve_run_id(workflow_id, handle.run_id)
            logger.info("Workflow start accepted", extra={"workflow_run_id": run_id})
            return self._result(
                target,
                workflow_id=workflow_id,
                run_id=run_id,
                source_digest=source_digest,
                scope=scope,
                status=StartStatus.STARTED,
                trigger_name=matched_trigger.name,
            )

    async def _resolve_target(
        self,
        scope: RuntimeScope,
        request: StartWorkflowRequest,
    ) -> WorkflowStartRegistration:
        registration = await self._target_resolver.resolve(scope, request.workflow_name)
        if registration is None:
            raise WorkflowStartError(
                StartErrorCode.UNKNOWN_WORKFLOW,
                "The requested workflow is not registered",
                retryable=False,
            )
        if (
            request.definition_digest is not None
            and request.definition_digest != registration.target.manifest.definition_digest
        ):
            raise WorkflowStartError(
                StartErrorCode.DEFINITION_UNAVAILABLE,
                "The requested workflow definition is not available",
                retryable=False,
            )
        return registration

    @staticmethod
    def _validate_resolved_target(
        scope: RuntimeScope,
        request: StartWorkflowRequest,
        target: WorkflowStartTarget,
    ) -> None:
        if request.workflow_name != target.manifest.logical_name:
            raise WorkflowStartError(
                StartErrorCode.DEFINITION_UNAVAILABLE,
                "The resolved workflow target does not match the request",
                retryable=False,
            )
        if not target.deployment.supports(target.manifest):
            raise WorkflowStartError(
                StartErrorCode.INCOMPATIBLE_WORKER,
                "The resolved workflow definition has no compatible worker",
                retryable=False,
            )
        if not LEGACY_LOCAL_UNSCOPED_POLICY.owns(target.scope_digest, scope):
            raise WorkflowStartError(
                StartErrorCode.DEFINITION_UNAVAILABLE,
                "The resolved workflow target does not belong to the runtime scope",
                retryable=False,
            )
        if (
            request.definition_digest is not None
            and request.definition_digest != target.manifest.definition_digest
        ):
            raise WorkflowStartError(
                StartErrorCode.DEFINITION_UNAVAILABLE,
                "The resolved workflow definition does not match the request",
                retryable=False,
            )

    async def _resolve_run_id(self, workflow_id: str, run_id: str | None) -> str:
        if run_id is not None:
            return run_id
        try:
            description = await self._client.get_workflow_handle(workflow_id).describe()
        except Exception as exc:
            raise WorkflowStartError(
                StartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal accepted the workflow identity but did not return its run identity",
                retryable=True,
            ) from exc
        return description.run_id

    async def _existing_result(
        self,
        workflow_id: str,
        *,
        run_id: str | None,
        scope: RuntimeScope,
    ) -> StartWorkflowResult:
        result = await self._lookup_existing_result(
            workflow_id,
            run_id=run_id,
            scope=scope,
        )
        if result is None:
            raise WorkflowStartError(
                StartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal accepted the workflow identity but its run metadata is unavailable",
                retryable=True,
                request_certainty=StartRequestCertainty.AMBIGUOUS,
            )
        return result

    async def _lookup_existing_result(
        self,
        workflow_id: str,
        *,
        run_id: str | None,
        scope: RuntimeScope,
    ) -> StartWorkflowResult | None:
        try:
            description = await self._client.get_workflow_handle(
                workflow_id,
                run_id=run_id,
            ).describe()
            memo = await description.memo()
            execution = description.raw_description.workflow_execution_info.execution
            actual_run_id = execution.run_id
            result = _start_result_from_memo(
                memo,
                workflow_id=workflow_id,
                run_id=actual_run_id,
            )
        except RPCError as exc:
            if exc.status is RPCStatusCode.NOT_FOUND:
                return None
            raise WorkflowStartError(
                StartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal workflow run metadata is unavailable",
                retryable=True,
                request_certainty=StartRequestCertainty.AMBIGUOUS,
            ) from exc
        except Exception as exc:
            raise WorkflowStartError(
                StartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal workflow run metadata is unavailable",
                retryable=True,
                request_certainty=StartRequestCertainty.AMBIGUOUS,
            ) from exc
        if result.scope_digest != scope.digest:
            raise WorkflowStartError(
                StartErrorCode.DEFINITION_UNAVAILABLE,
                "The existing workflow execution belongs to another runtime scope",
                retryable=False,
            )
        return result

    def _result(
        self,
        target: WorkflowStartTarget,
        *,
        workflow_id: str,
        run_id: str,
        source_digest: str,
        scope: RuntimeScope,
        status: StartStatus,
        trigger_name: TriggerName,
    ) -> StartWorkflowResult:
        return StartWorkflowResult(
            workflow_id=workflow_id,
            run_id=run_id,
            workflow_name=target.manifest.logical_name,
            definition_digest=target.manifest.definition_digest,
            artifact_identity=target.deployment.artifact_identity,
            environment_snapshot_digest=target.environment_snapshot_digest,
            execution_configuration=target.execution_configuration,
            trigger_name=trigger_name,
            source_identity_digest=source_digest,
            scope_digest=scope.digest,
            status=status,
        )


def _start_result_from_memo(
    memo: Mapping[str, object],
    *,
    workflow_id: str,
    run_id: str,
) -> StartWorkflowResult:
    workflow_name = _required_memo_text(memo, MEMO_LOGICAL_WORKFLOW)
    definition_digest = _required_memo_text(memo, MEMO_DEFINITION_DIGEST)
    scope_digest = _required_memo_text(memo, MEMO_SCOPE_DIGEST)
    trigger_name = _required_memo_text(memo, MEMO_TRIGGER_NAME)
    source_digest = _required_memo_text(memo, MEMO_SOURCE_IDENTITY_DIGEST)
    artifact_identity = WorkerArtifactIdentity(
        deployment_name=_required_memo_text(memo, MEMO_WORKER_DEPLOYMENT),
        build_id=_required_memo_text(memo, MEMO_WORKER_BUILD_ID),
        artifact_digest=_required_memo_text(memo, MEMO_WORKER_ARTIFACT_DIGEST),
        package_version=_required_memo_text(memo, MEMO_WORKER_PACKAGE_VERSION),
        source_revision=_memo_text(memo, MEMO_WORKER_SOURCE_REVISION),
    )
    configuration_revision = _memo_text(memo, MEMO_CONFIGURATION_REVISION)
    configuration_resolution = _memo_text(
        memo,
        MEMO_CONFIGURATION_RESOLUTION_DIGEST,
    )
    execution_configuration = None
    if configuration_revision is not None or configuration_resolution is not None:
        if configuration_revision is None or configuration_resolution is None:
            raise ValueError("Existing workflow configuration memo is incomplete")
        execution_configuration = ExecutionConfigurationIdentity(
            configuration_revision_id=configuration_revision,
            tenant_configuration_revision_id=_memo_text(
                memo,
                MEMO_TENANT_CONFIGURATION_REVISION,
            ),
            component_catalog_revision=_memo_text(
                memo,
                MEMO_COMPONENT_CATALOG_REVISION,
            ),
            component_identity_digest=_memo_text(
                memo,
                MEMO_COMPONENT_IDENTITY_DIGEST,
            ),
            resolution_digest=configuration_resolution,
        )
    return StartWorkflowResult(
        workflow_id=workflow_id,
        run_id=run_id,
        workflow_name=workflow_name,
        definition_digest=definition_digest,
        artifact_identity=artifact_identity,
        environment_snapshot_digest=_required_memo_text(
            memo,
            MEMO_ENVIRONMENT_SNAPSHOT_DIGEST,
        ),
        execution_configuration=execution_configuration,
        trigger_name=trigger_name,
        source_identity_digest=source_digest,
        scope_digest=scope_digest,
        status=StartStatus.DUPLICATE,
    )


def _required_memo_text(memo: Mapping[str, object], key: str) -> str:
    value = _memo_text(memo, key)
    if value is None:
        raise ValueError(f"Existing workflow memo is missing {key}")
    return value


def _memo_text(memo: Mapping[str, object], key: str) -> str | None:
    value = memo.get(key)
    return value if isinstance(value, str) and value else None


def _triggers_for_workflow(
    declarations: TriggersConfig,
    workflow_name: str,
) -> Mapping[TriggerName, TriggerDeclaration]:
    return MappingProxyType(
        {
            name: declaration
            for name, declaration in declarations.triggers.items()
            if declaration.workflow == workflow_name
        }
    )


def _match_trigger(
    declarations: Mapping[TriggerName, TriggerDeclaration],
    source: SourceIdentity,
    scope_binding: TrustedScopeBinding,
) -> MatchedTrigger:
    _validate_source_binding(source, scope_binding)
    for name, declaration in sorted(declarations.items()):
        if not _trigger_matches_source(name, declaration, source):
            continue
        if declaration.paused:
            raise WorkflowStartError(
                StartErrorCode.TRIGGER_PAUSED,
                "The matching workflow trigger is paused",
                retryable=False,
            )
        return MatchedTrigger(name=name, kind=declaration.kind)
    raise WorkflowStartError(
        StartErrorCode.TRIGGER_UNAVAILABLE,
        "No active workflow trigger matches the trusted ingress source",
        retryable=False,
    )


def _trigger_matches_source(
    name: TriggerName,
    declaration: TriggerDeclaration,
    source: SourceIdentity,
) -> bool:
    if isinstance(declaration, ApiTriggerDeclaration):
        return isinstance(source, ControlApiSourceIdentity)
    if isinstance(declaration, ScheduleTriggerDeclaration):
        return isinstance(source, ScheduleSourceIdentity) and source.schedule == name
    if isinstance(declaration, WebhookTriggerDeclaration):
        return isinstance(source, WebhookSourceIdentity) and source.provider == declaration.source
    if isinstance(declaration, EventTriggerDeclaration):
        return (
            isinstance(source, CloudEventSourceIdentity) and source.mapping == declaration.mapping
        )
    if isinstance(declaration, BrokerTriggerDeclaration):
        return isinstance(source, BrokerSourceIdentity) and source.broker == declaration.broker
    if isinstance(declaration, HostTriggerDeclaration):
        return isinstance(source, HostSourceIdentity) and source.adapter == declaration.adapter
    raise TypeError("Unsupported trigger declaration")


def _validate_source_binding(
    source: SourceIdentity,
    scope_binding: TrustedScopeBinding,
) -> None:
    expected_binding = _source_binding_id(source)
    if expected_binding is not None and scope_binding.binding_id != expected_binding:
        raise WorkflowStartError(
            StartErrorCode.INVALID_REQUEST,
            "Workflow start source does not match its trusted binding identity",
            retryable=False,
        )


def _source_binding_id(source: SourceIdentity) -> str | None:
    if isinstance(source, BrokerSourceIdentity):
        return source.broker
    if isinstance(source, WebhookSourceIdentity):
        return source.provider
    if isinstance(source, HostSourceIdentity):
        return source.adapter
    if isinstance(source, ScheduleSourceIdentity):
        return source.schedule
    if isinstance(source, CloudEventSourceIdentity):
        return source.mapping
    return None


def validate_workflow_start_input(
    workflow_name: str,
    input: Mapping[str, object],
    target: WorkflowStartTarget,
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
) -> dict[str, JSONValue]:
    try:
        normalized = normalize_json_object(
            input,
            path="start.input",
            max_collection_items=limits.collection_items,
        )
        enforce_payload_bytes(
            normalized,
            boundary="start.input",
            limit=limits.trigger_payload_bytes,
        )
    except (
        DataNormalizationError,
        LimitExceededError,
        PayloadSerializationError,
        ValueError,
        TypeError,
    ) as exc:
        raise WorkflowStartError(
            StartErrorCode.INVALID_REQUEST,
            "Workflow input is not valid bounded JSON",
            retryable=False,
        ) from exc

    schema = target.manifest.workflow.get("input_schema")
    if schema is None:
        return normalized
    try:
        validate_payload(
            schema,
            normalized,
            direction="input",
            step_name=workflow_name,
        )
    except ContractViolation as exc:
        raise WorkflowStartError(
            StartErrorCode.INPUT_REJECTED,
            "Workflow input does not satisfy its contract",
            retryable=False,
        ) from exc
    except (SchemaDeclarationError, SchemaLoadError) as exc:
        raise WorkflowStartError(
            StartErrorCode.CONFIGURATION_ERROR,
            "Workflow input contract is unavailable",
            retryable=False,
        ) from exc
    return normalized


def source_identity_digest(
    source: SourceIdentity,
    scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
) -> str:
    return identity_digest(
        json.dumps(
            {
                "scope_digest": scope.digest,
                "source": source.model_dump(mode="json", exclude_none=True),
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def identity_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _scope_binding_kind(source: SourceIdentity) -> ScopeBindingKind:
    if isinstance(source, BrokerSourceIdentity):
        return ScopeBindingKind.BROKER
    if isinstance(source, ControlApiSourceIdentity):
        return ScopeBindingKind.API
    if isinstance(source, WebhookSourceIdentity):
        return ScopeBindingKind.WEBHOOK
    if isinstance(source, ScheduleSourceIdentity):
        return ScopeBindingKind.SCHEDULE
    if isinstance(source, CloudEventSourceIdentity):
        return ScopeBindingKind.CLOUD_EVENT
    return ScopeBindingKind.HOST


def _source_provider(source: SourceIdentity) -> str | None:
    if isinstance(source, BrokerSourceIdentity):
        return source.broker
    if isinstance(source, WebhookSourceIdentity):
        return source.provider
    if isinstance(source, HostSourceIdentity):
        return source.adapter
    if isinstance(source, CloudEventSourceIdentity):
        return source.mapping
    return None
