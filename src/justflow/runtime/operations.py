"""Typed control operations hiding Temporal client and service exceptions."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any, Literal, TypedDict

from google.protobuf.message import Message
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from temporalio.api.common.v1 import Payloads
from temporalio.api.enums.v1 import PendingActivityState, PendingWorkflowTaskState
from temporalio.api.failure.v1 import Failure
from temporalio.client import (
    Client,
    WorkflowExecution,
    WorkflowExecutionDescription,
    WorkflowHandle,
    WorkflowHistoryEventFilterType,
)
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode

from justflow.config.grammar import SignalName, TriggerName, WorkflowName
from justflow.config.runtime_limits import DEFAULT_FAILURE_RECORD_BYTES
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
    runtime_workflow_type_name,
)
from justflow.engine.audit import FailureCategory, FailurePhase, FailureRecord
from justflow.engine.data import DataNormalizationError, normalize_json_value
from justflow.engine.limits import (
    LimitExceededError,
    PayloadSerializationError,
    enforce_payload_bytes,
)
from justflow.provenance import ExecutionConfigurationIdentity, WorkerArtifactIdentity
from justflow.runtime.starter import (
    MEMO_SOURCE_IDENTITY_DIGEST,
    MEMO_TRIGGER_NAME,
    MEMO_TRIGGER_SOURCE,
    TriggerSource,
)
from justflow.runtime.visibility import (
    DEFINITION_DIGEST_SEARCH_ATTRIBUTE,
    LOGICAL_WORKFLOW_SEARCH_ATTRIBUTE,
    TRIGGER_SOURCE_SEARCH_ATTRIBUTE,
    WORKER_ARTIFACT_SEARCH_ATTRIBUTE,
)
from justflow.scope import (
    LOCAL_RUNTIME_SCOPE,
    SCOPED_IDENTITY_VERSION,
    RuntimeScope,
    decode_scope_cursor,
    encode_scope_cursor,
    identity_belongs_to_scope,
    scoped_identity_prefix,
)
from justflow.sdk.message_contract import WORKFLOW_EVENT_SIGNAL, EventPayload

DEFAULT_CONTROL_RPC_TIMEOUT_SECONDS = 10.0
MAX_CONTROL_RPC_TIMEOUT_SECONDS = 60.0
DEFAULT_WORKFLOW_LIST_LIMIT = 50
MAX_WORKFLOW_LIST_LIMIT = 100
MAX_PAGE_TOKEN_BYTES = 4_096
MAX_WORKFLOW_ID_LENGTH = 256
MAX_RUN_ID_LENGTH = 256
MAX_WORKFLOW_TYPE_LENGTH = 256
MAX_TASK_QUEUE_LENGTH = 128
MAX_STATUS_LENGTH = 64
MAX_TIMESTAMP_LENGTH = 64
MAX_PENDING_WAITS = 20
MAX_FAILURE_CODE_LENGTH = 128
MAX_FAILURE_CAUSE_DEPTH = 10
FAILURE_DETAIL_ENCODING_OVERHEAD_FACTOR = 2
MAX_FAILURE_DETAIL_PAYLOAD_BYTES = (
    DEFAULT_FAILURE_RECORD_BYTES * FAILURE_DETAIL_ENCODING_OVERHEAD_FACTOR
)
CLOSE_HISTORY_PAGE_SIZE = 1
SHA256_HEX_LENGTH = 64
ARTIFACT_DIGEST_PREFIX = "sha256:"
DEFINITION_DIGEST_PATTERN = rf"^[0-9a-f]{{{SHA256_HEX_LENGTH}}}$"
ARTIFACT_DIGEST_PATTERN = rf"^sha256:[0-9a-f]{{{SHA256_HEX_LENGTH}}}$"

logger = logging.getLogger(__name__)


class ExecutionFields(TypedDict):
    workflow_id: str
    run_id: str
    workflow_type: str
    status: str | None
    task_queue: str
    start_time: str
    close_time: str | None


class StrictControlModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class ControlErrorCode(str, Enum):
    INVALID_REQUEST = "invalid_request"
    FILTER_UNAVAILABLE = "filter_unavailable"
    NOT_FOUND = "not_found"
    TEMPORAL_UNAVAILABLE = "temporal_unavailable"


class ControlOperationError(Exception):
    def __init__(self, code: ControlErrorCode, message: str, *, retryable: bool) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(message)


class WorkflowSummary(StrictControlModel):
    workflow_id: str = Field(min_length=1, max_length=MAX_WORKFLOW_ID_LENGTH)
    run_id: str = Field(min_length=1, max_length=MAX_RUN_ID_LENGTH)
    workflow_type: str = Field(min_length=1, max_length=MAX_WORKFLOW_TYPE_LENGTH)
    status: str | None = Field(default=None, max_length=MAX_STATUS_LENGTH)
    task_queue: str = Field(min_length=1, max_length=MAX_TASK_QUEUE_LENGTH)
    start_time: str = Field(min_length=1, max_length=MAX_TIMESTAMP_LENGTH)
    close_time: str | None = Field(default=None, max_length=MAX_TIMESTAMP_LENGTH)


class WorkflowListResult(StrictControlModel):
    workflows: tuple[WorkflowSummary, ...]
    next_page_token: str | None = Field(default=None, repr=False)


class WorkflowExecutionState(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    TERMINATED = "terminated"
    CONTINUED_AS_NEW = "continued_as_new"
    TIMED_OUT = "timed_out"


class WorkflowListQuery(StrictControlModel):
    workflow: WorkflowName | None = None
    state: WorkflowExecutionState | None = None
    started_after: datetime | None = None
    started_before: datetime | None = None
    definition_digest: str | None = Field(
        default=None,
        min_length=SHA256_HEX_LENGTH,
        max_length=SHA256_HEX_LENGTH,
        pattern=DEFINITION_DIGEST_PATTERN,
    )
    trigger_source: TriggerSource | None = None
    worker_artifact: str | None = Field(
        default=None,
        min_length=len(ARTIFACT_DIGEST_PREFIX) + SHA256_HEX_LENGTH,
        max_length=len(ARTIFACT_DIGEST_PREFIX) + SHA256_HEX_LENGTH,
        pattern=ARTIFACT_DIGEST_PATTERN,
    )
    scope: Literal["current"] | None = None

    @model_validator(mode="after")
    def validate_time_range(self) -> WorkflowListQuery:
        for value in (self.started_after, self.started_before):
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise ValueError("Workflow time filters must be timezone-aware")
        if (
            self.started_after is not None
            and self.started_before is not None
            and self.started_after > self.started_before
        ):
            raise ValueError("Workflow time filter range is invalid")
        return self


class PendingWaitKind(str, Enum):
    ACTIVITY = "activity"
    CHILD_WORKFLOW = "child_workflow"
    WORKFLOW_TASK = "workflow_task"


class PendingWait(StrictControlModel):
    kind: PendingWaitKind
    identity: str | None = Field(default=None, max_length=MAX_WORKFLOW_ID_LENGTH)
    state: str = Field(min_length=1, max_length=MAX_STATUS_LENGTH)
    scheduled_at: str | None = Field(default=None, max_length=MAX_TIMESTAMP_LENGTH)


class ContinuationChain(StrictControlModel):
    first_run_id: str = Field(min_length=1, max_length=MAX_RUN_ID_LENGTH)
    current_run_id: str = Field(min_length=1, max_length=MAX_RUN_ID_LENGTH)
    next_run_id: str | None = Field(default=None, max_length=MAX_RUN_ID_LENGTH)
    complete: bool = False


class WorkflowFailureClassification(StrictControlModel):
    code: str = Field(min_length=1, max_length=MAX_FAILURE_CODE_LENGTH)
    cause_code: str | None = Field(default=None, max_length=MAX_FAILURE_CODE_LENGTH)
    category: FailureCategory | None = None
    phase: FailurePhase | None = None
    retryable: bool | None = None
    step: str | None = Field(default=None, max_length=MAX_WORKFLOW_TYPE_LENGTH)


class WorkflowDescription(WorkflowSummary):
    logical_workflow: WorkflowName | None = None
    definition_digest: str | None = Field(
        default=None,
        min_length=SHA256_HEX_LENGTH,
        max_length=SHA256_HEX_LENGTH,
        pattern=DEFINITION_DIGEST_PATTERN,
    )
    artifact_identity: WorkerArtifactIdentity | None = None
    environment_snapshot_digest: str | None = Field(
        default=None,
        min_length=SHA256_HEX_LENGTH,
        max_length=SHA256_HEX_LENGTH,
        pattern=DEFINITION_DIGEST_PATTERN,
    )
    execution_configuration: ExecutionConfigurationIdentity | None = None
    trigger_source: TriggerSource | None = None
    trigger_name: TriggerName | None = None
    source_identity_digest: str | None = Field(
        default=None,
        min_length=SHA256_HEX_LENGTH,
        max_length=SHA256_HEX_LENGTH,
        pattern=DEFINITION_DIGEST_PATTERN,
        repr=False,
    )
    pending_waits: tuple[PendingWait, ...] = Field(
        default_factory=tuple,
        max_length=MAX_PENDING_WAITS,
    )
    pending_waits_truncated: bool = False
    continuation: ContinuationChain | None = None
    failure: WorkflowFailureClassification | None = None


class WorkflowControlService:
    def __init__(
        self,
        client: Client,
        *,
        scope: RuntimeScope | None = LOCAL_RUNTIME_SCOPE,
        rpc_timeout_seconds: float = DEFAULT_CONTROL_RPC_TIMEOUT_SECONDS,
        max_payload_bytes: int,
        indexed_search_attributes_enabled: bool = False,
    ) -> None:
        if not 0 < rpc_timeout_seconds <= MAX_CONTROL_RPC_TIMEOUT_SECONDS:
            raise ValueError(
                f"Control RPC timeout must be within {MAX_CONTROL_RPC_TIMEOUT_SECONDS} seconds"
            )
        if max_payload_bytes < 1:
            raise ValueError("Control payload byte limit must be positive")
        self._client = client
        self._default_scope = scope
        self._rpc_timeout = timedelta(seconds=rpc_timeout_seconds)
        self._max_payload_bytes = max_payload_bytes
        self._indexed_search_attributes_enabled = indexed_search_attributes_enabled

    async def describe(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        scope: RuntimeScope | None = None,
    ) -> WorkflowDescription:
        effective_scope = self._effective_scope(scope)
        _validate_execution_identity(workflow_id, run_id, scope=effective_scope)
        try:
            handle = self._client.get_workflow_handle(
                workflow_id,
                run_id=run_id,
            )
            description = await handle.describe(rpc_timeout=self._rpc_timeout)
            memo = await description.memo()
            memo_scope = _memo_text(memo, MEMO_SCOPE_DIGEST)
            if memo_scope != effective_scope.digest and not (
                memo_scope is None and effective_scope == LOCAL_RUNTIME_SCOPE
            ):
                raise ControlOperationError(
                    ControlErrorCode.NOT_FOUND,
                    "Cannot describe the workflow execution",
                    retryable=False,
                )
            continuation, failure = await _close_metadata(
                handle,
                description,
                rpc_timeout=self._rpc_timeout,
            )
            pending_waits, pending_waits_truncated = _pending_waits(description)
            return WorkflowDescription(
                **_execution_fields(description),
                logical_workflow=_memo_text(memo, MEMO_LOGICAL_WORKFLOW),
                definition_digest=_memo_text(memo, MEMO_DEFINITION_DIGEST),
                artifact_identity=_artifact_identity_from_memo(memo),
                environment_snapshot_digest=_memo_text(
                    memo,
                    MEMO_ENVIRONMENT_SNAPSHOT_DIGEST,
                ),
                execution_configuration=_configuration_identity_from_memo(memo),
                trigger_source=_trigger_source_from_memo(memo),
                trigger_name=_memo_text(memo, MEMO_TRIGGER_NAME),
                source_identity_digest=_memo_text(memo, MEMO_SOURCE_IDENTITY_DIGEST),
                pending_waits=pending_waits,
                pending_waits_truncated=pending_waits_truncated,
                continuation=continuation,
                failure=failure,
            )
        except ControlOperationError:
            raise
        except Exception as exc:
            raise _control_error(exc, "Cannot describe the workflow execution") from exc

    async def list(
        self,
        *,
        limit: int = DEFAULT_WORKFLOW_LIST_LIMIT,
        page_token: str | None = None,
        scope: RuntimeScope | None = None,
        query: WorkflowListQuery | None = None,
    ) -> WorkflowListResult:
        effective_scope = self._effective_scope(scope)
        if not 1 <= limit <= MAX_WORKFLOW_LIST_LIMIT:
            raise ControlOperationError(
                ControlErrorCode.INVALID_REQUEST,
                f"Workflow list limit must be within {MAX_WORKFLOW_LIST_LIMIT}",
                retryable=False,
            )
        decoded_token = _decode_page_token(page_token, effective_scope)
        visibility_query = _visibility_query(
            effective_scope,
            query or WorkflowListQuery(),
            indexed_search_attributes_enabled=self._indexed_search_attributes_enabled,
        )
        try:
            if visibility_query is None:
                executions = self._client.list_workflows(
                    limit=limit,
                    page_size=limit,
                    next_page_token=decoded_token,
                    rpc_timeout=self._rpc_timeout,
                )
            else:
                executions = self._client.list_workflows(
                    limit=limit,
                    page_size=limit,
                    next_page_token=decoded_token,
                    rpc_timeout=self._rpc_timeout,
                    query=visibility_query,
                )
            summaries = tuple(
                [_summary(execution, scope=effective_scope) async for execution in executions]
            )
            next_token = _encode_page_token(executions.next_page_token, effective_scope)
        except ControlOperationError:
            raise
        except Exception as exc:
            raise _control_error(exc, "Cannot list workflow executions") from exc
        return WorkflowListResult(workflows=summaries, next_page_token=next_token)

    async def signal_event(
        self,
        workflow_id: str,
        event_name: SignalName,
        payload: object,
        *,
        run_id: str | None = None,
        scope: RuntimeScope | None = None,
    ) -> None:
        effective_scope = self._effective_scope(scope)
        _validate_execution_identity(workflow_id, run_id, scope=effective_scope)
        try:
            normalized = normalize_json_value(payload, path="control.signal.payload")
            enforce_payload_bytes(
                normalized,
                boundary="control.signal.payload",
                limit=self._max_payload_bytes,
            )
        except (
            DataNormalizationError,
            LimitExceededError,
            PayloadSerializationError,
            ValueError,
            TypeError,
        ) as exc:
            raise ControlOperationError(
                ControlErrorCode.INVALID_REQUEST,
                "Signal payload is not valid bounded JSON",
                retryable=False,
            ) from exc
        try:
            await self._client.get_workflow_handle(workflow_id, run_id=run_id).signal(
                WORKFLOW_EVENT_SIGNAL,
                EventPayload(signal=event_name, data=normalized).model_dump(mode="json"),
                rpc_timeout=self._rpc_timeout,
            )
        except Exception as exc:
            raise _control_error(exc, "Cannot signal the workflow execution") from exc

    async def cancel(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        scope: RuntimeScope | None = None,
    ) -> None:
        effective_scope = self._effective_scope(scope)
        _validate_execution_identity(workflow_id, run_id, scope=effective_scope)
        try:
            await self._client.get_workflow_handle(workflow_id, run_id=run_id).cancel(
                reason="Requested through the Justflow control API",
                rpc_timeout=self._rpc_timeout,
            )
        except Exception as exc:
            raise _control_error(exc, "Cannot cancel the workflow execution") from exc

    async def terminate(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        scope: RuntimeScope | None = None,
    ) -> None:
        effective_scope = self._effective_scope(scope)
        _validate_execution_identity(workflow_id, run_id, scope=effective_scope)
        try:
            await self._client.get_workflow_handle(workflow_id, run_id=run_id).terminate(
                reason="Requested through the Justflow control API",
                rpc_timeout=self._rpc_timeout,
            )
        except Exception as exc:
            raise _control_error(exc, "Cannot terminate the workflow execution") from exc

    def _effective_scope(self, scope: RuntimeScope | None) -> RuntimeScope:
        effective_scope = scope or self._default_scope
        if effective_scope is None:
            raise ValueError("Workflow control operation requires an authorized runtime scope")
        return effective_scope


def _summary(execution: WorkflowExecution, *, scope: RuntimeScope) -> WorkflowSummary:
    if not _workflow_identity_allowed(execution.id, scope):
        raise ControlOperationError(
            ControlErrorCode.TEMPORAL_UNAVAILABLE,
            "Temporal returned a workflow outside the requested runtime scope",
            retryable=True,
        )
    return WorkflowSummary(**_execution_fields(execution))


def _execution_fields(execution: WorkflowExecution) -> ExecutionFields:
    status = execution.status
    status_name = status.name if status is not None else None
    start_time = execution.start_time
    close_time = execution.close_time
    return {
        "workflow_id": execution.id,
        "run_id": execution.run_id,
        "workflow_type": execution.workflow_type,
        "status": status_name,
        "task_queue": execution.task_queue,
        "start_time": start_time.isoformat(),
        "close_time": close_time.isoformat() if close_time is not None else None,
    }


def _control_error(exc: Exception, message: str) -> ControlOperationError:
    if isinstance(exc, RPCError) and exc.status == RPCStatusCode.NOT_FOUND:
        return ControlOperationError(ControlErrorCode.NOT_FOUND, message, retryable=False)
    return ControlOperationError(
        ControlErrorCode.TEMPORAL_UNAVAILABLE,
        message,
        retryable=True,
    )


def _validate_execution_identity(
    workflow_id: str,
    run_id: str | None,
    *,
    scope: RuntimeScope,
) -> None:
    if not workflow_id or len(workflow_id) > MAX_WORKFLOW_ID_LENGTH:
        raise ControlOperationError(
            ControlErrorCode.INVALID_REQUEST,
            "Workflow identity is invalid",
            retryable=False,
        )
    if run_id is not None and (not run_id or len(run_id) > MAX_RUN_ID_LENGTH):
        raise ControlOperationError(
            ControlErrorCode.INVALID_REQUEST,
            "Workflow run identity is invalid",
            retryable=False,
        )
    if not _workflow_identity_allowed(workflow_id, scope):
        raise ControlOperationError(
            ControlErrorCode.NOT_FOUND,
            "Workflow execution was not found",
            retryable=False,
        )


def _workflow_identity_allowed(workflow_id: str, scope: RuntimeScope) -> bool:
    if identity_belongs_to_scope(workflow_id, "workflow", scope):
        return True
    scoped_prefix = f"{SCOPED_IDENTITY_VERSION}.workflow."
    return scope == LOCAL_RUNTIME_SCOPE and not workflow_id.startswith(scoped_prefix)


def _decode_page_token(token: str | None, scope: RuntimeScope) -> bytes | None:
    if token is None:
        return None
    try:
        position = decode_scope_cursor(scope, token)
        if not isinstance(position, list) or any(
            not isinstance(value, int) or not 0 <= value <= 255 for value in position
        ):
            raise ValueError("Workflow page token position is invalid")
        decoded = bytes(position)
    except ValueError as exc:
        raise ControlOperationError(
            ControlErrorCode.INVALID_REQUEST,
            "Workflow page token is invalid",
            retryable=False,
        ) from exc
    if len(decoded) > MAX_PAGE_TOKEN_BYTES:
        raise ControlOperationError(
            ControlErrorCode.INVALID_REQUEST,
            "Workflow page token is invalid",
            retryable=False,
        )
    return decoded


def _encode_page_token(token: bytes | None, scope: RuntimeScope) -> str | None:
    if not token:
        return None
    if len(token) > MAX_PAGE_TOKEN_BYTES:
        raise ControlOperationError(
            ControlErrorCode.TEMPORAL_UNAVAILABLE,
            "Temporal returned an oversized workflow page token",
            retryable=True,
        )
    return encode_scope_cursor(scope, list(token))


def _scope_visibility_query(scope: RuntimeScope) -> str:
    return f'WorkflowId STARTS_WITH "{scoped_identity_prefix("workflow", scope)}"'


def _visibility_query(
    scope: RuntimeScope,
    query: WorkflowListQuery,
    *,
    indexed_search_attributes_enabled: bool,
) -> str | None:
    predicates: list[str] = []
    if scope != LOCAL_RUNTIME_SCOPE:
        predicates.append(_scope_visibility_query(scope))
    if query.state is not None:
        predicates.append(f'ExecutionStatus = "{_temporal_status(query.state)}"')
    if query.started_after is not None:
        predicates.append(f'StartTime >= "{_visibility_time(query.started_after)}"')
    if query.started_before is not None:
        predicates.append(f'StartTime <= "{_visibility_time(query.started_before)}"')

    indexed_filters = (
        query.workflow,
        query.definition_digest,
        query.trigger_source,
        query.worker_artifact,
    )
    if indexed_search_attributes_enabled:
        if query.workflow is not None:
            predicates.append(
                _keyword_predicate(LOGICAL_WORKFLOW_SEARCH_ATTRIBUTE, str(query.workflow))
            )
        if query.definition_digest is not None:
            predicates.append(
                _keyword_predicate(
                    DEFINITION_DIGEST_SEARCH_ATTRIBUTE,
                    query.definition_digest,
                )
            )
        if query.trigger_source is not None:
            predicates.append(
                _keyword_predicate(TRIGGER_SOURCE_SEARCH_ATTRIBUTE, query.trigger_source.value)
            )
        if query.worker_artifact is not None:
            predicates.append(
                _keyword_predicate(WORKER_ARTIFACT_SEARCH_ATTRIBUTE, query.worker_artifact)
            )
    elif any(value is not None for value in indexed_filters):
        if query.workflow is not None and query.definition_digest is not None:
            workflow_type = runtime_workflow_type_name(
                str(query.workflow),
                query.definition_digest,
                scope.digest,
            )
            predicates.append(_keyword_predicate("WorkflowType", workflow_type))
            if query.trigger_source is not None or query.worker_artifact is not None:
                raise _filter_unavailable()
        elif query.workflow is not None and scope == LOCAL_RUNTIME_SCOPE:
            prefix = f"{query.workflow}__"
            predicates.append(f'WorkflowType STARTS_WITH "{_escape_visibility_value(prefix)}"')
            if query.trigger_source is not None or query.worker_artifact is not None:
                raise _filter_unavailable()
        else:
            raise _filter_unavailable()
    return " AND ".join(predicates) if predicates else None


def _filter_unavailable() -> ControlOperationError:
    return ControlOperationError(
        ControlErrorCode.FILTER_UNAVAILABLE,
        "The requested filter requires host-enabled Temporal Visibility indexes",
        retryable=False,
    )


def _keyword_predicate(field: str, value: str) -> str:
    return f'{field} = "{_escape_visibility_value(value)}"'


def _escape_visibility_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _visibility_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _temporal_status(state: WorkflowExecutionState) -> str:
    return {
        WorkflowExecutionState.RUNNING: "Running",
        WorkflowExecutionState.COMPLETED: "Completed",
        WorkflowExecutionState.FAILED: "Failed",
        WorkflowExecutionState.CANCELED: "Canceled",
        WorkflowExecutionState.TERMINATED: "Terminated",
        WorkflowExecutionState.CONTINUED_AS_NEW: "ContinuedAsNew",
        WorkflowExecutionState.TIMED_OUT: "TimedOut",
    }[state]


def _pending_waits(
    description: WorkflowExecutionDescription,
) -> tuple[tuple[PendingWait, ...], bool]:
    raw = description.raw_description
    waits: list[PendingWait] = []
    for activity in raw.pending_activities:
        waits.append(
            PendingWait(
                kind=PendingWaitKind.ACTIVITY,
                identity=activity.activity_id,
                state=PendingActivityState.Name(activity.state).lower(),
                scheduled_at=_protobuf_timestamp(activity, "scheduled_time"),
            )
        )
    for child in raw.pending_children:
        waits.append(
            PendingWait(
                kind=PendingWaitKind.CHILD_WORKFLOW,
                identity=child.workflow_id,
                state="running",
            )
        )
    if raw.HasField("pending_workflow_task"):
        task = raw.pending_workflow_task
        waits.append(
            PendingWait(
                kind=PendingWaitKind.WORKFLOW_TASK,
                state=PendingWorkflowTaskState.Name(task.state).lower(),
                scheduled_at=_protobuf_timestamp(task, "scheduled_time"),
            )
        )
    return tuple(waits[:MAX_PENDING_WAITS]), len(waits) > MAX_PENDING_WAITS


def _protobuf_timestamp(message: Message, field: str) -> str | None:
    if not message.HasField(field):
        return None
    timestamp = getattr(message, field)
    return timestamp.ToDatetime(tzinfo=UTC).isoformat()


async def _close_metadata(
    handle: WorkflowHandle[Any, Any],
    description: WorkflowExecutionDescription,
    *,
    rpc_timeout: timedelta,
) -> tuple[ContinuationChain, WorkflowFailureClassification | None]:
    raw_info = description.raw_description.workflow_execution_info
    first_run_id = raw_info.first_run_id or description.run_id
    next_run_id: str | None = None
    failure: WorkflowFailureClassification | None = None
    if description.close_time is not None:
        events = handle.fetch_history_events(
            page_size=CLOSE_HISTORY_PAGE_SIZE,
            event_filter_type=WorkflowHistoryEventFilterType.CLOSE_EVENT,
            skip_archival=False,
            rpc_timeout=rpc_timeout,
        )
        async for event in events:
            if event.HasField("workflow_execution_continued_as_new_event_attributes"):
                next_run_id = (
                    event.workflow_execution_continued_as_new_event_attributes.new_execution_run_id
                    or None
                )
            elif event.HasField("workflow_execution_failed_event_attributes"):
                failure = await _failure_classification(
                    event.workflow_execution_failed_event_attributes.failure,
                    data_converter=description.data_converter,
                )
            elif event.HasField("workflow_execution_timed_out_event_attributes"):
                failure = WorkflowFailureClassification(
                    code="TEMPORAL_TIMEOUT",
                    category=FailureCategory.INFRASTRUCTURE,
                    retryable=True,
                )
            elif event.HasField("workflow_execution_terminated_event_attributes"):
                failure = WorkflowFailureClassification(
                    code="TERMINATED",
                    category=FailureCategory.EXECUTION,
                    retryable=False,
                )
            elif event.HasField("workflow_execution_canceled_event_attributes"):
                failure = WorkflowFailureClassification(
                    code="CANCELED",
                    category=FailureCategory.EXECUTION,
                    retryable=False,
                )
            break
    continuation = ContinuationChain(
        first_run_id=first_run_id,
        current_run_id=description.run_id,
        next_run_id=next_run_id,
        complete=(description.close_time is not None and next_run_id is None),
    )
    return continuation, failure


async def _failure_classification(
    failure: Failure,
    *,
    data_converter: DataConverter,
) -> WorkflowFailureClassification:
    current = failure
    for _ in range(MAX_FAILURE_CAUSE_DEPTH):
        if current.HasField("application_failure_info"):
            info = current.application_failure_info
            category = _known_failure_category(info.type)
            code = info.type if category is not None else "APPLICATION_FAILURE"
            detail = await _failure_detail(info.details, data_converter=data_converter)
            return WorkflowFailureClassification(
                code=code,
                cause_code=detail.error.cause_code if detail is not None else None,
                category=detail.error.category if detail is not None else category,
                phase=detail.error.phase if detail is not None else None,
                retryable=not info.non_retryable,
                step=detail.error.step if detail is not None else None,
            )
        if not current.HasField("cause"):
            break
        current = current.cause
    return WorkflowFailureClassification(code="WORKFLOW_FAILURE")


async def _failure_detail(
    payloads: Payloads,
    *,
    data_converter: DataConverter,
) -> FailureRecord | None:
    raw_payloads = payloads.payloads
    if not raw_payloads:
        return None
    payload_bytes = sum(len(payload.data) for payload in raw_payloads)
    if payload_bytes > MAX_FAILURE_DETAIL_PAYLOAD_BYTES:
        logger.warning("Workflow failure detail exceeded the operational decode bound")
        return None
    try:
        decoded = await data_converter.decode_wrapper(payloads, [dict])
        if len(decoded) != 1:
            raise ValueError("Workflow failure detail must contain one record")
        return FailureRecord.model_validate(decoded[0])
    except (TypeError, ValueError, ValidationError, UnicodeError):
        logger.warning("Workflow failure detail could not be decoded as bounded metadata")
        return None


def _known_failure_category(code: str) -> FailureCategory | None:
    return {
        "INVALID_TRIGGER": FailureCategory.INPUT,
        "INVALID_SIGNAL": FailureCategory.INPUT,
        "INPUT_CONTRACT_FAILED": FailureCategory.CONTRACT,
        "PARAMETERS_FAILED": FailureCategory.INPUT,
        "STEP_FAILED": FailureCategory.EXECUTION,
        "HANDLER_FAILED": FailureCategory.EXECUTION,
        "RESULT_FAILED": FailureCategory.CONTRACT,
        "ARCHIVAL_FAILED": FailureCategory.INFRASTRUCTURE,
        "INTERNAL_ERROR": FailureCategory.INTERNAL,
        "LIMIT_EXCEEDED": FailureCategory.EXECUTION,
    }.get(code)


def _trigger_source_from_memo(memo: Mapping[str, object]) -> TriggerSource | None:
    value = _memo_text(memo, MEMO_TRIGGER_SOURCE)
    try:
        return TriggerSource(value) if value is not None else None
    except ValueError:
        return None


def _memo_text(memo: Mapping[str, object], key: str) -> str | None:
    value = memo.get(key)
    return value if isinstance(value, str) else None


def _artifact_identity_from_memo(
    memo: Mapping[str, object],
) -> WorkerArtifactIdentity | None:
    deployment = _memo_text(memo, MEMO_WORKER_DEPLOYMENT)
    build_id = _memo_text(memo, MEMO_WORKER_BUILD_ID)
    digest = _memo_text(memo, MEMO_WORKER_ARTIFACT_DIGEST)
    package_version = _memo_text(memo, MEMO_WORKER_PACKAGE_VERSION)
    if deployment is None or build_id is None or digest is None or package_version is None:
        return None
    return WorkerArtifactIdentity(
        deployment_name=deployment,
        build_id=build_id,
        artifact_digest=digest,
        package_version=package_version,
        source_revision=_memo_text(memo, MEMO_WORKER_SOURCE_REVISION),
    )


def _configuration_identity_from_memo(
    memo: Mapping[str, object],
) -> ExecutionConfigurationIdentity | None:
    revision_id = _memo_text(memo, MEMO_CONFIGURATION_REVISION)
    resolution_digest = _memo_text(memo, MEMO_CONFIGURATION_RESOLUTION_DIGEST)
    if revision_id is None or resolution_digest is None:
        return None
    return ExecutionConfigurationIdentity(
        configuration_revision_id=revision_id,
        tenant_configuration_revision_id=_memo_text(
            memo,
            MEMO_TENANT_CONFIGURATION_REVISION,
        ),
        component_catalog_revision=_memo_text(memo, MEMO_COMPONENT_CATALOG_REVISION),
        component_identity_digest=_memo_text(memo, MEMO_COMPONENT_IDENTITY_DIGEST),
        resolution_digest=resolution_digest,
    )
