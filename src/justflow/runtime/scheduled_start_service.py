"""Temporal-backed application facade for one-off scheduled workflow starts."""

from __future__ import annotations

import asyncio
import base64
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, Self, TypeVar

from pydantic import Field, StrictInt, StrictStr, ValidationError, model_validator
from temporalio.api.common.v1 import Payload
from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleCalendarSpec,
    ScheduleDescription,
    ScheduleHandle,
    ScheduleListDescription,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
    ScheduleUpdateInput,
    WorkflowExecutionDescription,
    WorkflowExecutionStatus,
    WorkflowHandle,
)
from temporalio.common import (
    Priority,
    RetryPolicy,
    WorkflowIDConflictPolicy,
    WorkflowIDReusePolicy,
)
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from justflow.config.settings import (
    MAX_SCHEDULED_START_LIST_LIMIT,
    ScheduledStartSettings,
    ScheduledStartWorkloadClass,
    ScheduledStartWorkloadPolicySettings,
)
from justflow.config.triggers import TriggerKind
from justflow.definitions.routing import MEMO_SCOPE_DIGEST
from justflow.engine.limits import LimitExceededError, enforce_payload_bytes
from justflow.runtime.metrics import MetricsRegistry, ScheduledStartMetricOutcome
from justflow.runtime.scheduled_starts import (
    MEMO_SCHEDULED_START_ARBITER_COMMAND,
    MEMO_SCHEDULED_START_ARBITER_REQUEST_DIGEST,
    MEMO_SCHEDULED_START_ARBITER_VERSION,
    MEMO_SCHEDULED_START_FORMAT_VERSION,
    MEMO_SCHEDULED_START_ID,
    MEMO_SCHEDULED_START_OWNER,
    SCHEDULED_START_ARBITER_RESULT_ADAPTER,
    SCHEDULED_START_ARBITER_WORKFLOW_TYPE,
    SCHEDULED_START_DUE_WORKFLOW_TYPE,
    SCHEDULED_START_FORMAT_VERSION,
    SCHEDULED_START_OWNER,
    CanceledScheduledStartRecord,
    FailedScheduledStartRecord,
    PendingScheduledStartRecord,
    ScheduledStartArbiterApplied,
    ScheduledStartArbiterCommandKind,
    ScheduledStartArbiterInitialInput,
    ScheduledStartArbiterPreparation,
    ScheduledStartArbiterPreparedDue,
    ScheduledStartArbiterPreparedProjection,
    ScheduledStartArbiterPreparedStale,
    ScheduledStartArbiterResult,
    ScheduledStartArbiterStale,
    ScheduledStartArbiterTerminalInput,
    ScheduledStartAttemptAccepted,
    ScheduledStartAttemptAmbiguous,
    ScheduledStartAttemptOutcome,
    ScheduledStartAttemptRejected,
    ScheduledStartAttemptRequest,
    ScheduledStartAttemptResult,
    ScheduledStartCancelCommand,
    ScheduledStartCancelRequest,
    ScheduledStartCreateRequest,
    ScheduledStartDecision,
    ScheduledStartDecisionOutcome,
    ScheduledStartDescription,
    ScheduledStartDueClaimed,
    ScheduledStartDueClaimResult,
    ScheduledStartDueCommand,
    ScheduledStartDueInput,
    ScheduledStartDueSkipped,
    ScheduledStartError,
    ScheduledStartErrorCode,
    ScheduledStartFailureCode,
    ScheduledStartMutationResult,
    ScheduledStartMutationStatus,
    ScheduledStartPage,
    ScheduledStartProjectionRecord,
    ScheduledStartRecord,
    ScheduledStartRescheduleCommand,
    ScheduledStartRescheduleRequest,
    ScheduledStartResolvedTarget,
    ScheduledStartRun,
    ScheduledStartState,
    StartedScheduledStartRecord,
    StrictScheduledStartModel,
    cancel_scheduled_start,
    claim_scheduled_start,
    complete_scheduled_start,
    create_scheduled_start_record,
    describe_scheduled_start,
    fail_scheduled_start,
    make_scheduled_start_arbiter_id,
    make_scheduled_start_due_id,
    make_scheduled_start_id,
    make_scheduled_start_schedule_id,
    reschedule_scheduled_start,
    scheduled_start_request_digest,
)
from justflow.runtime.starter import (
    ControlApiSourceIdentity,
    StartErrorCode,
    StartRequestCertainty,
    StartStatus,
    StartWorkflowRequest,
    WorkflowStarter,
    WorkflowStartError,
)
from justflow.scope import (
    LEGACY_LOCAL_UNSCOPED_POLICY,
    LOCAL_RUNTIME_SCOPE,
    RuntimeScope,
    ScopeBindingKind,
    TrustedScopeBinding,
    decode_scope_cursor,
    encode_scope_cursor,
    identity_belongs_to_scope,
    safe_identity_digest,
    scoped_identity_prefix,
)
from justflow.sdk.message_contract import MAX_IDENTIFIER_LENGTH

IdempotencyKey = str
ScheduleCreateResultT = TypeVar("ScheduleCreateResultT")
logger = logging.getLogger(__name__)


class _ScheduledStartListCursorPayload(StrictScheduledStartModel):
    page_token: StrictStr | None = Field(default=None, repr=False)
    page_offset: StrictInt = Field(
        default=0,
        ge=0,
        le=MAX_SCHEDULED_START_LIST_LIMIT,
    )
    page_size: StrictInt | None = Field(
        default=None,
        ge=1,
        le=MAX_SCHEDULED_START_LIST_LIMIT,
    )
    state_filter: tuple[ScheduledStartState, ...] | None = None

    @model_validator(mode="after")
    def validate_page_position(self) -> Self:
        if self.page_offset > 0 and self.page_size is None:
            raise ValueError("An intra-page cursor requires its original page size")
        if self.page_size is not None and self.page_offset > self.page_size:
            raise ValueError("A cursor offset cannot exceed its page size")
        has_state_filter = "state_filter" in self.model_fields_set
        if self.page_size is None and has_state_filter:
            raise ValueError("A legacy cursor cannot contain a state filter")
        if self.page_size is not None and not has_state_filter:
            raise ValueError("A resumable cursor requires its state filter")
        return self


@dataclass(frozen=True, kw_only=True)
class _ScheduledStartListPosition:
    page_token: bytes | None
    page_offset: int
    page_size: int


class ScheduledStartDecisionRecorder(Protocol):
    def record(self, decision: ScheduledStartDecision) -> None: ...


class LoggingScheduledStartDecisionRecorder:
    def record(self, decision: ScheduledStartDecision) -> None:
        logger.info(
            "Scheduled-start decision",
            extra=decision.model_dump(mode="json", exclude_none=True),
        )


class ScheduledStartQuotaController(Protocol):
    async def create(
        self,
        *,
        scope: RuntimeScope,
        limit: int,
        count_pending: Callable[[], Awaitable[int]],
        create_schedule: Callable[[], Awaitable[ScheduleCreateResultT]],
    ) -> ScheduleCreateResultT: ...


class BestEffortLocalScheduledStartQuotaController:
    """Process-local, visibility-based scheduled-start admission for development."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    async def create(
        self,
        *,
        scope: RuntimeScope,
        limit: int,
        count_pending: Callable[[], Awaitable[int]],
        create_schedule: Callable[[], Awaitable[ScheduleCreateResultT]],
    ) -> ScheduleCreateResultT:
        if scope != LOCAL_RUNTIME_SCOPE:
            raise ScheduledStartError(
                ScheduledStartErrorCode.QUOTA_UNAVAILABLE,
                "The local scheduled-start quota controller only supports the local runtime scope",
                retryable=False,
            )
        async with self._lock:
            if await count_pending() >= limit:
                raise ScheduledStartError(
                    ScheduledStartErrorCode.QUOTA_EXCEEDED,
                    "Scheduled-start pending quota is exhausted",
                    retryable=False,
                )
            return await create_schedule()


class ScheduledStartWorkloadPolicy(Protocol):
    def priority(
        self,
        scope: RuntimeScope,
        workload_class: ScheduledStartWorkloadClass,
    ) -> Priority: ...


class SettingsScheduledStartWorkloadPolicy:
    def __init__(self, settings: ScheduledStartWorkloadPolicySettings) -> None:
        self._settings = settings

    def priority(
        self,
        scope: RuntimeScope,
        workload_class: ScheduledStartWorkloadClass,
    ) -> Priority:
        if workload_class not in self._settings.allowed_classes:
            raise ScheduledStartError(
                ScheduledStartErrorCode.INVALID_REQUEST,
                "Scheduled-start workload class is not granted for this runtime scope",
                retryable=False,
            )
        return Priority(
            priority_key=self._settings.priority_key(workload_class),
            fairness_key=scope.digest,
            fairness_weight=self._settings.fairness_weight,
        )


def _parse_arbiter_result(raw_result: object) -> ScheduledStartArbiterResult:
    try:
        return SCHEDULED_START_ARBITER_RESULT_ADAPTER.validate_python(raw_result)
    except ValidationError as exc:
        raise ScheduledStartError(
            ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
            "Temporal returned an invalid scheduled-start arbitration result",
            retryable=True,
        ) from exc


class ScheduledStartService:
    def __init__(
        self,
        client: Client,
        starter: WorkflowStarter,
        settings: ScheduledStartSettings,
        *,
        task_queue: str,
        workload_policy: ScheduledStartWorkloadPolicy | None = None,
        quota_controller: ScheduledStartQuotaController | None = None,
        metrics: MetricsRegistry | None = None,
        decision_recorder: ScheduledStartDecisionRecorder | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not task_queue:
            raise ValueError("Scheduled-start dispatch task queue must not be empty")
        self._client = client
        self._starter = starter
        self._settings = settings
        self._task_queue = task_queue
        self._workload_policy = workload_policy or SettingsScheduledStartWorkloadPolicy(
            settings.workload_policy
        )
        self._quota_controller = quota_controller
        self._metrics = metrics
        self._decision_recorder = decision_recorder or LoggingScheduledStartDecisionRecorder()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._rpc_timeout = timedelta(seconds=settings.dispatch_timeout_seconds)

    async def create(
        self,
        request: ScheduledStartCreateRequest,
        *,
        idempotency_key: IdempotencyKey,
        scope_binding: TrustedScopeBinding,
    ) -> ScheduledStartMutationResult:
        _validate_idempotency_key(idempotency_key)
        if scope_binding.kind is not ScopeBindingKind.API:
            raise ScheduledStartError(
                ScheduledStartErrorCode.INVALID_REQUEST,
                "Scheduled starts require a trusted API scope binding",
                retryable=False,
            )
        scope = scope_binding.scope
        scheduled_start_id = make_scheduled_start_id(
            scope,
            request.workflow_name,
            request.business_request_id,
        )
        request_digest = scheduled_start_request_digest(
            "create",
            request.model_dump(mode="json"),
            idempotency_key,
        )
        existing = await self._existing_record(scope, scheduled_start_id)
        if existing is not None:
            if existing.create_request_digest != request_digest:
                raise ScheduledStartError(
                    ScheduledStartErrorCode.CONFLICT,
                    "Scheduled-start identity is already bound to another request",
                    retryable=False,
                )
            result = ScheduledStartMutationResult(
                status=ScheduledStartMutationStatus.DUPLICATE,
                scheduled_start=describe_scheduled_start(existing),
            )
            self._record_decision(ScheduledStartMetricOutcome.DUPLICATE, existing)
            return result
        now = self._now()
        self._validate_due_time(request.start_at, now)
        priority = self._workload_policy.priority(scope, request.workload_class)
        start_request = StartWorkflowRequest(
            workflow_name=request.workflow_name,
            business_request_id=request.business_request_id,
            input=request.input,
            source=ControlApiSourceIdentity(request_id=request.business_request_id),
        )
        try:
            prepared = await self._starter.prepare(
                start_request,
                scope_binding=scope_binding,
            )
            if prepared.trigger.kind is not TriggerKind.API:
                raise WorkflowStartError(
                    StartErrorCode.TRIGGER_UNAVAILABLE,
                    "Scheduled starts require an active API trigger",
                    retryable=False,
                )
            enforce_payload_bytes(
                prepared.normalized_input,
                boundary="scheduled_start.input",
                limit=self._settings.max_input_bytes,
            )
        except WorkflowStartError as exc:
            raise _start_error(exc) from exc
        except LimitExceededError as exc:
            raise ScheduledStartError(
                ScheduledStartErrorCode.INVALID_REQUEST,
                "Scheduled-start input exceeds its configured byte bound",
                retryable=False,
            ) from exc

        record = create_scheduled_start_record(
            request,
            scheduled_start_id=scheduled_start_id,
            scope=scope,
            trigger_name=prepared.trigger.name,
            request_digest=request_digest,
            accepted_at=now,
            normalized_input=dict(prepared.normalized_input),
            dispatch_timeout_seconds=self._settings.dispatch_timeout_seconds,
            dispatch_attempts=self._settings.dispatch_attempts,
        )
        duplicate_create = False

        async def create_schedule() -> ScheduleHandle:
            nonlocal duplicate_create
            try:
                return await self._client.create_schedule(
                    make_scheduled_start_schedule_id(scope, scheduled_start_id),
                    self._schedule(record, scope, priority),
                    memo=_entity_memo(record),
                    rpc_timeout=self._rpc_timeout,
                )
            except ScheduleAlreadyRunningError:
                duplicate_create = True
                existing = await self._describe_record(scope, scheduled_start_id)
                if existing.create_request_digest != request_digest:
                    raise ScheduledStartError(
                        ScheduledStartErrorCode.CONFLICT,
                        "Scheduled-start identity is already bound to another request",
                        retryable=False,
                    ) from None
                return self._client.get_schedule_handle(
                    make_scheduled_start_schedule_id(scope, scheduled_start_id)
                )
            except RPCError as exc:
                if exc.status is RPCStatusCode.INVALID_ARGUMENT:
                    raise ScheduledStartError(
                        ScheduledStartErrorCode.PRIORITY_UNSUPPORTED,
                        "Temporal target does not support the configured workload priority",
                        retryable=False,
                    ) from exc
                raise ScheduledStartError(
                    ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                    "Temporal did not accept the scheduled start",
                    retryable=True,
                ) from exc
            except ScheduledStartError:
                raise
            except Exception as exc:
                raise ScheduledStartError(
                    ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                    "Temporal did not accept the scheduled start",
                    retryable=True,
                ) from exc

        quota = self._quota_controller
        if quota is None:
            raise ScheduledStartError(
                ScheduledStartErrorCode.QUOTA_UNAVAILABLE,
                "Exact scheduled-start pending quota enforcement is unavailable",
                retryable=False,
            )
        handle = await quota.create(
            scope=scope,
            limit=self._settings.max_pending_per_scope,
            count_pending=lambda: self._count_pending(scope),
            create_schedule=create_schedule,
        )
        description = await self._description_for_handle(handle)
        accepted = await self._record_from_description(description, scope)
        status = (
            ScheduledStartMutationStatus.DUPLICATE
            if duplicate_create
            else ScheduledStartMutationStatus.ACCEPTED
        )
        result = ScheduledStartMutationResult(
            status=status,
            scheduled_start=describe_scheduled_start(accepted),
        )
        self._record_decision(
            (
                ScheduledStartMetricOutcome.DUPLICATE
                if duplicate_create
                else ScheduledStartMetricOutcome.ACCEPTED
            ),
            accepted,
        )
        return result

    async def describe(
        self,
        scheduled_start_id: str,
        *,
        scope: RuntimeScope,
    ) -> ScheduledStartDescription:
        record = await self._describe_record(scope, scheduled_start_id)
        if not isinstance(record, PendingScheduledStartRecord):
            return describe_scheduled_start(record)
        return await self._authoritative_description(record, scope)

    async def list(
        self,
        *,
        scope: RuntimeScope,
        limit: int,
        cursor: str | None = None,
        states: frozenset[ScheduledStartState] | None = None,
    ) -> ScheduledStartPage:
        if not 1 <= limit <= self._settings.max_list_limit:
            raise ScheduledStartError(
                ScheduledStartErrorCode.INVALID_REQUEST,
                "Scheduled-start list limit is invalid",
                retryable=False,
            )
        state_filter = _list_state_filter(states)
        position = _decode_cursor(
            scope,
            cursor,
            default_page_size=limit,
            max_page_size=self._settings.max_list_limit,
            state_filter=state_filter,
        )
        query = (
            f'ScheduleId STARTS_WITH "{scoped_identity_prefix("scheduled-start-schedule", scope)}"'
        )
        next_position: _ScheduledStartListPosition | None = None
        try:
            iterator = await self._client.list_schedules(
                query=query,
                page_size=position.page_size,
                next_page_token=position.page_token,
                rpc_timeout=self._rpc_timeout,
            )
            records: list[ScheduledStartDescription] = []
            scanned = 0
            page_token = position.page_token
            page_offset = position.page_offset
            while True:
                await iterator.fetch_next_page()
                entries = iterator.current_page
                if entries is None:
                    raise RuntimeError("Temporal schedule iterator did not expose its fetched page")
                if page_offset > len(entries):
                    raise ScheduledStartError(
                        ScheduledStartErrorCode.INVALID_REQUEST,
                        "Scheduled-start cursor is invalid",
                        retryable=False,
                    )
                entry_index = page_offset
                while entry_index < len(entries):
                    if scanned >= self._settings.max_schedules:
                        raise ScheduledStartError(
                            ScheduledStartErrorCode.QUOTA_UNAVAILABLE,
                            "Scheduled-start collection exceeds its configured bound",
                            retryable=False,
                        )
                    batch_size = min(
                        self._settings.describe_concurrency,
                        len(entries) - entry_index,
                        self._settings.max_schedules - scanned,
                    )
                    batch = entries[entry_index : entry_index + batch_size]
                    descriptions = await self._describe_list_entries(batch)
                    for batch_index, description in enumerate(descriptions):
                        scanned += 1
                        record = await self._record_from_description(description, scope)
                        if states is None or record.state in states:
                            records.append(describe_scheduled_start(record))
                        if len(records) >= limit:
                            next_position = _next_list_position(
                                page_token=page_token,
                                page_offset=entry_index + batch_index + 1,
                                page_size=position.page_size,
                                page_length=len(entries),
                                next_page_token=iterator.next_page_token,
                            )
                            break
                    if len(records) >= limit:
                        break
                    entry_index += batch_size
                if len(records) >= limit:
                    break
                next_page_token = iterator.next_page_token
                if next_page_token is None:
                    break
                page_token = next_page_token
                page_offset = 0
        except ScheduledStartError:
            raise
        except Exception as exc:
            raise ScheduledStartError(
                ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal scheduled starts could not be listed",
                retryable=True,
            ) from exc
        return ScheduledStartPage(
            scheduled_starts=tuple(records),
            next_cursor=_encode_cursor(
                scope,
                next_position,
                state_filter=state_filter,
            ),
        )

    async def _describe_list_entries(
        self,
        entries: Sequence[ScheduleListDescription],
    ) -> tuple[ScheduleDescription, ...]:
        async def describe(entry: ScheduleListDescription) -> ScheduleDescription:
            return await self._client.get_schedule_handle(entry.id).describe(
                rpc_timeout=self._rpc_timeout
            )

        tasks: list[asyncio.Task[ScheduleDescription]] = []
        async with asyncio.TaskGroup() as task_group:
            tasks = [task_group.create_task(describe(entry)) for entry in entries]
        return tuple(task.result() for task in tasks)

    async def reschedule(
        self,
        scheduled_start_id: str,
        request: ScheduledStartRescheduleRequest,
        *,
        idempotency_key: IdempotencyKey,
        scope: RuntimeScope,
    ) -> ScheduledStartMutationResult:
        _validate_idempotency_key(idempotency_key)
        request_digest = scheduled_start_request_digest(
            "reschedule",
            scheduled_start_id,
            request.model_dump(mode="json"),
            idempotency_key,
        )
        record = await self._describe_record(scope, scheduled_start_id)
        if (
            isinstance(record, PendingScheduledStartRecord)
            and record.version == request.expected_version + 1
            and record.last_mutation_kind.value == ScheduledStartArbiterCommandKind.RESCHEDULE.value
            and record.last_mutation_digest == request_digest
        ):
            self._record_decision(ScheduledStartMetricOutcome.DUPLICATE, record)
            return ScheduledStartMutationResult(
                status=ScheduledStartMutationStatus.DUPLICATE,
                scheduled_start=describe_scheduled_start(record),
                expected_version=request.expected_version,
            )
        recovered = await self._recover_mutation(
            record,
            scope=scope,
            expected_version=request.expected_version,
            kind=ScheduledStartArbiterCommandKind.RESCHEDULE,
            request_digest=request_digest,
        )
        if recovered is not None:
            return recovered
        self._validate_due_time(request.start_at, self._now())
        pending = self._require_pending_version(record, request.expected_version)
        command = ScheduledStartRescheduleCommand(
            expected_version=request.expected_version,
            start_at=request.start_at,
            request_digest=request_digest,
        )
        return await self._run_mutation(
            ScheduledStartArbiterInitialInput(record=pending, command=command),
            scope=scope,
            accepted_status=ScheduledStartMutationStatus.RESCHEDULED,
        )

    async def cancel(
        self,
        scheduled_start_id: str,
        request: ScheduledStartCancelRequest,
        *,
        idempotency_key: IdempotencyKey,
        scope: RuntimeScope,
    ) -> ScheduledStartMutationResult:
        _validate_idempotency_key(idempotency_key)
        request_digest = scheduled_start_request_digest(
            "cancel",
            scheduled_start_id,
            request.model_dump(mode="json"),
            idempotency_key,
        )
        record = await self._describe_record(scope, scheduled_start_id)
        if (
            isinstance(record, CanceledScheduledStartRecord)
            and record.version == request.expected_version
            and record.last_mutation_digest == request_digest
        ):
            self._record_decision(ScheduledStartMetricOutcome.DUPLICATE, record)
            return ScheduledStartMutationResult(
                status=ScheduledStartMutationStatus.DUPLICATE,
                scheduled_start=describe_scheduled_start(record),
                expected_version=request.expected_version,
            )
        recovered = await self._recover_mutation(
            record,
            scope=scope,
            expected_version=request.expected_version,
            kind=ScheduledStartArbiterCommandKind.CANCEL,
            request_digest=request_digest,
        )
        if recovered is not None:
            return recovered
        pending = self._require_pending_version(record, request.expected_version)
        command = ScheduledStartCancelCommand(
            expected_version=request.expected_version,
            request_digest=request_digest,
        )
        return await self._run_mutation(
            ScheduledStartArbiterInitialInput(record=pending, command=command),
            scope=scope,
            accepted_status=ScheduledStartMutationStatus.CANCELED,
        )

    async def _recover_mutation(
        self,
        record: ScheduledStartRecord,
        *,
        scope: RuntimeScope,
        expected_version: int,
        kind: ScheduledStartArbiterCommandKind,
        request_digest: str,
    ) -> ScheduledStartMutationResult | None:
        handle = self._client.get_workflow_handle(
            make_scheduled_start_arbiter_id(scope, record.scheduled_start_id, expected_version),
            result_type=dict,
        )
        try:
            description = await handle.describe(rpc_timeout=self._rpc_timeout)
        except RPCError as exc:
            if exc.status is RPCStatusCode.NOT_FOUND:
                return None
            raise ScheduledStartError(
                ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal could not identify the scheduled-start winner",
                retryable=True,
            ) from exc
        except Exception as exc:
            raise ScheduledStartError(
                ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal could not identify the scheduled-start winner",
                retryable=True,
            ) from exc
        try:
            winner, saved_digest = await self._arbiter_identity_from_description(
                description,
                scope=scope,
                scheduled_start_id=record.scheduled_start_id,
                version=expected_version,
            )
        except (ValueError, TypeError) as exc:
            raise ScheduledStartError(
                ScheduledStartErrorCode.CONFLICT,
                "Scheduled-start arbitration identity is invalid",
                retryable=False,
            ) from exc
        if winner is not kind or saved_digest != request_digest:
            raise ScheduledStartError(
                ScheduledStartErrorCode.CONFLICT,
                "Another scheduled-start command consumed the expected version",
                retryable=False,
            )
        result = await self._await_arbiter(handle)
        if result is None:
            return ScheduledStartMutationResult(
                status=ScheduledStartMutationStatus.IN_PROGRESS,
                scheduled_start=describe_scheduled_start(record),
                expected_version=expected_version,
            )
        if (
            isinstance(result, ScheduledStartArbiterStale)
            or result.scheduled_start.state is ScheduledStartState.STARTED
        ):
            raise ScheduledStartError(
                ScheduledStartErrorCode.CONFLICT,
                "Scheduled start is no longer pending at the expected version",
                retryable=False,
            )
        self._record_decision(ScheduledStartMetricOutcome.DUPLICATE, record)
        return ScheduledStartMutationResult(
            status=ScheduledStartMutationStatus.DUPLICATE,
            scheduled_start=result.scheduled_start,
            expected_version=expected_version,
        )

    async def claim_due(
        self,
        action_record: PendingScheduledStartRecord,
        *,
        scope: RuntimeScope,
        priority: Priority,
    ) -> ScheduledStartDueClaimResult:
        self._require_record_scope(action_record, scope)
        current = await self._describe_record(scope, action_record.scheduled_start_id)
        if current != action_record:
            self._record_metric(ScheduledStartMetricOutcome.SKIPPED_STALE_FIRE)
            return ScheduledStartDueSkipped(current=describe_scheduled_start(current))
        initial = ScheduledStartArbiterInitialInput(
            record=action_record,
            command=ScheduledStartDueCommand(nominal_time=action_record.start_at),
        )
        _, duplicate = await self._start_arbiter(initial, scope=scope, priority=priority)
        if not duplicate:
            self._record_decision(ScheduledStartMetricOutcome.DUE, action_record)
            return ScheduledStartDueClaimed()
        winner, request_digest = await self._arbiter_identity(
            scope,
            action_record.scheduled_start_id,
            action_record.version,
        )
        if winner is ScheduledStartArbiterCommandKind.DUE and request_digest is None:
            self._record_metric(ScheduledStartMetricOutcome.ARBITER_RECLAIM)
            return ScheduledStartDueClaimed(duplicate=True)
        self._record_metric(ScheduledStartMetricOutcome.SKIPPED_STALE_FIRE)
        return ScheduledStartDueSkipped(current=describe_scheduled_start(current))

    async def prepare_arbiter(
        self,
        initial: ScheduledStartArbiterInitialInput,
        *,
        scope: RuntimeScope,
    ) -> ScheduledStartArbiterPreparation:
        self._require_record_scope(initial.record, scope)
        current = await self._describe_record(scope, initial.record.scheduled_start_id)
        if current != initial.record:
            return ScheduledStartArbiterPreparedStale(current=describe_scheduled_start(current))
        command = initial.command
        existing = await self._starter.find_existing(
            current.workflow_name,
            current.business_request_id,
            scope=scope,
        )
        if existing is not None:
            claimed = claim_scheduled_start(
                current,
                expected_version=current.version,
                claimed_at=self._now(),
            )
            if claimed is None:
                return ScheduledStartArbiterPreparedStale(current=describe_scheduled_start(current))
            started = complete_scheduled_start(
                claimed,
                ScheduledStartRun(
                    workflow_id=existing.workflow_id,
                    run_id=existing.run_id,
                    definition_digest=existing.definition_digest,
                    artifact_identity=existing.artifact_identity,
                    environment_snapshot_digest=(existing.environment_snapshot_digest),
                    execution_configuration=existing.execution_configuration,
                ),
                completed_at=self._now(),
            )
            return ScheduledStartArbiterPreparedProjection(
                terminal=ScheduledStartArbiterTerminalInput(
                    projection=started,
                    consumed_version=current.version,
                    winner=command.kind,
                    request_digest=(
                        command.request_digest
                        if not isinstance(command, ScheduledStartDueCommand)
                        else None
                    ),
                )
            )
        if isinstance(command, ScheduledStartCancelCommand):
            canceled, _ = cancel_scheduled_start(
                current,
                ScheduledStartCancelRequest(expected_version=command.expected_version),
                request_digest=command.request_digest,
                updated_at=self._now(),
            )
            return ScheduledStartArbiterPreparedProjection(
                terminal=ScheduledStartArbiterTerminalInput(
                    projection=canceled,
                    consumed_version=command.expected_version,
                    winner=command.kind,
                    request_digest=command.request_digest,
                )
            )
        if isinstance(command, ScheduledStartRescheduleCommand):
            rescheduled, _ = reschedule_scheduled_start(
                current,
                ScheduledStartRescheduleRequest(
                    start_at=command.start_at,
                    expected_version=command.expected_version,
                ),
                request_digest=command.request_digest,
                updated_at=self._now(),
            )
            return ScheduledStartArbiterPreparedProjection(
                terminal=ScheduledStartArbiterTerminalInput(
                    projection=rescheduled,
                    consumed_version=command.expected_version,
                    winner=command.kind,
                    request_digest=command.request_digest,
                )
            )
        claimed = claim_scheduled_start(
            current,
            expected_version=current.version,
            claimed_at=self._now(),
        )
        if claimed is None:
            return ScheduledStartArbiterPreparedStale(current=describe_scheduled_start(current))
        try:
            target = await self._starter.resolve_active_target(scope, claimed.workflow_name)
            if not LEGACY_LOCAL_UNSCOPED_POLICY.owns(target.scope_digest, scope):
                raise WorkflowStartError(
                    StartErrorCode.DEFINITION_UNAVAILABLE,
                    "Resolved scheduled-start target belongs to another scope",
                    retryable=False,
                )
            self._starter.validate_resolved_input(
                claimed.workflow_name,
                claimed.input,
                target,
            )
        except WorkflowStartError as exc:
            failed = fail_scheduled_start(
                claimed,
                _dispatch_failure_code(exc),
                completed_at=self._now(),
            )
            return ScheduledStartArbiterPreparedProjection(
                terminal=ScheduledStartArbiterTerminalInput(
                    projection=failed,
                    consumed_version=claimed.claimed_version,
                    winner=ScheduledStartArbiterCommandKind.DUE,
                )
            )
        return ScheduledStartArbiterPreparedDue(
            record=claimed,
            target=ScheduledStartResolvedTarget.from_target(target),
        )

    async def attempt_dispatch(
        self,
        attempt: ScheduledStartAttemptRequest,
        *,
        scope: RuntimeScope,
        priority: Priority,
    ) -> ScheduledStartAttemptResult:
        record = attempt.record
        self._require_record_scope(record, scope)
        target = attempt.target.to_target()
        try:
            result = await self._starter.start_accepted_api(
                StartWorkflowRequest(
                    workflow_name=record.workflow_name,
                    business_request_id=record.business_request_id,
                    input=record.input,
                    source=ControlApiSourceIdentity(request_id=record.business_request_id),
                    definition_digest=target.manifest.definition_digest,
                ),
                target,
                trigger_name=record.trigger_name,
                scope_binding=TrustedScopeBinding.create(
                    kind=ScopeBindingKind.API,
                    scope=scope,
                    binding_id=record.create_request_digest,
                ),
                priority=priority,
            )
        except WorkflowStartError as exc:
            if exc.request_certainty is StartRequestCertainty.AUTHORITATIVE_REJECTION:
                return ScheduledStartAttemptRejected()
            return ScheduledStartAttemptAmbiguous()
        run = ScheduledStartRun(
            workflow_id=result.workflow_id,
            run_id=result.run_id,
            definition_digest=result.definition_digest,
            artifact_identity=result.artifact_identity,
            environment_snapshot_digest=result.environment_snapshot_digest,
            execution_configuration=result.execution_configuration,
        )
        return ScheduledStartAttemptAccepted(
            outcome=(
                ScheduledStartAttemptOutcome.ACCEPTED
                if result.status is StartStatus.STARTED
                else ScheduledStartAttemptOutcome.DUPLICATE
            ),
            run=run,
        )

    async def commit_arbiter(
        self,
        terminal: ScheduledStartArbiterTerminalInput,
        *,
        scope: RuntimeScope,
    ) -> ScheduledStartArbiterApplied:
        projection = terminal.projection
        self._require_record_scope(projection, scope)

        async def update(input: ScheduleUpdateInput) -> ScheduleUpdate | None:
            current = await self._record_from_description(input.description, scope)
            if terminal.winner is ScheduledStartArbiterCommandKind.RESCHEDULE and isinstance(
                projection, PendingScheduledStartRecord
            ):
                if current.version >= projection.version:
                    return None
            elif current == projection:
                return None
            elif not (
                isinstance(current, PendingScheduledStartRecord)
                and current.version == terminal.consumed_version
            ):
                raise ScheduledStartError(
                    ScheduledStartErrorCode.CONFLICT,
                    "Scheduled-start projection no longer belongs to this arbiter",
                    retryable=False,
                )
            return ScheduleUpdate(
                schedule=self._schedule(
                    projection,
                    scope,
                    self._workload_policy.priority(scope, projection.workload_class),
                )
            )

        await self._update(scope, projection.scheduled_start_id, update)
        if isinstance(projection, StartedScheduledStartRecord):
            metric = ScheduledStartMetricOutcome.STARTED
            if terminal.winner is not ScheduledStartArbiterCommandKind.DUE:
                self._record_metric(ScheduledStartMetricOutcome.ARBITER_CONFLICT)
        elif isinstance(projection, FailedScheduledStartRecord):
            metric = ScheduledStartMetricOutcome.FAILED
        elif isinstance(projection, CanceledScheduledStartRecord):
            metric = ScheduledStartMetricOutcome.CANCELED
        else:
            metric = ScheduledStartMetricOutcome.RESCHEDULED
        self._record_decision(metric, projection)
        return ScheduledStartArbiterApplied(
            consumed_version=terminal.consumed_version,
            winner=terminal.winner,
            request_digest=terminal.request_digest,
            scheduled_start=describe_scheduled_start(projection),
        )

    async def _run_mutation(
        self,
        initial: ScheduledStartArbiterInitialInput,
        *,
        scope: RuntimeScope,
        accepted_status: ScheduledStartMutationStatus,
    ) -> ScheduledStartMutationResult:
        command = initial.command
        if isinstance(command, ScheduledStartDueCommand):
            raise TypeError("Due arbitration is not a public mutation")
        priority = self._workload_policy.priority(scope, initial.record.workload_class)
        handle, duplicate = await self._start_arbiter(
            initial,
            scope=scope,
            priority=priority,
        )
        if duplicate:
            winner, request_digest = await self._arbiter_identity(
                scope,
                initial.record.scheduled_start_id,
                initial.record.version,
            )
            if winner is not command.kind or request_digest != command.request_digest:
                self._record_metric(ScheduledStartMetricOutcome.ARBITER_CONFLICT)
                raise ScheduledStartError(
                    ScheduledStartErrorCode.CONFLICT,
                    "Another scheduled-start command consumed the expected version",
                    retryable=False,
                )
        result = await self._await_arbiter(handle)
        if result is None:
            self._record_metric(ScheduledStartMetricOutcome.MUTATION_WAIT_TIMEOUT)
            return ScheduledStartMutationResult(
                status=ScheduledStartMutationStatus.IN_PROGRESS,
                scheduled_start=describe_scheduled_start(initial.record),
                expected_version=command.expected_version,
            )
        if isinstance(result, ScheduledStartArbiterStale):
            raise ScheduledStartError(
                ScheduledStartErrorCode.CONFLICT,
                "Scheduled start is no longer pending at the expected version",
                retryable=False,
            )
        if (
            result.scheduled_start.state is ScheduledStartState.STARTED
            and result.winner is not ScheduledStartArbiterCommandKind.DUE
        ):
            raise ScheduledStartError(
                ScheduledStartErrorCode.CONFLICT,
                "Scheduled start already produced a workflow run",
                retryable=False,
            )
        status = ScheduledStartMutationStatus.DUPLICATE if duplicate else accepted_status
        if duplicate:
            record = await self._describe_record(
                scope,
                initial.record.scheduled_start_id,
            )
            self._record_decision(ScheduledStartMetricOutcome.DUPLICATE, record)
        return ScheduledStartMutationResult(
            status=status,
            scheduled_start=result.scheduled_start,
            expected_version=command.expected_version,
        )

    async def _start_arbiter(
        self,
        initial: ScheduledStartArbiterInitialInput,
        *,
        scope: RuntimeScope,
        priority: Priority,
    ) -> tuple[WorkflowHandle[dict[str, object], dict[str, object]], bool]:
        workflow_id = make_scheduled_start_arbiter_id(
            scope,
            initial.record.scheduled_start_id,
            initial.record.version,
        )
        try:
            handle = await self._client.start_workflow(
                SCHEDULED_START_ARBITER_WORKFLOW_TYPE,
                initial.model_dump(mode="json"),
                id=workflow_id,
                task_queue=self._task_queue,
                result_type=dict,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                memo=_arbiter_memo(initial),
                priority=priority,
                rpc_timeout=self._rpc_timeout,
            )
            return handle, False
        except WorkflowAlreadyStartedError:
            return (
                self._client.get_workflow_handle(workflow_id, result_type=dict),
                True,
            )
        except Exception as exc:
            raise ScheduledStartError(
                ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal could not claim the scheduled-start version",
                retryable=True,
            ) from exc

    async def _arbiter_identity(
        self,
        scope: RuntimeScope,
        scheduled_start_id: str,
        version: int,
    ) -> tuple[ScheduledStartArbiterCommandKind, str | None]:
        workflow_id = make_scheduled_start_arbiter_id(
            scope,
            scheduled_start_id,
            version,
        )
        try:
            description = await self._client.get_workflow_handle(workflow_id).describe(
                rpc_timeout=self._rpc_timeout
            )
            return await self._arbiter_identity_from_description(
                description,
                scope=scope,
                scheduled_start_id=scheduled_start_id,
                version=version,
            )
        except ScheduledStartError:
            raise
        except Exception as exc:
            raise ScheduledStartError(
                ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal could not identify the scheduled-start winner",
                retryable=True,
            ) from exc

    async def _authoritative_description(
        self,
        record: PendingScheduledStartRecord,
        scope: RuntimeScope,
    ) -> ScheduledStartDescription:
        workflow_id = make_scheduled_start_arbiter_id(
            scope,
            record.scheduled_start_id,
            record.version,
        )
        handle = self._client.get_workflow_handle(workflow_id, result_type=dict)
        try:
            execution = await handle.describe(rpc_timeout=self._rpc_timeout)
        except RPCError as exc:
            if exc.status is RPCStatusCode.NOT_FOUND:
                return describe_scheduled_start(record)
            raise ScheduledStartError(
                ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal could not read scheduled-start arbitration state",
                retryable=True,
            ) from exc
        except Exception as exc:
            raise ScheduledStartError(
                ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal could not read scheduled-start arbitration state",
                retryable=True,
            ) from exc
        try:
            winner, _ = await self._arbiter_identity_from_description(
                execution,
                scope=scope,
                scheduled_start_id=record.scheduled_start_id,
                version=record.version,
            )
        except Exception as exc:
            raise ScheduledStartError(
                ScheduledStartErrorCode.CONFLICT,
                "Temporal arbiter is not a valid owned scheduled-start workflow",
                retryable=False,
            ) from exc
        if execution.status is WorkflowExecutionStatus.RUNNING:
            if winner is not ScheduledStartArbiterCommandKind.DUE:
                return describe_scheduled_start(record)
            arbiter_started_at = execution.start_time.astimezone(UTC).replace(microsecond=0)
            claimed = claim_scheduled_start(
                record,
                expected_version=record.version,
                claimed_at=max(record.updated_at, arbiter_started_at),
            )
            if claimed is None:
                raise RuntimeError("Pending scheduled start could not be projected as due")
            return describe_scheduled_start(claimed)
        if execution.status is WorkflowExecutionStatus.COMPLETED:
            try:
                raw_result = await handle.result(rpc_timeout=self._rpc_timeout)
            except Exception as exc:
                raise ScheduledStartError(
                    ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                    "Temporal could not read the scheduled-start arbitration result",
                    retryable=True,
                ) from exc
            result = _parse_arbiter_result(raw_result)
            if isinstance(result, ScheduledStartArbiterApplied):
                return result.scheduled_start
            return result.current
        raise ScheduledStartError(
            ScheduledStartErrorCode.CONFLICT,
            "Scheduled-start arbiter closed without an authoritative result",
            retryable=False,
        )

    async def _arbiter_identity_from_description(
        self,
        description: WorkflowExecutionDescription,
        *,
        scope: RuntimeScope,
        scheduled_start_id: str,
        version: int,
    ) -> tuple[ScheduledStartArbiterCommandKind, str | None]:
        owner, memo_id, memo_scope, raw_command, request_digest, raw_version = await asyncio.gather(
            description.memo_value(
                MEMO_SCHEDULED_START_OWNER,
                None,
                type_hint=str,
            ),
            description.memo_value(
                MEMO_SCHEDULED_START_ID,
                None,
                type_hint=str,
            ),
            description.memo_value(MEMO_SCOPE_DIGEST, None, type_hint=str),
            description.memo_value(
                MEMO_SCHEDULED_START_ARBITER_COMMAND,
                None,
                type_hint=str,
            ),
            description.memo_value(
                MEMO_SCHEDULED_START_ARBITER_REQUEST_DIGEST,
                None,
                type_hint=str,
            ),
            description.memo_value(
                MEMO_SCHEDULED_START_ARBITER_VERSION,
                None,
                type_hint=str,
            ),
        )
        if (
            owner != SCHEDULED_START_OWNER
            or memo_id != scheduled_start_id
            or memo_scope != scope.digest
            or raw_version != str(version)
        ):
            raise ValueError("Arbiter memo does not match its scheduled start")
        command = ScheduledStartArbiterCommandKind(raw_command)
        if command is ScheduledStartArbiterCommandKind.DUE:
            if request_digest is not None:
                raise ValueError("Due arbiter unexpectedly retained a request digest")
        elif not isinstance(request_digest, str):
            raise ValueError("Mutation arbiter omitted its request digest")
        return command, request_digest

    async def _await_arbiter(
        self,
        handle: WorkflowHandle[dict[str, object], dict[str, object]],
    ) -> ScheduledStartArbiterResult | None:
        try:
            raw_result = await asyncio.wait_for(
                handle.result(rpc_timeout=self._rpc_timeout),
                timeout=self._settings.mutation_wait_seconds,
            )
        except TimeoutError:
            return None
        except Exception:
            logger.warning("Scheduled-start arbiter result is not yet available", exc_info=True)
            return None
        return _parse_arbiter_result(raw_result)

    @staticmethod
    def _require_pending_version(
        record: ScheduledStartRecord,
        expected_version: int,
    ) -> PendingScheduledStartRecord:
        if not isinstance(record, PendingScheduledStartRecord) or (
            record.version != expected_version
        ):
            raise ScheduledStartError(
                ScheduledStartErrorCode.CONFLICT,
                "Scheduled start is no longer pending at the expected version",
                retryable=False,
            )
        return record

    @staticmethod
    def _require_record_scope(
        record: ScheduledStartRecord,
        scope: RuntimeScope,
    ) -> None:
        if record.scope_digest != scope.digest:
            raise ScheduledStartError(
                ScheduledStartErrorCode.CONFLICT,
                "Scheduled-start scope is inconsistent",
                retryable=False,
            )

    async def _update(
        self,
        scope: RuntimeScope,
        scheduled_start_id: str,
        updater: Callable[
            [ScheduleUpdateInput],
            Awaitable[ScheduleUpdate | None],
        ],
    ) -> None:
        _require_scoped_scheduled_start_id(scope, scheduled_start_id)
        try:
            await self._client.get_schedule_handle(
                make_scheduled_start_schedule_id(scope, scheduled_start_id)
            ).update(updater, rpc_timeout=self._rpc_timeout)
        except ScheduledStartError:
            raise
        except RPCError as exc:
            if exc.status is RPCStatusCode.NOT_FOUND:
                raise ScheduledStartError(
                    ScheduledStartErrorCode.NOT_FOUND,
                    "Scheduled start does not exist",
                    retryable=False,
                ) from exc
            raise ScheduledStartError(
                ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal could not update the scheduled start",
                retryable=True,
            ) from exc
        except Exception as exc:
            raise ScheduledStartError(
                ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal could not update the scheduled start",
                retryable=True,
            ) from exc

    async def _count_pending(self, scope: RuntimeScope) -> int:
        page = await self.list(
            scope=scope,
            limit=self._settings.default_list_limit,
            states=frozenset({ScheduledStartState.SCHEDULED}),
        )
        count = len(page.scheduled_starts)
        cursor = page.next_cursor
        while cursor is not None:
            if count >= self._settings.max_pending_per_scope:
                return count
            page = await self.list(
                scope=scope,
                limit=self._settings.default_list_limit,
                cursor=cursor,
                states=frozenset({ScheduledStartState.SCHEDULED}),
            )
            count += len(page.scheduled_starts)
            cursor = page.next_cursor
        return count

    async def _describe_record(
        self,
        scope: RuntimeScope,
        scheduled_start_id: str,
    ) -> ScheduledStartRecord:
        _require_scoped_scheduled_start_id(scope, scheduled_start_id)
        try:
            description = await self._client.get_schedule_handle(
                make_scheduled_start_schedule_id(scope, scheduled_start_id)
            ).describe(rpc_timeout=self._rpc_timeout)
        except RPCError as exc:
            if exc.status is RPCStatusCode.NOT_FOUND:
                raise ScheduledStartError(
                    ScheduledStartErrorCode.NOT_FOUND,
                    "Scheduled start does not exist",
                    retryable=False,
                ) from exc
            raise ScheduledStartError(
                ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal could not describe the scheduled start",
                retryable=True,
            ) from exc
        except Exception as exc:
            raise ScheduledStartError(
                ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal could not describe the scheduled start",
                retryable=True,
            ) from exc
        return await self._record_from_description(description, scope)

    async def _existing_record(
        self,
        scope: RuntimeScope,
        scheduled_start_id: str,
    ) -> ScheduledStartRecord | None:
        try:
            return await self._describe_record(scope, scheduled_start_id)
        except ScheduledStartError as exc:
            if exc.code is ScheduledStartErrorCode.NOT_FOUND:
                return None
            raise

    async def _record_from_description(
        self,
        description: ScheduleDescription,
        scope: RuntimeScope,
    ) -> ScheduledStartRecord:
        try:
            owner, memo_id, memo_scope = await asyncio.gather(
                description.memo_value(MEMO_SCHEDULED_START_OWNER, None, type_hint=str),
                description.memo_value(MEMO_SCHEDULED_START_ID, None, type_hint=str),
                description.memo_value(MEMO_SCOPE_DIGEST, None, type_hint=str),
            )
            action = description.schedule.action
            if (
                owner != SCHEDULED_START_OWNER
                or not isinstance(memo_id, str)
                or memo_scope != scope.digest
                or not isinstance(action, ScheduleActionStartWorkflow)
                or action.workflow != SCHEDULED_START_DUE_WORKFLOW_TYPE
                or action.task_queue != self._task_queue
                or len(action.args) != 1
            ):
                raise ValueError("Temporal schedule is not an owned scheduled start")
            raw_action = await _decode_action_argument(description, action.args[0])
            record = ScheduledStartDueInput.model_validate(raw_action).record
            if (
                record.scheduled_start_id != memo_id
                or record.scope_digest != scope.digest
                or description.id
                != make_scheduled_start_schedule_id(scope, record.scheduled_start_id)
                or action.id
                != make_scheduled_start_due_id(
                    scope,
                    record.scheduled_start_id,
                    record.version,
                )
            ):
                raise ValueError("Scheduled-start Temporal identity is inconsistent")
            return record
        except ScheduledStartError:
            raise
        except Exception as exc:
            raise ScheduledStartError(
                ScheduledStartErrorCode.CONFLICT,
                "Temporal schedule is not a valid owned scheduled start",
                retryable=False,
            ) from exc

    async def _description_for_handle(self, handle: ScheduleHandle) -> ScheduleDescription:
        try:
            return await handle.describe(rpc_timeout=self._rpc_timeout)
        except Exception as exc:
            raise ScheduledStartError(
                ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal accepted the scheduled start but could not describe it",
                retryable=True,
            ) from exc

    def _schedule(
        self,
        record: ScheduledStartProjectionRecord,
        scope: RuntimeScope,
        priority: Priority,
    ) -> Schedule:
        pending = isinstance(record, PendingScheduledStartRecord)
        terminal = isinstance(
            record,
            (
                StartedScheduledStartRecord,
                CanceledScheduledStartRecord,
                FailedScheduledStartRecord,
            ),
        )
        return Schedule(
            action=ScheduleActionStartWorkflow(
                SCHEDULED_START_DUE_WORKFLOW_TYPE,
                ScheduledStartDueInput(record=record).model_dump(mode="json"),
                id=make_scheduled_start_due_id(
                    scope,
                    record.scheduled_start_id,
                    record.version,
                ),
                task_queue=self._task_queue,
                retry_policy=RetryPolicy(
                    maximum_interval=timedelta(seconds=self._settings.recovery_max_interval_seconds)
                ),
                memo=_entity_memo(record),
                priority=priority,
            ),
            spec=ScheduleSpec(
                calendars=(_calendar_instant(record.start_at),),
                time_zone_name="UTC",
            ),
            policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            state=ScheduleState(
                note=f"scheduled-start:{record.state.value}:v{record.version}",
                paused=terminal,
                limited_actions=True,
                remaining_actions=1 if pending else 0,
            ),
        )

    def _validate_due_time(self, start_at: datetime, now: datetime) -> None:
        earliest = now - timedelta(seconds=self._settings.clock_skew_seconds)
        latest = now + timedelta(seconds=self._settings.max_horizon_seconds)
        if start_at < earliest or start_at > latest:
            raise ScheduledStartError(
                ScheduledStartErrorCode.INVALID_REQUEST,
                "Scheduled-start due time is outside the configured window",
                retryable=False,
            )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise RuntimeError("Scheduled-start clock returned a timezone-naive value")
        return value.astimezone(UTC).replace(microsecond=0)

    def _record_decision(
        self,
        outcome: ScheduledStartMetricOutcome,
        record: ScheduledStartRecord,
    ) -> None:
        if self._metrics is not None:
            self._metrics.record_scheduled_start(outcome)
        self._decision_recorder.record(
            ScheduledStartDecision(
                outcome=ScheduledStartDecisionOutcome(outcome.value),
                scheduled_start_identity_digest=safe_identity_digest(
                    "scheduled-start-audit",
                    record.scheduled_start_id,
                ),
                scope_digest=record.scope_digest,
                workflow_name=record.workflow_name,
                trigger_name=record.trigger_name,
                start_at=record.start_at,
                workload_class=record.workload_class,
                state=record.state,
                version=record.version,
                failure_code=(
                    record.failure_code if isinstance(record, FailedScheduledStartRecord) else None
                ),
            )
        )

    def _record_metric(self, outcome: ScheduledStartMetricOutcome) -> None:
        if self._metrics is not None:
            self._metrics.record_scheduled_start(outcome)


async def _decode_action_argument(
    description: ScheduleDescription,
    value: object,
) -> object:
    if not isinstance(value, Payload):
        return value
    decoded = await description.data_converter.decode([value], [dict])
    if len(decoded) != 1:
        raise ValueError("Scheduled-start action payload could not be decoded")
    return decoded[0]


def _calendar_instant(start_at: datetime) -> ScheduleCalendarSpec:
    value = start_at.astimezone(UTC)
    return ScheduleCalendarSpec(
        second=(ScheduleRange(value.second),),
        minute=(ScheduleRange(value.minute),),
        hour=(ScheduleRange(value.hour),),
        day_of_month=(ScheduleRange(value.day),),
        month=(ScheduleRange(value.month),),
        year=(ScheduleRange(value.year),),
    )


def _entity_memo(record: ScheduledStartRecord) -> Mapping[str, str]:
    return {
        MEMO_SCHEDULED_START_OWNER: SCHEDULED_START_OWNER,
        MEMO_SCHEDULED_START_ID: record.scheduled_start_id,
        MEMO_SCHEDULED_START_FORMAT_VERSION: str(SCHEDULED_START_FORMAT_VERSION),
        MEMO_SCOPE_DIGEST: record.scope_digest,
    }


def _arbiter_memo(initial: ScheduledStartArbiterInitialInput) -> Mapping[str, str]:
    memo = {
        **_entity_memo(initial.record),
        MEMO_SCHEDULED_START_ARBITER_COMMAND: initial.command.kind.value,
        MEMO_SCHEDULED_START_ARBITER_VERSION: str(initial.record.version),
    }
    if isinstance(
        initial.command,
        (ScheduledStartCancelCommand, ScheduledStartRescheduleCommand),
    ):
        memo[MEMO_SCHEDULED_START_ARBITER_REQUEST_DIGEST] = initial.command.request_digest
    return memo


def _validate_idempotency_key(value: str) -> None:
    if not value or len(value) > MAX_IDENTIFIER_LENGTH:
        raise ScheduledStartError(
            ScheduledStartErrorCode.INVALID_REQUEST,
            "Scheduled-start idempotency key is invalid",
            retryable=False,
        )


def _require_scoped_scheduled_start_id(scope: RuntimeScope, value: str) -> None:
    if not identity_belongs_to_scope(value, "scheduled-start", scope):
        raise ScheduledStartError(
            ScheduledStartErrorCode.NOT_FOUND,
            "Scheduled start does not exist",
            retryable=False,
        )


def _start_error(exc: WorkflowStartError) -> ScheduledStartError:
    code = {
        StartErrorCode.INVALID_REQUEST: ScheduledStartErrorCode.INVALID_REQUEST,
        StartErrorCode.UNKNOWN_WORKFLOW: ScheduledStartErrorCode.TRIGGER_UNAVAILABLE,
        StartErrorCode.DEFINITION_UNAVAILABLE: ScheduledStartErrorCode.TRIGGER_UNAVAILABLE,
        StartErrorCode.INCOMPATIBLE_WORKER: ScheduledStartErrorCode.TRIGGER_UNAVAILABLE,
        StartErrorCode.INPUT_REJECTED: ScheduledStartErrorCode.INPUT_REJECTED,
        StartErrorCode.CONFIGURATION_ERROR: ScheduledStartErrorCode.TRIGGER_UNAVAILABLE,
        StartErrorCode.TRIGGER_UNAVAILABLE: ScheduledStartErrorCode.TRIGGER_UNAVAILABLE,
        StartErrorCode.TRIGGER_PAUSED: ScheduledStartErrorCode.TRIGGER_PAUSED,
        StartErrorCode.TEMPORAL_UNAVAILABLE: ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
    }[exc.code]
    return ScheduledStartError(code, str(exc), retryable=exc.retryable)


def _dispatch_failure_code(exc: WorkflowStartError) -> ScheduledStartFailureCode:
    if exc.code is StartErrorCode.INPUT_REJECTED:
        return ScheduledStartFailureCode.CONTRACT_DRIFT
    if exc.code is StartErrorCode.INCOMPATIBLE_WORKER:
        return ScheduledStartFailureCode.INCOMPATIBLE_WORKER
    return ScheduledStartFailureCode.TARGET_UNAVAILABLE


def _next_list_position(
    *,
    page_token: bytes | None,
    page_offset: int,
    page_size: int,
    page_length: int,
    next_page_token: bytes | None,
) -> _ScheduledStartListPosition | None:
    if page_offset < page_length:
        return _ScheduledStartListPosition(
            page_token=page_token,
            page_offset=page_offset,
            page_size=page_size,
        )
    if next_page_token is None:
        return None
    return _ScheduledStartListPosition(
        page_token=next_page_token,
        page_offset=0,
        page_size=page_size,
    )


def _encode_cursor(
    scope: RuntimeScope,
    position: _ScheduledStartListPosition | None,
    *,
    state_filter: tuple[ScheduledStartState, ...] | None,
) -> str | None:
    if position is None:
        return None
    encoded_token = (
        base64.urlsafe_b64encode(position.page_token).decode("ascii")
        if position.page_token is not None
        else None
    )
    payload = _ScheduledStartListCursorPayload(
        page_token=encoded_token,
        page_offset=position.page_offset,
        page_size=position.page_size,
        state_filter=state_filter,
    )
    return encode_scope_cursor(scope, payload.model_dump(mode="json"))


def _decode_cursor(
    scope: RuntimeScope,
    cursor: str | None,
    *,
    default_page_size: int,
    max_page_size: int,
    state_filter: tuple[ScheduledStartState, ...] | None,
) -> _ScheduledStartListPosition:
    if cursor is None:
        return _ScheduledStartListPosition(
            page_token=None,
            page_offset=0,
            page_size=default_page_size,
        )
    try:
        payload = _ScheduledStartListCursorPayload.model_validate(
            decode_scope_cursor(scope, cursor)
        )
        page_token = (
            base64.b64decode(
                payload.page_token.encode("ascii"),
                altchars=b"-_",
                validate=True,
            )
            if payload.page_token is not None
            else None
        )
        if page_token == b"" or (page_token is None and payload.page_offset == 0):
            raise ValueError("Scheduled-start cursor points to the beginning of the collection")
        page_size = payload.page_size or default_page_size
        if page_size > max_page_size:
            raise ValueError("Scheduled-start cursor page size exceeds the configured maximum")
        if payload.page_size is not None and payload.state_filter != state_filter:
            raise ValueError("Scheduled-start cursor state filter does not match the request")
    except ValueError as exc:
        raise ScheduledStartError(
            ScheduledStartErrorCode.INVALID_REQUEST,
            "Scheduled-start cursor is invalid",
            retryable=False,
        ) from exc
    return _ScheduledStartListPosition(
        page_token=page_token,
        page_offset=payload.page_offset,
        page_size=page_size,
    )


def _list_state_filter(
    states: frozenset[ScheduledStartState] | None,
) -> tuple[ScheduledStartState, ...] | None:
    if states is None:
        return None
    return tuple(sorted(states, key=lambda state: state.value))
