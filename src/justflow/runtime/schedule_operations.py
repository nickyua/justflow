"""Bounded operator controls for managed Temporal schedules."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Protocol

from pydantic import Field
from temporalio.client import (
    Client,
    ScheduleActionExecutionStartWorkflow,
    ScheduleActionResult,
    ScheduleActionStartWorkflow,
    ScheduleBackfill,
    ScheduleDescription,
)
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from justflow.config.grammar import MAX_IDENTIFIER_LENGTH
from justflow.config.schedules import (
    CalendarScheduleSpec,
    CronScheduleSpec,
    IntervalScheduleSpec,
    ScheduleOverlapPolicy,
)
from justflow.config.settings import ScheduleSettings, Settings
from justflow.config.triggers import ScheduleTriggerDeclaration, TriggersConfig
from justflow.definitions.catalog import DefinitionCatalogStore
from justflow.definitions.routing import WorkerDeploymentRouter
from justflow.provenance import ExecutionConfigurationIdentity, WorkerArtifactIdentity
from justflow.runtime.metrics import (
    MetricsRegistry,
    ScheduleMetricOperation,
    ScheduleMetricOutcome,
)
from justflow.runtime.schedule_reconciler import (
    ScheduleApplyResult,
    ScheduleReconciler,
    ScheduleReconciliationError,
    observed_schedule_from_description,
)
from justflow.runtime.schedules import (
    SCHEDULE_DISPATCH_WORKFLOW_TYPE,
    ScheduleDispatchPlan,
    UnscopedScheduleDecision,
    make_schedule_id,
    make_schedule_run_now_workflow_id,
)
from justflow.runtime.starter import StrictRuntimeModel
from justflow.scope import LEGACY_LOCAL_UNSCOPED_POLICY, LOCAL_RUNTIME_SCOPE, RuntimeScope
from justflow.sdk.message_contract import DEFINITION_DIGEST_LENGTH

logger = logging.getLogger(__name__)
OPERATOR_PAUSE_NOTE = "Paused by an authenticated Justflow operator"
OPERATOR_RESUME_NOTE = "Resumed by an authenticated Justflow operator"
MAX_TEMPORAL_IDENTITY_LENGTH = 1_000
MAX_SCHEDULE_DIGEST_LENGTH = 128
RUN_NOW_IDENTITY_DIGEST_LENGTH = 64


class ScheduleOperationErrorCode(str, Enum):
    UNKNOWN_SCHEDULE = "unknown_schedule"
    NOT_MANAGED = "not_managed"
    INVALID_OPERATION = "invalid_operation"
    CONFIRMATION_REQUIRED = "confirmation_required"
    TEMPORAL_UNAVAILABLE = "temporal_unavailable"


class ScheduleOperationError(Exception):
    def __init__(self, code: ScheduleOperationErrorCode, message: str) -> None:
        self.code = code
        super().__init__(message)


class TriggerRunNowStatus(str, Enum):
    ACCEPTED = "accepted"
    ALREADY_ACCEPTED = "already_accepted"


@dataclass(frozen=True, kw_only=True)
class _ManagedSchedule:
    description: ScheduleDescription
    schedule_name: str
    desired_digest: str
    plan: ScheduleDispatchPlan


class ScheduleRecentAction(StrictRuntimeModel):
    scheduled_at: datetime
    started_at: datetime
    workflow_id: str = Field(min_length=1, max_length=MAX_TEMPORAL_IDENTITY_LENGTH)
    run_id: str = Field(min_length=1, max_length=MAX_TEMPORAL_IDENTITY_LENGTH)
    outcome: str = "accepted"


class ManagedScheduleDescription(StrictRuntimeModel):
    schedule_name: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    schedule_id: str = Field(min_length=1, max_length=MAX_TEMPORAL_IDENTITY_LENGTH)
    desired_digest: str = Field(min_length=1, max_length=MAX_SCHEDULE_DIGEST_LENGTH)
    paused: bool
    workflow_name: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    definition_digest: str = Field(
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
    )
    artifact_identity: WorkerArtifactIdentity
    environment_snapshot_digest: str = Field(
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
    )
    overlap_policy: ScheduleOverlapPolicy
    next_run_times: tuple[datetime, ...]
    recent_actions: tuple[ScheduleRecentAction, ...]


class ScheduleControlService(Protocol):
    async def pause(
        self,
        schedule_name: str,
        *,
        scope: RuntimeScope,
    ) -> ManagedScheduleDescription: ...

    async def resume(
        self,
        schedule_name: str,
        *,
        scope: RuntimeScope,
    ) -> ManagedScheduleDescription: ...

    async def trigger_now(
        self,
        schedule_name: str,
        *,
        request_identity_digest: str,
        scope: RuntimeScope,
    ) -> TriggerRunNowStatus: ...

    async def delete(
        self,
        schedule_name: str,
        *,
        confirmation: str,
        scope: RuntimeScope,
    ) -> None: ...


class BoundScheduleControlService(Protocol):
    async def pause(self, schedule_name: str) -> ManagedScheduleDescription: ...

    async def resume(self, schedule_name: str) -> ManagedScheduleDescription: ...

    async def trigger_now(
        self,
        schedule_name: str,
        *,
        request_identity_digest: str,
    ) -> TriggerRunNowStatus: ...

    async def delete(self, schedule_name: str, *, confirmation: str) -> None: ...


class ScopedScheduleControlService:
    def __init__(
        self,
        operators: Mapping[RuntimeScope, BoundScheduleControlService],
    ) -> None:
        indexed: dict[str, tuple[RuntimeScope, BoundScheduleControlService]] = {}
        for scope, operator in operators.items():
            if scope.digest in indexed:
                raise ValueError("Schedule controls have a duplicate runtime scope")
            indexed[scope.digest] = (scope, operator)
        self._operators = indexed

    async def pause(
        self,
        schedule_name: str,
        *,
        scope: RuntimeScope,
    ) -> ManagedScheduleDescription:
        return await self._operator(scope).pause(schedule_name)

    async def resume(
        self,
        schedule_name: str,
        *,
        scope: RuntimeScope,
    ) -> ManagedScheduleDescription:
        return await self._operator(scope).resume(schedule_name)

    async def trigger_now(
        self,
        schedule_name: str,
        *,
        request_identity_digest: str,
        scope: RuntimeScope,
    ) -> TriggerRunNowStatus:
        return await self._operator(scope).trigger_now(
            schedule_name,
            request_identity_digest=request_identity_digest,
        )

    async def delete(
        self,
        schedule_name: str,
        *,
        confirmation: str,
        scope: RuntimeScope,
    ) -> None:
        await self._operator(scope).delete(schedule_name, confirmation=confirmation)

    def _operator(self, scope: RuntimeScope) -> BoundScheduleControlService:
        entry = self._operators.get(scope.digest)
        if entry is None or entry[0] != scope:
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.NOT_MANAGED,
                "Managed schedule controls are unavailable for the runtime scope",
            )
        return entry[1]


class ScheduleOperator:
    def __init__(
        self,
        client: Client,
        declarations: TriggersConfig,
        settings: ScheduleSettings,
        *,
        scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
        metrics: MetricsRegistry | None = None,
    ) -> None:
        self._client = client
        self._declarations = declarations
        self._settings = settings
        self._scope = scope
        self._rpc_timeout = timedelta(seconds=settings.temporal_rpc_timeout_seconds)
        self._metrics = metrics
        self._reconciler = ScheduleReconciler(
            client,
            settings,
            scope=scope,
            metrics=metrics,
        )

    async def list(self) -> tuple[ManagedScheduleDescription, ...]:
        try:
            observed = await self._reconciler.observe()
        except ScheduleReconciliationError as exc:
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.TEMPORAL_UNAVAILABLE,
                "Managed schedules could not be listed",
            ) from exc
        managed_ids = [
            schedule_id
            for schedule_id, schedule in sorted(observed.items())
            if schedule.valid_managed_identity
        ]
        semaphore = asyncio.Semaphore(self._settings.describe_concurrency)

        async def describe(schedule_id: str) -> ManagedScheduleDescription:
            async with semaphore:
                return await self._describe_id(schedule_id)

        result = tuple(
            await asyncio.gather(*(describe(schedule_id) for schedule_id in managed_ids))
        )
        if self._metrics is not None:
            self._metrics.record_schedule(
                ScheduleMetricOperation.LIST,
                ScheduleMetricOutcome.SUCCESS,
            )
        return result

    async def describe(self, schedule_name: str) -> ManagedScheduleDescription:
        return await self._describe_id(make_schedule_id(schedule_name, scope=self._scope))

    async def pause(self, schedule_name: str) -> ManagedScheduleDescription:
        await self._require_managed(schedule_name)
        await self._mutate(
            schedule_name,
            "pause",
            self._client.get_schedule_handle(
                make_schedule_id(schedule_name, scope=self._scope)
            ).pause(
                note=OPERATOR_PAUSE_NOTE,
                rpc_timeout=self._rpc_timeout,
            ),
        )
        return await self.describe(schedule_name)

    async def resume(self, schedule_name: str) -> ManagedScheduleDescription:
        await self._require_managed(schedule_name)
        await self._mutate(
            schedule_name,
            "resume",
            self._client.get_schedule_handle(
                make_schedule_id(schedule_name, scope=self._scope)
            ).unpause(
                note=OPERATOR_RESUME_NOTE,
                rpc_timeout=self._rpc_timeout,
            ),
        )
        return await self.describe(schedule_name)

    async def trigger_now(
        self,
        schedule_name: str,
        *,
        request_identity_digest: str,
    ) -> TriggerRunNowStatus:
        _validate_run_now_identity(request_identity_digest)
        declaration = self._declaration(schedule_name)
        managed = await self._load_managed_schedule(
            make_schedule_id(schedule_name, scope=self._scope)
        )
        if declaration.paused or managed.description.schedule.state.paused:
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.INVALID_OPERATION,
                "Inactive schedule triggers cannot be run",
            )
        if managed.schedule_name != schedule_name:
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.NOT_MANAGED,
                "Managed schedule identity does not match the declared trigger",
            )
        action = managed.description.schedule.action
        if not isinstance(action, ScheduleActionStartWorkflow) or not action.task_queue:
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.NOT_MANAGED,
                "Managed schedule dispatch action is invalid",
            )
        workflow_id = make_schedule_run_now_workflow_id(
            schedule_name,
            request_identity_digest,
            scope=self._scope,
        )
        try:
            await self._client.start_workflow(
                SCHEDULE_DISPATCH_WORKFLOW_TYPE,
                managed.plan.model_dump(mode="json"),
                id=workflow_id,
                task_queue=action.task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                rpc_timeout=self._rpc_timeout,
            )
        except WorkflowAlreadyStartedError:
            if self._metrics is not None:
                self._metrics.record_schedule(
                    ScheduleMetricOperation.TRIGGER,
                    ScheduleMetricOutcome.IDEMPOTENT,
                )
            return TriggerRunNowStatus.ALREADY_ACCEPTED
        except Exception as exc:
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal did not accept the schedule-trigger run-now request",
            ) from exc
        if self._metrics is not None:
            self._metrics.record_schedule(
                ScheduleMetricOperation.TRIGGER,
                ScheduleMetricOutcome.SUCCESS,
            )
        return TriggerRunNowStatus.ACCEPTED

    async def backfill(
        self,
        schedule_name: str,
        *,
        start_at: datetime,
        end_at: datetime,
    ) -> ManagedScheduleDescription:
        declaration = self._declaration(schedule_name)
        _validate_backfill(declaration, start_at=start_at, end_at=end_at)
        await self._require_managed(schedule_name)
        await self._mutate(
            schedule_name,
            "backfill",
            self._client.get_schedule_handle(
                make_schedule_id(schedule_name, scope=self._scope)
            ).backfill(
                ScheduleBackfill(
                    start_at=start_at.astimezone(UTC),
                    end_at=end_at.astimezone(UTC),
                ),
                rpc_timeout=self._rpc_timeout,
            ),
        )
        return await self.describe(schedule_name)

    async def delete(
        self,
        schedule_name: str,
        *,
        confirmation: str,
    ) -> None:
        description = await self.describe(schedule_name)
        if confirmation != description.desired_digest:
            if self._metrics is not None:
                self._metrics.record_schedule(
                    ScheduleMetricOperation.DELETE,
                    ScheduleMetricOutcome.REJECTED,
                )
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.CONFIRMATION_REQUIRED,
                "Schedule delete confirmation does not match the managed target",
            )
        await self._mutate(
            schedule_name,
            "delete",
            self._client.get_schedule_handle(description.schedule_id).delete(
                rpc_timeout=self._rpc_timeout
            ),
        )

    async def _describe_id(self, schedule_id: str) -> ManagedScheduleDescription:
        managed = await self._load_managed_schedule(schedule_id)
        description = managed.description
        recent_actions = tuple(
            _recent_action(action)
            for action in description.info.recent_actions[-self._settings.recent_action_limit :]
            if isinstance(action.action, ScheduleActionExecutionStartWorkflow)
        )
        return ManagedScheduleDescription(
            schedule_name=managed.schedule_name,
            schedule_id=description.id,
            desired_digest=managed.desired_digest,
            paused=description.schedule.state.paused,
            workflow_name=managed.plan.target.workflow_name,
            definition_digest=managed.plan.target.definition_digest,
            artifact_identity=managed.plan.target.artifact_identity,
            environment_snapshot_digest=managed.plan.target.environment_snapshot_digest,
            overlap_policy=ScheduleOverlapPolicy(description.schedule.policy.overlap.name.lower()),
            next_run_times=tuple(
                description.info.next_action_times[: self._settings.recent_action_limit]
            ),
            recent_actions=recent_actions,
        )

    async def _load_managed_schedule(
        self,
        schedule_id: str,
    ) -> _ManagedSchedule:
        try:
            description = await self._client.get_schedule_handle(schedule_id).describe(
                rpc_timeout=self._rpc_timeout
            )
        except RPCError as exc:
            if exc.status is RPCStatusCode.NOT_FOUND:
                raise ScheduleOperationError(
                    ScheduleOperationErrorCode.UNKNOWN_SCHEDULE,
                    "Managed schedule was not found",
                ) from exc
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.TEMPORAL_UNAVAILABLE,
                "Managed schedule could not be described",
            ) from exc
        except Exception as exc:
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.TEMPORAL_UNAVAILABLE,
                "Managed schedule could not be described",
            ) from exc
        observed = await observed_schedule_from_description(description)
        if not observed.valid_managed_identity or observed.schedule_name is None:
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.NOT_MANAGED,
                "Schedule does not have a valid managed identity",
            )
        if not LEGACY_LOCAL_UNSCOPED_POLICY.owns(observed.scope_digest, self._scope):
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.NOT_MANAGED,
                "Schedule does not belong to the runtime scope",
            )
        if observed.desired_digest is None:
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.NOT_MANAGED,
                "Schedule does not have a valid managed desired state",
            )
        plan = await _decode_dispatch_plan(description)
        return _ManagedSchedule(
            description=description,
            schedule_name=observed.schedule_name,
            desired_digest=observed.desired_digest,
            plan=plan,
        )

    async def _require_managed(self, schedule_name: str) -> None:
        await self.describe(schedule_name)

    def _declaration(self, schedule_name: str) -> ScheduleTriggerDeclaration:
        declaration = self._declarations.schedules.get(schedule_name)
        if declaration is None:
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.UNKNOWN_SCHEDULE,
                "Schedule is not present in authored configuration",
            )
        return declaration

    async def _mutate(
        self,
        schedule_name: str,
        operation: str,
        mutation: Awaitable[None],
    ) -> None:
        try:
            await mutation
        except Exception as exc:
            if self._metrics is not None:
                self._metrics.record_schedule(
                    ScheduleMetricOperation(operation),
                    ScheduleMetricOutcome.ERROR,
                )
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal schedule operation failed",
            ) from exc
        if self._metrics is not None:
            self._metrics.record_schedule(
                ScheduleMetricOperation(operation),
                ScheduleMetricOutcome.SUCCESS,
            )
        logger.info(
            "Managed schedule operation completed",
            extra={"schedule": schedule_name, "schedule_operation": operation},
        )


async def _decode_dispatch_plan(description: ScheduleDescription) -> ScheduleDispatchPlan:
    action = description.schedule.action
    if not isinstance(action, ScheduleActionStartWorkflow) or len(action.args) != 1:
        raise ScheduleOperationError(
            ScheduleOperationErrorCode.NOT_MANAGED,
            "Managed schedule dispatch action is invalid",
        )
    raw_plan = action.args[0]
    if not isinstance(raw_plan, dict):
        try:
            decoded = await description.data_converter.decode(list(action.args), [dict])
        except Exception as exc:
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.NOT_MANAGED,
                "Managed schedule dispatch payload is invalid",
            ) from exc
        if len(decoded) != 1:
            raise ScheduleOperationError(
                ScheduleOperationErrorCode.NOT_MANAGED,
                "Managed schedule dispatch payload is invalid",
            )
        raw_plan = decoded[0]
    try:
        return ScheduleDispatchPlan.model_validate(raw_plan)
    except ValueError as exc:
        raise ScheduleOperationError(
            ScheduleOperationErrorCode.NOT_MANAGED,
            "Managed schedule dispatch payload is invalid",
        ) from exc


def _recent_action(action: ScheduleActionResult) -> ScheduleRecentAction:
    execution = action.action
    if not isinstance(execution, ScheduleActionExecutionStartWorkflow):
        raise TypeError("Unsupported schedule action execution")
    return ScheduleRecentAction(
        scheduled_at=action.scheduled_at,
        started_at=action.started_at,
        workflow_id=execution.workflow_id,
        run_id=execution.first_execution_run_id,
    )


def _validate_backfill(
    declaration: ScheduleTriggerDeclaration,
    *,
    start_at: datetime,
    end_at: datetime,
) -> None:
    if not declaration.backfill.enabled:
        raise ScheduleOperationError(
            ScheduleOperationErrorCode.INVALID_OPERATION,
            "Backfill is disabled for this schedule",
        )
    if start_at.tzinfo is None or end_at.tzinfo is None:
        raise ScheduleOperationError(
            ScheduleOperationErrorCode.INVALID_OPERATION,
            "Backfill bounds must include explicit timezone offsets",
        )
    duration_seconds = (end_at.astimezone(UTC) - start_at.astimezone(UTC)).total_seconds()
    if duration_seconds <= 0:
        raise ScheduleOperationError(
            ScheduleOperationErrorCode.INVALID_OPERATION,
            "Backfill end must be after its start",
        )
    if duration_seconds > declaration.backfill.max_window_seconds:
        raise ScheduleOperationError(
            ScheduleOperationErrorCode.INVALID_OPERATION,
            "Backfill exceeds the configured time window",
        )
    maximum_actions = _maximum_backfill_actions(declaration, duration_seconds)
    if maximum_actions > declaration.backfill.max_actions:
        raise ScheduleOperationError(
            ScheduleOperationErrorCode.INVALID_OPERATION,
            "Backfill may exceed the configured action bound",
        )


def _validate_run_now_identity(value: str) -> None:
    if len(value) != RUN_NOW_IDENTITY_DIGEST_LENGTH or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ScheduleOperationError(
            ScheduleOperationErrorCode.INVALID_OPERATION,
            "Schedule-trigger run-now identity is invalid",
        )


def _maximum_backfill_actions(
    declaration: ScheduleTriggerDeclaration,
    duration_seconds: float,
) -> int:
    spec = declaration.spec
    if isinstance(spec, IntervalScheduleSpec):
        return math.ceil(duration_seconds / spec.every_seconds) + 1
    if isinstance(spec, CronScheduleSpec):
        seconds_per_minute = 60
        return (math.ceil(duration_seconds / seconds_per_minute) + 1) * len(spec.expressions)
    if isinstance(spec, CalendarScheduleSpec):
        seconds_per_day = 86_400
        intersected_days = math.ceil(duration_seconds / seconds_per_day) + 1
        return intersected_days * spec.maximum_daily_actions
    raise TypeError("Unsupported schedule specification")


class ScheduleApplier:
    """Scope-wide desired-state apply: plan and confirmed reconciliation in one
    call. The declarations are the runtime's active configuration; the plan
    digest computed here is its own confirmation because the caller asked for
    exactly the current desired state."""

    def __init__(
        self,
        client: Client,
        declarations: TriggersConfig,
        settings: Settings,
        *,
        catalog_store: DefinitionCatalogStore,
        router: WorkerDeploymentRouter,
        execution_configuration: ExecutionConfigurationIdentity | None,
        scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
        metrics: MetricsRegistry | None = None,
    ) -> None:
        self._client = client
        self._declarations = declarations
        self._settings = settings
        self._catalog_store = catalog_store
        self._router = router
        self._execution_configuration = execution_configuration
        self._scope = scope
        self._metrics = metrics

    async def apply(self) -> ScheduleApplyResult:
        from justflow.runtime.schedule_configuration import prepare_schedules

        prepared = prepare_schedules(
            self._declarations,
            settings=self._settings,
            catalog_store=self._catalog_store,
            router=self._router,
            execution_configuration=self._execution_configuration,
        )
        reconciler = ScheduleReconciler(
            self._client,
            self._settings.schedules,
            scope=self._scope,
            metrics=self._metrics,
        )
        plan = await reconciler.plan(
            dict(prepared.desired),
            unscoped_decision=UnscopedScheduleDecision.RETAIN,
        )
        reconciler.validate_apply(plan, confirmation=plan.plan_digest)
        for snapshot in prepared.environment_snapshots.values():
            self._catalog_store.store_environment_snapshot(snapshot)
        return await reconciler.apply(plan, confirmation=plan.plan_digest)
