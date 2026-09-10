"""Bounded Temporal schedule observation and confirmed reconciliation."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, TypeAlias

from temporalio.api.common.v1 import Payload
from temporalio.client import (
    Client,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleDescription,
    ScheduleListDescription,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
from temporalio.service import RPCError, RPCStatusCode

from justflow.config.settings import ScheduleSettings
from justflow.definitions.routing import MEMO_SCOPE_DIGEST
from justflow.runtime.metrics import (
    MetricsRegistry,
    ScheduleMetricOperation,
    ScheduleMetricOutcome,
)
from justflow.runtime.scheduled_starts import (
    SCHEDULED_START_MAINTENANCE_ID_PREFIX,
    SCHEDULED_START_SCHEDULE_ID_PREFIX,
)
from justflow.runtime.schedules import (
    MEMO_SCHEDULE_DESIRED_DIGEST,
    MEMO_SCHEDULE_NAME,
    MEMO_SCHEDULE_OWNER,
    SCHEDULE_DISPATCH_WORKFLOW_TYPE,
    SCHEDULE_ID_PREFIX,
    SCHEDULE_OWNER,
    DesiredSchedule,
    ObservedSchedule,
    ScheduleChange,
    ScheduleChangeKind,
    SchedulePlan,
    UnscopedScheduleDecision,
    make_unscoped_schedule_dispatch_workflow_id,
    plan_schedule_reconciliation,
)
from justflow.scope import (
    LEGACY_LOCAL_UNSCOPED_POLICY,
    LOCAL_RUNTIME_SCOPE,
    RuntimeScope,
    identity_belongs_to_scope,
    scoped_identity_from_digest,
)

logger = logging.getLogger(__name__)


class ScheduleReconciliationErrorCode(str, Enum):
    CATALOG_UNAVAILABLE = "catalog_unavailable"
    COLLECTION_LIMIT = "collection_limit"
    CONFIRMATION_REQUIRED = "confirmation_required"
    PLAN_CONFLICT = "plan_conflict"
    TEMPORAL_UNAVAILABLE = "temporal_unavailable"


class ScheduleReconciliationError(Exception):
    def __init__(self, code: ScheduleReconciliationErrorCode, message: str) -> None:
        self.code = code
        super().__init__(message)


class ScheduleApplyStatus(str, Enum):
    APPLIED = "applied"
    ALREADY_APPLIED = "already_applied"
    FAILED = "failed"


class ScheduleApplyErrorCode(str, Enum):
    OWNERSHIP_CHANGED = "ownership_changed"
    TEMPORAL_UNAVAILABLE = "temporal_unavailable"


@dataclass(frozen=True, kw_only=True)
class ScheduleApplyItem:
    schedule_id: str
    schedule_name: str | None
    change: ScheduleChangeKind
    status: ScheduleApplyStatus
    error_code: ScheduleApplyErrorCode | None = None


@dataclass(frozen=True, kw_only=True)
class ScheduleApplyResult:
    plan_digest: str
    items: tuple[ScheduleApplyItem, ...]

    @property
    def successful(self) -> bool:
        return all(item.status is not ScheduleApplyStatus.FAILED for item in self.items)


class _ScheduleMutationError(Exception):
    def __init__(self, code: ScheduleApplyErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


@dataclass(frozen=True, kw_only=True)
class _MissingMemoString:
    pass


@dataclass(frozen=True, kw_only=True)
class _DecodedMemoString:
    value: str


@dataclass(frozen=True, kw_only=True)
class _CorruptMemoString:
    error_type: str


_MemoStringResult: TypeAlias = _MissingMemoString | _DecodedMemoString | _CorruptMemoString
_MISSING_MEMO_STRING = _MissingMemoString()


class ScheduleReconciler:
    def __init__(
        self,
        client: Client,
        settings: ScheduleSettings,
        *,
        scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
        metrics: MetricsRegistry | None = None,
    ) -> None:
        self._client = client
        self._settings = settings
        self._scope = scope
        self._rpc_timeout = timedelta(seconds=settings.temporal_rpc_timeout_seconds)
        self._metrics = metrics

    async def plan(
        self,
        desired: dict[str, DesiredSchedule],
        *,
        unscoped_decision: UnscopedScheduleDecision = (UnscopedScheduleDecision.REQUIRE_EXPLICIT),
    ) -> SchedulePlan:
        observed = await self.observe()
        return plan_schedule_reconciliation(
            desired,
            observed,
            unscoped_decision=unscoped_decision,
        )

    async def observe(self) -> dict[str, ObservedSchedule]:
        listed: list[
            tuple[
                ScheduleListDescription,
                str | None,
                str | None,
                str | None,
                tuple[str, ...],
            ]
        ] = []
        scanned = 0
        try:
            iterator = await self._client.list_schedules(
                page_size=self._settings.page_size,
                rpc_timeout=self._rpc_timeout,
            )
            async for entry in iterator:
                if entry.id.startswith(
                    (
                        SCHEDULED_START_SCHEDULE_ID_PREFIX,
                        SCHEDULED_START_MAINTENANCE_ID_PREFIX,
                    )
                ):
                    continue
                scanned += 1
                if scanned > self._settings.max_schedules:
                    raise ScheduleReconciliationError(
                        ScheduleReconciliationErrorCode.COLLECTION_LIMIT,
                        "Schedule collection exceeds the configured observation bound",
                    )
                owner_result, name_result, scope_result = await asyncio.gather(
                    _entity_memo_string(entry, MEMO_SCHEDULE_OWNER),
                    _entity_memo_string(entry, MEMO_SCHEDULE_NAME),
                    _entity_memo_string(entry, MEMO_SCOPE_DIGEST),
                )
                owner = _memo_string_value(owner_result)
                schedule_name = _memo_string_value(name_result)
                scope_digest = _memo_string_value(scope_result)
                corrupt_keys = _corrupt_memo_keys(
                    {
                        MEMO_SCHEDULE_OWNER: owner_result,
                        MEMO_SCHEDULE_NAME: name_result,
                        MEMO_SCOPE_DIGEST: scope_result,
                    }
                )
                corrupt_scope_belongs_here = isinstance(scope_result, _CorruptMemoString) and (
                    identity_belongs_to_scope(entry.id, "schedule", self._scope)
                    or (
                        self._scope == LOCAL_RUNTIME_SCOPE
                        and entry.id.startswith(SCHEDULE_ID_PREFIX)
                    )
                )
                if (
                    not LEGACY_LOCAL_UNSCOPED_POLICY.owns(scope_digest, self._scope)
                    and not corrupt_scope_belongs_here
                ):
                    continue
                listed.append((entry, owner, schedule_name, scope_digest, corrupt_keys))
        except ScheduleReconciliationError:
            raise
        except Exception as exc:
            raise ScheduleReconciliationError(
                ScheduleReconciliationErrorCode.TEMPORAL_UNAVAILABLE,
                "Temporal schedules could not be listed",
            ) from exc

        semaphore = asyncio.Semaphore(self._settings.describe_concurrency)

        async def observe_entry(
            entry: ScheduleListDescription,
            owner: str | None,
            schedule_name: str | None,
            scope_digest: str | None,
            corrupt_keys: tuple[str, ...],
        ) -> ObservedSchedule:
            if corrupt_keys:
                self._record_corrupt_metadata(entry.id, corrupt_keys)
                return ObservedSchedule(
                    schedule_id=entry.id,
                    owner=owner,
                    schedule_name=schedule_name,
                    desired_digest=None,
                    scope_digest=scope_digest,
                    corrupt_metadata_keys=corrupt_keys,
                )
            if owner != SCHEDULE_OWNER:
                return ObservedSchedule(
                    schedule_id=entry.id,
                    owner=owner,
                    schedule_name=schedule_name,
                    desired_digest=None,
                    scope_digest=scope_digest,
                )
            async with semaphore:
                try:
                    description = await self._client.get_schedule_handle(entry.id).describe(
                        rpc_timeout=self._rpc_timeout
                    )
                except Exception as exc:
                    raise ScheduleReconciliationError(
                        ScheduleReconciliationErrorCode.TEMPORAL_UNAVAILABLE,
                        "A managed Temporal schedule could not be described",
                    ) from exc
            observed = await observed_schedule_from_description(description)
            if observed.corrupt_metadata_keys:
                self._record_corrupt_metadata(entry.id, observed.corrupt_metadata_keys)
            return observed

        return {
            observed.schedule_id: observed
            for observed in await asyncio.gather(
                *(
                    observe_entry(entry, owner, name, scope_digest, corrupt_keys)
                    for entry, owner, name, scope_digest, corrupt_keys in listed
                )
            )
        }

    def _record_corrupt_metadata(
        self,
        schedule_id: str,
        corrupt_keys: tuple[str, ...],
    ) -> None:
        logger.warning(
            "Temporal schedule metadata is corrupt",
            extra={"schedule_id": schedule_id, "memo_keys": corrupt_keys},
        )
        if self._metrics is not None:
            self._metrics.record_schedule(
                ScheduleMetricOperation.OBSERVE,
                ScheduleMetricOutcome.CORRUPT,
            )

    async def apply(
        self,
        plan: SchedulePlan,
        *,
        confirmation: str,
    ) -> ScheduleApplyResult:
        self.validate_apply(plan, confirmation=confirmation)

        items: list[ScheduleApplyItem] = []
        for change in plan.changes:
            try:
                status = await self._apply_change(change, plan)
            except _ScheduleMutationError as exc:
                items.append(
                    ScheduleApplyItem(
                        schedule_id=change.schedule_id,
                        schedule_name=change.schedule_name,
                        change=change.kind,
                        status=ScheduleApplyStatus.FAILED,
                        error_code=exc.code,
                    )
                )
            except Exception:  # noqa: BLE001 - per-schedule Temporal mutation boundary
                items.append(
                    ScheduleApplyItem(
                        schedule_id=change.schedule_id,
                        schedule_name=change.schedule_name,
                        change=change.kind,
                        status=ScheduleApplyStatus.FAILED,
                        error_code=ScheduleApplyErrorCode.TEMPORAL_UNAVAILABLE,
                    )
                )
            else:
                items.append(
                    ScheduleApplyItem(
                        schedule_id=change.schedule_id,
                        schedule_name=change.schedule_name,
                        change=change.kind,
                        status=status,
                    )
                )
        result = ScheduleApplyResult(plan_digest=plan.plan_digest, items=tuple(items))
        if self._metrics is not None:
            for item in result.items:
                outcome = {
                    ScheduleApplyStatus.APPLIED: ScheduleMetricOutcome.SUCCESS,
                    ScheduleApplyStatus.ALREADY_APPLIED: ScheduleMetricOutcome.IDEMPOTENT,
                    ScheduleApplyStatus.FAILED: ScheduleMetricOutcome.ERROR,
                }[item.status]
                self._metrics.record_schedule(
                    ScheduleMetricOperation(item.change.value),
                    outcome,
                )
        return result

    @staticmethod
    def validate_apply(plan: SchedulePlan, *, confirmation: str) -> None:
        if confirmation != plan.plan_digest:
            raise ScheduleReconciliationError(
                ScheduleReconciliationErrorCode.CONFIRMATION_REQUIRED,
                "Schedule apply confirmation does not match the current plan",
            )
        if plan.has_conflicts:
            raise ScheduleReconciliationError(
                ScheduleReconciliationErrorCode.PLAN_CONFLICT,
                "Schedule plan contains ownership conflicts",
            )

    async def _apply_change(
        self,
        change: ScheduleChange,
        plan: SchedulePlan,
    ) -> ScheduleApplyStatus:
        if change.kind is ScheduleChangeKind.CREATE:
            return await self._create(plan.desired[change.schedule_id])
        if change.kind is ScheduleChangeKind.UPDATE:
            return await self._update(
                plan.desired[change.schedule_id],
                plan.observed[change.schedule_id],
            )
        if change.kind is ScheduleChangeKind.DELETE:
            return await self._delete(plan.observed[change.schedule_id])
        raise _ScheduleMutationError(ScheduleApplyErrorCode.OWNERSHIP_CHANGED)

    async def _create(self, desired: DesiredSchedule) -> ScheduleApplyStatus:
        try:
            await self._client.create_schedule(
                desired.schedule_id,
                desired.schedule,
                memo=desired.memo,
                rpc_timeout=self._rpc_timeout,
            )
            return ScheduleApplyStatus.APPLIED
        except ScheduleAlreadyRunningError:
            actual = await self._describe_observed(desired.schedule_id)
            if _matches_desired(actual, desired):
                return ScheduleApplyStatus.ALREADY_APPLIED
            raise _ScheduleMutationError(ScheduleApplyErrorCode.OWNERSHIP_CHANGED) from None

    async def _update(
        self,
        desired: DesiredSchedule,
        expected: ObservedSchedule,
    ) -> ScheduleApplyStatus:
        already_applied = False

        async def updater(update_input: ScheduleUpdateInput) -> ScheduleUpdate | None:
            nonlocal already_applied
            actual = await observed_schedule_from_description(update_input.description)
            if not _same_managed_identity(actual, desired):
                raise _ScheduleMutationError(ScheduleApplyErrorCode.OWNERSHIP_CHANGED)
            if actual.desired_digest == desired.desired_digest:
                already_applied = True
                return None
            if actual != expected:
                raise _ScheduleMutationError(ScheduleApplyErrorCode.OWNERSHIP_CHANGED)
            return ScheduleUpdate(desired.schedule)

        try:
            await self._client.get_schedule_handle(desired.schedule_id).update(
                updater,
                rpc_timeout=self._rpc_timeout,
            )
        except _ScheduleMutationError:
            raise
        except RPCError as exc:
            if exc.status is RPCStatusCode.NOT_FOUND:
                raise _ScheduleMutationError(ScheduleApplyErrorCode.OWNERSHIP_CHANGED) from exc
            raise
        return (
            ScheduleApplyStatus.ALREADY_APPLIED if already_applied else ScheduleApplyStatus.APPLIED
        )

    async def _delete(self, expected: ObservedSchedule) -> ScheduleApplyStatus:
        try:
            actual = await self._describe_observed(expected.schedule_id)
        except RPCError as exc:
            if exc.status is RPCStatusCode.NOT_FOUND:
                return ScheduleApplyStatus.ALREADY_APPLIED
            raise
        if actual != expected or not actual.valid_managed_identity:
            raise _ScheduleMutationError(ScheduleApplyErrorCode.OWNERSHIP_CHANGED)
        try:
            await self._client.get_schedule_handle(expected.schedule_id).delete(
                rpc_timeout=self._rpc_timeout
            )
        except RPCError as exc:
            if exc.status is RPCStatusCode.NOT_FOUND:
                return ScheduleApplyStatus.ALREADY_APPLIED
            raise
        return ScheduleApplyStatus.APPLIED

    async def _describe_observed(self, schedule_id: str) -> ObservedSchedule:
        description = await self._client.get_schedule_handle(schedule_id).describe(
            rpc_timeout=self._rpc_timeout
        )
        return await observed_schedule_from_description(description)


async def observed_schedule_from_description(
    description: ScheduleDescription,
) -> ObservedSchedule:
    owner_result, name_result, scope_result = await asyncio.gather(
        _entity_memo_string(description, MEMO_SCHEDULE_OWNER),
        _entity_memo_string(description, MEMO_SCHEDULE_NAME),
        _entity_memo_string(description, MEMO_SCOPE_DIGEST),
    )
    owner = _memo_string_value(owner_result)
    schedule_name = _memo_string_value(name_result)
    scope_digest = _memo_string_value(scope_result)
    entity_corrupt_keys = _corrupt_memo_keys(
        {
            MEMO_SCHEDULE_OWNER: owner_result,
            MEMO_SCHEDULE_NAME: name_result,
            MEMO_SCOPE_DIGEST: scope_result,
        }
    )
    if entity_corrupt_keys:
        return ObservedSchedule(
            schedule_id=description.id,
            owner=owner,
            schedule_name=schedule_name,
            desired_digest=None,
            scope_digest=scope_digest,
            corrupt_metadata_keys=entity_corrupt_keys,
        )
    if owner != SCHEDULE_OWNER or schedule_name is None:
        return ObservedSchedule(
            schedule_id=description.id,
            owner=owner,
            schedule_name=schedule_name,
            desired_digest=None,
            scope_digest=scope_digest,
        )
    action = description.schedule.action
    if not isinstance(action, ScheduleActionStartWorkflow):
        return ObservedSchedule(
            schedule_id=description.id,
            owner=owner,
            schedule_name=schedule_name,
            desired_digest=None,
            scope_digest=scope_digest,
            action_valid=False,
        )
    action_owner_result, action_name_result, desired_digest_result = await asyncio.gather(
        _action_memo_string(description, action, MEMO_SCHEDULE_OWNER),
        _action_memo_string(description, action, MEMO_SCHEDULE_NAME),
        _action_memo_string(description, action, MEMO_SCHEDULE_DESIRED_DIGEST),
    )
    action_owner = _memo_string_value(action_owner_result)
    action_name = _memo_string_value(action_name_result)
    desired_digest = _memo_string_value(desired_digest_result)
    action_corrupt_keys = _corrupt_memo_keys(
        {
            MEMO_SCHEDULE_OWNER: action_owner_result,
            MEMO_SCHEDULE_NAME: action_name_result,
            MEMO_SCHEDULE_DESIRED_DIGEST: desired_digest_result,
        }
    )
    expected_action_id = (
        make_unscoped_schedule_dispatch_workflow_id(schedule_name)
        if scope_digest is None
        else scoped_identity_from_digest("schedule-dispatch", scope_digest, schedule_name)
    )
    action_valid = (
        action_owner == SCHEDULE_OWNER
        and action_name == schedule_name
        and action.workflow == SCHEDULE_DISPATCH_WORKFLOW_TYPE
        and action.id == expected_action_id
    )
    return ObservedSchedule(
        schedule_id=description.id,
        owner=owner,
        schedule_name=schedule_name,
        desired_digest=desired_digest,
        scope_digest=scope_digest,
        action_valid=action_valid,
        corrupt_metadata_keys=action_corrupt_keys,
    )


async def _entity_memo_string(description: Any, key: str) -> _MemoStringResult:
    try:
        value = await description.memo_value(key, None, type_hint=str)
    except Exception as exc:  # noqa: BLE001 - schedule memo codec boundary
        return _CorruptMemoString(error_type=type(exc).__name__)
    if value is None:
        return _MISSING_MEMO_STRING
    if isinstance(value, str):
        return _DecodedMemoString(value=value)
    return _CorruptMemoString(error_type=type(value).__name__)


async def _action_memo_string(
    description: ScheduleDescription,
    action: ScheduleActionStartWorkflow,
    key: str,
) -> _MemoStringResult:
    if action.memo is None:
        return _MISSING_MEMO_STRING
    value = action.memo.get(key)
    if isinstance(value, str):
        return _DecodedMemoString(value=value)
    if value is None:
        return _MISSING_MEMO_STRING
    if not isinstance(value, Payload):
        return _CorruptMemoString(error_type=type(value).__name__)
    try:
        decoded = await description.data_converter.decode([value], [str])
    except Exception as exc:  # noqa: BLE001 - schedule action codec boundary
        return _CorruptMemoString(error_type=type(exc).__name__)
    if len(decoded) == 1 and isinstance(decoded[0], str):
        return _DecodedMemoString(value=decoded[0])
    return _CorruptMemoString(error_type="DecodedValue")


def _memo_string_value(result: _MemoStringResult) -> str | None:
    return result.value if isinstance(result, _DecodedMemoString) else None


def _corrupt_memo_keys(results: dict[str, _MemoStringResult]) -> tuple[str, ...]:
    return tuple(
        sorted(key for key, result in results.items() if isinstance(result, _CorruptMemoString))
    )


def _matches_desired(actual: ObservedSchedule, desired: DesiredSchedule) -> bool:
    return _same_managed_identity(actual, desired) and (
        actual.desired_digest == desired.desired_digest
    )


def _same_managed_identity(actual: ObservedSchedule, desired: DesiredSchedule) -> bool:
    return (
        actual.valid_managed_identity
        and actual.schedule_id == desired.schedule_id
        and actual.schedule_name == desired.schedule_name
        and actual.scope_digest == desired.target.scope_digest
    )
