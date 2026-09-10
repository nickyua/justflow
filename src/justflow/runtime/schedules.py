"""Pure schedule compilation, immutable targeting, and reconciliation planning."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from types import MappingProxyType

from pydantic import Field
from pydantic.types import StrictInt
from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleCalendarSpec,
    ScheduleIntervalSpec,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
)
from temporalio.client import ScheduleOverlapPolicy as TemporalScheduleOverlapPolicy

from justflow.config.grammar import TriggerName, WorkflowName
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.config.schedules import (
    CalendarRange,
    CalendarScheduleSpec,
    CronScheduleSpec,
    IntervalScheduleSpec,
    ScheduleOverlapPolicy,
)
from justflow.config.settings import (
    DEFAULT_SCHEDULE_DISPATCH_ATTEMPTS,
    DEFAULT_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
    MAX_SCHEDULE_DISPATCH_ATTEMPTS,
    MAX_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
    MIN_SCHEDULE_DISPATCH_ATTEMPTS,
    MIN_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
)
from justflow.config.triggers import ScheduleTriggerDeclaration
from justflow.definitions.routing import MEMO_SCOPE_DIGEST, WorkflowStartTarget
from justflow.provenance import (
    ExecutionConfigurationIdentity,
    WorkerArtifactIdentity,
    provenance_digest,
)
from justflow.runtime.starter import StrictRuntimeModel, validate_workflow_start_input
from justflow.scope import (
    LOCAL_RUNTIME_SCOPE,
    SCOPE_DIGEST_LENGTH,
    RuntimeScope,
    scoped_identity,
    scoped_identity_from_digest,
)
from justflow.sdk.message_contract import DEFINITION_DIGEST_LENGTH

SCHEDULE_OWNER = "justflow"
SCHEDULE_ID_PREFIX = "justflow.schedule."
SCHEDULE_DISPATCH_WORKFLOW_ID_PREFIX = "justflow.schedule-dispatch."
SCHEDULE_DISPATCH_WORKFLOW_TYPE = "justflow.schedule-dispatch.v1"
SCHEDULE_DISPATCH_FORMAT_VERSION = 1
MEMO_SCHEDULE_OWNER = "justflow.schedule_owner"
MEMO_SCHEDULE_NAME = "justflow.schedule_name"
MEMO_SCHEDULE_DESIRED_DIGEST = "justflow.schedule_desired_digest"
MEMO_SCHEDULE_FORMAT_VERSION = "justflow.schedule_format_version"


class ScheduleConfigurationError(ValueError):
    """A schedule cannot be resolved into a safe immutable action."""


class ScheduleTargetIdentity(StrictRuntimeModel):
    scope_digest: str | None = Field(
        default=None,
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$",
    )
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

    @classmethod
    def from_target(
        cls,
        target: WorkflowStartTarget,
        scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
    ) -> ScheduleTargetIdentity:
        return cls(
            scope_digest=scope.digest,
            workflow_name=target.manifest.logical_name,
            definition_digest=target.manifest.definition_digest,
            artifact_identity=target.deployment.artifact_identity,
            environment_snapshot_digest=target.environment_snapshot_digest,
            execution_configuration=target.execution_configuration,
        )


class ScheduleDispatchPlan(StrictRuntimeModel):
    format_version: int = SCHEDULE_DISPATCH_FORMAT_VERSION
    schedule_name: TriggerName
    target: ScheduleTargetIdentity
    input: Mapping[str, object] = Field(default_factory=dict, repr=False)
    dispatch_timeout_seconds: StrictInt = Field(
        default=DEFAULT_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
        ge=MIN_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
        le=MAX_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
    )
    dispatch_attempts: StrictInt = Field(
        default=DEFAULT_SCHEDULE_DISPATCH_ATTEMPTS,
        ge=MIN_SCHEDULE_DISPATCH_ATTEMPTS,
        le=MAX_SCHEDULE_DISPATCH_ATTEMPTS,
    )


@dataclass(frozen=True, kw_only=True)
class DesiredSchedule:
    schedule_name: str
    schedule_id: str
    desired_digest: str
    target: ScheduleTargetIdentity
    schedule: Schedule = field(repr=False)
    memo: Mapping[str, str] = field(repr=False)


@dataclass(frozen=True, kw_only=True)
class ObservedSchedule:
    schedule_id: str
    owner: str | None
    schedule_name: str | None
    desired_digest: str | None
    scope_digest: str | None = None
    action_valid: bool = True
    corrupt_metadata_keys: tuple[str, ...] = ()

    @property
    def managed(self) -> bool:
        return self.owner == SCHEDULE_OWNER

    @property
    def valid_managed_identity(self) -> bool:
        return (
            not self.corrupt_metadata_keys
            and self.managed
            and self.schedule_name is not None
            and self.desired_digest is not None
            and self.action_valid
        )


class ScheduleChangeKind(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    CONFLICT = "conflict"


class UnscopedScheduleDecision(str, Enum):
    REQUIRE_EXPLICIT = "require_explicit"
    MIGRATE = "migrate"
    RETAIN = "retain"


@dataclass(frozen=True, kw_only=True)
class ScheduleChange:
    kind: ScheduleChangeKind
    schedule_id: str
    schedule_name: str | None
    reason: str


@dataclass(frozen=True, kw_only=True)
class SchedulePlan:
    desired: Mapping[str, DesiredSchedule]
    observed: Mapping[str, ObservedSchedule]
    changes: tuple[ScheduleChange, ...]
    plan_digest: str
    unscoped_decision: UnscopedScheduleDecision

    @property
    def has_conflicts(self) -> bool:
        return any(change.kind is ScheduleChangeKind.CONFLICT for change in self.changes)

    @property
    def mutation_count(self) -> int:
        return sum(change.kind is not ScheduleChangeKind.CONFLICT for change in self.changes)


def compile_schedule(
    schedule_name: str,
    declaration: ScheduleTriggerDeclaration,
    target: WorkflowStartTarget,
    *,
    task_queue: str,
    dispatch_task_queue: str | None = None,
    scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
    dispatch_timeout_seconds: int = DEFAULT_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
    dispatch_attempts: int = DEFAULT_SCHEDULE_DISPATCH_ATTEMPTS,
) -> DesiredSchedule:
    if not task_queue:
        raise ScheduleConfigurationError("Schedule task queue must not be empty")
    resolved_dispatch_task_queue = dispatch_task_queue or task_queue
    if not resolved_dispatch_task_queue:
        raise ScheduleConfigurationError("Schedule dispatch task queue must not be empty")
    if declaration.workflow != target.manifest.logical_name:
        raise ScheduleConfigurationError(
            f"Schedule '{schedule_name}' resolved to the wrong workflow target"
        )
    if (
        declaration.definition_digest is not None
        and declaration.definition_digest != target.manifest.definition_digest
    ):
        raise ScheduleConfigurationError(
            f"Schedule '{schedule_name}' resolved to the wrong definition target"
        )
    normalized_input = validate_workflow_start_input(
        declaration.workflow,
        declaration.input,
        target,
        limits,
    )
    target_identity = ScheduleTargetIdentity.from_target(target, scope)
    dispatch_plan = ScheduleDispatchPlan(
        schedule_name=schedule_name,
        target=target_identity,
        input=normalized_input,
        dispatch_timeout_seconds=dispatch_timeout_seconds,
        dispatch_attempts=dispatch_attempts,
    )
    desired_identity = {
        "format_version": SCHEDULE_DISPATCH_FORMAT_VERSION,
        "scope_digest": scope.digest,
        "schedule_name": schedule_name,
        "declaration": declaration.model_dump(mode="json"),
        "target": target_identity.model_dump(mode="json", exclude_none=True),
        "task_queue_identity": provenance_digest({"task_queue": task_queue}),
        "dispatch_task_queue_identity": provenance_digest(
            {"task_queue": resolved_dispatch_task_queue}
        ),
        "dispatch_timeout_seconds": dispatch_plan.dispatch_timeout_seconds,
        "dispatch_attempts": dispatch_plan.dispatch_attempts,
    }
    desired_digest = provenance_digest(desired_identity)
    schedule_id = make_schedule_id(schedule_name, scope=scope)
    memo = MappingProxyType(
        {
            **target.memo,
            MEMO_SCHEDULE_OWNER: SCHEDULE_OWNER,
            MEMO_SCHEDULE_NAME: schedule_name,
            MEMO_SCHEDULE_DESIRED_DIGEST: desired_digest,
            MEMO_SCHEDULE_FORMAT_VERSION: str(SCHEDULE_DISPATCH_FORMAT_VERSION),
            MEMO_SCOPE_DIGEST: scope.digest,
        }
    )
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            SCHEDULE_DISPATCH_WORKFLOW_TYPE,
            dispatch_plan.model_dump(mode="json"),
            id=make_schedule_dispatch_workflow_id(schedule_name, scope=scope),
            task_queue=resolved_dispatch_task_queue,
            memo=memo,
        ),
        spec=_temporal_schedule_spec(declaration),
        policy=SchedulePolicy(
            overlap=_TEMPORAL_OVERLAP_POLICIES[declaration.overlap_policy],
            catchup_window=timedelta(seconds=declaration.catch_up_window_seconds),
        ),
        state=ScheduleState(paused=declaration.paused),
    )
    return DesiredSchedule(
        schedule_name=schedule_name,
        schedule_id=schedule_id,
        desired_digest=desired_digest,
        target=target_identity,
        schedule=schedule,
        memo=memo,
    )


def plan_schedule_reconciliation(
    desired: Mapping[str, DesiredSchedule],
    observed: Mapping[str, ObservedSchedule],
    *,
    unscoped_decision: UnscopedScheduleDecision = UnscopedScheduleDecision.REQUIRE_EXPLICIT,
) -> SchedulePlan:
    _validate_schedule_mapping(desired, mapping_name="desired")
    _validate_schedule_mapping(observed, mapping_name="observed")
    unscoped_by_name: dict[str, list[ObservedSchedule]] = {}
    for schedule in observed.values():
        if (
            schedule.managed
            and schedule.scope_digest is None
            and schedule.schedule_name is not None
        ):
            unscoped_by_name.setdefault(schedule.schedule_name, []).append(schedule)
    ambiguous_names = {name for name, schedules in unscoped_by_name.items() if len(schedules) > 1}
    effective_desired = {
        schedule_id: schedule
        for schedule_id, schedule in desired.items()
        if not (
            unscoped_decision is UnscopedScheduleDecision.RETAIN
            and schedule.schedule_name in unscoped_by_name
            and schedule.schedule_name not in ambiguous_names
        )
    }
    changes: list[ScheduleChange] = []
    for schedule_id, wanted in sorted(effective_desired.items()):
        actual = observed.get(schedule_id)
        if actual is None:
            changes.append(
                ScheduleChange(
                    kind=ScheduleChangeKind.CREATE,
                    schedule_id=schedule_id,
                    schedule_name=wanted.schedule_name,
                    reason="managed schedule is absent",
                )
            )
        elif actual.corrupt_metadata_keys:
            changes.append(
                ScheduleChange(
                    kind=ScheduleChangeKind.CONFLICT,
                    schedule_id=schedule_id,
                    schedule_name=wanted.schedule_name,
                    reason="schedule metadata is corrupt",
                )
            )
        elif not actual.managed:
            changes.append(
                ScheduleChange(
                    kind=ScheduleChangeKind.CONFLICT,
                    schedule_id=schedule_id,
                    schedule_name=wanted.schedule_name,
                    reason="schedule identity is occupied by an unmanaged schedule",
                )
            )
        elif not actual.valid_managed_identity or actual.schedule_name != wanted.schedule_name:
            changes.append(
                ScheduleChange(
                    kind=ScheduleChangeKind.CONFLICT,
                    schedule_id=schedule_id,
                    schedule_name=wanted.schedule_name,
                    reason="managed schedule identity metadata is invalid",
                )
            )
        elif actual.scope_digest != wanted.target.scope_digest:
            changes.append(
                ScheduleChange(
                    kind=ScheduleChangeKind.CONFLICT,
                    schedule_id=schedule_id,
                    schedule_name=wanted.schedule_name,
                    reason="managed schedule belongs to a different scope",
                )
            )
        elif actual.desired_digest != wanted.desired_digest:
            changes.append(
                ScheduleChange(
                    kind=ScheduleChangeKind.UPDATE,
                    schedule_id=schedule_id,
                    schedule_name=wanted.schedule_name,
                    reason="managed schedule differs from desired state",
                )
            )

    for schedule_id, actual in sorted(observed.items()):
        if schedule_id in effective_desired:
            continue
        if actual.corrupt_metadata_keys:
            changes.append(
                ScheduleChange(
                    kind=ScheduleChangeKind.CONFLICT,
                    schedule_id=schedule_id,
                    schedule_name=actual.schedule_name,
                    reason="orphaned schedule metadata is corrupt",
                )
            )
            continue
        if not actual.managed:
            continue
        if actual.scope_digest is None:
            if (
                actual.valid_managed_identity
                and actual.schedule_name not in ambiguous_names
                and unscoped_decision is UnscopedScheduleDecision.RETAIN
            ):
                continue
            if (
                actual.valid_managed_identity
                and actual.schedule_name is not None
                and actual.schedule_name in {item.schedule_name for item in desired.values()}
                and actual.schedule_name not in ambiguous_names
                and unscoped_decision is UnscopedScheduleDecision.MIGRATE
            ):
                changes.append(
                    ScheduleChange(
                        kind=ScheduleChangeKind.DELETE,
                        schedule_id=schedule_id,
                        schedule_name=actual.schedule_name,
                        reason="unscoped managed schedule is replaced by scoped desired state",
                    )
                )
                continue
            changes.append(
                ScheduleChange(
                    kind=ScheduleChangeKind.CONFLICT,
                    schedule_id=schedule_id,
                    schedule_name=actual.schedule_name,
                    reason="unscoped managed schedule requires an explicit migration decision",
                )
            )
            continue
        if not actual.valid_managed_identity:
            changes.append(
                ScheduleChange(
                    kind=ScheduleChangeKind.CONFLICT,
                    schedule_id=schedule_id,
                    schedule_name=actual.schedule_name,
                    reason="orphaned managed schedule has invalid identity metadata",
                )
            )
            continue
        changes.append(
            ScheduleChange(
                kind=ScheduleChangeKind.DELETE,
                schedule_id=schedule_id,
                schedule_name=actual.schedule_name,
                reason="managed schedule is absent from desired state",
            )
        )

    frozen_desired = MappingProxyType(effective_desired)
    frozen_observed = MappingProxyType(dict(observed))
    normalized_changes = tuple(
        sorted(
            changes,
            key=lambda change: (_SCHEDULE_CHANGE_ORDER[change.kind], change.schedule_id),
        )
    )
    plan_digest = provenance_digest(
        {
            "desired": [
                {
                    "schedule_id": schedule_id,
                    "schedule_name": schedule.schedule_name,
                    "desired_digest": schedule.desired_digest,
                    "scope_digest": schedule.target.scope_digest,
                }
                for schedule_id, schedule in sorted(desired.items())
            ],
            "observed": [
                {
                    "schedule_id": schedule_id,
                    "owner": schedule.owner,
                    "schedule_name": schedule.schedule_name,
                    "desired_digest": schedule.desired_digest,
                    "action_valid": schedule.action_valid,
                    "scope_digest": schedule.scope_digest,
                    "corrupt_metadata_keys": schedule.corrupt_metadata_keys,
                }
                for schedule_id, schedule in sorted(observed.items())
            ],
            "changes": [
                {
                    "kind": change.kind.value,
                    "schedule_id": change.schedule_id,
                    "schedule_name": change.schedule_name,
                }
                for change in normalized_changes
            ],
            "unscoped_decision": unscoped_decision.value,
        }
    )
    return SchedulePlan(
        desired=frozen_desired,
        observed=frozen_observed,
        changes=normalized_changes,
        plan_digest=plan_digest,
        unscoped_decision=unscoped_decision,
    )


def _validate_schedule_mapping(
    schedules: Mapping[str, DesiredSchedule] | Mapping[str, ObservedSchedule],
    *,
    mapping_name: str,
) -> None:
    for schedule_id, schedule in schedules.items():
        if schedule_id != schedule.schedule_id:
            raise ScheduleConfigurationError(
                f"{mapping_name.title()} schedule mapping key does not match its identity"
            )


def make_schedule_id(
    schedule_name: str,
    *,
    scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
) -> str:
    return scoped_identity("schedule", scope, schedule_name)


def make_unscoped_schedule_id(schedule_name: str) -> str:
    return f"{SCHEDULE_ID_PREFIX}{schedule_name}"


def make_schedule_dispatch_workflow_id(
    schedule_name: str,
    *,
    scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
) -> str:
    return scoped_identity("schedule-dispatch", scope, schedule_name)


def make_schedule_run_now_workflow_id(
    schedule_name: str,
    request_identity_digest: str,
    *,
    scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
) -> str:
    return scoped_identity(
        "schedule-run-now",
        scope,
        schedule_name,
        request_identity_digest,
    )


def make_unscoped_schedule_dispatch_workflow_id(schedule_name: str) -> str:
    return f"{SCHEDULE_DISPATCH_WORKFLOW_ID_PREFIX}{schedule_name}"


def make_schedule_occurrence_id(
    schedule_name: str,
    dispatch_workflow_id: str,
    *,
    scope_digest: str | None = None,
) -> str:
    canonical = json.dumps(
        ("schedule_occurrence", schedule_name, dispatch_workflow_id),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    occurrence_digest = hashlib.sha256(canonical).hexdigest()
    if scope_digest is None:
        return occurrence_digest
    return scoped_identity_from_digest(
        "schedule-occurrence",
        scope_digest,
        schedule_name,
        occurrence_digest,
    )


def _temporal_schedule_spec(declaration: ScheduleTriggerDeclaration) -> ScheduleSpec:
    authored = declaration.spec
    if isinstance(authored, CronScheduleSpec):
        return ScheduleSpec(
            cron_expressions=authored.expressions,
            time_zone_name=declaration.timezone,
        )
    if isinstance(authored, IntervalScheduleSpec):
        return ScheduleSpec(
            intervals=(
                ScheduleIntervalSpec(
                    every=timedelta(seconds=authored.every_seconds),
                    offset=timedelta(seconds=authored.offset_seconds),
                ),
            ),
            time_zone_name=declaration.timezone,
        )
    if isinstance(authored, CalendarScheduleSpec):
        return ScheduleSpec(
            calendars=(
                ScheduleCalendarSpec(
                    second=_temporal_ranges(authored.second),
                    minute=_temporal_ranges(authored.minute),
                    hour=_temporal_ranges(authored.hour),
                    day_of_month=_temporal_ranges(authored.day_of_month),
                    month=_temporal_ranges(authored.month),
                    year=_temporal_ranges(authored.year),
                    day_of_week=_temporal_ranges(authored.day_of_week),
                ),
            ),
            time_zone_name=declaration.timezone,
        )
    raise ScheduleConfigurationError("Unsupported schedule specification")


def _temporal_ranges(ranges: tuple[CalendarRange, ...]) -> tuple[ScheduleRange, ...]:
    return tuple(
        ScheduleRange(
            start=value.start,
            end=value.start if value.end is None else value.end,
            step=value.step,
        )
        for value in ranges
    )


_TEMPORAL_OVERLAP_POLICIES = {
    ScheduleOverlapPolicy.SKIP: TemporalScheduleOverlapPolicy.SKIP,
    ScheduleOverlapPolicy.BUFFER_ONE: TemporalScheduleOverlapPolicy.BUFFER_ONE,
    ScheduleOverlapPolicy.BUFFER_ALL: TemporalScheduleOverlapPolicy.BUFFER_ALL,
    ScheduleOverlapPolicy.CANCEL_OTHER: TemporalScheduleOverlapPolicy.CANCEL_OTHER,
    ScheduleOverlapPolicy.TERMINATE_OTHER: TemporalScheduleOverlapPolicy.TERMINATE_OTHER,
    ScheduleOverlapPolicy.ALLOW_ALL: TemporalScheduleOverlapPolicy.ALLOW_ALL,
}

_SCHEDULE_CHANGE_ORDER = {
    ScheduleChangeKind.CREATE: 0,
    ScheduleChangeKind.UPDATE: 1,
    ScheduleChangeKind.CONFLICT: 2,
    ScheduleChangeKind.DELETE: 3,
}
