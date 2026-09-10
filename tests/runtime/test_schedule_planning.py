"""Deterministic schedule compilation and reconciliation planning tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import pytest
from temporalio.client import (
    ScheduleActionStartWorkflow,
    ScheduleCalendarSpec,
    ScheduleIntervalSpec,
)
from temporalio.client import ScheduleOverlapPolicy as TemporalScheduleOverlapPolicy

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.config.schedules import (
    CalendarRange,
    CalendarScheduleSpec,
    CronScheduleSpec,
    IntervalScheduleSpec,
    ScheduleOverlapPolicy,
)
from justflow.config.triggers import ScheduleTriggerDeclaration
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    SHA256_HEX_LENGTH,
    build_definition_manifests,
)
from justflow.definitions.routing import WorkerDeployment, WorkflowStartTarget
from justflow.provenance import WorkerArtifactIdentity
from justflow.runtime.schedules import (
    MEMO_SCHEDULE_DESIRED_DIGEST,
    MEMO_SCHEDULE_NAME,
    MEMO_SCHEDULE_OWNER,
    SCHEDULE_DISPATCH_WORKFLOW_TYPE,
    SCHEDULE_OWNER,
    ObservedSchedule,
    ScheduleChangeKind,
    UnscopedScheduleDecision,
    compile_schedule,
    make_schedule_dispatch_workflow_id,
    make_schedule_id,
    make_schedule_occurrence_id,
    make_schedule_run_now_workflow_id,
    make_unscoped_schedule_id,
    plan_schedule_reconciliation,
)
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope

SCHEDULE_NAME = "daily_orders"
WORKFLOW_NAME = "record_flow"
TASK_QUEUE = "test-queue"
INPUT_SENTINEL = "customer-private-value"
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * SHA256_HEX_LENGTH
WORKFLOW_CONFIG = WorkflowConfig(
    workflow=WORKFLOW_NAME,
    input_schema={
        "type": "object",
        "properties": {"label": {"type": "string"}},
        "required": ["label"],
        "additionalProperties": False,
    },
    steps={},
    flow=[FlowStep(name="done", terminal=True)],
)
MANIFEST = build_definition_manifests(
    {WORKFLOW_NAME: WORKFLOW_CONFIG},
    {},
    DEFAULT_RUNTIME_LIMITS,
)[WORKFLOW_NAME]
DEPLOYMENT = WorkerDeployment(
    artifact_identity=WorkerArtifactIdentity(
        deployment_name="justflow",
        build_id="test-build",
        artifact_digest=f"sha256:{'a' * SHA256_HEX_LENGTH}",
        package_version="0.1.0",
    ),
    compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
)
TARGET = WorkflowStartTarget(
    manifest=MANIFEST,
    deployment=DEPLOYMENT,
    environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
)


@dataclass(frozen=True, kw_only=True)
class CompileCase:
    id: str
    declaration: ScheduleTriggerDeclaration
    expected_spec_type: type
    expected_overlap: TemporalScheduleOverlapPolicy


COMPILE_CASES = [
    CompileCase(
        id="cron-with-iana-timezone",
        declaration=ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            input={"label": INPUT_SENTINEL},
            spec=CronScheduleSpec(expressions=("15 8 * * 1-5",)),
            timezone="Europe/Zurich",
        ),
        expected_spec_type=str,
        expected_overlap=TemporalScheduleOverlapPolicy.SKIP,
    ),
    CompileCase(
        id="absolute-interval",
        declaration=ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            input={"label": INPUT_SENTINEL},
            spec=IntervalScheduleSpec(every_seconds=300, offset_seconds=30),
            overlap_policy=ScheduleOverlapPolicy.BUFFER_ONE,
        ),
        expected_spec_type=ScheduleIntervalSpec,
        expected_overlap=TemporalScheduleOverlapPolicy.BUFFER_ONE,
    ),
    CompileCase(
        id="calendar-with-explicit-ranges",
        declaration=ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            input={"label": INPUT_SENTINEL},
            spec=CalendarScheduleSpec(
                minute=(CalendarRange(start=0, end=45, step=15),),
                hour=(CalendarRange(start=9, end=17),),
            ),
            timezone="America/New_York",
            overlap_policy=ScheduleOverlapPolicy.ALLOW_ALL,
        ),
        expected_spec_type=ScheduleCalendarSpec,
        expected_overlap=TemporalScheduleOverlapPolicy.ALLOW_ALL,
    ),
]

OVERLAP_CASES = [
    pytest.param(
        ScheduleOverlapPolicy.SKIP,
        TemporalScheduleOverlapPolicy.SKIP,
        id="skip",
    ),
    pytest.param(
        ScheduleOverlapPolicy.BUFFER_ONE,
        TemporalScheduleOverlapPolicy.BUFFER_ONE,
        id="buffer-one",
    ),
    pytest.param(
        ScheduleOverlapPolicy.BUFFER_ALL,
        TemporalScheduleOverlapPolicy.BUFFER_ALL,
        id="buffer-all",
    ),
    pytest.param(
        ScheduleOverlapPolicy.CANCEL_OTHER,
        TemporalScheduleOverlapPolicy.CANCEL_OTHER,
        id="cancel-other",
    ),
    pytest.param(
        ScheduleOverlapPolicy.TERMINATE_OTHER,
        TemporalScheduleOverlapPolicy.TERMINATE_OTHER,
        id="terminate-other",
    ),
    pytest.param(
        ScheduleOverlapPolicy.ALLOW_ALL,
        TemporalScheduleOverlapPolicy.ALLOW_ALL,
        id="allow-all",
    ),
]


@pytest.mark.parametrize("case", COMPILE_CASES, ids=lambda case: case.id)
def test_compile_schedule(case: CompileCase):
    desired = compile_schedule(
        SCHEDULE_NAME,
        case.declaration,
        TARGET,
        task_queue=TASK_QUEUE,
    )

    action = desired.schedule.action
    assert isinstance(action, ScheduleActionStartWorkflow)
    assert action.workflow == SCHEDULE_DISPATCH_WORKFLOW_TYPE
    assert action.id == make_schedule_dispatch_workflow_id(SCHEDULE_NAME)
    assert action.task_queue == TASK_QUEUE
    assert desired.schedule.policy.overlap is case.expected_overlap
    if case.expected_spec_type is str:
        assert desired.schedule.spec.cron_expressions == case.declaration.spec.expressions
    elif case.expected_spec_type is ScheduleIntervalSpec:
        assert isinstance(desired.schedule.spec.intervals[0], ScheduleIntervalSpec)
        assert desired.schedule.spec.intervals[0].every == timedelta(seconds=300)
    else:
        assert isinstance(desired.schedule.spec.calendars[0], ScheduleCalendarSpec)
    assert desired.target.definition_digest == MANIFEST.definition_digest
    assert desired.memo[MEMO_SCHEDULE_OWNER] == SCHEDULE_OWNER
    assert desired.memo[MEMO_SCHEDULE_NAME] == SCHEDULE_NAME
    assert desired.memo[MEMO_SCHEDULE_DESIRED_DIGEST] == desired.desired_digest
    assert INPUT_SENTINEL not in repr(desired)
    assert INPUT_SENTINEL not in str(desired.memo)


@pytest.mark.parametrize(("authored", "expected"), OVERLAP_CASES)
def test_compile_maps_every_overlap_policy(
    authored: ScheduleOverlapPolicy,
    expected: TemporalScheduleOverlapPolicy,
):
    desired = compile_schedule(
        SCHEDULE_NAME,
        ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            input={"label": "safe"},
            spec=IntervalScheduleSpec(every_seconds=60),
            overlap_policy=authored,
            catch_up_window_seconds=123,
        ),
        TARGET,
        task_queue=TASK_QUEUE,
    )

    assert desired.schedule.policy.overlap is expected
    assert desired.schedule.policy.catchup_window == timedelta(seconds=123)


def test_paused_only_change_is_an_authoritative_reconciliation_update() -> None:
    active = compile_schedule(
        SCHEDULE_NAME,
        ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            input={"label": "safe"},
            spec=IntervalScheduleSpec(every_seconds=60),
            paused=False,
        ),
        TARGET,
        task_queue=TASK_QUEUE,
    )
    paused = compile_schedule(
        SCHEDULE_NAME,
        ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            input={"label": "safe"},
            spec=IntervalScheduleSpec(every_seconds=60),
            paused=True,
        ),
        TARGET,
        task_queue=TASK_QUEUE,
    )
    observed = ObservedSchedule(
        schedule_id=active.schedule_id,
        owner=SCHEDULE_OWNER,
        schedule_name=SCHEDULE_NAME,
        desired_digest=active.desired_digest,
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
    )

    plan = plan_schedule_reconciliation(
        {paused.schedule_id: paused},
        {observed.schedule_id: observed},
    )

    assert active.desired_digest != paused.desired_digest
    assert paused.schedule.state.paused is True
    assert tuple(change.kind for change in plan.changes) == (ScheduleChangeKind.UPDATE,)


@dataclass(frozen=True, kw_only=True)
class PlanCase:
    id: str
    desired_present: bool
    observed: ObservedSchedule | None
    expected: tuple[ScheduleChangeKind, ...]
    unscoped_decision: UnscopedScheduleDecision = UnscopedScheduleDecision.REQUIRE_EXPLICIT


PLAN_CASES = [
    PlanCase(
        id="create-missing",
        desired_present=True,
        observed=None,
        expected=(ScheduleChangeKind.CREATE,),
    ),
    PlanCase(
        id="unchanged-managed",
        desired_present=True,
        observed=ObservedSchedule(
            schedule_id=make_schedule_id(SCHEDULE_NAME),
            owner=SCHEDULE_OWNER,
            schedule_name=SCHEDULE_NAME,
            desired_digest="use-desired",
            scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        ),
        expected=(),
    ),
    PlanCase(
        id="update-managed",
        desired_present=True,
        observed=ObservedSchedule(
            schedule_id=make_schedule_id(SCHEDULE_NAME),
            owner=SCHEDULE_OWNER,
            schedule_name=SCHEDULE_NAME,
            desired_digest="sha256:stale",
            scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        ),
        expected=(ScheduleChangeKind.UPDATE,),
    ),
    PlanCase(
        id="never-overwrite-unmanaged",
        desired_present=True,
        observed=ObservedSchedule(
            schedule_id=make_schedule_id(SCHEDULE_NAME),
            owner=None,
            schedule_name=None,
            desired_digest=None,
            scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        ),
        expected=(ScheduleChangeKind.CONFLICT,),
    ),
    PlanCase(
        id="delete-retired-managed",
        desired_present=False,
        observed=ObservedSchedule(
            schedule_id=make_schedule_id(SCHEDULE_NAME),
            owner=SCHEDULE_OWNER,
            schedule_name=SCHEDULE_NAME,
            desired_digest="sha256:retired",
            scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        ),
        expected=(ScheduleChangeKind.DELETE,),
    ),
    PlanCase(
        id="ignore-unmanaged-retirement",
        desired_present=False,
        observed=ObservedSchedule(
            schedule_id=make_schedule_id(SCHEDULE_NAME),
            owner="host",
            schedule_name=SCHEDULE_NAME,
            desired_digest="host-state",
        ),
        expected=(),
    ),
    PlanCase(
        id="require-decision-for-unscoped",
        desired_present=True,
        observed=ObservedSchedule(
            schedule_id=make_unscoped_schedule_id(SCHEDULE_NAME),
            owner=SCHEDULE_OWNER,
            schedule_name=SCHEDULE_NAME,
            desired_digest="legacy",
        ),
        expected=(ScheduleChangeKind.CREATE, ScheduleChangeKind.CONFLICT),
    ),
    PlanCase(
        id="migrate-unscoped",
        desired_present=True,
        observed=ObservedSchedule(
            schedule_id=make_unscoped_schedule_id(SCHEDULE_NAME),
            owner=SCHEDULE_OWNER,
            schedule_name=SCHEDULE_NAME,
            desired_digest="legacy",
        ),
        expected=(ScheduleChangeKind.CREATE, ScheduleChangeKind.DELETE),
        unscoped_decision=UnscopedScheduleDecision.MIGRATE,
    ),
    PlanCase(
        id="retain-unscoped",
        desired_present=True,
        observed=ObservedSchedule(
            schedule_id=make_unscoped_schedule_id(SCHEDULE_NAME),
            owner=SCHEDULE_OWNER,
            schedule_name=SCHEDULE_NAME,
            desired_digest="legacy",
        ),
        expected=(),
        unscoped_decision=UnscopedScheduleDecision.RETAIN,
    ),
]


@pytest.mark.parametrize("case", PLAN_CASES, ids=lambda case: case.id)
def test_schedule_reconciliation_plan(case: PlanCase):
    desired_schedule = compile_schedule(
        SCHEDULE_NAME,
        ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            input={"label": "safe"},
            spec=IntervalScheduleSpec(every_seconds=60),
        ),
        TARGET,
        task_queue=TASK_QUEUE,
    )
    desired = {desired_schedule.schedule_id: desired_schedule} if case.desired_present else {}
    observed_schedule = case.observed
    if observed_schedule is not None and observed_schedule.desired_digest == "use-desired":
        observed_schedule = ObservedSchedule(
            schedule_id=observed_schedule.schedule_id,
            owner=observed_schedule.owner,
            schedule_name=observed_schedule.schedule_name,
            desired_digest=desired_schedule.desired_digest,
            scope_digest=observed_schedule.scope_digest,
        )
    observed = (
        {observed_schedule.schedule_id: observed_schedule} if observed_schedule is not None else {}
    )

    plan = plan_schedule_reconciliation(
        desired,
        observed,
        unscoped_decision=case.unscoped_decision,
    )

    assert tuple(change.kind for change in plan.changes) == case.expected
    assert plan.mutation_count == sum(
        kind is not ScheduleChangeKind.CONFLICT for kind in case.expected
    )
    assert plan.has_conflicts is (ScheduleChangeKind.CONFLICT in case.expected)


def test_plan_confirmation_identifies_the_complete_desired_and_observed_state():
    first = compile_schedule(
        SCHEDULE_NAME,
        ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            input={"label": "first"},
            spec=IntervalScheduleSpec(every_seconds=60),
        ),
        TARGET,
        task_queue=TASK_QUEUE,
    )
    second = compile_schedule(
        SCHEDULE_NAME,
        ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            input={"label": "second"},
            spec=IntervalScheduleSpec(every_seconds=60),
        ),
        TARGET,
        task_queue=TASK_QUEUE,
    )

    first_plan = plan_schedule_reconciliation({first.schedule_id: first}, {})
    second_plan = plan_schedule_reconciliation({second.schedule_id: second}, {})
    observed_plan = plan_schedule_reconciliation(
        {first.schedule_id: first},
        {
            first.schedule_id: ObservedSchedule(
                schedule_id=first.schedule_id,
                owner=SCHEDULE_OWNER,
                schedule_name=SCHEDULE_NAME,
                desired_digest="stale",
            )
        },
    )

    assert first_plan.plan_digest != second_plan.plan_digest
    assert first_plan.plan_digest != observed_plan.plan_digest


@pytest.mark.parametrize("mapping_name", ["desired", "observed"])
def test_plan_rejects_schedule_mapping_key_identity_mismatch(mapping_name: str):
    desired = compile_schedule(
        SCHEDULE_NAME,
        ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            input={"label": "safe"},
            spec=IntervalScheduleSpec(every_seconds=60),
        ),
        TARGET,
        task_queue=TASK_QUEUE,
    )
    observed = ObservedSchedule(
        schedule_id=desired.schedule_id,
        owner=SCHEDULE_OWNER,
        schedule_name=SCHEDULE_NAME,
        desired_digest=desired.desired_digest,
    )
    desired_mapping = {"wrong": desired} if mapping_name == "desired" else {}
    observed_mapping = {"wrong": observed} if mapping_name == "observed" else {}

    with pytest.raises(ValueError, match=f"{mapping_name.title()} schedule mapping key"):
        plan_schedule_reconciliation(desired_mapping, observed_mapping)


def test_occurrence_identity_is_stable_and_schedule_scoped():
    first = make_schedule_occurrence_id(SCHEDULE_NAME, "dispatch-id-2026-01-01T00:00:00Z")

    assert first == make_schedule_occurrence_id(
        SCHEDULE_NAME,
        "dispatch-id-2026-01-01T00:00:00Z",
    )
    assert first != make_schedule_occurrence_id(
        "another_schedule",
        "dispatch-id-2026-01-01T00:00:00Z",
    )
    assert len(first) == SHA256_HEX_LENGTH


def test_run_now_dispatch_identity_is_stable_and_runtime_scope_isolated():
    request_identity = "f" * SHA256_HEX_LENGTH
    other_scope = RuntimeScope.create(
        tenant="other-tenant",
        application="orders",
        environment="production",
    )

    first = make_schedule_run_now_workflow_id(SCHEDULE_NAME, request_identity)

    assert first == make_schedule_run_now_workflow_id(SCHEDULE_NAME, request_identity)
    assert first != make_schedule_run_now_workflow_id(
        SCHEDULE_NAME,
        request_identity,
        scope=other_scope,
    )
