"""FlowRunner unit tests — orchestration semantics without Temporal.

Covers the failure paths integration tests can't reach cheaply: iteration
failure strategies, cause unwrapping, resolution errors, branch ordering,
convergence, and parallelism bounds.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeAlias

import pytest
from temporalio.exceptions import CancelledError as TemporalCancelledError

from justflow.config.models import (
    AuditCaptureMode,
    FlowStep,
    IterationFailStrategy,
    OnCompleteConfig,
    OnResultBranch,
    ServiceConfig,
    StepDefinition,
    WaitForConfig,
    WorkflowConfig,
)
from justflow.config.runtime_limits import (
    DEFAULT_FANOUT_ITEMS,
    DEFAULT_PARALLELISM,
    DEFAULT_RUNTIME_LIMITS,
    RuntimeLimits,
)
from justflow.engine.continuation import IterationCheckpoint, UntilCheckpoint
from justflow.engine.contracts import ContractViolation, validate_payload
from justflow.engine.errors import FailureCode, FlowErrorKind, FlowExecutionError
from justflow.engine.runner import (
    TIMED_OUT,
    CacheDirective,
    FlowContinuation,
    FlowRunner,
    HistoryObservation,
    StepInvocation,
    StepResult,
    _deadline_epoch,
)
from justflow.provenance import ExecutionConfigurationIdentity
from justflow.transports.builtins import builtin_transport_registry
from justflow.transports.registry import ResolvedService

SERVICES = builtin_transport_registry().resolve_services(
    {
        "svc": ServiceConfig(
            transport="direct",
            transport_config={"class": "tests.engine.test_runner.Unused"},
            dispatch_timeout_sec=5,
            retries=0,
        )
    }
)

REQUEST_ID = "req-1"
MAX_CONCURRENCY = 2
TEST_WAIT_TIMEOUT_SEC = 1
OVER_LIMIT_COUNT = 2
CONFIGURATION_DIGEST_HEX_LENGTH = 64
EXECUTION_CONFIGURATION = ExecutionConfigurationIdentity(
    configuration_revision_id="c" * CONFIGURATION_DIGEST_HEX_LENGTH,
    resolution_digest=f"sha256:{'d' * CONFIGURATION_DIGEST_HEX_LENGTH}",
)
TINY_LIMIT = 1
RETENTION_POLICY = "test"
FANOUT_ITEM_COUNT = 7
FANOUT_CHUNK_ITEMS = 3


class FatalIterationError(BaseException):
    pass


class FakeExecutor:
    """Dispatches to per-action handler callables; records everything."""

    def __init__(
        self,
        handlers: dict[str, Callable[[StepInvocation], Any]],
        evaluators: dict[str, bool] | None = None,
        events: dict[str, Any] | None = None,
        subworkflows: dict[str, Any] | None = None,
        archive_error: BaseException | None = None,
        history_observations: list[HistoryObservation] | None = None,
    ):
        self._handlers = handlers
        self._evaluators = evaluators or {}
        self._events = events or {}
        self._subworkflows = subworkflows or {}
        self._archive_error = archive_error
        self._history_observations = list(history_observations or ())
        self.invocations: list[StepInvocation] = []
        self.evaluator_calls: list[tuple[str, Any, list[str]]] = []
        self.wait_calls: list[tuple[str, float]] = []
        self.sleep_calls: list[float] = []
        self.subworkflow_calls: list[tuple[str, str, dict]] = []
        self.contract_validations: list[tuple[dict[str, Any] | str, Any, str, str]] = []
        self.archived: list[tuple[str, str, str, AuditCaptureMode, dict]] = []
        self._clock = 0.0

    async def run_subworkflow(
        self, workflow_name: str, step_name: str, trigger_globals: dict
    ) -> Any:
        self.subworkflow_calls.append((workflow_name, step_name, trigger_globals))
        return self._subworkflows[workflow_name]

    async def run_evaluator(
        self, evaluator: str, data: Any, resource_names: list[str], step_name: str
    ) -> bool:
        self.evaluator_calls.append((evaluator, data, resource_names))
        return self._evaluators[evaluator]

    async def wait_for_event(self, signal: str, timeout_sec: float) -> Any:
        self.wait_calls.append((signal, timeout_sec))
        return self._events.get(signal, TIMED_OUT)

    async def sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)

    async def run_step(
        self,
        invocation: StepInvocation,
        service: ResolvedService,
    ) -> StepResult:
        self.invocations.append(invocation)
        result = self._handlers[invocation.action](invocation)
        if asyncio.iscoroutine(result):
            result = await result
        if isinstance(result, BaseException):
            raise result
        if isinstance(result, StepResult):
            return result
        return StepResult(result)

    async def archive(
        self,
        resource: str,
        path: str,
        retention_policy: str,
        capture_mode: AuditCaptureMode,
        record: dict,
    ) -> None:
        self.archived.append((resource, path, retention_policy, capture_mode, record))
        if self._archive_error is not None:
            raise self._archive_error

    async def validate_contract(
        self,
        schema: dict[str, Any] | str,
        payload: Any,
        *,
        direction: str,
        boundary_name: str,
    ) -> None:
        self.contract_validations.append((schema, payload, direction, boundary_name))
        validate_payload(
            schema,
            payload,
            direction=direction,
            step_name=boundary_name,
        )

    def now(self) -> float:
        self._clock += 1.0
        return self._clock

    def observe_history(self) -> HistoryObservation:
        if self._history_observations:
            return self._history_observations.pop(0)
        return HistoryObservation(events=0, bytes=0)


def _step_defs(*actions: str) -> dict[str, StepDefinition]:
    return {action: StepDefinition(service="svc", action=action) for action in actions}


def _wf(steps: dict[str, StepDefinition], flow: list[FlowStep], **kwargs) -> WorkflowConfig:
    return WorkflowConfig(workflow="test_flow", steps=steps, flow=flow, **kwargs)


async def _run(
    config: WorkflowConfig,
    handlers: dict[str, Callable[[StepInvocation], Any]],
    trigger_globals: dict[str, Any] | None = None,
    evaluators: dict[str, bool] | None = None,
    events: dict[str, Any] | None = None,
    subworkflows: dict[str, Any] | None = None,
    archive_error: BaseException | None = None,
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
    timezone_aware_deadlines_enabled: bool = True,
) -> tuple[dict, FakeExecutor]:
    executor = FakeExecutor(
        handlers,
        evaluators=evaluators,
        events=events,
        subworkflows=subworkflows,
        archive_error=archive_error,
    )
    runner = FlowRunner(
        config,
        SERVICES,
        executor,
        limits=limits,
        timezone_aware_deadlines_enabled=timezone_aware_deadlines_enabled,
    )
    record = await runner.run(REQUEST_ID, trigger_globals or {})
    return record, executor


class TestLinearFlow:
    async def test_outputs_flow_between_steps_and_audit_is_recorded(self):
        config = _wf(
            _step_defs("fetch", "process"),
            [
                FlowStep(name="s1", op="fetch", output="data", then="s2"),
                FlowStep(name="s2", op="process", input="s1.data", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, executor = await _run(
            config, {"fetch": lambda inv: {"n": 1}, "process": lambda inv: {"n2": 2}}
        )

        assert executor.invocations[1].input == {"n": 1}
        assert record["status"] == "completed"
        assert record["reason"] is None
        assert set(record["steps"]) == {"s1", "s2"}
        assert record["steps"]["s2"]["output"] == {"n2": 2}

    async def test_source_identity_digests_are_in_audit_only_when_supplied(self) -> None:
        config = _wf({}, [FlowStep(name="done", terminal=True)])
        executor = FakeExecutor({})
        runner = FlowRunner(
            config,
            SERVICES,
            executor,
            trigger_source="webhook",
            source_identity_digest="a" * 64,
            correlation_identity_digest="b" * 64,
        )

        identified = await runner.run(REQUEST_ID, {})
        legacy, _ = await _run(config, {})

        assert identified["trigger_source"] == "webhook"
        assert identified["source_identity_digest"] == "a" * 64
        assert identified["correlation_identity_digest"] == "b" * 64
        assert "trigger_source" not in legacy
        assert "source_identity_digest" not in legacy
        assert "correlation_identity_digest" not in legacy


class TestRuntimeInvariants:
    async def test_falling_out_without_terminal_is_typed_failure(self):
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(name="s1", op="fetch", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )
        config.flow[0].then = None

        with pytest.raises(FlowExecutionError) as exc_info:
            await _run(config, {"fetch": lambda invocation: {}})

        assert exc_info.value.classification.code == FlowErrorKind.INVARIANT_VIOLATION
        assert "without reaching an explicit terminal" in str(exc_info.value)

    async def test_empty_flow_bypass_is_typed_failure(self):
        config = WorkflowConfig.model_construct(
            workflow="test_flow",
            steps={},
            flow=[],
            params={},
            input_schema=None,
        )
        executor = FakeExecutor({})

        with pytest.raises(FlowExecutionError) as exc_info:
            await FlowRunner(config, SERVICES, executor).run(REQUEST_ID, {})

        assert exc_info.value.classification.code == FlowErrorKind.INVARIANT_VIOLATION
        assert "has no entry step" in str(exc_info.value)

    async def test_terminal_reason_recorded(self):
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(name="s1", op="fetch", then="rejected"),
                FlowStep(name="rejected", terminal=True, reason="nope"),
            ],
        )

        record, _ = await _run(config, {"fetch": lambda inv: {}})

        assert record["reason"] == "nope"
        assert record["status"] == "terminated"

    async def test_step_exception_has_typed_failure_record(self):
        config = _wf(
            _step_defs("fetch"),
            [FlowStep(name="s1", op="fetch", then="done"), FlowStep(name="done", terminal=True)],
        )

        with pytest.raises(FlowExecutionError, match="activity blew up") as exc_info:
            await _run(config, {"fetch": lambda inv: RuntimeError("activity blew up")})

        assert exc_info.value.classification.code == "STEP_FAILED"
        assert exc_info.value.classification.cause_code == "RuntimeError"
        assert exc_info.value.record.status == "failed"


class GuardScenario(str, Enum):
    FANOUT = "fanout"
    PARALLELISM = "parallelism"
    LOOP = "loop"


@dataclass(frozen=True, kw_only=True)
class RuntimeGuardCase:
    id: str
    scenario: GuardScenario
    limits: RuntimeLimits
    match: str


RUNTIME_GUARD_CASES = [
    RuntimeGuardCase(
        id="fanout-items",
        scenario=GuardScenario.FANOUT,
        limits=RuntimeLimits(
            fanout_items=TINY_LIMIT,
            fanout_chunk_items=TINY_LIMIT,
        ),
        match="fanout_items",
    ),
    RuntimeGuardCase(
        id="parallelism",
        scenario=GuardScenario.PARALLELISM,
        limits=RuntimeLimits(parallelism=TINY_LIMIT),
        match="parallelism",
    ),
    RuntimeGuardCase(
        id="loop-attempts",
        scenario=GuardScenario.LOOP,
        limits=RuntimeLimits(loop_attempts=TINY_LIMIT),
        match="loop_attempts",
    ),
]


@dataclass(frozen=True, kw_only=True)
class HistoryContinuationCase:
    id: str
    observation: HistoryObservation
    limits: RuntimeLimits


HISTORY_CONTINUATION_CASES = [
    HistoryContinuationCase(
        id="event-threshold",
        observation=HistoryObservation(events=TINY_LIMIT, bytes=0),
        limits=RuntimeLimits(history_events=TINY_LIMIT),
    ),
    HistoryContinuationCase(
        id="byte-threshold",
        observation=HistoryObservation(events=0, bytes=TINY_LIMIT),
        limits=RuntimeLimits(history_bytes=TINY_LIMIT),
    ),
    HistoryContinuationCase(
        id="server-suggestion",
        observation=HistoryObservation(events=0, bytes=0, server_suggested=True),
        limits=RuntimeLimits(),
    ),
]


class TestRuntimeLimits:
    @pytest.mark.parametrize(
        "case",
        RUNTIME_GUARD_CASES,
        ids=lambda case: case.id,
    )
    async def test_declaration_guard_rejects_before_work(self, case: RuntimeGuardCase) -> None:
        trigger_globals: dict[str, Any] = {}
        if case.scenario is GuardScenario.LOOP:
            step = FlowStep(
                name="work",
                op="work",
                until="input.ready == true",
                max_iterations=OVER_LIMIT_COUNT,
                then="done",
            )
        else:
            trigger_globals = {"items": list(range(OVER_LIMIT_COUNT))}
            step = FlowStep(
                name="work",
                op="work",
                for_each="items",
                parallel=case.scenario is GuardScenario.PARALLELISM,
                max_concurrency=(
                    OVER_LIMIT_COUNT if case.scenario is GuardScenario.PARALLELISM else None
                ),
                then="done",
            )
        config = _wf(
            _step_defs("work"),
            [step, FlowStep(name="done", terminal=True)],
        )
        executor = FakeExecutor({"work": lambda invocation: {"ready": False}})

        with pytest.raises(FlowExecutionError, match=case.match) as exc_info:
            await FlowRunner(
                config,
                SERVICES,
                executor,
                limits=case.limits,
            ).run(REQUEST_ID, trigger_globals)

        assert exc_info.value.classification.code == FailureCode.LIMIT_EXCEEDED
        assert executor.invocations == []

    async def test_total_invocation_limit_stops_before_next_call(self) -> None:
        config = _wf(
            _step_defs("work"),
            [
                FlowStep(
                    name="work",
                    op="work",
                    until="input.ready == true",
                    max_iterations=OVER_LIMIT_COUNT,
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )
        executor = FakeExecutor({"work": lambda invocation: {"ready": False}})
        limits = RuntimeLimits(total_invocations=TINY_LIMIT)

        with pytest.raises(FlowExecutionError, match="total_invocations") as exc_info:
            await FlowRunner(config, SERVICES, executor, limits=limits).run(REQUEST_ID, {})

        assert exc_info.value.classification.code == FailureCode.LIMIT_EXCEEDED
        assert len(executor.invocations) == TINY_LIMIT

    async def test_checkpoint_state_limit_stops_before_next_step(self) -> None:
        config = _wf(
            _step_defs("first", "second"),
            [
                FlowStep(name="first", op="first", then="second"),
                FlowStep(name="second", op="second", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )
        executor = FakeExecutor(
            {
                "first": lambda invocation: {"value": "bounded"},
                "second": lambda invocation: {},
            }
        )

        with pytest.raises(FlowExecutionError, match="workflow.continuation.state") as exc_info:
            await FlowRunner(
                config,
                SERVICES,
                executor,
                limits=RuntimeLimits(workflow_state_bytes=TINY_LIMIT),
            ).run(REQUEST_ID, {})

        assert exc_info.value.classification.code == FailureCode.LIMIT_EXCEEDED
        assert [invocation.action for invocation in executor.invocations] == ["first"]

    @pytest.mark.parametrize(
        "case",
        HISTORY_CONTINUATION_CASES,
        ids=lambda case: case.id,
    )
    async def test_history_boundary_requests_continuation(
        self,
        case: HistoryContinuationCase,
    ) -> None:
        config = _wf(
            _step_defs("work"),
            [
                FlowStep(name="work", op="work", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )
        executor = FakeExecutor(
            {"work": lambda invocation: {}},
            history_observations=[case.observation],
        )

        with pytest.raises(FlowContinuation) as continuation_info:
            await FlowRunner(config, SERVICES, executor, limits=case.limits).run(REQUEST_ID, {})

        assert continuation_info.value.checkpoint.next_step_name == "done"
        assert continuation_info.value.observation == case.observation

    async def test_runner_resumes_from_typed_checkpoint(self) -> None:
        config = _wf(
            _step_defs("first", "second"),
            [
                FlowStep(name="first", op="first", output="first_result", then="second"),
                FlowStep(
                    name="second",
                    op="second",
                    input="first.first_result",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )
        limits = RuntimeLimits(history_events=TINY_LIMIT)
        first_executor = FakeExecutor(
            {"first": lambda invocation: {"value": "preserved"}},
            history_observations=[HistoryObservation(events=TINY_LIMIT, bytes=0)],
        )

        with pytest.raises(FlowContinuation) as continuation_info:
            await FlowRunner(
                config,
                SERVICES,
                first_executor,
                run_id="first-run",
                limits=limits,
            ).run(REQUEST_ID, {})

        checkpoint = continuation_info.value.checkpoint
        second_executor = FakeExecutor(
            {"second": lambda invocation: {"received": invocation.input}}
        )
        record = await FlowRunner(
            config,
            SERVICES,
            second_executor,
            run_id="continued-run",
            limits=limits,
        ).run(REQUEST_ID, {}, checkpoint)

        assert checkpoint.next_step_name == "second"
        assert checkpoint.invocations == 1
        assert [invocation.action for invocation in first_executor.invocations] == ["first"]
        assert [invocation.action for invocation in second_executor.invocations] == ["second"]
        assert second_executor.invocations[0].input == {"value": "preserved"}
        assert set(record["steps"]) == {"first", "second"}

    async def test_parallel_iteration_resumes_after_completed_chunk(self) -> None:
        config = _wf(
            _step_defs("work"),
            [
                FlowStep(
                    name="work",
                    op="work",
                    for_each="items",
                    as_var="item",
                    parallel=True,
                    max_concurrency=MAX_CONCURRENCY,
                    output="results",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )
        limits = RuntimeLimits(
            fanout_items=FANOUT_ITEM_COUNT,
            fanout_chunk_items=MAX_CONCURRENCY,
            history_events=TINY_LIMIT,
        )
        first_executor = FakeExecutor(
            {"work": lambda invocation: invocation.globals["item"]},
            history_observations=[HistoryObservation(events=TINY_LIMIT, bytes=0)],
        )

        with pytest.raises(FlowContinuation) as continuation_info:
            await FlowRunner(config, SERVICES, first_executor, limits=limits).run(
                REQUEST_ID,
                {"items": list(range(FANOUT_ITEM_COUNT))},
            )

        checkpoint = continuation_info.value.checkpoint
        assert isinstance(checkpoint.loop, IterationCheckpoint)
        assert checkpoint.loop.next_offset == MAX_CONCURRENCY
        assert checkpoint.invocations == MAX_CONCURRENCY

        second_executor = FakeExecutor({"work": lambda invocation: invocation.globals["item"]})
        record = await FlowRunner(config, SERVICES, second_executor, limits=limits).run(
            REQUEST_ID,
            {"items": list(range(FANOUT_ITEM_COUNT))},
            checkpoint,
        )

        assert [invocation.globals["item"] for invocation in second_executor.invocations] == list(
            range(MAX_CONCURRENCY, FANOUT_ITEM_COUNT)
        )
        assert record["steps"]["work"]["output"] == list(range(FANOUT_ITEM_COUNT))

    async def test_until_loop_resumes_after_completed_attempt(self) -> None:
        config = _wf(
            _step_defs("poll"),
            [
                FlowStep(
                    name="poll",
                    op="poll",
                    until="input.ready == true",
                    max_iterations=OVER_LIMIT_COUNT,
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )
        limits = RuntimeLimits(history_events=TINY_LIMIT)
        first_executor = FakeExecutor(
            {"poll": lambda invocation: {"ready": False}},
            history_observations=[HistoryObservation(events=TINY_LIMIT, bytes=0)],
        )

        with pytest.raises(FlowContinuation) as continuation_info:
            await FlowRunner(config, SERVICES, first_executor, limits=limits).run(REQUEST_ID, {})

        checkpoint = continuation_info.value.checkpoint
        assert isinstance(checkpoint.loop, UntilCheckpoint)
        assert checkpoint.loop.attempts_completed == 1
        assert checkpoint.invocations == 1

        second_executor = FakeExecutor({"poll": lambda invocation: {"ready": True}})
        record = await FlowRunner(config, SERVICES, second_executor, limits=limits).run(
            REQUEST_ID,
            {},
            checkpoint,
        )

        assert len(second_executor.invocations) == 1
        assert record["steps"]["poll"]["attempts"] == OVER_LIMIT_COUNT
        assert record["steps"]["poll"]["output"] == {"ready": True}

    async def test_parallel_fanout_creates_only_one_task_chunk_at_a_time(self) -> None:
        entered = 0
        active_chunk = asyncio.Event()
        release = asyncio.Event()

        async def wait_for_release(invocation: StepInvocation) -> dict[str, int]:
            nonlocal entered
            entered += 1
            if entered == MAX_CONCURRENCY:
                active_chunk.set()
            await release.wait()
            return {"value": invocation.input}

        config = _wf(
            _step_defs("work"),
            [
                FlowStep(
                    name="work",
                    op="work",
                    for_each="items",
                    parallel=True,
                    max_concurrency=MAX_CONCURRENCY,
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )
        executor = FakeExecutor({"work": wait_for_release})
        run_task = asyncio.create_task(
            FlowRunner(
                config,
                SERVICES,
                executor,
                limits=RuntimeLimits(
                    fanout_items=FANOUT_ITEM_COUNT,
                    fanout_chunk_items=FANOUT_CHUNK_ITEMS,
                ),
            ).run(REQUEST_ID, {"items": list(range(FANOUT_ITEM_COUNT))})
        )

        await active_chunk.wait()
        created_iteration_tasks = len(
            [task for task in asyncio.all_tasks() if task.get_name().startswith("iteration:work[")]
        )
        release.set()
        record = await run_task

        assert created_iteration_tasks == FANOUT_CHUNK_ITEMS
        assert record["steps"]["work"]["items_total"] == FANOUT_ITEM_COUNT

    async def test_default_profile_accepts_maximum_fanout(self) -> None:
        config = _wf(
            _step_defs("work"),
            [
                FlowStep(
                    name="work",
                    op="work",
                    for_each="items",
                    as_var="item",
                    parallel=True,
                    max_concurrency=DEFAULT_PARALLELISM,
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, executor = await _run(
            config,
            {"work": lambda invocation: invocation.globals["item"]},
            {"items": list(range(DEFAULT_FANOUT_ITEMS))},
        )

        assert len(executor.invocations) == DEFAULT_FANOUT_ITEMS
        assert record["steps"]["work"]["output"] == list(range(DEFAULT_FANOUT_ITEMS))

    @pytest.mark.parametrize(
        "boundary",
        [
            pytest.param("trigger", id="trigger"),
            pytest.param("result", id="result"),
            pytest.param("audit", id="audit"),
        ],
    )
    async def test_payload_boundaries_raise_typed_failures(self, boundary: str) -> None:
        if boundary == "trigger":
            config = _wf({}, [FlowStep(name="done", terminal=True)])
            handlers: dict[str, Callable[[StepInvocation], Any]] = {}
            trigger_globals = {"value": "too large"}
            limits = RuntimeLimits(trigger_payload_bytes=TINY_LIMIT)
        else:
            config = _wf(
                _step_defs("work"),
                [
                    FlowStep(name="work", op="work", output="value", then="done"),
                    FlowStep(name="done", terminal=True),
                ],
                result="work.value" if boundary == "result" else None,
            )
            handlers = {"work": lambda invocation: {"value": "too large"}}
            trigger_globals = {}
            limits = (
                RuntimeLimits(workflow_output_bytes=TINY_LIMIT)
                if boundary == "result"
                else RuntimeLimits(audit_record_bytes=TINY_LIMIT)
            )

        with pytest.raises(FlowExecutionError) as exc_info:
            await _run(
                config,
                handlers,
                trigger_globals=trigger_globals,
                limits=limits,
            )

        assert exc_info.value.classification.code == FailureCode.LIMIT_EXCEEDED


class TestParams:
    async def test_missing_trigger_param_fails_fast(self):
        config = _wf(
            _step_defs("fetch"),
            [FlowStep(name="s1", op="fetch", then="done"), FlowStep(name="done", terminal=True)],
            params={"source_id": "${source_id}"},
        )

        with pytest.raises(FlowExecutionError, match="source_id") as exc_info:
            await _run(config, {"fetch": lambda inv: {}}, trigger_globals={})

        assert exc_info.value.classification.code == FlowErrorKind.MISSING_PARAMS

    async def test_globals_merge_precedence_and_interpolation(self):
        config = _wf(
            {
                "fetch": StepDefinition(
                    service="svc", action="fetch", params={"a": "def", "b": "def"}
                )
            },
            [
                FlowStep(name="s1", op="fetch", params={"b": "${source_id}"}, then="done"),
                FlowStep(name="done", terminal=True),
            ],
            params={"source_id": "${source_id}"},
        )

        _, executor = await _run(
            config, {"fetch": lambda inv: {}}, trigger_globals={"source_id": "s42"}
        )

        assert executor.invocations[0].globals == {"a": "def", "b": "s42"}


class TestBranchingAndConditions:
    async def test_on_result_first_match_wins(self):
        config = _wf(
            _step_defs("fetch", "a", "b"),
            [
                FlowStep(
                    name="s1",
                    op="fetch",
                    on_result=[
                        OnResultBranch(when="input.n > 10", then="big"),
                        OnResultBranch(when="input.n > 1", then="small"),
                        OnResultBranch(default="done"),
                    ],
                ),
                FlowStep(name="big", op="a", then="done"),
                FlowStep(name="small", op="b", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, _ = await _run(
            config,
            {"fetch": lambda inv: {"n": 5}, "a": lambda inv: {}, "b": lambda inv: {}},
        )

        assert "small" in record["steps"]
        assert "big" not in record["steps"]

    async def test_on_result_default_taken_when_nothing_matches(self):
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(
                    name="s1",
                    op="fetch",
                    on_result=[
                        OnResultBranch(when="input.n > 10", then="done"),
                        OnResultBranch(default="rejected"),
                    ],
                ),
                FlowStep(name="rejected", terminal=True, reason="defaulted"),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, _ = await _run(config, {"fetch": lambda inv: {"n": 1}})

        assert record["reason"] == "defaulted"

    async def test_false_condition_skips_step_and_records_the_skip(self):
        config = _wf(
            _step_defs("fetch", "opt"),
            [
                FlowStep(name="s1", op="fetch", output="data", then="maybe"),
                FlowStep(
                    name="maybe",
                    op="opt",
                    input="s1.data",
                    condition="input.n > 10",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, executor = await _run(config, {"fetch": lambda inv: {"n": 1}})

        assert [inv.step_name for inv in executor.invocations] == ["s1"]
        skipped = record["steps"]["maybe"]
        assert skipped["status"] == "skipped"
        assert skipped["condition"] == "input.n > 10"
        assert "output" not in skipped

    async def test_evaluator_condition_routes_through_executor(self):
        from justflow.config.models import EvaluatorCondition

        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(
                    name="s1",
                    op="fetch",
                    on_result=[
                        OnResultBranch(
                            when=EvaluatorCondition(
                                evaluator="myapp.conditions.is_supported",
                                resources=["runtime_config"],
                            ),
                            then="matched",
                        ),
                        OnResultBranch(default="done"),
                    ],
                ),
                FlowStep(name="matched", terminal=True, reason="via_evaluator"),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, executor = await _run(
            config,
            {"fetch": lambda inv: {"item": "alpha"}},
            evaluators={"myapp.conditions.is_supported": True},
        )

        assert executor.evaluator_calls == [
            ("myapp.conditions.is_supported", {"item": "alpha"}, ["runtime_config"])
        ]
        assert record["reason"] == "via_evaluator"
        assert record["transitions"][0]["matched"] == "evaluator:myapp.conditions.is_supported"

    async def test_compound_expression_condition(self):
        config = _wf(
            _step_defs("fetch", "opt"),
            [
                FlowStep(name="s1", op="fetch", output="data", then="maybe"),
                FlowStep(
                    name="maybe",
                    op="opt",
                    input="s1.data",
                    condition="input.n > 3 and exists(input.tag) and input.tag in ('a', 'b')",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, _ = await _run(
            config,
            {"fetch": lambda inv: {"n": 5, "tag": "a"}, "opt": lambda inv: {}},
        )

        assert record["steps"]["maybe"]["status"] == "succeeded"

    async def test_unknown_input_reference_is_resolution_error(self):
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(name="s1", op="fetch", input="ghost.data", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

        with pytest.raises(FlowExecutionError) as exc_info:
            await _run(config, {"fetch": lambda inv: {}})

        assert exc_info.value.classification.code == FlowErrorKind.RESOLUTION_ERROR

    async def test_convergence_alias_reads_whichever_branch_ran(self):
        config = _wf(
            _step_defs("fetch", "a", "consume"),
            [
                FlowStep(
                    name="s1",
                    op="fetch",
                    on_result=[
                        OnResultBranch(when="input.cached == true", then="hot"),
                        OnResultBranch(default="cold"),
                    ],
                ),
                FlowStep(name="hot", op="a", output="shared", then="use"),
                FlowStep(name="cold", op="a", output="shared", then="use"),
                FlowStep(name="use", op="consume", input="shared", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

        _, executor = await _run(
            config,
            {
                "fetch": lambda inv: {"cached": True},
                "a": lambda inv: {"from": inv.step_name},
                "consume": lambda inv: {},
            },
        )

        consume_inv = next(i for i in executor.invocations if i.action == "consume")
        assert consume_inv.input == {"from": "hot"}


def _loop_config(**step_kwargs) -> WorkflowConfig:
    return _wf(
        _step_defs("fetch", "work"),
        [
            FlowStep(name="s1", op="fetch", output="data", then="loop"),
            FlowStep(
                name="loop",
                op="work",
                input="s1.data",
                for_each="input.items",
                as_var="item",
                output="results",
                then="done",
                **step_kwargs,
            ),
            FlowStep(name="done", terminal=True),
        ],
    )


def _failing_on(bad_item: Any, error: Callable[[], BaseException] | None = None):
    def handler(inv: StepInvocation) -> Any:
        item = inv.globals["item"]
        if item == bad_item:
            raise (error() if error else ValueError(f"cannot process {item}"))
        return {"item": item, "ok": True}

    return handler


class TestIteration:
    async def test_sequential_list_in_list_out_with_as_var(self):
        config = _loop_config()

        record, executor = await _run(
            config,
            {"fetch": lambda inv: {"items": ["a", "b"]}, "work": _failing_on(bad_item=None)},
        )

        loop_invocations = [i for i in executor.invocations if i.action == "work"]
        assert [i.step_name for i in loop_invocations] == ["loop[0]", "loop[1]"]
        assert [i.input for i in loop_invocations] == ["a", "b"]
        assert [i.globals["item"] for i in loop_invocations] == ["a", "b"]
        assert record["steps"]["loop"]["output"] == [
            {"item": "a", "ok": True},
            {"item": "b", "ok": True},
        ]

    async def test_dict_in_dict_out(self):
        config = _loop_config()

        record, _ = await _run(
            config,
            {
                "fetch": lambda inv: {"items": {"x": 1, "y": 2}},
                "work": lambda inv: inv.globals["item"] * 10,
            },
        )

        assert record["steps"]["loop"]["output"] == {"x": 10, "y": 20}

    async def test_stop_strategy_raises_first_failure(self):
        config = _loop_config(on_iteration_fail=IterationFailStrategy.STOP)

        with pytest.raises(FlowExecutionError, match="cannot process b") as exc_info:
            await _run(
                config,
                {"fetch": lambda inv: {"items": ["a", "b", "c"]}, "work": _failing_on("b")},
            )

        assert exc_info.value.classification.cause_code == "ValueError"

    async def test_skip_strategy_omits_failed_items(self):
        config = _loop_config(on_iteration_fail=IterationFailStrategy.SKIP)

        record, _ = await _run(
            config,
            {"fetch": lambda inv: {"items": ["a", "b", "c"]}, "work": _failing_on("b")},
        )

        assert [r["item"] for r in record["steps"]["loop"]["output"]] == ["a", "c"]

    async def test_collect_strategy_unwraps_wrapped_causes(self):
        class FakeActivityError(Exception):
            """Mimics temporalio ActivityError: useless message, cause with type/message."""

            def __init__(self):
                super().__init__("Activity task failed")
                self.cause = FakeApplicationError()

        class FakeApplicationError(Exception):
            type = "LookupError"
            message = "sku not stocked"

        config = _loop_config(on_iteration_fail=IterationFailStrategy.COLLECT)

        record, _ = await _run(
            config,
            {
                "fetch": lambda inv: {"items": ["a", "b"]},
                "work": _failing_on("b", error=FakeActivityError),
            },
        )

        collected = record["steps"]["loop"]["output"][1]
        assert collected["_error"] is True
        assert collected["code"] == "LookupError"
        assert collected["message"] == "sku not stocked"

    async def test_collect_strategy_plain_exception_uses_type_name(self):
        config = _loop_config(on_iteration_fail=IterationFailStrategy.COLLECT)

        record, _ = await _run(
            config,
            {"fetch": lambda inv: {"items": ["a", "b"]}, "work": _failing_on("b")},
        )

        collected = record["steps"]["loop"]["output"][1]
        assert collected["code"] == "ValueError"
        assert "cannot process b" in collected["message"]

    async def test_parallel_respects_max_concurrency(self):
        config = _loop_config(
            parallel=True,
            max_concurrency=MAX_CONCURRENCY,
            on_iteration_fail=IterationFailStrategy.COLLECT,
        )
        active = 0
        max_active = 0

        async def tracked(inv: StepInvocation) -> Any:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.005)
            active -= 1
            return {"item": inv.globals["item"]}

        record, _ = await _run(
            config,
            {"fetch": lambda inv: {"items": list("abcdef")}, "work": tracked},
        )

        assert max_active <= MAX_CONCURRENCY
        assert [r["item"] for r in record["steps"]["loop"]["output"]] == list("abcdef")

    async def test_parallel_runtime_guard_requires_concurrency_bound(self):
        config = _loop_config()
        config.flow[1].parallel = True

        with pytest.raises(FlowExecutionError, match="has no max_concurrency") as exc_info:
            await _run(
                config,
                {
                    "fetch": lambda inv: {"items": ["a"]},
                    "work": lambda inv: {},
                },
            )

        assert exc_info.value.classification.code == FlowErrorKind.INVARIANT_VIOLATION

    async def test_parallel_collect_preserves_order_with_failures(self):
        config = _loop_config(
            parallel=True,
            max_concurrency=MAX_CONCURRENCY,
            on_iteration_fail=IterationFailStrategy.COLLECT,
        )

        record, _ = await _run(
            config,
            {"fetch": lambda inv: {"items": ["a", "bad", "c"]}, "work": _failing_on("bad")},
        )

        output = record["steps"]["loop"]["output"]
        assert output[0]["item"] == "a"
        assert output[1]["_error"] is True
        assert output[2]["item"] == "c"

    async def test_parent_cancellation_awaits_started_children(self):
        config = _loop_config(
            parallel=True,
            max_concurrency=MAX_CONCURRENCY,
            on_iteration_fail=IterationFailStrategy.COLLECT,
        )
        all_started = asyncio.Event()
        release = asyncio.Event()
        started: set[str] = set()
        finished: set[str] = set()

        async def blocked(inv: StepInvocation) -> Any:
            item = inv.globals["item"]
            started.add(item)
            if len(started) == MAX_CONCURRENCY:
                all_started.set()
            try:
                await release.wait()
            finally:
                finished.add(item)

        task = asyncio.create_task(
            _run(
                config,
                {
                    "fetch": lambda inv: {"items": ["a", "b"]},
                    "work": blocked,
                },
            ),
            name="cancelled-iteration-parent",
        )
        await asyncio.wait_for(all_started.wait(), timeout=TEST_WAIT_TIMEOUT_SEC)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert finished == started

    @pytest.mark.parametrize(
        "cancel_factory,expected_type",
        [
            pytest.param(asyncio.CancelledError, asyncio.CancelledError, id="asyncio"),
            pytest.param(
                TemporalCancelledError,
                TemporalCancelledError,
                id="temporal-sdk",
            ),
        ],
    )
    async def test_child_cancellation_is_not_item_data(
        self,
        cancel_factory: Callable[[], BaseException],
        expected_type: type[BaseException],
    ):
        config = _loop_config(
            parallel=True,
            max_concurrency=MAX_CONCURRENCY,
            on_iteration_fail=IterationFailStrategy.COLLECT,
        )
        started: set[str] = set()
        finished: set[str] = set()
        release = asyncio.Event()

        async def cancel_one(inv: StepInvocation) -> Any:
            item = inv.globals["item"]
            started.add(item)
            try:
                await asyncio.sleep(0)
                if item == "cancel":
                    raise cancel_factory()
                await release.wait()
            finally:
                finished.add(item)

        with pytest.raises(expected_type):
            await _run(
                config,
                {
                    "fetch": lambda inv: {"items": ["cancel", "blocked"]},
                    "work": cancel_one,
                },
            )

        assert finished == started

    async def test_fatal_child_failure_propagates_and_awaits_siblings(self):
        config = _loop_config(
            parallel=True,
            max_concurrency=MAX_CONCURRENCY,
            on_iteration_fail=IterationFailStrategy.COLLECT,
        )
        started: set[str] = set()
        finished: set[str] = set()
        release = asyncio.Event()

        async def fail_fatally(inv: StepInvocation) -> Any:
            item = inv.globals["item"]
            started.add(item)
            try:
                await asyncio.sleep(0)
                if item == "fatal":
                    raise FatalIterationError("fatal child")
                await release.wait()
            finally:
                finished.add(item)

        with pytest.raises(FatalIterationError, match="fatal child"):
            await _run(
                config,
                {
                    "fetch": lambda inv: {"items": ["fatal", "blocked"]},
                    "work": fail_fatally,
                },
            )

        assert finished == started

    async def test_mixed_item_failure_and_cancellation_propagates_cancellation(self):
        config = _loop_config(
            parallel=True,
            max_concurrency=MAX_CONCURRENCY,
            on_iteration_fail=IterationFailStrategy.COLLECT,
        )
        all_started = asyncio.Event()
        started: set[str] = set()
        finished: set[str] = set()

        async def mixed_outcome(inv: StepInvocation) -> Any:
            item = inv.globals["item"]
            started.add(item)
            if len(started) == MAX_CONCURRENCY:
                all_started.set()
            try:
                await all_started.wait()
                if item == "error":
                    raise ValueError("ordinary item failure")
                raise asyncio.CancelledError
            finally:
                finished.add(item)

        with pytest.raises(asyncio.CancelledError):
            await _run(
                config,
                {
                    "fetch": lambda inv: {"items": ["error", "cancel"]},
                    "work": mixed_outcome,
                },
            )

        assert finished == {"error", "cancel"}

    async def test_non_iterable_for_each_is_resolution_error(self):
        config = _loop_config()

        with pytest.raises(FlowExecutionError, match="expected a list or dict") as exc_info:
            await _run(config, {"fetch": lambda inv: {"items": 42}, "work": lambda inv: {}})

        assert exc_info.value.classification.code == FlowErrorKind.RESOLUTION_ERROR


class TestAuditRecordV2:
    async def test_records_pinned_execution_configuration(self):
        config = _wf({}, [FlowStep(name="done", terminal=True)])
        executor = FakeExecutor({})
        runner = FlowRunner(
            config,
            SERVICES,
            executor,
            execution_configuration=EXECUTION_CONFIGURATION,
        )

        record = await runner.run(REQUEST_ID, {})

        assert record["execution_configuration"] == EXECUTION_CONFIGURATION.model_dump(
            mode="json",
            exclude_none=True,
        )

    async def test_seq_reflects_execution_order_and_metadata_shape(self):
        config = _wf(
            _step_defs("fetch", "process"),
            [
                FlowStep(name="s1", op="fetch", output="data", then="s2"),
                FlowStep(name="s2", op="process", input="s1.data", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, _ = await _run(config, {"fetch": lambda inv: {"n": 1}, "process": lambda inv: {}})

        assert record["audit_version"] == 4
        assert record["serialization_version"] == 1
        assert record["steps"]["s1"]["seq"] == 0
        assert record["steps"]["s2"]["seq"] == 1
        assert record["steps"]["s1"]["status"] == "succeeded"
        # ISO-8601 timestamps, not epoch floats
        assert record["started_at"].endswith("+00:00")
        assert record["steps"]["s1"]["started_at"].endswith("+00:00")

    async def test_on_result_decisions_are_recorded(self):
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(
                    name="s1",
                    op="fetch",
                    on_result=[
                        OnResultBranch(when="input.n > 10", then="done"),
                        OnResultBranch(default="rejected"),
                    ],
                ),
                FlowStep(name="rejected", terminal=True, reason="too_small"),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, _ = await _run(config, {"fetch": lambda inv: {"n": 1}})

        assert record["transitions"] == [{"step": "s1", "matched": "default", "target": "rejected"}]

    async def test_matched_when_expression_is_recorded(self):
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(
                    name="s1",
                    op="fetch",
                    on_result=[
                        OnResultBranch(when="input.n > 0", then="done"),
                        OnResultBranch(default="done"),
                    ],
                ),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, _ = await _run(config, {"fetch": lambda inv: {"n": 1}})

        assert record["transitions"] == [{"step": "s1", "matched": "input.n > 0", "target": "done"}]

    async def test_iteration_counts_in_audit_entry(self):
        config = _loop_config(on_iteration_fail=IterationFailStrategy.COLLECT)

        record, _ = await _run(
            config,
            {"fetch": lambda inv: {"items": ["a", "b", "c"]}, "work": _failing_on("b")},
        )

        entry = record["steps"]["loop"]
        assert entry["items_total"] == 3
        assert entry["items_failed"] == 1


def _wait_config(events: dict[str, Any] | None = None, **wait_kwargs: Any) -> WorkflowConfig:
    wait_for = WaitForConfig(signal="form_submitted", **(wait_kwargs or {"timeout_sec": 60}))
    return _wf(
        _step_defs("notify"),
        [
            FlowStep(name="wait_form", wait_for=wait_for, output="form", then="done"),
            FlowStep(name="fallback", op="notify", then="done"),
            FlowStep(name="done", terminal=True),
        ],
    )


@dataclass(frozen=True, kw_only=True)
class DeadlineReturns:
    value: float


@dataclass(frozen=True, kw_only=True)
class DeadlineRaises:
    exc: type[Exception]
    match: str


DeadlineOutcome: TypeAlias = DeadlineReturns | DeadlineRaises


@dataclass(frozen=True, kw_only=True)
class DeadlineCase:
    id: str
    value: object
    outcome: DeadlineOutcome


DEADLINE_CASES = [
    DeadlineCase(
        id="epoch-number",
        value=40,
        outcome=DeadlineReturns(value=40.0),
    ),
    DeadlineCase(
        id="utc-designator",
        value="1970-01-01T00:00:40Z",
        outcome=DeadlineReturns(value=40.0),
    ),
    DeadlineCase(
        id="explicit-offset-normalized-to-utc",
        value="1970-01-01T01:00:40+01:00",
        outcome=DeadlineReturns(value=40.0),
    ),
    DeadlineCase(
        id="naive-datetime",
        value="1970-01-01T00:00:40",
        outcome=DeadlineRaises(exc=ValueError, match="must include 'Z' or an explicit UTC offset"),
    ),
    DeadlineCase(
        id="invalid-iso-string",
        value="not-a-deadline",
        outcome=DeadlineRaises(exc=ValueError, match="valid ISO-8601 deadline"),
    ),
    DeadlineCase(
        id="boolean-is-not-an-epoch",
        value=True,
        outcome=DeadlineRaises(exc=TypeError, match="ISO-8601 string or epoch number"),
    ),
]


@pytest.mark.parametrize("case", DEADLINE_CASES, ids=lambda case: case.id)
def test_deadline_contract(case: DeadlineCase) -> None:
    if isinstance(case.outcome, DeadlineReturns):
        assert _deadline_epoch(case.value) == case.outcome.value
        return

    with pytest.raises(case.outcome.exc, match=case.outcome.match):
        _deadline_epoch(case.value)


class TestWaitSteps:
    async def test_event_payload_becomes_output(self):
        config = _wait_config()

        record, executor = await _run(
            config, {"notify": lambda inv: {}}, events={"form_submitted": {"answers": [1]}}
        )

        assert executor.wait_calls == [("form_submitted", 60.0)]
        entry = record["steps"]["wait_form"]
        assert entry["status"] == "succeeded"
        assert entry["signal"] == "form_submitted"
        assert entry["output"] == {"answers": [1]}
        assert record["status"] == "completed"

    async def test_wait_output_feeds_on_result_branching(self):
        config = _wf(
            _step_defs("notify"),
            [
                FlowStep(
                    name="wait_form",
                    wait_for=WaitForConfig(signal="form_submitted", timeout_sec=60),
                    output="form",
                    on_result=[
                        OnResultBranch(when="input.complete == true", then="done"),
                        OnResultBranch(default="chase"),
                    ],
                ),
                FlowStep(name="chase", op="notify", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, _ = await _run(
            config, {"notify": lambda inv: {}}, events={"form_submitted": {"complete": False}}
        )

        assert "chase" in record["steps"]
        assert record["transitions"][0] == {
            "step": "wait_form",
            "matched": "default",
            "target": "chase",
        }

    async def test_timeout_branches_to_on_timeout_and_keeps_going(self):
        config = _wait_config(timeout_sec=60, on_timeout="fallback")

        record, _ = await _run(config, {"notify": lambda inv: {}}, events={})

        assert record["steps"]["wait_form"]["status"] == "timed_out"
        assert record["transitions"][0] == {
            "step": "wait_form",
            "matched": "timeout",
            "target": "fallback",
        }
        assert "fallback" in record["steps"]
        assert record["status"] == "completed"

    async def test_timeout_without_on_timeout_fails(self):
        config = _wait_config(timeout_sec=60)

        with pytest.raises(FlowExecutionError) as exc_info:
            await _run(config, {"notify": lambda inv: {}}, events={})

        assert exc_info.value.classification.code == FlowErrorKind.WAIT_TIMEOUT

    async def test_timeout_until_caps_the_wait(self):
        config = _wf(
            _step_defs("notify"),
            [
                FlowStep(
                    name="wait_form",
                    wait_for=WaitForConfig(
                        signal="form_submitted",
                        timeout_sec=100_000,
                        timeout_until="visit_deadline",
                    ),
                    output="form",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )

        _, executor = await _run(
            config,
            {"notify": lambda inv: {}},
            trigger_globals={"visit_deadline": 50.0},
            events={"form_submitted": {"x": 1}},
        )

        _, timeout = executor.wait_calls[0]
        assert timeout < 100_000  # capped by the deadline, not the static bound

    async def test_passed_deadline_times_out_immediately(self):
        config = _wf(
            _step_defs("notify"),
            [
                FlowStep(
                    name="wait_form",
                    wait_for=WaitForConfig(
                        signal="form_submitted",
                        timeout_until="visit_deadline",
                        on_timeout="fallback",
                    ),
                    output="form",
                    then="done",
                ),
                FlowStep(name="fallback", op="notify", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, executor = await _run(
            config,
            {"notify": lambda inv: {}},
            trigger_globals={"visit_deadline": -100.0},
            events={},
        )

        assert executor.wait_calls[0][1] == 0.0
        assert record["steps"]["wait_form"]["status"] == "timed_out"

    async def test_iso_deadline_is_parsed(self):
        config = _wait_config(timeout_sec=100_000, timeout_until="visit_deadline")

        _, executor = await _run(
            config,
            {"notify": lambda inv: {}},
            trigger_globals={"visit_deadline": "1970-01-01T00:00:40+00:00"},
            events={"form_submitted": {"x": 1}},
        )

        assert executor.wait_calls[0][1] < 100_000

    async def test_naive_iso_deadline_is_a_resolution_error(self) -> None:
        config = _wait_config(timeout_sec=60, timeout_until="visit_deadline")

        with pytest.raises(FlowExecutionError) as exc_info:
            await _run(
                config,
                {"notify": lambda inv: {}},
                trigger_globals={"visit_deadline": "2100-01-01T00:00:00"},
            )

        assert exc_info.value.classification.code == FlowErrorKind.RESOLUTION_ERROR

    async def test_legacy_history_preserves_naive_deadline_behavior(self) -> None:
        config = _wait_config(timeout_until="visit_deadline")

        record, _ = await _run(
            config,
            {"notify": lambda inv: {}},
            trigger_globals={"visit_deadline": "2100-01-01T00:00:00"},
            events={"form_submitted": {"x": 1}},
            timezone_aware_deadlines_enabled=False,
        )

        assert record["status"] == "completed"

    async def test_bad_deadline_reference_is_resolution_error(self):
        config = _wait_config(timeout_sec=60, timeout_until="ghost.deadline")

        with pytest.raises(FlowExecutionError) as exc_info:
            await _run(config, {"notify": lambda inv: {}})

        assert exc_info.value.classification.code == FlowErrorKind.RESOLUTION_ERROR


def _until_config(**step_kwargs: Any) -> WorkflowConfig:
    return _wf(
        _step_defs("check"),
        [
            FlowStep(
                name="poll",
                op="check",
                until="input.status == 'ready'",
                max_iterations=3,
                output="status",
                then="done",
                **step_kwargs,
            ),
            FlowStep(name="give_up", terminal=True, reason="never_ready"),
            FlowStep(name="done", terminal=True),
        ],
    )


def _ready_after(n: int) -> Callable[[StepInvocation], Any]:
    calls = {"count": 0}

    def handler(inv: StepInvocation) -> Any:
        calls["count"] += 1
        return {"status": "ready" if calls["count"] >= n else "pending"}

    return handler


class TestUntilLoops:
    async def test_satisfied_after_retries_with_interval_sleeps(self):
        config = _until_config(interval_sec=30)

        record, executor = await _run(config, {"check": _ready_after(3)})

        entry = record["steps"]["poll"]
        assert entry["status"] == "succeeded"
        assert entry["attempts"] == 3
        assert entry["output"] == {"status": "ready"}
        assert executor.sleep_calls == [30, 30]
        assert [i.step_name for i in executor.invocations] == ["poll#1", "poll#2", "poll#3"]

    async def test_satisfied_first_try_no_sleep(self):
        config = _until_config(interval_sec=30)

        record, executor = await _run(config, {"check": _ready_after(1)})

        assert record["steps"]["poll"]["attempts"] == 1
        assert executor.sleep_calls == []

    async def test_exhausted_branches_to_on_exhausted(self):
        config = _until_config(on_exhausted="give_up")

        record, _ = await _run(config, {"check": _ready_after(99)})

        entry = record["steps"]["poll"]
        assert entry["status"] == "exhausted"
        assert entry["attempts"] == 3
        assert entry["output"] == {"status": "pending"}
        assert record["transitions"][0] == {
            "step": "poll",
            "matched": "exhausted",
            "target": "give_up",
        }
        assert record["reason"] == "never_ready"

    async def test_exhausted_without_target_fails(self):
        config = _until_config()

        with pytest.raises(FlowExecutionError) as exc_info:
            await _run(config, {"check": _ready_after(99)})

        assert exc_info.value.classification.code == FlowErrorKind.LOOP_EXHAUSTED

    async def test_bad_until_expression_is_resolution_error(self):
        config = _wf(
            _step_defs("check"),
            [
                FlowStep(
                    name="poll",
                    op="check",
                    until="input.ghost == 'x'",
                    max_iterations=2,
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )

        with pytest.raises(FlowExecutionError) as exc_info:
            await _run(config, {"check": lambda inv: {"status": "pending"}})

        assert exc_info.value.classification.code == FlowErrorKind.RESOLUTION_ERROR


class TestSleepSteps:
    async def test_sleep_records_and_continues(self):
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(name="s1", op="fetch", then="pause"),
                FlowStep(name="pause", sleep_sec=300, then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, executor = await _run(config, {"fetch": lambda inv: {}})

        assert executor.sleep_calls == [300]
        assert record["steps"]["pause"] == {"seq": 1, "status": "slept", "sleep_sec": 300}
        assert record["status"] == "completed"


class TestSubWorkflows:
    def _config(self, **flow_step_kwargs: Any) -> WorkflowConfig:
        return _wf(
            {
                "fetch": StepDefinition(service="svc", action="fetch"),
                "transform": StepDefinition(workflow="data_enrichment", params={"p": "1"}),
            },
            [
                FlowStep(name="s1", op="fetch", output="data", then="child"),
                FlowStep(
                    name="child",
                    op="transform",
                    input="s1.data",
                    output="enriched",
                    then="done",
                    **flow_step_kwargs,
                ),
                FlowStep(name="done", terminal=True),
            ],
        )

    async def test_child_gets_params_and_input_and_result_is_unwrapped(self):
        config = self._config()

        record, executor = await _run(
            config,
            {"fetch": lambda inv: {"record": "R1"}},
            subworkflows={
                "data_enrichment": {
                    "audit_version": 2,
                    "result": {"processed": True},
                }
            },
        )

        assert executor.subworkflow_calls == [
            (
                "data_enrichment",
                "child",
                {"p": "1", "input": {"record": "R1"}},
            )
        ]
        assert record["steps"]["child"]["output"] == {"processed": True}

    async def test_child_without_result_returns_full_record(self):
        config = self._config()
        child_record = {"audit_version": 2, "status": "completed", "steps": {}}

        record, _ = await _run(
            config,
            {"fetch": lambda inv: {"record": "R1"}},
            subworkflows={"data_enrichment": child_record},
        )

        assert record["steps"]["child"]["output"] == child_record

    async def test_for_each_fans_out_child_workflows(self):
        config = _wf(
            {
                "fetch": StepDefinition(service="svc", action="fetch"),
                "transform": StepDefinition(workflow="data_enrichment"),
            },
            [
                FlowStep(name="s1", op="fetch", output="data", then="each"),
                FlowStep(
                    name="each",
                    op="transform",
                    input="s1.data",
                    for_each="input.items",
                    as_var="item",
                    output="results",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, executor = await _run(
            config,
            {"fetch": lambda inv: {"items": ["a", "b"]}},
            subworkflows={"data_enrichment": {"result": "ok"}},
        )

        assert [(c[1], c[2]["item"]) for c in executor.subworkflow_calls] == [
            ("each[0]", "a"),
            ("each[1]", "b"),
        ]
        assert record["steps"]["each"]["output"] == ["ok", "ok"]


class TestStepCaching:
    def _config(self) -> WorkflowConfig:
        return _wf(
            {
                "compute": StepDefinition(
                    service="svc",
                    action="compute",
                    cache={"resource": "redis", "key": "compute:${source_id}", "ttl_sec": 60},
                ),
            },
            [
                FlowStep(name="s1", op="compute", output="data", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

    async def test_cache_key_is_interpolated_and_shipped(self):
        record, executor = await _run(
            self._config(),
            {"compute": lambda inv: StepResult({"x": 1}, cache="miss")},
            trigger_globals={"source_id": "s42"},
        )

        assert executor.invocations[0].cache == CacheDirective(
            resource="redis", key="compute:s42", ttl_sec=60
        )
        assert record["steps"]["s1"]["cache"] == "miss"

    async def test_cache_hit_recorded_in_audit(self):
        record, _ = await _run(
            self._config(),
            {"compute": lambda inv: StepResult({"x": 1}, cache="hit")},
            trigger_globals={"source_id": "s42"},
        )

        assert record["steps"]["s1"]["cache"] == "hit"

    async def test_uncached_steps_have_no_cache_field(self):
        config = _wf(
            _step_defs("fetch"),
            [FlowStep(name="s1", op="fetch", then="done"), FlowStep(name="done", terminal=True)],
        )

        record, executor = await _run(config, {"fetch": lambda inv: {}})

        assert executor.invocations[0].cache is None
        assert "cache" not in record["steps"]["s1"]


class TestWorkflowResult:
    async def test_result_reference_recorded_in_audit(self):
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(name="s1", op="fetch", output="data", then="done"),
                FlowStep(name="done", terminal=True),
            ],
            result="s1.data",
        )

        record, _ = await _run(config, {"fetch": lambda inv: {"n": 7}})

        assert record["result"] == {"n": 7}

    async def test_early_terminal_omits_unreachable_result(self):
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(name="s1", op="fetch", output="data", then="skip_to"),
                FlowStep(name="skip_to", terminal=True, reason="early"),
                FlowStep(name="unreached", op="fetch", output="final", then="skip_to"),
            ],
            result="unreached.final",
        )

        record, _ = await _run(config, {"fetch": lambda inv: {"n": 7}})

        assert "result" not in record

    async def test_no_result_field_means_no_result_key(self):
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(name="s1", op="fetch", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, _ = await _run(config, {"fetch": lambda inv: {}})

        assert "result" not in record


class TestWorkflowContracts:
    async def test_input_contract_validates_raw_trigger_globals(self):
        input_schema = {
            "type": "object",
            "properties": {"source_id": {"type": "string"}},
            "required": ["source_id"],
            "additionalProperties": False,
        }
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(name="s1", op="fetch", then="done"),
                FlowStep(name="done", terminal=True),
            ],
            params={"tenant": "fixed"},
            input_schema=input_schema,
        )

        record, executor = await _run(
            config,
            {"fetch": lambda invocation: {}},
            trigger_globals={"source_id": "R1"},
        )

        assert record["status"] == "completed"
        assert executor.contract_validations == [
            (
                input_schema,
                {"source_id": "R1"},
                "workflow input",
                "test_flow",
            )
        ]

    async def test_input_contract_failure_has_typed_record_before_work(self):
        input_schema = {
            "type": "object",
            "properties": {"source_id": {"type": "string"}},
            "required": ["source_id"],
        }
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(name="s1", op="fetch", then="done"),
                FlowStep(name="done", terminal=True),
            ],
            input_schema=input_schema,
        )
        executor = FakeExecutor({"fetch": lambda invocation: {}})

        with pytest.raises(FlowExecutionError, match="violates its schema") as exc_info:
            await FlowRunner(config, SERVICES, executor).run(REQUEST_ID, {})

        assert exc_info.value.classification.code == "INPUT_CONTRACT_FAILED"
        assert exc_info.value.classification.cause_code == ContractViolation.__name__
        assert exc_info.value.classification.step is None
        assert executor.invocations == []

    async def test_output_contract_failure_archives_typed_record(self):
        output_schema = {
            "type": "object",
            "properties": {"record_id": {"type": "string"}},
            "required": ["record_id"],
        }
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(name="s1", op="fetch", output="data", then="done"),
                FlowStep(name="done", terminal=True),
            ],
            result="s1.data",
            output_schema=output_schema,
            on_complete=OnCompleteConfig(
                resource="store",
                path="audit.json",
                retention_policy=RETENTION_POLICY,
            ),
        )
        executor = FakeExecutor({"fetch": lambda invocation: {"unexpected": True}})
        runner = FlowRunner(config, SERVICES, executor)

        with pytest.raises(FlowExecutionError, match="violates its schema") as exc_info:
            await runner.run(REQUEST_ID, {})

        assert exc_info.value.classification.code == "RESULT_FAILED"
        assert exc_info.value.classification.cause_code == ContractViolation.__name__
        assert exc_info.value.classification.step is None
        assert executor.archived[0][4] == {
            **exc_info.value.record.dump(),
            "capture_mode": AuditCaptureMode.METADATA_ONLY,
        }


@dataclass(frozen=True, kw_only=True)
class LoopReturns:
    value: int


@dataclass(frozen=True, kw_only=True)
class LoopRaises:
    exc: type[BaseException]
    match: str


LoopOutcome = LoopReturns | LoopRaises


@dataclass(frozen=True, kw_only=True)
class RuntimeLoopCacheCase:
    id: str
    loop_kind: str
    parallel: bool
    cached: bool
    outcome: LoopOutcome


RUNTIME_LOOP_CACHE_CASES = [
    RuntimeLoopCacheCase(
        id="cached-sequential-for-each",
        loop_kind="for_each",
        parallel=False,
        cached=True,
        outcome=LoopRaises(
            exc=FlowExecutionError,
            match="operation 'work'.*for_each.*cache resource='cache'.*key='fixed'",
        ),
    ),
    RuntimeLoopCacheCase(
        id="cached-parallel-for-each",
        loop_kind="for_each",
        parallel=True,
        cached=True,
        outcome=LoopRaises(
            exc=FlowExecutionError,
            match="operation 'work'.*for_each.*cache resource='cache'.*key='fixed'",
        ),
    ),
    RuntimeLoopCacheCase(
        id="cached-until",
        loop_kind="until",
        parallel=False,
        cached=True,
        outcome=LoopRaises(
            exc=FlowExecutionError,
            match="operation 'work'.*until.*cache resource='cache'.*key='fixed'",
        ),
    ),
    RuntimeLoopCacheCase(
        id="uncached-for-each",
        loop_kind="for_each",
        parallel=False,
        cached=False,
        outcome=LoopReturns(value=2),
    ),
    RuntimeLoopCacheCase(
        id="uncached-until",
        loop_kind="until",
        parallel=False,
        cached=False,
        outcome=LoopReturns(value=1),
    ),
]


class TestRuntimeLoopCacheGuard:
    @pytest.mark.parametrize(
        "case",
        RUNTIME_LOOP_CACHE_CASES,
        ids=lambda case: case.id,
    )
    async def test_loop_cache_behavior(self, case: RuntimeLoopCacheCase):
        cache = {"resource": "cache", "key": "fixed"} if case.cached else None
        step_definition = StepDefinition(
            service="svc",
            action="work",
            cache=cache,
        )
        if case.loop_kind == "for_each":
            loop_step = FlowStep(
                name="loop",
                op="work",
                for_each="items",
                parallel=case.parallel,
                max_concurrency=MAX_CONCURRENCY if case.parallel else None,
                then="done",
            )
            trigger_globals = {"items": [1, 2]}
        else:
            loop_step = FlowStep(
                name="loop",
                op="work",
                until="input.ready == true",
                max_iterations=2,
                then="done",
            )
            trigger_globals = {}

        config = _wf(
            {"work": step_definition},
            [loop_step, FlowStep(name="done", terminal=True)],
        )
        executor = FakeExecutor({"work": lambda invocation: {"ready": True}})
        runner = FlowRunner(config, SERVICES, executor)

        if isinstance(case.outcome, LoopRaises):
            with pytest.raises(case.outcome.exc, match=case.outcome.match) as exc_info:
                await runner.run(REQUEST_ID, trigger_globals)
            assert exc_info.value.classification.code == FlowErrorKind.UNSAFE_LOOP_CACHE
            assert executor.invocations == []
        else:
            await runner.run(REQUEST_ID, trigger_globals)
            assert len(executor.invocations) == case.outcome.value


class TestOnError:
    def _config(self, **wf_kwargs: Any) -> WorkflowConfig:
        return _wf(
            _step_defs("fetch", "alert"),
            [
                FlowStep(name="s1", op="fetch", output="data", then="done"),
                FlowStep(name="notify_ops", op="alert", input="error", then="failed"),
                FlowStep(name="failed", terminal=True, reason="handled"),
                FlowStep(name="done", terminal=True),
            ],
            on_error={"then": "notify_ops"},
            **wf_kwargs,
        )

    async def test_step_failure_routes_to_handler_with_error_alias(self):
        config = self._config(
            on_complete={
                "resource": "s3",
                "path": "audit/${request_id}.json",
                "retention_policy": RETENTION_POLICY,
            }
        )

        record, executor = await _run(
            config,
            {
                "fetch": lambda inv: RuntimeError("db exploded"),
                "alert": lambda inv: {"sent": True},
            },
        )

        failed_entry = record["steps"]["s1"]
        assert failed_entry["status"] == "failed"
        assert failed_entry["code"] == "STEP_FAILED"

        alert_inv = next(i for i in executor.invocations if i.action == "alert")
        assert alert_inv.input == {
            "step": "s1",
            "code": "STEP_FAILED",
            "cause_code": "RuntimeError",
            "message": "db exploded",
        }

        assert record["error"] == {
            "step": "s1",
            "code": "STEP_FAILED",
            "cause_code": "RuntimeError",
            "message": "db exploded",
        }
        assert record["status"] == "terminated"
        assert record["reason"] == "handled"
        assert record["transitions"][0] == {
            "step": "s1",
            "matched": "error",
            "target": "notify_ops",
        }
        archived_record = executor.archived[0][4]
        assert archived_record["capture_mode"] == AuditCaptureMode.METADATA_ONLY
        assert "message" not in archived_record["error"]
        assert "result" not in archived_record

    async def test_failure_in_handler_chain_fails_the_workflow(self):
        config = self._config()

        with pytest.raises(FlowExecutionError, match="db exploded") as exc_info:
            await _run(
                config,
                {
                    "fetch": lambda inv: RuntimeError("db exploded"),
                    "alert": lambda inv: RuntimeError("alerting broke too"),
                },
            )

        assert exc_info.value.classification.code == "STEP_FAILED"
        assert exc_info.value.secondary_failures[0].code == "HANDLER_FAILED"
        assert exc_info.value.secondary_failures[0].cause_code == "RuntimeError"
        assert exc_info.value.secondary_failures[0].step == "notify_ops"

    async def test_archive_failure_after_recovery_is_a_new_failure(self):
        config = self._config(
            on_complete={
                "resource": "s3",
                "path": "audit.json",
                "retention_policy": RETENTION_POLICY,
            }
        )

        with pytest.raises(FlowExecutionError, match="archive unavailable") as exc_info:
            await _run(
                config,
                {
                    "fetch": lambda inv: RuntimeError("db exploded"),
                    "alert": lambda inv: {"sent": True},
                },
                archive_error=OSError("archive unavailable"),
            )

        assert exc_info.value.classification.code == "ARCHIVAL_FAILED"
        assert exc_info.value.secondary_failures == ()

    async def test_definition_errors_still_fail_hard(self):
        config = self._config()
        config.flow[0].input = "ghost.data"

        with pytest.raises(FlowExecutionError) as exc_info:
            await _run(config, {"fetch": lambda inv: {}, "alert": lambda inv: {}})

        assert exc_info.value.classification.code == FlowErrorKind.RESOLUTION_ERROR

    async def test_no_on_error_keeps_propagation(self):
        config = _wf(
            _step_defs("fetch"),
            [FlowStep(name="s1", op="fetch", then="done"), FlowStep(name="done", terminal=True)],
        )

        with pytest.raises(FlowExecutionError, match="db exploded") as exc_info:
            await _run(config, {"fetch": lambda inv: RuntimeError("db exploded")})

        assert exc_info.value.classification.code == "STEP_FAILED"

    async def test_wrapped_cause_is_unwrapped_in_error_info(self):
        class FakeActivityError(Exception):
            def __init__(self):
                super().__init__("Activity task failed")
                self.cause = FakeCause()

        class FakeCause(Exception):
            type = "CONTRACT_VIOLATION"
            message = "output violates schema"

        config = self._config()

        record, _ = await _run(
            config,
            {"fetch": lambda inv: FakeActivityError(), "alert": lambda inv: {}},
        )

        assert record["error"]["code"] == "STEP_FAILED"
        assert record["error"]["cause_code"] == "CONTRACT_VIOLATION"


class TestPerStepOnFailure:
    def _config(
        self,
        *,
        on_error: dict[str, str] | None = None,
        **step_kwargs: Any,
    ) -> WorkflowConfig:
        return _wf(
            _step_defs("process", "notify", "other"),
            [
                FlowStep(
                    name="s1",
                    op="process",
                    output="data",
                    on_failure="notify",
                    then="s2",
                    **step_kwargs,
                ),
                FlowStep(name="notify", op="notify", input="error", then="failed"),
                FlowStep(name="failed", terminal=True, reason="process_error"),
                FlowStep(name="s2", op="other", then="done"),
                FlowStep(name="done", terminal=True),
            ],
            on_error=on_error,
        )

    async def test_step_failure_routes_to_its_handler(self):
        record, executor = await _run(
            self._config(),
            {
                "process": lambda inv: RuntimeError("remote service changed"),
                "notify": lambda inv: {"sent": True},
                "other": lambda inv: {},
            },
        )

        assert record["steps"]["s1"]["status"] == "failed"
        notification = next(i for i in executor.invocations if i.action == "notify")
        assert notification.input["message"] == "remote service changed"
        assert record["reason"] == "process_error"
        assert record["error"]["step"] == "s1"
        assert record["transitions"][0] == {
            "step": "s1",
            "matched": "error",
            "target": "notify",
        }

    async def test_per_step_handler_wins_over_workflow_on_error(self):
        config = self._config(on_error={"then": "s2"})

        record, _ = await _run(
            config,
            {
                "process": lambda inv: RuntimeError("boom"),
                "notify": lambda inv: {},
                "other": lambda inv: {},
            },
        )

        assert "notify" in record["steps"]
        assert "s2" not in record["steps"]

    async def test_success_ignores_on_failure(self):
        record, _ = await _run(
            self._config(),
            {"process": lambda inv: {"ok": 1}, "notify": lambda inv: {}, "other": lambda inv: {}},
        )

        assert "notify" not in record["steps"]
        assert record["status"] == "completed"
        assert "error" not in record

    async def test_second_failure_fails_hard(self):
        with pytest.raises(FlowExecutionError, match="boom") as exc_info:
            await _run(
                self._config(),
                {
                    "process": lambda inv: RuntimeError("boom"),
                    "notify": lambda inv: RuntimeError("alert broke"),
                    "other": lambda inv: {},
                },
            )

        assert exc_info.value.classification.step == "s1"
        assert exc_info.value.secondary_failures[0].step == "notify"


class TestFailureEpisodes:
    async def test_later_independent_failure_uses_its_own_handler(self):
        config = _wf(
            _step_defs("first", "recover_first", "second", "recover_second"),
            [
                FlowStep(
                    name="first",
                    op="first",
                    on_failure="recover_first",
                    then="second",
                ),
                FlowStep(
                    name="recover_first",
                    op="recover_first",
                    input="error",
                    then="second",
                ),
                FlowStep(
                    name="second",
                    op="second",
                    on_failure="recover_second",
                    then="done",
                ),
                FlowStep(
                    name="recover_second",
                    op="recover_second",
                    input="error",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )

        record, executor = await _run(
            config,
            {
                "first": lambda inv: RuntimeError("first failed"),
                "recover_first": lambda inv: {"recovered": inv.input["step"]},
                "second": lambda inv: LookupError("second failed"),
                "recover_second": lambda inv: {"recovered": inv.input["step"]},
            },
        )

        recovery_inputs = [
            invocation.input
            for invocation in executor.invocations
            if invocation.action in {"recover_first", "recover_second"}
        ]
        assert [failure["step"] for failure in recovery_inputs] == [
            "first",
            "second",
        ]
        assert [failure["cause_code"] for failure in recovery_inputs] == [
            "RuntimeError",
            "LookupError",
        ]
        assert record["status"] == "recovered"
        assert [failure["step"] for failure in record["failures"]] == [
            "first",
            "second",
        ]


class TestArchival:
    async def test_on_complete_archives_with_interpolated_path(self):
        config = _wf(
            _step_defs("fetch"),
            [FlowStep(name="s1", op="fetch", then="done"), FlowStep(name="done", terminal=True)],
            on_complete=OnCompleteConfig(
                resource="s3",
                path="audit/${request_id}.json",
                retention_policy=RETENTION_POLICY,
            ),
        )

        record, executor = await _run(config, {"fetch": lambda inv: {}})

        assert executor.archived == [
            (
                "s3",
                f"audit/{REQUEST_ID}.json",
                RETENTION_POLICY,
                AuditCaptureMode.METADATA_ONLY,
                {
                    "audit_version": 4,
                    "serialization_version": 1,
                    "request_id": REQUEST_ID,
                    "workflow": config.workflow,
                    "status": "completed",
                    "reason": None,
                    "steps": {
                        "s1": {
                            "seq": 0,
                            "status": "succeeded",
                            "started_at": record["steps"]["s1"]["started_at"],
                            "duration_ms": record["steps"]["s1"]["duration_ms"],
                        }
                    },
                    "transitions": [],
                    "started_at": record["started_at"],
                    "completed_at": record["completed_at"],
                    "capture_mode": AuditCaptureMode.METADATA_ONLY,
                },
            )
        ]

    async def test_no_on_complete_means_no_archive(self):
        config = _wf(
            _step_defs("fetch"),
            [FlowStep(name="s1", op="fetch", then="done"), FlowStep(name="done", terminal=True)],
        )

        _, executor = await _run(config, {"fetch": lambda inv: {}})

        assert executor.archived == []

    async def test_success_archive_failure_has_typed_record(self):
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(name="s1", op="fetch", then="done"),
                FlowStep(name="done", terminal=True),
            ],
            on_complete=OnCompleteConfig(
                resource="s3",
                path="audit.json",
                retention_policy=RETENTION_POLICY,
            ),
        )

        with pytest.raises(FlowExecutionError, match="archive unavailable") as exc_info:
            await _run(
                config,
                {"fetch": lambda inv: {}},
                archive_error=OSError("archive unavailable"),
            )

        assert exc_info.value.classification.code == "ARCHIVAL_FAILED"
        assert exc_info.value.classification.phase.value == "archive"
        assert exc_info.value.classification.step is None
        assert exc_info.value.record.status == "failed"

    async def test_failure_archive_does_not_mask_primary_failure(self):
        config = _wf(
            _step_defs("fetch"),
            [
                FlowStep(name="s1", op="fetch", then="done"),
                FlowStep(name="done", terminal=True),
            ],
            on_complete=OnCompleteConfig(
                resource="s3",
                path="audit.json",
                retention_policy=RETENTION_POLICY,
            ),
        )
        executor = FakeExecutor(
            {"fetch": lambda inv: RuntimeError("primary failure")},
            archive_error=OSError("archive unavailable"),
        )

        with pytest.raises(FlowExecutionError, match="primary failure") as exc_info:
            await FlowRunner(config, SERVICES, executor).run(REQUEST_ID, {})

        assert exc_info.value.classification.code == "STEP_FAILED"
        assert exc_info.value.classification.cause_code == "RuntimeError"
        assert exc_info.value.secondary_failures[0].code == "ARCHIVAL_FAILED"
        assert exc_info.value.secondary_failures[0].cause_code == "OSError"
        assert len(executor.archived) == 1
