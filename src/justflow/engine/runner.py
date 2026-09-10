"""Pure workflow orchestration logic, independent of Temporal.

FlowRunner executes a WorkflowConfig's flow (branching, conditions, iteration,
convergence, audit assembly) against a StepExecutor. The compiler provides an
executor backed by Temporal activities and signals; tests provide fakes.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Protocol

from justflow.config.grammar import ReferencePath
from justflow.config.models import (
    AuditCaptureMode,
    ChildWorkflowTarget,
    EvaluatorCondition,
    FlowStep,
    IterationFailStrategy,
    ServiceOperationTarget,
    StepDefinition,
    WorkflowConfig,
)
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.engine.audit import (
    AuditRecord,
    CorrelationIdentity,
    FailureDetail,
    FailurePhase,
    IterationFailure,
    StepAuditEntry,
    StepFailure,
    TransitionRecord,
)
from justflow.engine.audit_capture import capture_audit_record
from justflow.engine.continuation import (
    FailureEpisodeCheckpoint,
    IncludedIterationResult,
    IterationCheckpoint,
    LoopCheckpoint,
    OmittedIterationResult,
    RunnerCheckpoint,
    UntilCheckpoint,
)
from justflow.engine.data import (
    DataNormalizationError,
    DataPathError,
    normalize_json_object,
    normalize_json_value,
    resolve_json_path,
)
from justflow.engine.errors import (
    FailureCode,
    FlowDefinitionError,
    FlowErrorKind,
    FlowExecutionError,
    build_execution_error,
    classify_failure,
    correlation_identity,
    is_cancellation,
)
from justflow.engine.evaluator import ConditionEvaluationError, evaluate_condition
from justflow.engine.limits import (
    LimitKind,
    enforce_limit,
    enforce_payload_bytes,
)
from justflow.engine.references import (
    ReferenceResolutionError,
    find_unresolved_placeholders,
    interpolate_params,
    resolve_input,
    resolve_reference,
)
from justflow.engine.serialization import STRICT_JSON_VERSION
from justflow.provenance import ExecutionConfigurationIdentity, WorkerArtifactIdentity
from justflow.transports.registry import ResolvedService

ITERATION_ERROR_FIELD = "_error"


class _TimedOutType:
    """Sentinel returned by StepExecutor.wait_for_event on timeout."""


TIMED_OUT = _TimedOutType()

# Temporal's JSON converter sorts dict keys, so audit consumers cannot rely on
# insertion order — every step entry carries an explicit `seq` instead.
AUDIT_VERSION = 4


class StepStatus(str, Enum):
    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    TIMED_OUT = "timed_out"
    SLEPT = "slept"
    EXHAUSTED = "exhausted"
    FAILED = "failed"


# Reserved output alias exposing stable and underlying failure identity to handlers.
ERROR_ALIAS = "error"


class WorkflowStatus(str, Enum):
    COMPLETED = "completed"
    RECOVERED = "recovered"
    TERMINATED = "terminated"


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat()


@dataclass(frozen=True)
class _FailureEpisode:
    primary: FailureDetail
    handler: str
    handler_steps: frozenset[str]


@dataclass(frozen=True, kw_only=True)
class HistoryObservation:
    events: int
    bytes: int
    server_suggested: bool = False


class FlowContinuation(Exception):
    def __init__(self, checkpoint: RunnerCheckpoint, observation: HistoryObservation) -> None:
        self.checkpoint = checkpoint
        self.observation = observation
        super().__init__("Workflow execution requires a fresh Temporal history")


class _HandlerFailure(Exception):
    def __init__(self, primary: FailureDetail, handler: FailureDetail) -> None:
        self.primary = primary
        self.handler = handler
        super().__init__(f"Handler step '{handler.step}' failed while handling '{primary.step}'")


@dataclass
class _RunState:
    correlation: CorrelationIdentity
    started_at: str
    params: dict[str, Any]
    step_outputs: dict[str, Any]
    step_timings: dict[str, dict[str, Any]]
    transitions: list[TransitionRecord]
    failures: list[StepFailure]
    active_episode: _FailureEpisode | None = None
    phase: FailurePhase = FailurePhase.INTERNAL
    current_step_name: str | None = None
    current_step_started: float | None = None
    next_seq: int = 0


@dataclass(frozen=True)
class CacheDirective:
    """Interpolated cache instruction shipped from the runner to the activity."""

    resource: str
    key: str
    ttl_sec: int | None = None
    definition_digest: str | None = None
    contract_identity: str | None = None


@dataclass
class StepInvocation:
    step_name: str
    service_name: str
    action: str
    input: Any
    globals: dict[str, Any]
    cache: CacheDirective | None = None
    # Contract schemas (inline JSON Schema or pydantic dotted path) or None.
    input_schema: dict[str, Any] | str | None = None
    output_schema: dict[str, Any] | str | None = None
    required_resources: tuple[str, ...] = ()


@dataclass
class StepResult:
    data: Any
    cache: str | None = None  # "hit" | "miss" | None (caching not configured)


class StepExecutor(Protocol):
    async def run_step(
        self, invocation: StepInvocation, service: ResolvedService
    ) -> StepResult: ...

    async def run_evaluator(
        self, evaluator: str, data: Any, resource_names: list[str], step_name: str
    ) -> bool: ...

    async def wait_for_event(self, signal: str, timeout_sec: float) -> Any:
        """Wait for an external event; returns its payload or TIMED_OUT."""
        ...

    async def sleep(self, seconds: float) -> None: ...

    async def validate_contract(
        self,
        schema: dict[str, Any] | str,
        payload: Any,
        *,
        direction: str,
        boundary_name: str,
    ) -> None: ...

    async def run_subworkflow(
        self, workflow_name: str, step_name: str, trigger_globals: dict[str, Any]
    ) -> Any:
        """Execute a child workflow and return its audit record."""
        ...

    async def archive(
        self,
        resource: str,
        path: str,
        retention_policy: str,
        capture_mode: AuditCaptureMode,
        record: dict[str, Any],
    ) -> None: ...

    def observe_history(self) -> HistoryObservation: ...

    def now(self) -> float: ...


class FlowRunner:
    """Executes one workflow run. Instantiate per run."""

    def __init__(
        self,
        workflow_config: WorkflowConfig,
        services: Mapping[str, ResolvedService],
        executor: StepExecutor,
        run_id: str | None = None,
        limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
        definition_digest: str | None = None,
        worker_deployment: str | None = None,
        worker_build_id: str | None = None,
        worker_artifact: WorkerArtifactIdentity | None = None,
        environment_snapshot_digest: str | None = None,
        trigger_source: str | None = None,
        trigger_name: str | None = None,
        source_identity_digest: str | None = None,
        correlation_identity_digest: str | None = None,
        scope_digest: str | None = None,
        execution_configuration: ExecutionConfigurationIdentity | None = None,
        bounded_execution_enabled: bool = True,
        timezone_aware_deadlines_enabled: bool = True,
    ):
        self._config = workflow_config
        self._services = services
        self._executor = executor
        self._flow_steps = {step.name: step for step in workflow_config.flow}
        self._run_id = run_id
        self._limits = limits
        self._definition_digest = definition_digest
        self._worker_deployment = worker_deployment
        self._worker_build_id = worker_build_id
        self._worker_artifact = worker_artifact
        self._environment_snapshot_digest = environment_snapshot_digest
        self._trigger_source = trigger_source
        self._trigger_name = trigger_name
        self._source_identity_digest = source_identity_digest
        self._correlation_identity_digest = correlation_identity_digest
        self._scope_digest = scope_digest
        self._execution_configuration = execution_configuration
        self._bounded_execution_enabled = bounded_execution_enabled
        self._timezone_aware_deadlines_enabled = timezone_aware_deadlines_enabled
        self._invocations = 0
        self._normal_reachable = self._reachable_from_entry()

    async def run(
        self,
        request_id: str,
        trigger_globals: dict[str, Any],
        checkpoint: RunnerCheckpoint | None = None,
    ) -> dict[str, Any]:
        state = self._restore_state(request_id, checkpoint)
        try:
            return await self._execute(trigger_globals, state, checkpoint)
        except BaseException as exc:
            if is_cancellation(exc) or not isinstance(exc, Exception):
                raise
            if isinstance(exc, (FlowContinuation, FlowExecutionError)):
                raise
            failure = self._build_execution_error(exc, state)
            failure = await self._archive_failure(failure, state)
            raise failure from exc

    def _restore_state(
        self,
        request_id: str,
        checkpoint: RunnerCheckpoint | None,
    ) -> _RunState:
        correlation = correlation_identity(
            workflow=self._config.workflow,
            request_id=request_id,
            run_id=self._run_id,
            definition_digest=self._definition_digest,
            worker_deployment=self._worker_deployment,
            worker_build_id=self._worker_build_id,
            worker_artifact=self._worker_artifact,
            environment_snapshot_digest=self._environment_snapshot_digest,
            trigger_source=self._trigger_source,
            trigger_name=self._trigger_name,
            source_identity_digest=self._source_identity_digest,
            correlation_identity_digest=self._correlation_identity_digest,
            scope_digest=self._scope_digest,
            execution_configuration=self._execution_configuration,
        )
        if checkpoint is None:
            return _RunState(
                correlation=correlation,
                started_at=_iso(self._executor.now()),
                params={},
                step_outputs={},
                step_timings={},
                transitions=[],
                failures=[],
            )
        self._invocations = checkpoint.invocations
        active_episode = checkpoint.active_episode
        return _RunState(
            correlation=correlation,
            started_at=checkpoint.started_at,
            params=dict(checkpoint.params),
            step_outputs=dict(checkpoint.step_outputs),
            step_timings={name: dict(entry) for name, entry in checkpoint.step_timings.items()},
            transitions=list(checkpoint.transitions),
            failures=list(checkpoint.failures),
            active_episode=(
                _FailureEpisode(
                    primary=active_episode.primary,
                    handler=active_episode.handler,
                    handler_steps=active_episode.handler_steps,
                )
                if active_episode is not None
                else None
            ),
        )

    async def _execute(
        self,
        trigger_globals: dict[str, Any],
        state: _RunState,
        checkpoint: RunnerCheckpoint | None,
    ) -> dict[str, Any]:
        request_id = state.correlation.request_id
        if request_id is None:
            raise FlowDefinitionError(
                "Workflow execution requires a request id",
                FlowErrorKind.INVARIANT_VIOLATION,
            )
        if checkpoint is None:
            state.phase = FailurePhase.WORKFLOW_INPUT
            trigger_globals = normalize_json_object(
                trigger_globals,
                path="trigger.globals",
                max_collection_items=self._limits.collection_items,
            )
            enforce_payload_bytes(
                trigger_globals,
                boundary="workflow.trigger",
                limit=self._limits.trigger_payload_bytes,
            )
            if self._config.input_schema is not None:
                await self._executor.validate_contract(
                    self._config.input_schema,
                    trigger_globals,
                    direction="workflow input",
                    boundary_name=self._config.workflow,
                )
            state.phase = FailurePhase.PARAMETERS
            state.params = self._build_params(request_id, trigger_globals)
        params = state.params

        if not self._config.flow:
            raise FlowDefinitionError(
                f"Workflow '{self._config.workflow}' has no entry step",
                FlowErrorKind.INVARIANT_VIOLATION,
            )

        step_outputs = state.step_outputs
        step_timings = state.step_timings
        transitions = state.transitions

        current_step_name: str | None = (
            checkpoint.next_step_name if checkpoint is not None else self._config.flow[0].name
        )
        final_reason: str | None = None
        reached_terminal = False
        seq = checkpoint.next_seq if checkpoint is not None else 0
        while current_step_name:
            state.phase = FailurePhase.STEP
            state.current_step_name = current_step_name
            state.current_step_started = None
            state.next_seq = seq
            if (
                state.active_episode is not None
                and current_step_name not in state.active_episode.handler_steps
            ):
                state.active_episode = None
            step = self._flow_steps[current_step_name]
            loop_checkpoint = checkpoint.loop if checkpoint is not None else None
            checkpoint = None
            if loop_checkpoint is not None:
                checkpoint_matches_step = loop_checkpoint.step_name == step.name
                checkpoint_matches_kind = (
                    isinstance(loop_checkpoint, IterationCheckpoint) and step.for_each is not None
                ) or (isinstance(loop_checkpoint, UntilCheckpoint) and step.until is not None)
                if not checkpoint_matches_step or not checkpoint_matches_kind:
                    raise FlowDefinitionError(
                        f"Workflow continuation does not match step '{step.name}'",
                        FlowErrorKind.INVARIANT_VIOLATION,
                    )

            if step.terminal:
                final_reason = step.reason
                reached_terminal = True
                break

            if step.sleep_sec is not None:
                await self._executor.sleep(step.sleep_sec)
                step_timings[step.name] = StepAuditEntry(
                    seq=seq, status=StepStatus.SLEPT.value, sleep_sec=step.sleep_sec
                ).dump()
                seq += 1
                current_step_name = step.then
                self._checkpoint_after_step(state, current_step_name, seq)
                continue

            if step.wait_for is not None:
                step_started = self._executor.now()
                payload = await self._run_wait(step, step_outputs, params)
                if payload is TIMED_OUT:
                    step_timings[step.name] = StepAuditEntry(
                        seq=seq,
                        status=StepStatus.TIMED_OUT.value,
                        signal=step.wait_for.signal,
                    ).dump()
                    seq += 1
                    if step.wait_for.on_timeout is None:
                        raise FlowDefinitionError(
                            f"Step '{step.name}': no '{step.wait_for.signal}' event "
                            f"within the configured bound and no on_timeout target",
                            FlowErrorKind.WAIT_TIMEOUT,
                        )
                    transitions.append(
                        TransitionRecord(
                            step=step.name, matched="timeout", target=step.wait_for.on_timeout
                        )
                    )
                    current_step_name = step.wait_for.on_timeout
                    self._checkpoint_after_step(state, current_step_name, seq)
                    continue

                payload = normalize_json_value(
                    payload,
                    path=f"steps.{step.name}.output",
                    max_collection_items=self._limits.collection_items,
                )
                self._store_output(step, payload, step_outputs)
                step_timings[step.name] = StepAuditEntry(
                    seq=seq,
                    status=StepStatus.SUCCEEDED.value,
                    signal=step.wait_for.signal,
                    output=payload,
                    started_at=_iso(step_started),
                    duration_ms=int((self._executor.now() - step_started) * 1000),
                ).dump()
                seq += 1
                current_step_name, decision = await self._next_step(
                    step, payload, step_outputs, params
                )
                if decision is not None:
                    transitions.append(decision)
                self._checkpoint_after_step(state, current_step_name, seq)
                continue

            step_started = (
                loop_checkpoint.started_at_epoch
                if loop_checkpoint is not None
                else self._executor.now()
            )
            state.current_step_started = step_started
            if step.op is None:
                raise FlowDefinitionError(
                    f"Non-terminal step '{step.name}' has no operation",
                    FlowErrorKind.INVARIANT_VIOLATION,
                )
            step_def = self._config.steps[step.op]
            service = (
                self._services[step_def.target.service]
                if isinstance(step_def.target, ServiceOperationTarget)
                else None
            )
            service_params = service.params if service else {}

            merged_globals = normalize_json_object(
                interpolate_params({**service_params, **step_def.params, **step.params}, params),
                path=f"steps.{step.name}.globals",
                max_collection_items=self._limits.collection_items,
            )

            try:
                resolved_input = resolve_input(step.input, step_outputs, params)

                if step.condition and not evaluate_condition(
                    step.condition, resolved_input, step_outputs, params
                ):
                    step_timings[step.name] = StepAuditEntry(
                        seq=seq,
                        status=StepStatus.SKIPPED.value,
                        condition=step.condition,
                        then=step.then,
                    ).dump()
                    seq += 1
                    current_step_name = step.then
                    self._checkpoint_after_step(state, current_step_name, seq)
                    continue
            except (ReferenceResolutionError, ConditionEvaluationError) as e:
                raise FlowDefinitionError(
                    f"Step '{step.name}': {e}", FlowErrorKind.RESOLUTION_ERROR
                ) from e

            iteration_stats: dict[str, int] | None = None
            until_attempts: int | None = None
            until_exhausted = False
            cache_state: str | None = None
            try:
                if step.for_each:
                    result, iteration_stats = await self._run_iteration(
                        step,
                        step.for_each,
                        step_def,
                        resolved_input,
                        merged_globals,
                        step_outputs,
                        params,
                        state,
                        seq,
                        step_started,
                        (
                            loop_checkpoint
                            if isinstance(loop_checkpoint, IterationCheckpoint)
                            else None
                        ),
                    )
                elif step.until:
                    result, until_attempts, satisfied = await self._run_until(
                        step,
                        step_def,
                        resolved_input,
                        merged_globals,
                        step_outputs,
                        params,
                        state,
                        seq,
                        step_started,
                        loop_checkpoint if isinstance(loop_checkpoint, UntilCheckpoint) else None,
                    )
                    until_exhausted = not satisfied
                else:
                    step_result = await self._invoke(
                        step_def, step.name, resolved_input, merged_globals, params
                    )
                    result = step_result.data
                    cache_state = step_result.cache
            except FlowDefinitionError:
                raise  # config bugs still fail hard
            except Exception as e:
                if is_cancellation(e):
                    raise
                if state.active_episode is not None:
                    handler_failure = classify_failure(
                        e,
                        phase=FailurePhase.HANDLER,
                        step=step.name,
                        code=FailureCode.HANDLER_FAILED,
                    )
                    raise _HandlerFailure(state.active_episode.primary, handler_failure) from e
                handler = step.on_failure or (
                    self._config.on_error.then if self._config.on_error else None
                )
                if handler is None:
                    raise
                failure = classify_failure(
                    e,
                    phase=FailurePhase.STEP,
                    step=step.name,
                    code=FailureCode.STEP_FAILED,
                )
                step_failure = _step_failure(failure)
                state.failures.append(step_failure)
                step_timings[step.name] = StepAuditEntry(
                    seq=seq,
                    status=StepStatus.FAILED.value,
                    code=failure.code,
                    message=failure.message,
                    started_at=_iso(step_started),
                ).dump()
                seq += 1
                step_outputs[ERROR_ALIAS] = step_failure.model_dump()
                transitions.append(
                    TransitionRecord(step=step.name, matched="error", target=handler)
                )
                state.active_episode = _FailureEpisode(
                    primary=failure,
                    handler=handler,
                    handler_steps=self._handler_region(handler),
                )
                current_step_name = handler
                self._checkpoint_after_step(state, current_step_name, seq)
                continue

            if not until_exhausted:
                self._store_output(step, result, step_outputs)

            entry = StepAuditEntry(
                seq=seq,
                status=(
                    StepStatus.EXHAUSTED.value if until_exhausted else StepStatus.SUCCEEDED.value
                ),
                globals=merged_globals,
                input=resolved_input,
                output=result,
                started_at=_iso(step_started),
                duration_ms=int((self._executor.now() - step_started) * 1000),
            )
            if iteration_stats is not None:
                entry.items_total = iteration_stats["items_total"]
                entry.items_failed = iteration_stats["items_failed"]
            if until_attempts is not None:
                entry.attempts = until_attempts
            if cache_state is not None:
                entry.cache = cache_state
            step_timings[step.name] = entry.dump()
            seq += 1

            if until_exhausted:
                if step.on_exhausted is None:
                    raise FlowDefinitionError(
                        f"Step '{step.name}': until '{step.until}' unsatisfied "
                        f"after {step.max_iterations} attempts and no "
                        f"on_exhausted target",
                        FlowErrorKind.LOOP_EXHAUSTED,
                    )
                transitions.append(
                    TransitionRecord(step=step.name, matched="exhausted", target=step.on_exhausted)
                )
                current_step_name = step.on_exhausted
                self._checkpoint_after_step(state, current_step_name, seq)
                continue

            current_step_name, decision = await self._next_step(step, result, step_outputs, params)
            if decision is not None:
                transitions.append(decision)
            self._checkpoint_after_step(state, current_step_name, seq)

        if not reached_terminal:
            raise FlowDefinitionError(
                f"Workflow '{self._config.workflow}' exhausted control flow "
                f"without reaching an explicit terminal",
                FlowErrorKind.INVARIANT_VIOLATION,
            )
        state.active_episode = None

        record = AuditRecord(
            audit_version=AUDIT_VERSION,
            serialization_version=STRICT_JSON_VERSION,
            request_id=request_id,
            workflow=self._config.workflow,
            status=(
                WorkflowStatus.TERMINATED.value
                if final_reason is not None
                else (
                    WorkflowStatus.RECOVERED.value
                    if state.failures
                    else WorkflowStatus.COMPLETED.value
                )
            ),
            reason=final_reason,
            params=params,
            steps=step_timings,
            transitions=transitions,
            started_at=state.started_at,
            completed_at=_iso(self._executor.now()),
        )
        if self._definition_digest is not None:
            record.definition_digest = self._definition_digest
        if self._worker_deployment is not None:
            record.worker_deployment = self._worker_deployment
        if self._worker_build_id is not None:
            record.worker_build_id = self._worker_build_id
        if self._worker_artifact is not None:
            record.worker_artifact = self._worker_artifact
        if self._environment_snapshot_digest is not None:
            record.environment_snapshot_digest = self._environment_snapshot_digest
        if self._trigger_source is not None:
            record.trigger_source = self._trigger_source
        if self._trigger_name is not None:
            record.trigger_name = self._trigger_name
        if self._source_identity_digest is not None:
            record.source_identity_digest = self._source_identity_digest
        if self._correlation_identity_digest is not None:
            record.correlation_identity_digest = self._correlation_identity_digest
        if self._scope_digest is not None:
            record.scope_digest = self._scope_digest
        if self._execution_configuration is not None:
            record.execution_configuration = self._execution_configuration
        if state.failures:
            record.error = state.failures[-1]
            record.failures = list(state.failures)

        state.phase = FailurePhase.RESULT
        if self._config.result is not None:
            try:
                resolved_result = resolve_reference(self._config.result, step_outputs, params)
            except ReferenceResolutionError as exc:
                if final_reason is None:
                    raise FlowDefinitionError(
                        f"Workflow result '{self._config.result}' is unavailable "
                        f"at normal completion",
                        FlowErrorKind.INVARIANT_VIOLATION,
                    ) from exc
            else:
                if self._config.output_schema is not None:
                    await self._executor.validate_contract(
                        self._config.output_schema,
                        resolved_result,
                        direction="workflow output",
                        boundary_name=self._config.workflow,
                    )
                enforce_payload_bytes(
                    resolved_result,
                    boundary="workflow.result",
                    limit=self._limits.workflow_output_bytes,
                )
                record.result = resolved_result

        audit_record = record.dump()
        enforce_payload_bytes(
            audit_record,
            boundary="workflow.audit",
            limit=self._limits.audit_record_bytes,
        )
        enforce_payload_bytes(
            audit_record,
            boundary="workflow.output",
            limit=self._limits.workflow_output_bytes,
        )

        if self._config.on_complete:
            state.phase = FailurePhase.ARCHIVE
            archive_record = capture_audit_record(
                audit_record,
                self._config.on_complete.capture,
            )
            enforce_payload_bytes(
                archive_record,
                boundary="workflow.archive",
                limit=self._limits.audit_record_bytes,
            )
            await self._executor.archive(
                self._config.on_complete.resource,
                interpolate_params(self._config.on_complete.path, params),
                self._config.on_complete.retention_policy,
                self._config.on_complete.capture.mode,
                archive_record,
            )

        return audit_record

    def _checkpoint_after_step(
        self,
        state: _RunState,
        next_step_name: str | None,
        next_seq: int,
    ) -> None:
        if not self._bounded_execution_enabled or next_step_name is None:
            return
        checkpoint = self._build_checkpoint(state, next_step_name, next_seq)
        self._enforce_checkpoint(checkpoint)

    def _checkpoint_during_loop(
        self,
        state: _RunState,
        step_name: str,
        seq: int,
        loop: LoopCheckpoint,
    ) -> None:
        if not self._bounded_execution_enabled:
            return
        checkpoint = self._build_checkpoint(state, step_name, seq, loop=loop)
        self._enforce_checkpoint(checkpoint)

    def _build_checkpoint(
        self,
        state: _RunState,
        next_step_name: str,
        next_seq: int,
        *,
        loop: LoopCheckpoint | None = None,
    ) -> RunnerCheckpoint:
        active_episode = state.active_episode
        return RunnerCheckpoint(
            started_at=state.started_at,
            params=state.params,
            step_outputs=state.step_outputs,
            step_timings=state.step_timings,
            transitions=tuple(state.transitions),
            failures=tuple(state.failures),
            active_episode=(
                FailureEpisodeCheckpoint(
                    primary=active_episode.primary,
                    handler=active_episode.handler,
                    handler_steps=active_episode.handler_steps,
                )
                if active_episode is not None
                else None
            ),
            next_step_name=next_step_name,
            next_seq=next_seq,
            invocations=self._invocations,
            loop=loop,
        )

    def _enforce_checkpoint(self, checkpoint: RunnerCheckpoint) -> None:
        enforce_payload_bytes(
            checkpoint.model_dump(mode="json"),
            boundary="workflow.continuation.state",
            limit=self._limits.workflow_state_bytes,
        )
        observation = self._executor.observe_history()
        if (
            observation.server_suggested
            or observation.events >= self._limits.history_events
            or observation.bytes >= self._limits.history_bytes
        ):
            raise FlowContinuation(checkpoint, observation)

    def _build_execution_error(
        self,
        exc: Exception,
        state: _RunState,
    ) -> FlowExecutionError:
        step_name = state.current_step_name if state.phase is FailurePhase.STEP else None
        secondary: tuple[FailureDetail, ...] = ()
        primary: FailureDetail | None = None
        if isinstance(exc, _HandlerFailure):
            primary = exc.primary
            secondary = (exc.handler,)
            failure_for_step = exc.handler
        elif state.active_episode is not None:
            primary = state.active_episode.primary
            handler_failure = classify_failure(
                exc,
                phase=FailurePhase.HANDLER,
                step=state.current_step_name,
                code=FailureCode.HANDLER_FAILED,
            )
            secondary = (handler_failure,)
            failure_for_step = handler_failure
        else:
            failure_for_step = classify_failure(
                exc,
                phase=state.phase,
                step=step_name,
            )

        self._record_unhandled_step(failure_for_step, state)
        return build_execution_error(
            exc=exc,
            correlation=state.correlation,
            phase=state.phase,
            audit_version=AUDIT_VERSION,
            started_at=state.started_at,
            failed_at=_iso(self._executor.now()),
            step=step_name,
            step_entries=state.step_timings,
            transitions=(transition.model_dump() for transition in state.transitions),
            primary=primary,
            secondary=secondary,
            max_record_bytes=self._limits.failure_record_bytes,
        )

    async def _archive_failure(
        self,
        failure: FlowExecutionError,
        state: _RunState,
    ) -> FlowExecutionError:
        if self._config.on_complete is None or state.phase is FailurePhase.ARCHIVE:
            return failure

        archive_params = dict(state.params)
        if state.correlation.request_id is not None:
            archive_params.setdefault("request_id", state.correlation.request_id)
        try:
            path = interpolate_params(self._config.on_complete.path, archive_params)
            unresolved = find_unresolved_placeholders(path)
            if unresolved:
                raise FlowDefinitionError(
                    f"Failure archive path has unresolved placeholders {sorted(set(unresolved))}",
                    FlowErrorKind.MISSING_PARAMS,
                )
            await self._executor.archive(
                self._config.on_complete.resource,
                path,
                self._config.on_complete.retention_policy,
                self._config.on_complete.capture.mode,
                capture_audit_record(
                    failure.record.dump(),
                    self._config.on_complete.capture,
                ),
            )
            return failure
        except BaseException as archive_exc:
            if is_cancellation(archive_exc) or not isinstance(archive_exc, Exception):
                raise
            archive_failure = classify_failure(
                archive_exc,
                phase=FailurePhase.ARCHIVE,
                code=FailureCode.ARCHIVAL_FAILED,
            )
            return build_execution_error(
                exc=archive_exc,
                correlation=state.correlation,
                phase=failure.classification.phase,
                audit_version=AUDIT_VERSION,
                started_at=state.started_at,
                failed_at=_iso(self._executor.now()),
                step=failure.classification.step,
                step_entries=state.step_timings,
                transitions=(transition.model_dump() for transition in state.transitions),
                primary=failure.classification,
                secondary=(*failure.secondary_failures, archive_failure),
                max_record_bytes=self._limits.failure_record_bytes,
            )

    @staticmethod
    def _record_unhandled_step(
        failure: FailureDetail,
        state: _RunState,
    ) -> None:
        if failure.step is None:
            return
        existing = state.step_timings.get(failure.step)
        if existing is not None and existing.get("status") == StepStatus.FAILED.value:
            return
        started_at = (
            _iso(state.current_step_started) if state.current_step_started is not None else None
        )
        state.step_timings[failure.step] = StepAuditEntry(
            seq=state.next_seq,
            status=StepStatus.FAILED.value,
            code=failure.code,
            message=failure.message,
            started_at=started_at,
        ).dump()

    def _reachable_from_entry(self) -> frozenset[str]:
        if not self._config.flow:
            return frozenset()
        reachable: set[str] = set()
        pending = [self._config.flow[0].name]
        while pending:
            current = pending.pop()
            if current in reachable or current not in self._flow_steps:
                continue
            reachable.add(current)
            pending.extend(self._normal_successors(self._flow_steps[current]))
        return frozenset(reachable)

    def _handler_region(self, entry: str) -> frozenset[str]:
        region: set[str] = set()
        pending = [entry]
        while pending:
            current = pending.pop()
            if current in region or current not in self._flow_steps:
                continue
            if current in self._normal_reachable and current != entry:
                continue
            region.add(current)
            if current in self._normal_reachable:
                continue
            pending.extend(self._normal_successors(self._flow_steps[current]))
        return frozenset(region)

    @staticmethod
    def _normal_successors(step: FlowStep) -> set[str]:
        successors: set[str] = set()
        if step.then is not None:
            successors.add(step.then)
        if step.on_result is not None:
            successors.update(
                target
                for branch in step.on_result
                if (target := branch.then or branch.default) is not None
            )
        if step.wait_for is not None and step.wait_for.on_timeout is not None:
            successors.add(step.wait_for.on_timeout)
        if step.on_exhausted is not None:
            successors.add(step.on_exhausted)
        return successors

    def _build_params(self, request_id: str, trigger_globals: dict[str, Any]) -> dict[str, Any]:
        params: dict[str, Any] = dict(self._config.params)
        params.update(trigger_globals)
        params["request_id"] = request_id
        params = normalize_json_object(
            interpolate_params(params, params),
            path="workflow.params",
            max_collection_items=self._limits.collection_items,
        )

        unresolved = find_unresolved_placeholders(params)
        if unresolved:
            raise FlowDefinitionError(
                f"Workflow params contain unresolved placeholders "
                f"{sorted(set(unresolved))}; the trigger message must supply "
                f"them in 'globals'",
                FlowErrorKind.MISSING_PARAMS,
            )
        return params

    async def _invoke(
        self,
        step_def: StepDefinition,
        step_name: str,
        resolved_input: Any,
        merged_globals: dict[str, Any],
        params: dict[str, Any],
    ) -> StepResult:
        """Execute one step-definition invocation: service call or sub-workflow."""
        self._invocations += 1
        enforce_limit(
            self._invocations,
            limit=self._limits.total_invocations,
            kind=LimitKind.TOTAL_INVOCATIONS,
            boundary="workflow.step_invocations",
        )
        target = step_def.target
        if isinstance(target, ChildWorkflowTarget):
            child_globals = dict(merged_globals)
            if resolved_input is not None:
                child_globals["input"] = resolved_input
            raw_child_record = await self._executor.run_subworkflow(
                target.workflow, step_name, child_globals
            )
            child_record = normalize_json_value(
                raw_child_record,
                path=f"steps.{step_name}.child_record",
                max_collection_items=self._limits.collection_items,
            )
            if isinstance(child_record, dict) and "result" in child_record:
                return StepResult(child_record["result"])
            return StepResult(child_record)

        cache: CacheDirective | None = None
        if step_def.cache is not None:
            cache = CacheDirective(
                resource=step_def.cache.resource,
                key=interpolate_params(step_def.cache.key, params),
                ttl_sec=step_def.cache.ttl_sec,
            )
        result = await self._executor.run_step(
            StepInvocation(
                step_name=step_name,
                service_name=target.service,
                action=target.action,
                input=resolved_input,
                globals=merged_globals,
                cache=cache,
                input_schema=step_def.input_schema,
                output_schema=step_def.output_schema,
                required_resources=tuple(step_def.required_resources),
            ),
            self._services[target.service],
        )
        return StepResult(
            normalize_json_value(
                result.data,
                path=f"steps.{step_name}.output",
                max_collection_items=self._limits.collection_items,
            ),
            cache=result.cache,
        )

    async def _run_until(
        self,
        step: FlowStep,
        step_def: StepDefinition,
        resolved_input: Any,
        merged_globals: dict[str, Any],
        step_outputs: dict[str, Any],
        params: dict[str, Any],
        state: _RunState,
        seq: int,
        step_started: float,
        continuation: UntilCheckpoint | None,
    ) -> tuple[Any, int, bool]:
        """Re-execute the step until its `until` expression is true.

        Returns (last result, attempts made, satisfied). Each attempt is a
        fresh activity with the service's own timeout/retry policy.
        """
        self._reject_loop_cache(step, step_def, loop_kind="until")
        if step.until is None or step.max_iterations is None:
            raise FlowDefinitionError(
                f"Step '{step.name}' has an invalid until-loop declaration",
                FlowErrorKind.INVARIANT_VIOLATION,
            )
        enforce_limit(
            step.max_iterations,
            limit=self._limits.loop_attempts,
            kind=LimitKind.LOOP_ATTEMPTS,
            boundary=f"steps.{step.name}.max_iterations",
        )
        attempts_completed = continuation.attempts_completed if continuation is not None else 0
        result: Any = continuation.last_result if continuation is not None else None
        if attempts_completed > step.max_iterations:
            raise FlowDefinitionError(
                f"Step '{step.name}' has an invalid until continuation attempt",
                FlowErrorKind.INVARIANT_VIOLATION,
            )
        if attempts_completed == step.max_iterations:
            return result, attempts_completed, False
        for attempt in range(attempts_completed + 1, step.max_iterations + 1):
            result = (
                await self._invoke(
                    step_def, f"{step.name}#{attempt}", resolved_input, merged_globals, params
                )
            ).data
            try:
                if evaluate_condition(step.until, result, step_outputs, params):
                    return result, attempt, True
            except ConditionEvaluationError as e:
                raise FlowDefinitionError(
                    f"Step '{step.name}': {e}", FlowErrorKind.RESOLUTION_ERROR
                ) from e
            if attempt < step.max_iterations and step.interval_sec:
                await self._executor.sleep(step.interval_sec)
            self._checkpoint_during_loop(
                state,
                step.name,
                seq,
                UntilCheckpoint(
                    step_name=step.name,
                    started_at_epoch=step_started,
                    attempts_completed=attempt,
                    last_result=result,
                ),
            )
        return result, step.max_iterations, False

    async def _run_wait(
        self, step: FlowStep, step_outputs: dict[str, Any], params: dict[str, Any]
    ) -> Any:
        """Wait for the step's event; returns the payload or TIMED_OUT."""
        if step.wait_for is None:
            raise FlowDefinitionError(
                f"Step '{step.name}' has no wait declaration",
                FlowErrorKind.INVARIANT_VIOLATION,
            )
        timeout_sec: float | None = step.wait_for.timeout_sec

        if step.wait_for.timeout_until is not None:
            try:
                deadline_value = resolve_reference(
                    step.wait_for.timeout_until, step_outputs, params
                )
                remaining = (
                    _deadline_epoch(
                        deadline_value,
                        require_timezone=self._timezone_aware_deadlines_enabled,
                    )
                    - self._executor.now()
                )
            except (ReferenceResolutionError, ValueError, TypeError) as e:
                raise FlowDefinitionError(
                    f"Step '{step.name}': cannot resolve timeout_until "
                    f"'{step.wait_for.timeout_until}': {e}",
                    FlowErrorKind.RESOLUTION_ERROR,
                ) from e
            timeout_sec = remaining if timeout_sec is None else min(timeout_sec, remaining)

        if timeout_sec is None:
            raise FlowDefinitionError(
                f"Step '{step.name}' has an unbounded wait",
                FlowErrorKind.INVARIANT_VIOLATION,
            )
        return await self._executor.wait_for_event(step.wait_for.signal, max(0.0, timeout_sec))

    @staticmethod
    def _store_output(step: FlowStep, result: Any, step_outputs: dict[str, Any]) -> None:
        # Store under the step namespace AND the convergence alias:
        # step_outputs["fetch"]["record"] / step_outputs["record"]
        output_key = step.output or step.name
        if step.name not in step_outputs:
            step_outputs[step.name] = {}
        step_outputs[step.name][output_key] = result
        step_outputs[output_key] = result

    async def _next_step(
        self,
        step: FlowStep,
        result: Any,
        step_outputs: dict[str, Any],
        params: dict[str, Any],
    ) -> tuple[str | None, TransitionRecord | None]:
        """Pick the next step; for on_result steps also return the decision
        record (which branch matched and why) for the audit trail."""
        if not step.on_result:
            return step.then, None

        try:
            for branch in step.on_result:
                if branch.default is not None:
                    return branch.default, TransitionRecord(
                        step=step.name, matched="default", target=branch.default
                    )
                if branch.when is None:
                    raise FlowDefinitionError(
                        f"Step '{step.name}' has an invalid result branch",
                        FlowErrorKind.INVARIANT_VIOLATION,
                    )
                if isinstance(branch.when, EvaluatorCondition):
                    matched_branch = await self._executor.run_evaluator(
                        branch.when.evaluator, result, branch.when.resources, step.name
                    )
                    matched = f"evaluator:{branch.when.evaluator}"
                else:
                    matched_branch = evaluate_condition(branch.when, result, step_outputs, params)
                    matched = branch.when
                if matched_branch:
                    if branch.then is None:
                        raise FlowDefinitionError(
                            f"Step '{step.name}' result branch has no target",
                            FlowErrorKind.INVARIANT_VIOLATION,
                        )
                    return branch.then, TransitionRecord(
                        step=step.name, matched=matched, target=branch.then
                    )
        except ConditionEvaluationError as e:
            raise FlowDefinitionError(
                f"Step '{step.name}': {e}", FlowErrorKind.RESOLUTION_ERROR
            ) from e
        return None, None

    async def _run_iteration(
        self,
        step: FlowStep,
        for_each_ref: ReferencePath,
        step_def: StepDefinition,
        resolved_input: Any,
        merged_globals: dict[str, Any],
        step_outputs: dict[str, Any],
        params: dict[str, Any],
        state: _RunState,
        seq: int,
        step_started: float,
        continuation: IterationCheckpoint | None,
    ) -> tuple[Any, dict[str, int]]:
        self._reject_loop_cache(step, step_def, loop_kind="for_each")
        parallelism = step.max_concurrency if step.parallel else None
        if step.parallel and parallelism is None:
            raise FlowDefinitionError(
                f"Parallel step '{step.name}' has no max_concurrency",
                FlowErrorKind.INVARIANT_VIOLATION,
            )
        if parallelism is not None:
            enforce_limit(
                parallelism,
                limit=self._limits.parallelism,
                kind=LimitKind.PARALLELISM,
                boundary=f"steps.{step.name}.max_concurrency",
            )
        items = self._resolve_iteration_items(
            step.name, for_each_ref, resolved_input, step_outputs, params
        )
        enforce_limit(
            len(items),
            limit=self._limits.fanout_items,
            kind=LimitKind.FANOUT_ITEMS,
            boundary=f"steps.{step.name}.for_each",
        )

        is_dict = isinstance(items, dict)
        if is_dict:
            iter_keys = list(items.keys())
            iter_values = list(items.values())
        else:
            iter_keys = list(range(len(items)))
            iter_values = list(items)

        fail_strategy = step.on_iteration_fail or IterationFailStrategy.STOP

        async def run_one(item: Any, idx: int | str) -> Any:
            item_input = item if step.as_var else resolved_input
            item_globals = {**merged_globals}
            if step.as_var:
                item_globals[step.as_var] = item

            return (
                await self._invoke(
                    step_def, f"{step.name}[{idx}]", item_input, item_globals, params
                )
            ).data

        processed = list(continuation.results) if continuation is not None else []
        failed = continuation.failed if continuation is not None else 0
        next_offset = continuation.next_offset if continuation is not None else 0
        if len(processed) != next_offset or next_offset > len(iter_keys):
            raise FlowDefinitionError(
                f"Step '{step.name}' has an invalid iteration continuation offset",
                FlowErrorKind.INVARIANT_VIOLATION,
            )
        if any(entry.key != iter_keys[offset] for offset, entry in enumerate(processed)):
            raise FlowDefinitionError(
                f"Step '{step.name}' iteration continuation does not match its input",
                FlowErrorKind.INVARIANT_VIOLATION,
            )

        if parallelism is not None:
            semaphore = asyncio.Semaphore(parallelism)

            async def bounded_outcome(item: Any, idx: Any) -> Any:
                try:
                    async with semaphore:
                        return await run_one(item, idx)
                except Exception as exc:
                    if is_cancellation(exc) or fail_strategy is IterationFailStrategy.STOP:
                        raise
                    return exc

            chunk_items = (
                self._limits.fanout_chunk_items
                if self._bounded_execution_enabled
                else len(iter_values) or self._limits.fanout_chunk_items
            )
            for chunk_start in range(next_offset, len(iter_values), chunk_items):
                chunk_keys = iter_keys[chunk_start : chunk_start + chunk_items]
                chunk_values = iter_values[chunk_start : chunk_start + chunk_items]
                tasks = [
                    asyncio.create_task(
                        bounded_outcome(value, key),
                        name=f"iteration:{step.name}[{key}]",
                    )
                    for key, value in zip(chunk_keys, chunk_values)
                ]
                try:
                    completed = await asyncio.gather(*tasks)
                except BaseException:
                    await _cancel_and_await(tasks)
                    raise
                failed += self._append_iteration_results(
                    step,
                    fail_strategy,
                    zip(chunk_keys, completed),
                    processed,
                )
                self._checkpoint_during_loop(
                    state,
                    step.name,
                    seq,
                    IterationCheckpoint(
                        step_name=step.name,
                        started_at_epoch=step_started,
                        next_offset=chunk_start + len(chunk_keys),
                        results=tuple(processed),
                        failed=failed,
                    ),
                )
        else:
            for offset in range(next_offset, len(iter_values)):
                key = iter_keys[offset]
                item = iter_values[offset]
                try:
                    outcome = await run_one(item, key)
                except Exception as exc:
                    if is_cancellation(exc) or fail_strategy is IterationFailStrategy.STOP:
                        raise
                    outcome = exc
                failed += self._append_iteration_results(
                    step,
                    fail_strategy,
                    ((key, outcome),),
                    processed,
                )
                self._checkpoint_during_loop(
                    state,
                    step.name,
                    seq,
                    IterationCheckpoint(
                        step_name=step.name,
                        started_at_epoch=step_started,
                        next_offset=offset + 1,
                        results=tuple(processed),
                        failed=failed,
                    ),
                )

        stats = {"items_total": len(iter_keys), "items_failed": failed}
        if is_dict:
            return {
                entry.key: entry.value
                for entry in processed
                if isinstance(entry, IncludedIterationResult)
            }, stats
        return [
            entry.value for entry in processed if isinstance(entry, IncludedIterationResult)
        ], stats

    @staticmethod
    def _append_iteration_results(
        step: FlowStep,
        fail_strategy: IterationFailStrategy,
        outcomes: Iterable[tuple[int | str, Any]],
        processed: list[IncludedIterationResult | OmittedIterationResult],
    ) -> int:
        failed = 0
        for key, outcome in outcomes:
            if isinstance(outcome, Exception):
                failed += 1
                if fail_strategy is IterationFailStrategy.COLLECT:
                    failure = classify_failure(
                        outcome,
                        phase=FailurePhase.STEP,
                        step=f"{step.name}[{key}]",
                        code=FailureCode.STEP_FAILED,
                    )
                    processed.append(
                        IncludedIterationResult(
                            key=key,
                            value=IterationFailure(
                                code=failure.cause_code,
                                message=failure.message,
                            ).model_dump(by_alias=True),
                        )
                    )
                else:
                    processed.append(OmittedIterationResult(key=key))
            else:
                processed.append(IncludedIterationResult(key=key, value=outcome))
        return failed

    @staticmethod
    def _reject_loop_cache(step: FlowStep, step_def: StepDefinition, *, loop_kind: str) -> None:
        if step_def.cache is None:
            return
        raise FlowDefinitionError(
            f"Step '{step.name}' uses operation '{step.op}' with {loop_kind}, "
            f"but that operation declares cache resource='{step_def.cache.resource}' "
            f"key='{step_def.cache.key}'",
            FlowErrorKind.UNSAFE_LOOP_CACHE,
        )

    def _resolve_iteration_items(
        self,
        step_name: str,
        for_each_ref: ReferencePath,
        resolved_input: Any,
        step_outputs: dict[str, Any],
        params: dict[str, Any],
    ) -> Any:
        try:
            if not for_each_ref.path and for_each_ref.root == "input":
                items = resolved_input
            elif for_each_ref.root == "input":
                items = resolve_json_path(
                    resolved_input,
                    for_each_ref.path,
                    root="input",
                )
            else:
                items = resolve_reference(for_each_ref, step_outputs, params)
        except (
            DataNormalizationError,
            DataPathError,
            ReferenceResolutionError,
        ) as e:
            raise FlowDefinitionError(
                f"Step '{step_name}': cannot resolve for_each '{for_each_ref}': {e}",
                FlowErrorKind.RESOLUTION_ERROR,
            ) from e

        if not isinstance(items, (list, tuple, dict)):
            raise FlowDefinitionError(
                f"Step '{step_name}': for_each '{for_each_ref}' resolved to "
                f"{type(items).__name__}, expected a list or dict",
                FlowErrorKind.RESOLUTION_ERROR,
            )
        return items


def _deadline_epoch(value: Any, *, require_timezone: bool = True) -> float:
    """Interpret a resolved timeout_until value as an epoch timestamp."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            deadline = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("expected a valid ISO-8601 deadline") from exc
        if require_timezone and (deadline.tzinfo is None or deadline.utcoffset() is None):
            raise ValueError("ISO-8601 deadline must include 'Z' or an explicit UTC offset")
        return deadline.astimezone(UTC).timestamp() if require_timezone else deadline.timestamp()
    raise TypeError(f"expected an ISO-8601 string or epoch number, got {type(value).__name__}")


async def _cancel_and_await(tasks: list[asyncio.Task[Any]]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def _step_failure(failure: FailureDetail) -> StepFailure:
    if failure.step is None:
        raise ValueError("Step failure requires a step identity")
    return StepFailure(
        step=failure.step,
        code=failure.code,
        cause_code=failure.cause_code,
        message=failure.message,
    )
