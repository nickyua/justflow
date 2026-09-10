"""YAML-to-Temporal workflow compiler.

Builds a dynamic Temporal workflow class whose run method delegates all
orchestration to FlowRunner; this module owns only the Temporal-facing parts
(activity execution, signal waits, timeouts/retry policies, class synthesis).
"""

from __future__ import annotations

import hashlib
import sys
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any

from pydantic import ValidationError
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from justflow.config.models import AuditCaptureMode, WorkflowConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.definitions.manifest import DefinitionManifest
from justflow.definitions.routing import (
    WorkerDeployment,
    execution_identity_memo,
    runtime_workflow_type_name,
)
from justflow.engine.activities import (
    ActivityInput,
    ContractValidationInput,
    EvaluatorInput,
    StepActivityResult,
)
from justflow.engine.archival import ArchiveRequest
from justflow.engine.audit import FailurePhase
from justflow.engine.continuation import RunnerCheckpoint, WorkflowContinuationInput
from justflow.engine.contracts import SchemaSpec, validate_contract_declaration
from justflow.engine.data import normalize_json_object, normalize_json_value
from justflow.engine.errors import (
    FailureCode,
    FlowExecutionError,
    build_execution_error,
    correlation_identity,
)
from justflow.engine.limits import (
    LimitExceededError,
    LimitKind,
    enforce_limit,
    enforce_payload_bytes,
    strict_json_bytes,
)
from justflow.engine.runner import (
    AUDIT_VERSION,
    TIMED_OUT,
    FlowContinuation,
    FlowRunner,
    HistoryObservation,
    StepInvocation,
    StepResult,
)
from justflow.provenance import ExecutionConfigurationIdentity, WorkerArtifactIdentity
from justflow.scope import scoped_identity_from_digest
from justflow.sdk.message_contract import (
    STEP_RESPONSE_SIGNAL,
    WORKFLOW_EVENT_SIGNAL,
    EventPayload,
    ResponseStatus,
    SignalPayload,
    WorkflowTrigger,
    make_signal_key,
    make_step_invocation_id,
)
from justflow.transports.registry import ResolvedService

RESOURCE_GRANTS_PATCH = "resource-grants-v1"

INITIAL_RETRY_INTERVAL = timedelta(seconds=1)
MAX_RETRY_INTERVAL = timedelta(seconds=30)

EVALUATOR_TIMEOUT = timedelta(seconds=30)
EVALUATOR_RETRY_ATTEMPTS = 3
CONTRACT_VALIDATION_TIMEOUT = timedelta(seconds=30)
CONTRACT_VALIDATION_ATTEMPTS = 1
INVALID_ASYNC_DISPATCH_ERROR = "INVALID_ASYNC_DISPATCH"
UNCONTRACTED_CACHE_IDENTITY = "uncontracted"
BOUNDED_EXECUTION_PATCH = "justflow-bounded-execution-v1"
TIMEZONE_AWARE_DEADLINES_PATCH = "justflow-timezone-aware-deadlines-v1"
EXECUTION_PROVENANCE_PATCH = "justflow-execution-provenance-v1"
ENVIRONMENT_SNAPSHOT_PATCH = "justflow-environment-snapshot-v1"
RUNTIME_SCOPE_PATCH = "justflow-runtime-scope-v1"
EXECUTION_CONFIGURATION_PATCH = "justflow-execution-configuration-v1"
QUEUE_INVOCATION_RESPONSE_PATCH = "justflow-queue-invocation-response-v1"

# Archival must not retry forever: fail the workflow loudly after these attempts
# rather than silently losing the audit record.
ARCHIVAL_TIMEOUT = timedelta(seconds=30)
ARCHIVAL_RETRY_ATTEMPTS = 3

# Module-level registry so Temporal can find compiled workflows by qualname


class DefinitionIdentityMismatchError(ValueError):
    """A start payload disagrees with the workflow type selected by the catalog."""


def _timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, tz=UTC).isoformat()


def _application_error(error: FlowExecutionError) -> ApplicationError:
    return ApplicationError(
        f"Workflow execution failed with code {error.classification.code}",
        error.record.dump(),
        type=error.classification.code,
        non_retryable=not error.classification.retryable,
    )


def _envelope_error(
    *,
    workflow_name: str,
    run_id: str,
    request_id: str | None,
    phase: FailurePhase,
    code: FailureCode,
    timestamp: str,
    cause: Exception | None = None,
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
    definition_digest: str | None = None,
    worker_deployment: str | None = None,
    worker_build_id: str | None = None,
    worker_artifact: WorkerArtifactIdentity | None = None,
    environment_snapshot_digest: str | None = None,
    scope_digest: str | None = None,
    execution_configuration: ExecutionConfigurationIdentity | None = None,
) -> FlowExecutionError:
    validation_error = cause or ValueError(f"{phase.value} payload failed validation")
    return build_execution_error(
        exc=validation_error,
        correlation=correlation_identity(
            workflow=workflow_name,
            request_id=request_id,
            run_id=run_id,
            definition_digest=definition_digest,
            worker_deployment=worker_deployment,
            worker_build_id=worker_build_id,
            worker_artifact=worker_artifact,
            environment_snapshot_digest=environment_snapshot_digest,
            scope_digest=scope_digest,
            execution_configuration=execution_configuration,
        ),
        phase=phase,
        code=code,
        audit_version=AUDIT_VERSION,
        started_at=timestamp,
        failed_at=timestamp,
        max_record_bytes=limits.failure_record_bytes,
    )


def _request_id(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("request_id")
    if isinstance(value, str):
        return value
    trigger = payload.get("trigger")
    if not isinstance(trigger, dict):
        return None
    nested_value = trigger.get("request_id")
    return nested_value if isinstance(nested_value, str) else None


@dataclass
class PendingMessages:
    signals: dict[str, list[dict[str, Any]]]
    events: dict[str, list[Any]]

    def validate(self, limits: RuntimeLimits) -> None:
        enforce_limit(
            self.count,
            limit=limits.queued_messages,
            kind=LimitKind.QUEUED_MESSAGES,
            boundary="workflow.pending_messages",
        )
        enforce_limit(
            self.byte_count,
            limit=limits.queued_message_bytes,
            kind=LimitKind.QUEUED_MESSAGE_BYTES,
            boundary="workflow.pending_messages",
        )

    @property
    def count(self) -> int:
        return sum(len(queue) for queue in (*self.signals.values(), *self.events.values()))

    @property
    def byte_count(self) -> int:
        return len(strict_json_bytes({"signals": self.signals, "events": self.events}))

    def append_signal(
        self,
        key: str,
        value: dict[str, Any],
        limits: RuntimeLimits,
    ) -> None:
        signals = dict(self.signals)
        signals[key] = [*signals.get(key, ()), value]
        self._validate_append(signals, self.events, limits, boundary="workflow.signals")
        self.signals.setdefault(key, []).append(value)

    def append_event(self, key: str, value: Any, limits: RuntimeLimits) -> None:
        events = dict(self.events)
        events[key] = [*events.get(key, ()), value]
        self._validate_append(self.signals, events, limits, boundary="workflow.events")
        self.events.setdefault(key, []).append(value)

    def pop_signal(self, key: str) -> dict[str, Any]:
        queue = self.signals[key]
        value = queue.pop(0)
        if not queue:
            del self.signals[key]
        return value

    def pop_event(self, key: str) -> Any:
        queue = self.events[key]
        value = queue.pop(0)
        if not queue:
            del self.events[key]
        return value

    @staticmethod
    def _validate_append(
        signals: dict[str, list[dict[str, Any]]],
        events: dict[str, list[Any]],
        limits: RuntimeLimits,
        *,
        boundary: str,
    ) -> None:
        enforce_limit(
            sum(len(queue) for queue in (*signals.values(), *events.values())),
            limit=limits.queued_messages,
            kind=LimitKind.QUEUED_MESSAGES,
            boundary=boundary,
        )
        enforce_limit(
            len(strict_json_bytes({"signals": signals, "events": events})),
            limit=limits.queued_message_bytes,
            kind=LimitKind.QUEUED_MESSAGE_BYTES,
            boundary=boundary,
        )


class TemporalStepExecutor:
    """StepExecutor backed by Temporal activities and signals.

    Runs inside workflow code; all methods must stay deterministic.
    """

    def __init__(
        self,
        pending_messages: PendingMessages,
        request_id: str,
        workflow_id: str,
        workflow_run_id: str,
        flow_name: str,
        limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
        correlation_id: str | None = None,
        trace_id: str | None = None,
        definition_digest: str | None = None,
        worker_deployment: str | None = None,
        worker_build_id: str | None = None,
        worker_artifact: WorkerArtifactIdentity | None = None,
        environment_snapshot_digest: str | None = None,
        contract_identities: Mapping[str, str] | None = None,
        child_workflow_types: Mapping[str, str] | None = None,
        child_definition_digests: Mapping[str, str] | None = None,
        child_environment_snapshot_digests: Mapping[str, str] | None = None,
        runtime_scope_digest: str | None = None,
        execution_configuration: ExecutionConfigurationIdentity | None = None,
    ):
        self._pending_messages = pending_messages
        self._request_id = request_id
        self._propagated_correlation_id = correlation_id
        self._propagated_trace_id = trace_id
        self._correlation_id = correlation_id or request_id
        self._trace_id = trace_id
        self._workflow_id = workflow_id
        self._workflow_run_id = workflow_run_id
        self._flow_name = flow_name
        self._limits = limits
        self._definition_digest = definition_digest
        self._worker_deployment = worker_deployment
        self._worker_build_id = worker_build_id
        self._worker_artifact = worker_artifact
        self._environment_snapshot_digest = environment_snapshot_digest
        self._contract_identities = contract_identities or {}
        self._child_workflow_types = child_workflow_types or {}
        self._child_definition_digests = child_definition_digests or {}
        self._child_environment_snapshot_digests = child_environment_snapshot_digests or {}
        self._runtime_scope_digest = runtime_scope_digest
        self._execution_configuration = execution_configuration

    async def run_step(self, invocation: StepInvocation, service: ResolvedService) -> StepResult:
        cache = invocation.cache
        if cache is not None and self._definition_digest is not None:
            contract_identity = UNCONTRACTED_CACHE_IDENTITY
            if invocation.output_schema is not None:
                contract_identity = self._contract_identities[
                    _contract_spec_key(invocation.output_schema)
                ]
            cache = replace(
                cache,
                definition_digest=self._definition_digest,
                contract_identity=contract_identity,
            )
        activity_input = ActivityInput(
            service_name=invocation.service_name,
            action=invocation.action,
            input=invocation.input,
            globals=invocation.globals,
            request_id=self._request_id,
            correlation_id=self._correlation_id,
            trace_id=self._trace_id,
            workflow_id=self._workflow_id,
            workflow_run_id=self._workflow_run_id,
            flow_name=self._flow_name,
            definition_digest=self._definition_digest,
            scope_digest=self._runtime_scope_digest,
            step_name=invocation.step_name,
            cache=cache,
            input_schema=invocation.input_schema,
            output_schema=invocation.output_schema,
            required_resources=list(invocation.required_resources),
        )
        activity_payload = asdict(activity_input)
        if self._runtime_scope_digest is None:
            activity_payload.pop("scope_digest")
        if not invocation.required_resources or not workflow.patched(RESOURCE_GRANTS_PATCH):
            activity_payload.pop("required_resources")
        enforce_payload_bytes(
            activity_payload,
            boundary=f"activities.{invocation.step_name}.input",
            limit=self._limits.activity_input_bytes,
        )

        raw_result: dict[str, Any] = await workflow.execute_activity(
            "execute_step",
            arg=activity_payload,
            result_type=dict,
            start_to_close_timeout=timedelta(seconds=service.dispatch_timeout_sec),
            retry_policy=RetryPolicy(
                maximum_attempts=service.retries + 1,
                initial_interval=INITIAL_RETRY_INTERVAL,
                maximum_interval=MAX_RETRY_INTERVAL,
            ),
        )
        enforce_payload_bytes(
            raw_result,
            boundary=f"activities.{invocation.step_name}.output",
            limit=self._limits.activity_output_bytes,
        )
        result = StepActivityResult(**raw_result)

        if result.awaiting_signal:
            if result.signal_key is None or result.response_timeout_sec is None:
                raise ApplicationError(
                    f"Step {invocation.step_name}: asynchronous transport result "
                    "is missing its response key or timeout",
                    type=INVALID_ASYNC_DISPATCH_ERROR,
                    non_retryable=True,
                )
            alternate_keys: tuple[str, ...] = (
                make_signal_key(
                    make_step_invocation_id(
                        self._workflow_id, self._workflow_run_id, invocation.step_name
                    ),
                    invocation.step_name,
                    invocation.action,
                ),
            )
            if workflow.patched(QUEUE_INVOCATION_RESPONSE_PATCH):
                alternate_keys += (
                    make_signal_key(self._correlation_id, invocation.step_name, invocation.action),
                )
            payload = await self._wait_for_signal(
                result.signal_key,
                invocation.step_name,
                result.response_timeout_sec,
                alternate_keys=alternate_keys,
            )
            if invocation.output_schema is not None:
                await self.validate_contract(
                    invocation.output_schema,
                    payload,
                    direction="asynchronous output",
                    boundary_name=invocation.step_name,
                )
            return StepResult(payload)

        return StepResult(result.data, cache=result.cache)

    async def _wait_for_signal(
        self,
        signal_key: str,
        step_name: str,
        timeout_sec: int,
        *,
        alternate_keys: tuple[str, ...] = (),
    ) -> Any:
        accepted_keys = tuple(dict.fromkeys((signal_key, *alternate_keys)))
        try:
            await workflow.wait_condition(
                lambda: any(self._pending_messages.signals.get(key) for key in accepted_keys),
                timeout=timedelta(seconds=timeout_sec),
            )
        except TimeoutError:
            raise ApplicationError(
                f"Step {step_name}: no response within {timeout_sec}s (signal key '{signal_key}')",
                type="SIGNAL_TIMEOUT",
            ) from None

        received_key = next(key for key in accepted_keys if self._pending_messages.signals.get(key))
        response = self._pending_messages.pop_signal(received_key)

        if response.get("status") == ResponseStatus.ERROR.value:
            error = response.get("error", {})
            raise ApplicationError(
                f"Step {step_name} failed: {error}",
                type="STEP_FAILED",
                non_retryable=not (isinstance(error, dict) and bool(error.get("retryable"))),
            )
        return response.get("step_response")

    async def run_evaluator(
        self, evaluator: str, data: Any, resource_names: list[str], step_name: str
    ) -> bool:
        evaluator_input = EvaluatorInput(
            evaluator=evaluator,
            data=data,
            request_id=self._request_id,
            flow_name=self._flow_name,
            step_name=step_name,
            resource_names=list(resource_names),
        )
        enforce_payload_bytes(
            asdict(evaluator_input),
            boundary=f"activities.{step_name}.evaluator_input",
            limit=self._limits.activity_input_bytes,
        )
        return await workflow.execute_activity(
            "evaluate_condition",
            arg=evaluator_input,
            start_to_close_timeout=EVALUATOR_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=EVALUATOR_RETRY_ATTEMPTS),
        )

    async def wait_for_event(self, signal: str, timeout_sec: float) -> Any:
        if not self._pending_messages.events.get(signal):
            try:
                await workflow.wait_condition(
                    lambda: bool(self._pending_messages.events.get(signal)),
                    timeout=timedelta(seconds=timeout_sec),
                )
            except TimeoutError:
                return TIMED_OUT
        return self._pending_messages.pop_event(signal)

    async def sleep(self, seconds: float) -> None:
        await workflow.sleep(seconds)

    async def validate_contract(
        self,
        schema: dict[str, Any] | str,
        payload: Any,
        *,
        direction: str,
        boundary_name: str,
    ) -> None:
        validation_input = ContractValidationInput(
            schema=schema,
            payload=payload,
            direction=direction,
            boundary_name=boundary_name,
        )
        enforce_payload_bytes(
            asdict(validation_input),
            boundary=f"activities.{boundary_name}.contract_input",
            limit=self._limits.activity_input_bytes,
        )
        await workflow.execute_activity(
            "validate_contract",
            arg=validation_input,
            start_to_close_timeout=CONTRACT_VALIDATION_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=CONTRACT_VALIDATION_ATTEMPTS),
        )

    async def run_subworkflow(
        self, workflow_name: str, step_name: str, trigger_globals: dict[str, Any]
    ) -> Any:
        # The step name (iteration-unique) keeps child ids collision-free;
        # the child gets its own history, retries, and UI node.
        child_request_id = (
            f"{self._request_id}:{step_name}"
            if self._runtime_scope_digest is None
            else scoped_identity_from_digest(
                "child-workflow",
                self._runtime_scope_digest,
                self._workflow_id,
                step_name,
            )
        )
        child_workflow_type = self._child_workflow_types.get(workflow_name, workflow_name)
        child_digest = self._child_definition_digests.get(workflow_name)
        child_environment_snapshot_digest = self._child_environment_snapshot_digests.get(
            workflow_name
        )
        trigger = WorkflowTrigger(
            request_id=child_request_id,
            globals=trigger_globals,
            correlation_id=self._propagated_correlation_id,
            trace_id=self._propagated_trace_id,
            definition_digest=child_digest,
            worker_deployment=self._worker_deployment,
            worker_build_id=self._worker_build_id,
            worker_artifact=self._worker_artifact,
            environment_snapshot_digest=child_environment_snapshot_digest,
            execution_configuration=self._execution_configuration,
            scope_digest=self._runtime_scope_digest,
        ).model_dump(mode="json", exclude_none=True)
        memo = None
        if child_digest is not None:
            if self._worker_artifact is None:
                if self._worker_deployment is None or self._worker_build_id is None:
                    raise DefinitionIdentityMismatchError(
                        "Child workflow execution has no selected worker identity"
                    )
                memo = {
                    "justflow.logical_workflow": workflow_name,
                    "justflow.definition_digest": child_digest,
                    "justflow.worker_deployment": self._worker_deployment,
                    "justflow.worker_build_id": self._worker_build_id,
                }
            else:
                memo = execution_identity_memo(
                    logical_name=workflow_name,
                    definition_digest=child_digest,
                    artifact_identity=self._worker_artifact,
                    environment_snapshot_digest=child_environment_snapshot_digest,
                    scope_digest=self._runtime_scope_digest,
                    execution_configuration=self._execution_configuration,
                )
        return await workflow.execute_child_workflow(
            child_workflow_type,
            trigger,
            id=child_request_id,
            memo=memo,
        )

    async def archive(
        self,
        resource: str,
        path: str,
        retention_policy: str,
        capture_mode: AuditCaptureMode,
        record: dict[str, Any],
    ) -> None:
        archive_request = ArchiveRequest(
            resource=resource,
            path=path,
            retention_policy=retention_policy,
            capture_mode=capture_mode,
            record=record,
        )
        enforce_payload_bytes(
            asdict(archive_request),
            boundary="activities.archive.input",
            limit=self._limits.activity_input_bytes,
        )
        await workflow.execute_activity(
            "archive_workflow",
            arg=archive_request,
            start_to_close_timeout=ARCHIVAL_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=ARCHIVAL_RETRY_ATTEMPTS),
        )

    def now(self) -> float:
        return workflow.time()

    def observe_history(self) -> HistoryObservation:
        info = workflow.info()
        return HistoryObservation(
            events=info.get_current_history_length(),
            bytes=info.get_current_history_size(),
            server_suggested=info.is_continue_as_new_suggested(),
        )


def compile_workflow(
    workflow_config: WorkflowConfig,
    services: Mapping[str, ResolvedService],
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
    *,
    manifest: DefinitionManifest | None = None,
    deployment: WorkerDeployment | None = None,
    child_manifests: Mapping[str, DefinitionManifest] | None = None,
    environment_snapshot_digest: str | None = None,
    child_environment_snapshot_digests: Mapping[str, str] | None = None,
    runtime_scope_digest: str | None = None,
    execution_configuration: ExecutionConfigurationIdentity | None = None,
) -> type:
    """Compile a WorkflowConfig into a Temporal workflow class.

    Returns a class decorated with @workflow.defn that can be registered
    with a Temporal worker.
    """
    if (manifest is None) != (deployment is None):
        raise ValueError("Versioned workflow compilation requires both manifest and deployment")
    if (manifest is None) != (environment_snapshot_digest is None):
        raise ValueError(
            "Versioned workflow compilation requires an execution environment snapshot"
        )
    if manifest is not None:
        if manifest.logical_name != workflow_config.workflow:
            raise ValueError(
                f"Manifest workflow '{manifest.logical_name}' does not match "
                f"configuration '{workflow_config.workflow}'"
            )
        if not manifest.matches_workflow(workflow_config):
            raise ValueError(
                f"Manifest content for workflow '{manifest.logical_name}' does not match configuration"
            )
        expected_children = {child.workflow: child.definition_digest for child in manifest.children}
        supplied_children = {
            name: child.definition_digest for name, child in (child_manifests or {}).items()
        }
        if supplied_children != expected_children:
            raise ValueError(
                f"Child definition identities for workflow '{manifest.logical_name}' do not match "
                f"its manifest: expected={expected_children}, supplied={supplied_children}"
            )
    workflow_config = workflow_config.model_copy(deep=True)
    services = {
        name: replace(
            service,
            transport_config=service.transport_config.model_copy(deep=True),
            params=MappingProxyType(deepcopy(dict(service.params))),
        )
        for name, service in services.items()
    }
    child_manifests = dict(child_manifests or {})
    child_environment_snapshot_digests = dict(child_environment_snapshot_digests or {})
    wf_name = workflow_config.workflow
    temporal_name = (
        runtime_workflow_type_name(
            wf_name,
            manifest.definition_digest,
            runtime_scope_digest,
        )
        if manifest is not None
        else wf_name
    )
    registration = {
        "workflow": workflow_config.model_dump(mode="json"),
        "services": {
            name: {
                "provider": service.provider_name,
                "contract_version": service.provider_contract_version,
                "config": service.transport_config.model_dump(mode="json"),
                "connect_timeout_sec": service.connect_timeout_sec,
                "dispatch_timeout_sec": service.dispatch_timeout_sec,
                "response_timeout_sec": service.response_timeout_sec,
                "retries": service.retries,
                "params": dict(service.params),
            }
            for name, service in services.items()
        },
        "limits": asdict(limits),
        "manifest": manifest.model_dump(mode="json") if manifest is not None else None,
        "artifact": deployment.artifact_identity.model_dump(mode="json")
        if deployment is not None
        else None,
        "scope": runtime_scope_digest,
        "environment": environment_snapshot_digest,
        "children": {name: child.definition_digest for name, child in child_manifests.items()},
        "child_environments": child_environment_snapshot_digests,
        "configuration": execution_configuration.model_dump(mode="json")
        if execution_configuration is not None
        else None,
    }
    class_name = _workflow_class_name(wf_name, registration)
    registered = getattr(sys.modules[__name__], class_name, None)
    if isinstance(registered, type):
        return registered
    definition_digest = manifest.definition_digest if manifest is not None else None
    worker_deployment = deployment.name if deployment is not None else None
    worker_build_id = deployment.build_id if deployment is not None else None
    worker_artifact = deployment.artifact_identity if deployment is not None else None
    child_manifests = child_manifests or {}
    child_workflow_types = {
        name: runtime_workflow_type_name(
            name,
            child.definition_digest,
            runtime_scope_digest,
        )
        for name, child in child_manifests.items()
    }
    child_definition_digests = {
        name: child.definition_digest for name, child in child_manifests.items()
    }
    child_environment_snapshot_digests = child_environment_snapshot_digests or {}
    if set(child_environment_snapshot_digests) != set(child_manifests):
        raise ValueError(
            "Child execution environment snapshots do not match child definitions: "
            f"snapshots={sorted(child_environment_snapshot_digests)}, "
            f"definitions={sorted(child_manifests)}"
        )
    contract_identities = _contract_identity_lookup(workflow_config)
    has_runtime_deadline = any(
        step.wait_for is not None and step.wait_for.timeout_until is not None
        for step in workflow_config.flow
    )

    async def _run(self, trigger: Any) -> dict[str, Any]:
        workflow_info = workflow.info()
        run_id = workflow_info.run_id
        started_at = _timestamp(workflow.time())
        continued_from_run_id = getattr(workflow_info, "continued_run_id", None)
        continuation: WorkflowContinuationInput | None = None
        checkpoint: RunnerCheckpoint | None = None
        provenance_enabled = workflow.patched(EXECUTION_PROVENANCE_PATCH)
        environment_snapshot_enabled = workflow.patched(ENVIRONMENT_SNAPSHOT_PATCH)
        runtime_scope_enabled = runtime_scope_digest is not None and workflow.patched(
            RUNTIME_SCOPE_PATCH
        )
        execution_configuration_enabled = execution_configuration is not None and workflow.patched(
            EXECUTION_CONFIGURATION_PATCH
        )
        selected_worker_artifact = worker_artifact if provenance_enabled else None
        selected_environment_snapshot_digest = (
            environment_snapshot_digest if environment_snapshot_enabled else None
        )
        selected_scope_digest = runtime_scope_digest if runtime_scope_enabled else None
        selected_execution_configuration = (
            execution_configuration if execution_configuration_enabled else None
        )
        self._worker_artifact = selected_worker_artifact
        self._environment_snapshot_digest = selected_environment_snapshot_digest
        self._scope_digest = selected_scope_digest
        self._execution_configuration = selected_execution_configuration
        try:
            if continued_from_run_id is None:
                enforce_payload_bytes(
                    trigger,
                    boundary="workflow.trigger",
                    limit=limits.trigger_payload_bytes,
                )
                parsed = WorkflowTrigger.model_validate(trigger)
                pending_messages = self._pending_messages
            else:
                enforce_payload_bytes(
                    trigger,
                    boundary="workflow.continuation.input",
                    limit=limits.continuation_input_bytes,
                )
                continuation = WorkflowContinuationInput.model_validate(trigger)
                if continuation.previous_run_id != continued_from_run_id:
                    raise DefinitionIdentityMismatchError(
                        "Continuation input does not identify the previous Temporal run"
                    )
                parsed = continuation.trigger
                checkpoint = continuation.checkpoint
                signal_keys = [
                    *continuation.signals,
                    *(
                        key
                        for key in self._pending_messages.signals
                        if key not in continuation.signals
                    ),
                ]
                event_keys = [
                    *continuation.events,
                    *(
                        key
                        for key in self._pending_messages.events
                        if key not in continuation.events
                    ),
                ]
                pending_messages = PendingMessages(
                    signals={
                        key: [
                            *(dict(value) for value in continuation.signals.get(key, ())),
                            *self._pending_messages.signals.get(key, ()),
                        ]
                        for key in signal_keys
                    },
                    events={
                        key: [
                            *continuation.events.get(key, ()),
                            *self._pending_messages.events.get(key, ()),
                        ]
                        for key in event_keys
                    },
                )
                pending_messages.validate(limits)
            _verify_trigger_identity(
                parsed,
                definition_digest=definition_digest,
                worker_deployment=worker_deployment,
                worker_build_id=worker_build_id,
                worker_artifact=self._worker_artifact,
                environment_snapshot_digest=self._environment_snapshot_digest,
                scope_digest=selected_scope_digest,
                execution_configuration=self._execution_configuration,
            )
        except LimitExceededError as exc:
            failure = _envelope_error(
                workflow_name=wf_name,
                run_id=run_id,
                request_id=_request_id(trigger),
                phase=FailurePhase.TRIGGER,
                code=FailureCode.LIMIT_EXCEEDED,
                timestamp=started_at,
                cause=exc,
                limits=limits,
                definition_digest=definition_digest,
                worker_deployment=worker_deployment,
                worker_build_id=worker_build_id,
                worker_artifact=self._worker_artifact,
                environment_snapshot_digest=self._environment_snapshot_digest,
                scope_digest=self._scope_digest,
                execution_configuration=self._execution_configuration,
            )
            raise _application_error(failure) from None
        except ValidationError:
            failure = _envelope_error(
                workflow_name=wf_name,
                run_id=run_id,
                request_id=_request_id(trigger),
                phase=FailurePhase.TRIGGER,
                code=FailureCode.INVALID_TRIGGER,
                timestamp=started_at,
                limits=limits,
                definition_digest=definition_digest,
                worker_deployment=worker_deployment,
                worker_build_id=worker_build_id,
                worker_artifact=self._worker_artifact,
                environment_snapshot_digest=self._environment_snapshot_digest,
                scope_digest=self._scope_digest,
                execution_configuration=self._execution_configuration,
            )
            raise _application_error(failure) from None
        except DefinitionIdentityMismatchError as exc:
            failure = _envelope_error(
                workflow_name=wf_name,
                run_id=run_id,
                request_id=_request_id(trigger),
                phase=FailurePhase.TRIGGER,
                code=FailureCode.INVALID_TRIGGER,
                timestamp=started_at,
                cause=exc,
                limits=limits,
                definition_digest=definition_digest,
                worker_deployment=worker_deployment,
                worker_build_id=worker_build_id,
                worker_artifact=self._worker_artifact,
                environment_snapshot_digest=self._environment_snapshot_digest,
                scope_digest=self._scope_digest,
                execution_configuration=self._execution_configuration,
            )
            raise _application_error(failure) from None
        bounded_execution_enabled = workflow.patched(BOUNDED_EXECUTION_PATCH)
        timezone_aware_deadlines_enabled = not has_runtime_deadline or workflow.patched(
            TIMEZONE_AWARE_DEADLINES_PATCH
        )
        self._pending_messages = pending_messages
        self._signals = pending_messages.signals
        self._events = pending_messages.events
        executor = TemporalStepExecutor(
            pending_messages=pending_messages,
            request_id=parsed.request_id,
            correlation_id=parsed.correlation_id,
            trace_id=parsed.trace_id,
            workflow_id=workflow_info.workflow_id,
            workflow_run_id=run_id,
            flow_name=wf_name,
            limits=limits,
            definition_digest=definition_digest,
            worker_deployment=worker_deployment,
            worker_build_id=worker_build_id,
            worker_artifact=self._worker_artifact,
            environment_snapshot_digest=self._environment_snapshot_digest,
            contract_identities=contract_identities,
            child_workflow_types=child_workflow_types,
            child_definition_digests=child_definition_digests,
            child_environment_snapshot_digests=child_environment_snapshot_digests,
            runtime_scope_digest=selected_scope_digest,
            execution_configuration=self._execution_configuration,
        )
        runner = FlowRunner(
            workflow_config,
            services,
            executor,
            run_id=run_id,
            limits=limits,
            definition_digest=definition_digest,
            worker_deployment=worker_deployment,
            worker_build_id=worker_build_id,
            worker_artifact=self._worker_artifact,
            environment_snapshot_digest=self._environment_snapshot_digest,
            trigger_source=parsed.trigger_source,
            trigger_name=parsed.trigger_name,
            source_identity_digest=parsed.source_identity_digest,
            correlation_identity_digest=parsed.correlation_identity_digest,
            scope_digest=selected_scope_digest,
            execution_configuration=self._execution_configuration,
            bounded_execution_enabled=bounded_execution_enabled,
            timezone_aware_deadlines_enabled=timezone_aware_deadlines_enabled,
        )

        try:
            return await runner.run(parsed.request_id, parsed.globals, checkpoint)
        except FlowContinuation as exc:
            await workflow.wait_condition(lambda: workflow.all_handlers_finished())
            continuation_input = WorkflowContinuationInput(
                sequence=continuation.sequence + 1 if continuation is not None else 1,
                previous_run_id=run_id,
                trigger=parsed,
                checkpoint=exc.checkpoint,
                signals=pending_messages.signals,
                events=pending_messages.events,
                observed_history_events=exc.observation.events,
                observed_history_bytes=exc.observation.bytes,
                server_suggested=exc.observation.server_suggested,
            ).model_dump(mode="json")
            if not provenance_enabled:
                continuation_trigger = continuation_input.get("trigger")
                if isinstance(continuation_trigger, dict):
                    continuation_trigger.pop("worker_artifact", None)
            if not environment_snapshot_enabled:
                continuation_trigger = continuation_input.get("trigger")
                if isinstance(continuation_trigger, dict):
                    continuation_trigger.pop("environment_snapshot_digest", None)
            if not runtime_scope_enabled:
                continuation_trigger = continuation_input.get("trigger")
                if isinstance(continuation_trigger, dict):
                    continuation_trigger.pop("scope_digest", None)
            if not execution_configuration_enabled:
                continuation_trigger = continuation_input.get("trigger")
                if isinstance(continuation_trigger, dict):
                    continuation_trigger.pop("execution_configuration", None)
            try:
                enforce_payload_bytes(
                    continuation_input,
                    boundary="workflow.continuation.input",
                    limit=limits.continuation_input_bytes,
                )
            except LimitExceededError as limit_error:
                failure = _envelope_error(
                    workflow_name=wf_name,
                    run_id=run_id,
                    request_id=parsed.request_id,
                    phase=FailurePhase.INTERNAL,
                    code=FailureCode.LIMIT_EXCEEDED,
                    timestamp=_timestamp(workflow.time()),
                    cause=limit_error,
                    limits=limits,
                    definition_digest=definition_digest,
                    worker_deployment=worker_deployment,
                    worker_build_id=worker_build_id,
                    worker_artifact=self._worker_artifact,
                    environment_snapshot_digest=self._environment_snapshot_digest,
                    scope_digest=self._scope_digest,
                    execution_configuration=self._execution_configuration,
                )
                raise _application_error(failure) from None
            workflow.continue_as_new(continuation_input)
        except FlowExecutionError as exc:
            raise _application_error(exc) from None

    async def _handle_signal(self, signal_data: Any) -> None:
        try:
            enforce_payload_bytes(
                signal_data,
                boundary="workflow.signal",
                limit=limits.signal_payload_bytes,
            )
            parsed = SignalPayload.model_validate(signal_data)
            normalized_signal = normalize_json_object(
                parsed.model_dump(mode="json"),
                path="workflow.signal",
                max_collection_items=limits.collection_items,
            )
            key = make_signal_key(parsed.request_id, parsed.step_name, parsed.action)
            self._pending_messages.append_signal(key, normalized_signal, limits)
        except LimitExceededError as exc:
            failure = _envelope_error(
                workflow_name=wf_name,
                run_id=workflow.info().run_id,
                request_id=_request_id(signal_data),
                phase=FailurePhase.SIGNAL,
                code=FailureCode.LIMIT_EXCEEDED,
                timestamp=_timestamp(workflow.time()),
                cause=exc,
                limits=limits,
                definition_digest=definition_digest,
                worker_deployment=worker_deployment,
                worker_build_id=worker_build_id,
                worker_artifact=self._worker_artifact,
                environment_snapshot_digest=self._environment_snapshot_digest,
                scope_digest=self._scope_digest,
                execution_configuration=self._execution_configuration,
            )
            raise _application_error(failure) from None
        except ValidationError:
            failure = _envelope_error(
                workflow_name=wf_name,
                run_id=workflow.info().run_id,
                request_id=_request_id(signal_data),
                phase=FailurePhase.SIGNAL,
                code=FailureCode.INVALID_SIGNAL,
                timestamp=_timestamp(workflow.time()),
                limits=limits,
                definition_digest=definition_digest,
                worker_deployment=worker_deployment,
                worker_build_id=worker_build_id,
                worker_artifact=self._worker_artifact,
                environment_snapshot_digest=self._environment_snapshot_digest,
                scope_digest=self._scope_digest,
                execution_configuration=self._execution_configuration,
            )
            raise _application_error(failure) from None

    async def _handle_event(self, event: Any) -> None:
        try:
            enforce_payload_bytes(
                event,
                boundary="workflow.event",
                limit=limits.signal_payload_bytes,
            )
            parsed = EventPayload.model_validate(event)
            normalized_data = normalize_json_value(
                parsed.data,
                path="workflow.event.data",
                max_collection_items=limits.collection_items,
            )
            self._pending_messages.append_event(parsed.signal, normalized_data, limits)
        except LimitExceededError as exc:
            info = workflow.info()
            failure = _envelope_error(
                workflow_name=wf_name,
                run_id=info.run_id,
                request_id=info.workflow_id,
                phase=FailurePhase.SIGNAL,
                code=FailureCode.LIMIT_EXCEEDED,
                timestamp=_timestamp(workflow.time()),
                cause=exc,
                limits=limits,
                definition_digest=definition_digest,
                worker_deployment=worker_deployment,
                worker_build_id=worker_build_id,
                worker_artifact=self._worker_artifact,
                environment_snapshot_digest=self._environment_snapshot_digest,
                scope_digest=self._scope_digest,
                execution_configuration=self._execution_configuration,
            )
            raise _application_error(failure) from None
        except ValidationError:
            info = workflow.info()
            failure = _envelope_error(
                workflow_name=wf_name,
                run_id=info.run_id,
                request_id=info.workflow_id,
                phase=FailurePhase.SIGNAL,
                code=FailureCode.INVALID_SIGNAL,
                timestamp=_timestamp(workflow.time()),
                limits=limits,
                definition_digest=definition_digest,
                worker_deployment=worker_deployment,
                worker_build_id=worker_build_id,
                worker_artifact=self._worker_artifact,
                environment_snapshot_digest=self._environment_snapshot_digest,
                scope_digest=self._scope_digest,
                execution_configuration=self._execution_configuration,
            )
            raise _application_error(failure) from None

    def _init(self):
        pending_messages = PendingMessages(signals={}, events={})
        self._pending_messages = pending_messages
        self._signals = pending_messages.signals
        self._events = pending_messages.events
        self._worker_artifact = None
        self._environment_snapshot_digest = None
        self._execution_configuration = None
        self._scope_digest = None

    # Qualnames must look module-level (not <locals>) for Temporal's registration
    _run.__qualname__ = f"{class_name}.run"
    _handle_signal.__qualname__ = f"{class_name}.handle_signal"
    _handle_event.__qualname__ = f"{class_name}.handle_event"
    _init.__qualname__ = f"{class_name}.__init__"

    cls = type(
        class_name,
        (),
        {
            "__init__": _init,
            "run": workflow.run(_run),
            "handle_signal": workflow.signal(name=STEP_RESPONSE_SIGNAL)(_handle_signal),
            "handle_event": workflow.signal(name=WORKFLOW_EVENT_SIGNAL)(_handle_event),
            "__module__": __name__,
        },
    )
    cls.__qualname__ = class_name

    cls = workflow.defn(name=temporal_name)(cls)

    setattr(sys.modules[__name__], class_name, cls)

    return cls


def _contract_spec_key(schema: SchemaSpec) -> str:
    if isinstance(schema, str):
        return f"pydantic:{schema}"
    return f"json-schema:{strict_json_bytes(schema).decode('utf-8')}"


def _contract_identity_lookup(workflow_config: WorkflowConfig) -> dict[str, str]:
    schemas = {
        _contract_spec_key(schema): str(validate_contract_declaration(schema))
        for step in workflow_config.steps.values()
        for schema in (step.output_schema,)
        if schema is not None
    }
    return schemas


def _workflow_class_name(
    logical_name: str,
    registration: dict[str, Any],
) -> str:
    safe_name = "".join(character if character.isalnum() else "_" for character in logical_name)
    digest = hashlib.sha256(strict_json_bytes(registration)).hexdigest()
    return f"Workflow_{safe_name}_{digest}"


def _verify_trigger_identity(
    trigger: WorkflowTrigger,
    *,
    definition_digest: str | None,
    worker_deployment: str | None,
    worker_build_id: str | None,
    worker_artifact: WorkerArtifactIdentity | None,
    environment_snapshot_digest: str | None,
    scope_digest: str | None,
    execution_configuration: ExecutionConfigurationIdentity | None,
) -> None:
    expected = (
        ("definition_digest", definition_digest),
        ("worker_deployment", worker_deployment),
        ("worker_build_id", worker_build_id),
        ("environment_snapshot_digest", environment_snapshot_digest),
    )
    for field_name, expected_value in expected:
        actual = getattr(trigger, field_name)
        if actual != expected_value:
            raise DefinitionIdentityMismatchError(
                f"Workflow trigger {field_name} '{actual}' does not match selected "
                f"identity '{expected_value}'"
            )
    if worker_artifact is not None and trigger.worker_artifact != worker_artifact:
        raise DefinitionIdentityMismatchError(
            "Workflow trigger worker artifact does not match the selected artifact identity"
        )
    if trigger.scope_digest != scope_digest:
        raise DefinitionIdentityMismatchError(
            "Workflow trigger scope does not match the selected runtime scope"
        )
    if trigger.execution_configuration != execution_configuration:
        raise DefinitionIdentityMismatchError(
            "Workflow trigger configuration does not match the selected execution identity"
        )
