"""Pure engine failure taxonomy and bounded Temporal-detail records."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from enum import Enum
from typing import Any

from justflow.config.runtime_limits import DEFAULT_FAILURE_RECORD_BYTES
from justflow.engine.audit import (
    CorrelationIdentity,
    FailureCategory,
    FailureDetail,
    FailureMetadata,
    FailurePhase,
    FailureRecord,
    FailureStepMetadata,
    FailureTransitionMetadata,
)
from justflow.engine.limits import LimitExceededError
from justflow.engine.serialization import (
    STRICT_JSON_VERSION,
    StrictJsonLayout,
    strict_json_bytes,
)
from justflow.provenance import ExecutionConfigurationIdentity, WorkerArtifactIdentity

MAX_CAUSE_DEPTH = 10
MAX_FAILURE_CODE_LENGTH = 128
MAX_CORRELATION_VALUE_LENGTH = 128
MAX_FAILURE_MESSAGE_LENGTH = 1_024
MAX_FAILURE_STEPS = 50
MAX_FAILURE_TRANSITIONS = 50
MAX_SECONDARY_FAILURES = 4
MAX_FAILURE_RECORD_BYTES = DEFAULT_FAILURE_RECORD_BYTES
TEMPORAL_CANCELLED_MODULE = "temporalio.exceptions"
TEMPORAL_CANCELLED_NAME = "CancelledError"


class FlowErrorKind(str, Enum):
    MISSING_PARAMS = "MISSING_PARAMS"
    RESOLUTION_ERROR = "RESOLUTION_ERROR"
    WAIT_TIMEOUT = "WAIT_TIMEOUT"
    LOOP_EXHAUSTED = "LOOP_EXHAUSTED"
    UNSAFE_LOOP_CACHE = "UNSAFE_LOOP_CACHE"
    INVARIANT_VIOLATION = "INVARIANT_VIOLATION"


class FailureCode(str, Enum):
    INVALID_TRIGGER = "INVALID_TRIGGER"
    INVALID_SIGNAL = "INVALID_SIGNAL"
    INPUT_CONTRACT_FAILED = "INPUT_CONTRACT_FAILED"
    PARAMETERS_FAILED = "PARAMETERS_FAILED"
    STEP_FAILED = "STEP_FAILED"
    HANDLER_FAILED = "HANDLER_FAILED"
    RESULT_FAILED = "RESULT_FAILED"
    ARCHIVAL_FAILED = "ARCHIVAL_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    LIMIT_EXCEEDED = "LIMIT_EXCEEDED"


class FlowDefinitionError(Exception):
    """The flow cannot proceed because its declaration or input is invalid."""

    def __init__(self, message: str, kind: FlowErrorKind):
        self.kind = kind
        super().__init__(message)


class FlowExecutionError(Exception):
    """An ordinary execution failure with durable, bounded failure details."""

    def __init__(
        self,
        *,
        classification: FailureDetail,
        record: FailureRecord,
        correlation: CorrelationIdentity,
        secondary_failures: tuple[FailureDetail, ...] = (),
    ) -> None:
        self.classification = classification
        self.record = record
        self.correlation = correlation
        self.secondary_failures = secondary_failures
        super().__init__(f"{classification.code}: {classification.message}")


def correlation_identity(
    *,
    workflow: str,
    request_id: str | None = None,
    run_id: str | None = None,
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
) -> CorrelationIdentity:
    return CorrelationIdentity(
        workflow=_bounded(workflow, MAX_CORRELATION_VALUE_LENGTH),
        request_id=(
            _bounded(request_id, MAX_CORRELATION_VALUE_LENGTH) if request_id is not None else None
        ),
        run_id=(_bounded(run_id, MAX_CORRELATION_VALUE_LENGTH) if run_id is not None else None),
        definition_digest=(
            _bounded(definition_digest, MAX_CORRELATION_VALUE_LENGTH)
            if definition_digest is not None
            else None
        ),
        worker_deployment=(
            _bounded(worker_deployment, MAX_CORRELATION_VALUE_LENGTH)
            if worker_deployment is not None
            else None
        ),
        worker_build_id=(
            _bounded(worker_build_id, MAX_CORRELATION_VALUE_LENGTH)
            if worker_build_id is not None
            else None
        ),
        worker_artifact=worker_artifact,
        environment_snapshot_digest=environment_snapshot_digest,
        trigger_source=(
            _bounded(trigger_source, MAX_CORRELATION_VALUE_LENGTH)
            if trigger_source is not None
            else None
        ),
        trigger_name=(
            _bounded(trigger_name, MAX_CORRELATION_VALUE_LENGTH)
            if trigger_name is not None
            else None
        ),
        source_identity_digest=source_identity_digest,
        correlation_identity_digest=correlation_identity_digest,
        scope_digest=scope_digest,
        execution_configuration=execution_configuration,
    )


def is_cancellation(exc: BaseException) -> bool:
    current: BaseException | None = exc
    visited: set[int] = set()
    for _ in range(MAX_CAUSE_DEPTH):
        if current is None or id(current) in visited:
            break
        visited.add(id(current))
        exception_type = type(current)
        if isinstance(current, asyncio.CancelledError) or (
            exception_type.__module__ == TEMPORAL_CANCELLED_MODULE
            and exception_type.__name__ == TEMPORAL_CANCELLED_NAME
        ):
            return True
        current = getattr(current, "cause", None) or current.__cause__
    return False


def classify_failure(
    exc: BaseException,
    *,
    phase: FailurePhase,
    step: str | None = None,
    code: FailureCode | None = None,
) -> FailureDetail:
    cause_code, message, non_retryable = _error_details(exc)
    if isinstance(exc, LimitExceededError):
        stable_code = FailureCode.LIMIT_EXCEEDED.value
        category = _phase_category(phase)
        retryable = False
    elif isinstance(exc, FlowDefinitionError):
        stable_code = exc.kind.value
        category = FailureCategory.DEFINITION
        retryable = False
    else:
        stable_code = (code or _phase_code(phase)).value
        category = _phase_category(phase)
        retryable = not non_retryable if non_retryable is not None else False
    return FailureDetail(
        code=_bounded(stable_code, MAX_FAILURE_CODE_LENGTH),
        cause_code=_bounded(cause_code, MAX_FAILURE_CODE_LENGTH),
        category=category,
        phase=phase,
        message=_bounded(message, MAX_FAILURE_MESSAGE_LENGTH),
        retryable=retryable,
        step=step,
    )


def build_execution_error(
    *,
    exc: BaseException,
    correlation: CorrelationIdentity,
    phase: FailurePhase,
    audit_version: int,
    started_at: str,
    failed_at: str,
    step: str | None = None,
    step_entries: Mapping[str, Mapping[str, Any]] | None = None,
    transitions: Iterable[Mapping[str, Any]] = (),
    primary: FailureDetail | None = None,
    secondary: Iterable[FailureDetail] = (),
    code: FailureCode | None = None,
    max_record_bytes: int = MAX_FAILURE_RECORD_BYTES,
) -> FlowExecutionError:
    classification = primary or classify_failure(exc, phase=phase, step=step, code=code)
    secondary_failures = tuple(secondary)[:MAX_SECONDARY_FAILURES]
    record = FailureRecord(
        audit_version=audit_version,
        serialization_version=STRICT_JSON_VERSION,
        correlation=correlation,
        error=FailureMetadata.from_detail(classification),
        secondary=[FailureMetadata.from_detail(failure) for failure in secondary_failures],
        steps=_failure_steps(step_entries or {}),
        transitions=_failure_transitions(transitions),
        started_at=started_at,
        failed_at=failed_at,
    )
    if _record_size(record) > max_record_bytes:
        record.steps = []
        record.transitions = []
    return FlowExecutionError(
        classification=classification,
        record=record,
        correlation=correlation,
        secondary_failures=secondary_failures,
    )


def _phase_code(phase: FailurePhase) -> FailureCode:
    return {
        FailurePhase.TRIGGER: FailureCode.INVALID_TRIGGER,
        FailurePhase.SIGNAL: FailureCode.INVALID_SIGNAL,
        FailurePhase.WORKFLOW_INPUT: FailureCode.INPUT_CONTRACT_FAILED,
        FailurePhase.PARAMETERS: FailureCode.PARAMETERS_FAILED,
        FailurePhase.STEP: FailureCode.STEP_FAILED,
        FailurePhase.HANDLER: FailureCode.HANDLER_FAILED,
        FailurePhase.RESULT: FailureCode.RESULT_FAILED,
        FailurePhase.ARCHIVE: FailureCode.ARCHIVAL_FAILED,
    }.get(phase, FailureCode.INTERNAL_ERROR)


def _phase_category(phase: FailurePhase) -> FailureCategory:
    if phase in {FailurePhase.TRIGGER, FailurePhase.SIGNAL, FailurePhase.PARAMETERS}:
        return FailureCategory.INPUT
    if phase in {FailurePhase.WORKFLOW_INPUT, FailurePhase.RESULT}:
        return FailureCategory.CONTRACT
    if phase in {FailurePhase.STEP, FailurePhase.HANDLER}:
        return FailureCategory.EXECUTION
    if phase is FailurePhase.ARCHIVE:
        return FailureCategory.INFRASTRUCTURE
    return FailureCategory.INTERNAL


def _error_details(exc: BaseException) -> tuple[str, str, bool | None]:
    best: tuple[str, str, bool | None] = (
        type(exc).__name__,
        str(exc),
        None,
    )
    current: BaseException | None = exc
    visited: set[int] = set()
    for _ in range(MAX_CAUSE_DEPTH):
        if current is None or id(current) in visited:
            break
        visited.add(id(current))
        cause_code = getattr(current, "type", None)
        message = getattr(current, "message", None)
        if cause_code and message:
            non_retryable_value = getattr(current, "non_retryable", None)
            best = (
                str(cause_code),
                str(message),
                (non_retryable_value if isinstance(non_retryable_value, bool) else None),
            )
        current = getattr(current, "cause", None) or current.__cause__
    return best


def _bounded(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return f"{value[: limit - 1]}…"


def _failure_steps(
    step_entries: Mapping[str, Mapping[str, Any]],
) -> list[FailureStepMetadata]:
    ordered = sorted(
        step_entries.items(),
        key=lambda item: int(item[1].get("seq", 0)),
    )[-MAX_FAILURE_STEPS:]
    return [
        FailureStepMetadata(
            name=_bounded(name, MAX_FAILURE_CODE_LENGTH),
            seq=int(entry.get("seq", 0)),
            status=_bounded(str(entry.get("status", "unknown")), MAX_FAILURE_CODE_LENGTH),
            code=(
                _bounded(str(entry["code"]), MAX_FAILURE_CODE_LENGTH)
                if entry.get("code") is not None
                else None
            ),
        )
        for name, entry in ordered
    ]


def _failure_transitions(
    transitions: Iterable[Mapping[str, Any]],
) -> list[FailureTransitionMetadata]:
    bounded = list(transitions)[-MAX_FAILURE_TRANSITIONS:]
    return [
        FailureTransitionMetadata(
            step=_bounded(str(transition.get("step", "")), MAX_FAILURE_CODE_LENGTH),
            matched=_bounded(str(transition.get("matched", "")), MAX_FAILURE_CODE_LENGTH),
            target=_bounded(str(transition.get("target", "")), MAX_FAILURE_CODE_LENGTH),
        )
        for transition in bounded
    ]


def _record_size(record: FailureRecord) -> int:
    return len(strict_json_bytes(record.dump(), layout=StrictJsonLayout.CANONICAL))
