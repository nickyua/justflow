"""Tests for pure execution failure classification and record bounds."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.exceptions import CancelledError as TemporalCancelledError

from justflow.engine.audit import FailureCategory, FailurePhase
from justflow.engine.errors import (
    MAX_FAILURE_MESSAGE_LENGTH,
    MAX_FAILURE_RECORD_BYTES,
    MAX_FAILURE_STEPS,
    MAX_FAILURE_TRANSITIONS,
    FailureCode,
    FlowDefinitionError,
    FlowErrorKind,
    build_execution_error,
    classify_failure,
    correlation_identity,
    is_cancellation,
)


@dataclass(frozen=True, kw_only=True)
class ExpectedClassification:
    code: str
    cause_code: str
    category: FailureCategory
    retryable: bool


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: ExpectedClassification


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[BaseException]
    match: str


ClassificationOutcome = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class ClassificationCase:
    id: str
    error_factory: Callable[[], BaseException]
    phase: FailurePhase
    code: FailureCode | None
    outcome: ClassificationOutcome


CLASSIFICATION_CASES = [
    ClassificationCase(
        id="definition",
        error_factory=lambda: FlowDefinitionError(
            "missing parameter", FlowErrorKind.MISSING_PARAMS
        ),
        phase=FailurePhase.PARAMETERS,
        code=None,
        outcome=Returns(
            value=ExpectedClassification(
                code="MISSING_PARAMS",
                cause_code="FlowDefinitionError",
                category=FailureCategory.DEFINITION,
                retryable=False,
            )
        ),
    ),
    ClassificationCase(
        id="trigger",
        error_factory=lambda: ValueError("invalid trigger"),
        phase=FailurePhase.TRIGGER,
        code=FailureCode.INVALID_TRIGGER,
        outcome=Returns(
            value=ExpectedClassification(
                code="INVALID_TRIGGER",
                cause_code="ValueError",
                category=FailureCategory.INPUT,
                retryable=False,
            )
        ),
    ),
    ClassificationCase(
        id="step-retryable",
        error_factory=lambda: ApplicationError(
            "temporarily unavailable",
            type="REMOTE_UNAVAILABLE",
            non_retryable=False,
        ),
        phase=FailurePhase.STEP,
        code=None,
        outcome=Returns(
            value=ExpectedClassification(
                code="STEP_FAILED",
                cause_code="REMOTE_UNAVAILABLE",
                category=FailureCategory.EXECUTION,
                retryable=True,
            )
        ),
    ),
    ClassificationCase(
        id="parameters",
        error_factory=lambda: TypeError("parameter interpolation failed"),
        phase=FailurePhase.PARAMETERS,
        code=None,
        outcome=Returns(
            value=ExpectedClassification(
                code="PARAMETERS_FAILED",
                cause_code="TypeError",
                category=FailureCategory.INPUT,
                retryable=False,
            )
        ),
    ),
    ClassificationCase(
        id="handler-non-retryable",
        error_factory=lambda: ApplicationError(
            "invalid handler request",
            type="INVALID_HANDLER_REQUEST",
            non_retryable=True,
        ),
        phase=FailurePhase.HANDLER,
        code=FailureCode.HANDLER_FAILED,
        outcome=Returns(
            value=ExpectedClassification(
                code="HANDLER_FAILED",
                cause_code="INVALID_HANDLER_REQUEST",
                category=FailureCategory.EXECUTION,
                retryable=False,
            )
        ),
    ),
    ClassificationCase(
        id="result",
        error_factory=lambda: ValueError("result unavailable"),
        phase=FailurePhase.RESULT,
        code=None,
        outcome=Returns(
            value=ExpectedClassification(
                code="RESULT_FAILED",
                cause_code="ValueError",
                category=FailureCategory.CONTRACT,
                retryable=False,
            )
        ),
    ),
    ClassificationCase(
        id="archive",
        error_factory=lambda: OSError("archive unavailable"),
        phase=FailurePhase.ARCHIVE,
        code=None,
        outcome=Returns(
            value=ExpectedClassification(
                code="ARCHIVAL_FAILED",
                cause_code="OSError",
                category=FailureCategory.INFRASTRUCTURE,
                retryable=False,
            )
        ),
    ),
]


@pytest.mark.parametrize(
    "case",
    CLASSIFICATION_CASES,
    ids=lambda case: case.id,
)
def test_failure_classification(case: ClassificationCase):
    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            classify_failure(case.error_factory(), phase=case.phase, code=case.code)
        return

    detail = classify_failure(case.error_factory(), phase=case.phase, code=case.code)
    assert detail.code == case.outcome.value.code
    assert detail.cause_code == case.outcome.value.cause_code
    assert detail.category is case.outcome.value.category
    assert detail.retryable is case.outcome.value.retryable


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(asyncio.CancelledError(), id="asyncio"),
        pytest.param(TemporalCancelledError(), id="temporal-sdk"),
    ],
)
def test_cancellation_forms_are_detected(error: BaseException):
    assert is_cancellation(error)


def test_wrapped_sdk_cancellation_is_detected():
    wrapper = RuntimeError("activity failed")
    wrapper.__cause__ = TemporalCancelledError()

    assert is_cancellation(wrapper)


def test_failure_record_is_bounded_and_metadata_only():
    secret = "secret-payload-sentinel"
    long_value = "x" * MAX_FAILURE_MESSAGE_LENGTH
    step_entries = {
        f"step-{index}-{long_value}": {
            "seq": index,
            "status": "failed",
            "code": long_value,
            "input": secret,
            "output": secret,
        }
        for index in range(MAX_FAILURE_STEPS * 2)
    }
    transitions = [
        {
            "step": f"step-{index}-{long_value}",
            "matched": long_value,
            "target": long_value,
            "payload": secret,
        }
        for index in range(MAX_FAILURE_TRANSITIONS * 2)
    ]
    failure = build_execution_error(
        exc=RuntimeError(secret),
        correlation=correlation_identity(
            workflow=long_value,
            request_id=long_value,
            run_id=long_value,
        ),
        phase=FailurePhase.STEP,
        audit_version=2,
        started_at="2026-07-31T00:00:00+00:00",
        failed_at="2026-07-31T00:00:01+00:00",
        step="failed-step",
        step_entries=step_entries,
        transitions=transitions,
    )
    serialized = json.dumps(
        failure.record.dump(), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")

    assert len(serialized) <= MAX_FAILURE_RECORD_BYTES
    assert len(failure.record.steps) <= MAX_FAILURE_STEPS
    assert len(failure.record.transitions) <= MAX_FAILURE_TRANSITIONS
    assert secret.encode() not in serialized
    assert failure.classification.message == secret


def test_failure_message_is_bounded():
    failure = build_execution_error(
        exc=RuntimeError("x" * (MAX_FAILURE_MESSAGE_LENGTH * 2)),
        correlation=correlation_identity(workflow="flow"),
        phase=FailurePhase.STEP,
        audit_version=2,
        started_at="2026-07-31T00:00:00+00:00",
        failed_at="2026-07-31T00:00:01+00:00",
    )

    assert len(failure.classification.message) == MAX_FAILURE_MESSAGE_LENGTH
    assert failure.classification.message.endswith("…")
