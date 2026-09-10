"""Tests for typed workflow continuation state."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from justflow.engine.continuation import (
    IncludedIterationResult,
    IterationCheckpoint,
    RunnerCheckpoint,
    WorkflowContinuationInput,
)
from justflow.sdk.message_contract import WorkflowTrigger


def _runner_checkpoint(request_id: str) -> RunnerCheckpoint:
    return RunnerCheckpoint(
        started_at="2026-08-02T00:00:00+00:00",
        params={"request_id": request_id},
        step_outputs={},
        step_timings={},
        transitions=(),
        failures=(),
        next_step_name="next",
        next_seq=0,
        invocations=0,
    )


def test_continuation_requires_matching_request_identity() -> None:
    with pytest.raises(ValidationError, match="request identity does not match"):
        WorkflowContinuationInput(
            sequence=1,
            previous_run_id="previous-run",
            trigger=WorkflowTrigger(request_id="trigger-request"),
            checkpoint=_runner_checkpoint("checkpoint-request"),
            signals={},
            events={},
            observed_history_events=10,
            observed_history_bytes=100,
            server_suggested=False,
        )


def test_iteration_checkpoint_requires_one_result_per_processed_item() -> None:
    with pytest.raises(ValidationError, match="results do not match its offset"):
        IterationCheckpoint(
            step_name="fanout",
            started_at_epoch=0.0,
            next_offset=2,
            results=(IncludedIterationResult(key=0, value="first"),),
            failed=0,
        )


def test_iteration_checkpoint_rejects_impossible_failure_count() -> None:
    with pytest.raises(ValidationError, match="failure count exceeds processed items"):
        IterationCheckpoint(
            step_name="fanout",
            started_at_epoch=0.0,
            next_offset=1,
            results=(IncludedIterationResult(key=0, value="first"),),
            failed=2,
        )
