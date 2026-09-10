"""Tests for pure control-flow and path-sensitive dataflow analysis."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from justflow.config.models import (
    FlowStep,
    OnResultBranch,
    StepDefinition,
    WorkflowConfig,
)
from justflow.engine.analysis import analyze_workflow

STEP_DEFINITIONS = {
    "work": StepDefinition(service="svc", action="work"),
}


def _workflow(
    flow: list[FlowStep],
    *,
    result: str | None = None,
    on_error: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
) -> WorkflowConfig:
    return WorkflowConfig(
        workflow="analysis",
        steps=STEP_DEFINITIONS,
        flow=flow,
        result=result,
        on_error=on_error,
        params=params or {},
    )


@dataclass(frozen=True, kw_only=True)
class AnalysisCase:
    id: str
    workflow: WorkflowConfig
    expected_issues: tuple[str, ...]


ANALYSIS_CASES = [
    AnalysisCase(
        id="valid-linear-flow",
        workflow=_workflow(
            [
                FlowStep(name="produce", op="work", output="data", then="consume"),
                FlowStep(name="consume", op="work", input="produce.data", then="done"),
                FlowStep(name="done", terminal=True),
            ]
        ),
        expected_issues=(),
    ),
    AnalysisCase(
        id="later-producer",
        workflow=_workflow(
            [
                FlowStep(name="consume", op="work", input="produce.data", then="produce"),
                FlowStep(name="produce", op="work", output="data", then="done"),
                FlowStep(name="done", terminal=True),
            ]
        ),
        expected_issues=("Reference root 'produce' is not available",),
    ),
    AnalysisCase(
        id="condition-skip-does-not-produce-output",
        workflow=_workflow(
            [
                FlowStep(
                    name="produce",
                    op="work",
                    output="data",
                    condition="input.enabled == true",
                    then="consume",
                ),
                FlowStep(name="consume", op="work", input="data", then="done"),
                FlowStep(name="done", terminal=True),
            ]
        ),
        expected_issues=("Reference root 'data' is not available",),
    ),
    AnalysisCase(
        id="wait-timeout-does-not-produce-output",
        workflow=_workflow(
            [
                FlowStep(
                    name="wait",
                    wait_for={
                        "signal": "event",
                        "timeout_sec": 10,
                        "on_timeout": "consume",
                    },
                    output="payload",
                    then="consume",
                ),
                FlowStep(name="consume", op="work", input="payload", then="done"),
                FlowStep(name="done", terminal=True),
            ]
        ),
        expected_issues=("Reference root 'payload' is not available",),
    ),
    AnalysisCase(
        id="loop-exhaustion-does-not-produce-output",
        workflow=_workflow(
            [
                FlowStep(
                    name="poll",
                    op="work",
                    output="state",
                    until="input.ready == true",
                    max_iterations=2,
                    on_exhausted="consume",
                    then="consume",
                ),
                FlowStep(name="consume", op="work", input="state", then="done"),
                FlowStep(name="done", terminal=True),
            ]
        ),
        expected_issues=("Reference root 'state' is not available",),
    ),
    AnalysisCase(
        id="result-branch-diamond",
        workflow=_workflow(
            [
                FlowStep(
                    name="decide",
                    op="work",
                    on_result=[
                        OnResultBranch(when="input.ok == true", then="left"),
                        OnResultBranch(default="right"),
                    ],
                ),
                FlowStep(name="left", op="work", then="join"),
                FlowStep(name="right", op="work", then="join"),
                FlowStep(name="join", op="work", input="left", then="done"),
                FlowStep(name="done", terminal=True),
            ]
        ),
        expected_issues=("Reference root 'left' is not available",),
    ),
    AnalysisCase(
        id="failure-edge-provides-only-error",
        workflow=_workflow(
            [
                FlowStep(
                    name="produce",
                    op="work",
                    output="data",
                    on_failure="handler",
                    then="done",
                ),
                FlowStep(name="handler", op="work", input="data", then="failed"),
                FlowStep(name="failed", terminal=True, reason="handled"),
                FlowStep(name="done", terminal=True),
            ]
        ),
        expected_issues=("Reference root 'data' is not available",),
    ),
    AnalysisCase(
        id="failure-handler-error-root",
        workflow=_workflow(
            [
                FlowStep(
                    name="produce",
                    op="work",
                    on_failure="handler",
                    then="done",
                ),
                FlowStep(name="handler", op="work", input="error", then="failed"),
                FlowStep(name="failed", terminal=True, reason="handled"),
                FlowStep(name="done", terminal=True),
            ]
        ),
        expected_issues=(),
    ),
    AnalysisCase(
        id="unreachable-step",
        workflow=_workflow(
            [
                FlowStep(name="entry", op="work", then="done"),
                FlowStep(name="orphan", op="work", then="done"),
                FlowStep(name="done", terminal=True),
            ]
        ),
        expected_issues=("Flow step 'orphan' is unreachable",),
    ),
    AnalysisCase(
        id="execution-cycle",
        workflow=_workflow(
            [
                FlowStep(name="a", op="work", then="b"),
                FlowStep(name="b", op="work", then="a"),
            ]
        ),
        expected_issues=("Circular control flow detected",),
    ),
    AnalysisCase(
        id="reachable-non-terminal-sink",
        workflow=WorkflowConfig.model_construct(
            workflow="analysis",
            steps=STEP_DEFINITIONS,
            flow=[
                FlowStep.model_construct(
                    name="dangling",
                    op="work",
                    then=None,
                )
            ],
            params={},
            result=None,
            on_error=None,
        ),
        expected_issues=("has no explicit terminal successor",),
    ),
    AnalysisCase(
        id="normal-terminal-requires-result",
        workflow=_workflow(
            [
                FlowStep(
                    name="produce",
                    op="work",
                    condition="input.enabled == true",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
            result="produce",
        ),
        expected_issues=("Workflow result 'produce' is not available",),
    ),
    AnalysisCase(
        id="early-terminal-may-omit-result",
        workflow=_workflow(
            [
                FlowStep(
                    name="produce",
                    op="work",
                    condition="input.enabled == true",
                    then="done",
                ),
                FlowStep(name="done", terminal=True, reason="not-produced"),
            ],
            result="produce",
        ),
        expected_issues=(),
    ),
]


class TestAnalyzeWorkflow:
    @pytest.mark.parametrize(
        "case",
        ANALYSIS_CASES,
        ids=lambda case: case.id,
    )
    def test_analysis(self, case: AnalysisCase):
        messages = [issue.message for issue in analyze_workflow(case.workflow)]

        for expected in case.expected_issues:
            assert any(expected in message for message in messages), messages
        if not case.expected_issues:
            assert messages == []
