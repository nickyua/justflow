"""Tests for config models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from pydantic import ValidationError

from justflow.config.models import (
    ChildWorkflowTarget,
    FlowStep,
    OnResultBranch,
    RedactedAuditCapture,
    ServiceConfig,
    ServiceOperationTarget,
    StepDefinition,
    WorkflowConfig,
)


@dataclass(frozen=True, kw_only=True)
class ModelCase:
    """One construct-or-reject case; error_match=None means the model is valid."""

    id: str
    kwargs: dict[str, Any] = field(default_factory=dict)
    error_match: str | None = None


class TestServiceConfig:
    def test_accepts_open_provider_declaration(self):
        config = ServiceConfig(
            transport="acme.custom",
            transport_config={"endpoint": "custom://service"},
            dispatch_timeout_sec=30,
            retries=2,
        )
        assert config.transport == "acme.custom"
        assert config.transport_config == {"endpoint": "custom://service"}

    def test_rejects_unknown_outer_field(self):
        with pytest.raises(ValidationError, match="unexpected"):
            ServiceConfig(
                transport="custom",
                transport_config={},
                dispatch_timeout_sec=30,
                retries=2,
                unexpected=True,
            )


FLOW_STEP_CASES = [
    ModelCase(id="terminal-with-reason", kwargs={"name": "done", "terminal": True, "reason": "ok"}),
    ModelCase(id="regular-step", kwargs={"name": "s", "op": "op1", "then": "next"}),
    ModelCase(
        id="parallel-loop",
        kwargs={
            "name": "s",
            "op": "op1",
            "for_each": "input.items",
            "parallel": True,
            "max_concurrency": 2,
            "then": "next",
        },
    ),
    ModelCase(
        id="wait-step",
        kwargs={"name": "w", "wait_for": {"signal": "s1", "timeout_sec": 60}, "then": "next"},
    ),
    ModelCase(id="sleep-step", kwargs={"name": "z", "sleep_sec": 30, "then": "next"}),
    ModelCase(id="non-terminal-without-op", kwargs={"name": "bad"}, error_match="exactly one of"),
    ModelCase(
        id="op-and-wait-for",
        kwargs={"name": "bad", "op": "x", "wait_for": {"signal": "s", "timeout_sec": 5}},
        error_match="exactly one of",
    ),
    ModelCase(
        id="wait-and-sleep",
        kwargs={"name": "bad", "wait_for": {"signal": "s", "timeout_sec": 5}, "sleep_sec": 5},
        error_match="exactly one of",
    ),
    ModelCase(
        id="unbounded-wait",
        kwargs={"name": "bad", "wait_for": {"signal": "s"}},
        error_match="must be bounded",
    ),
    ModelCase(
        id="wait-with-for-each",
        kwargs={
            "name": "bad",
            "wait_for": {"signal": "s", "timeout_sec": 5},
            "for_each": "input.items",
        },
        error_match="cannot be combined",
    ),
    ModelCase(
        id="sleep-with-output",
        kwargs={"name": "bad", "sleep_sec": 5, "output": "x"},
        error_match="produce no output",
    ),
    ModelCase(
        id="until-loop",
        kwargs={
            "name": "p",
            "op": "x",
            "until": "input.ok == true",
            "max_iterations": 5,
            "interval_sec": 10,
            "on_exhausted": "z",
            "then": "n",
        },
    ),
    ModelCase(
        id="until-without-max-iterations",
        kwargs={"name": "p", "op": "x", "until": "input.ok == true"},
        error_match="requires max_iterations",
    ),
    ModelCase(
        id="until-with-for-each",
        kwargs={
            "name": "p",
            "op": "x",
            "until": "input.ok == true",
            "max_iterations": 2,
            "for_each": "input.items",
        },
        error_match="cannot be combined with for_each",
    ),
    ModelCase(
        id="max-iterations-without-until",
        kwargs={"name": "p", "op": "x", "max_iterations": 5},
        error_match="only valid with 'until'",
    ),
    ModelCase(
        id="until-on-wait-step",
        kwargs={
            "name": "p",
            "wait_for": {"signal": "s", "timeout_sec": 5},
            "until": "input.ok == true",
            "max_iterations": 2,
        },
        error_match="only valid on op steps",
    ),
    ModelCase(
        id="on-failure-on-op-step",
        kwargs={"name": "s", "op": "x", "on_failure": "handler", "then": "n"},
    ),
    ModelCase(
        id="on-failure-on-wait-step",
        kwargs={
            "name": "s",
            "wait_for": {"signal": "e", "timeout_sec": 5},
            "on_failure": "handler",
        },
        error_match="only valid on op steps",
    ),
    ModelCase(
        id="terminal-with-on-failure",
        kwargs={"name": "s", "terminal": True, "on_failure": "handler"},
        error_match="should not have",
    ),
    ModelCase(
        id="terminal-with-wait",
        kwargs={"name": "bad", "terminal": True, "wait_for": {"signal": "s", "timeout_sec": 5}},
        error_match="should not have",
    ),
    ModelCase(
        id="terminal-with-op",
        kwargs={"name": "bad", "terminal": True, "op": "x"},
        error_match="should not have execution",
    ),
    ModelCase(
        id="then-and-on-result",
        kwargs={
            "name": "x",
            "op": "op1",
            "then": "next",
            "on_result": [OnResultBranch(default="fallback")],
        },
        error_match="cannot have both",
    ),
    ModelCase(
        id="parallel-without-for-each",
        kwargs={"name": "x", "op": "op1", "parallel": True},
        error_match="parallel=true but no for_each",
    ),
    ModelCase(
        id="parallel-without-concurrency-bound",
        kwargs={
            "name": "x",
            "op": "op1",
            "for_each": "input.items",
            "parallel": True,
            "then": "next",
        },
        error_match="requires max_concurrency",
    ),
    ModelCase(
        id="max-concurrency-zero",
        kwargs={
            "name": "x",
            "op": "op1",
            "for_each": "input.items",
            "parallel": True,
            "max_concurrency": 0,
        },
        error_match="greater than or equal to 1",
    ),
    ModelCase(
        id="max-concurrency-without-parallel",
        kwargs={"name": "x", "op": "op1", "for_each": "input.items", "max_concurrency": 2},
        error_match="parallel is not true",
    ),
]

ON_RESULT_BRANCH_CASES = [
    ModelCase(id="when-with-then", kwargs={"when": "input.x == 'y'", "then": "next"}),
    ModelCase(id="default-only", kwargs={"default": "fallback"}),
    ModelCase(
        id="when-without-then",
        kwargs={"when": "input.x == 'y'"},
        error_match="must have 'then'",
    ),
    ModelCase(
        id="neither-when-nor-default",
        kwargs={"then": "x"},
        error_match="either 'when' or 'default'",
    ),
    ModelCase(
        id="both-when-and-default",
        kwargs={"when": "input.x == 'y'", "then": "a", "default": "b"},
        error_match="cannot have both",
    ),
]


class TestFlowStep:
    @pytest.mark.parametrize("case", FLOW_STEP_CASES, ids=lambda c: c.id)
    def test_flow_step_validation(self, case: ModelCase):
        if case.error_match is None:
            step = FlowStep(**case.kwargs)
            assert step.name == case.kwargs["name"]
        else:
            with pytest.raises(ValidationError, match=case.error_match):
                FlowStep(**case.kwargs)


class TestOnResultBranch:
    @pytest.mark.parametrize("case", ON_RESULT_BRANCH_CASES, ids=lambda c: c.id)
    def test_branch_validation(self, case: ModelCase):
        if case.error_match is None:
            OnResultBranch(**case.kwargs)
        else:
            with pytest.raises(ValidationError, match=case.error_match):
                OnResultBranch(**case.kwargs)


STEP_DEFINITION_CASES = [
    ModelCase(id="service-and-action", kwargs={"service": "source_api", "action": "get"}),
    ModelCase(id="workflow-only", kwargs={"workflow": "child_flow"}),
    ModelCase(
        id="workflow-and-service",
        kwargs={"workflow": "child_flow", "service": "source_api", "action": "get"},
        error_match="not both",
    ),
    ModelCase(
        id="service-without-action",
        kwargs={"service": "source_api"},
        error_match="requires service and action",
    ),
    ModelCase(id="nothing", kwargs={}, error_match="requires service and action"),
    ModelCase(
        id="workflow-with-schema",
        kwargs={"workflow": "child_flow", "output_schema": {"type": "object"}},
        error_match="children own their contracts",
    ),
    ModelCase(
        id="workflow-with-cache",
        kwargs={"workflow": "child_flow", "cache": {"resource": "r", "key": "k"}},
        error_match="cache inside the child",
    ),
]


class TestStepDefinition:
    @pytest.mark.parametrize("case", STEP_DEFINITION_CASES, ids=lambda c: c.id)
    def test_target_validation(self, case: ModelCase):
        if case.error_match is None:
            StepDefinition(**case.kwargs)
        else:
            with pytest.raises(ValidationError, match=case.error_match):
                StepDefinition(**case.kwargs)

    @pytest.mark.parametrize(
        "target",
        [
            pytest.param(
                ServiceOperationTarget(service="source_api", action="get"),
                id="service-operation",
            ),
            pytest.param(
                ChildWorkflowTarget(workflow="child_flow"),
                id="child-workflow",
            ),
        ],
    )
    def test_discriminated_target_is_canonical(
        self,
        target: ServiceOperationTarget | ChildWorkflowTarget,
    ) -> None:
        definition = StepDefinition(target=target)

        assert definition.target == target
        assert definition.model_dump(mode="json")["target"]["kind"] == target.kind


class TestWorkflowConfig:
    def test_on_result_requires_default(self):
        with pytest.raises(ValidationError, match="must end with a 'default'"):
            WorkflowConfig(
                workflow="test",
                steps={"op1": StepDefinition(service="svc", action="act")},
                flow=[
                    FlowStep(
                        name="step1",
                        op="op1",
                        on_result=[OnResultBranch(when="input.x == 1", then="step2")],
                    ),
                    FlowStep(name="step2", terminal=True),
                ],
            )

    def test_on_result_default_must_be_last(self):
        with pytest.raises(ValidationError, match="must be last"):
            WorkflowConfig(
                workflow="test",
                steps={"op1": StepDefinition(service="svc", action="act")},
                flow=[
                    FlowStep(
                        name="step1",
                        op="op1",
                        on_result=[
                            OnResultBranch(default="step2"),
                            OnResultBranch(when="input.x == 1", then="step2"),
                        ],
                    ),
                    FlowStep(name="step2", terminal=True),
                ],
            )

    def test_empty_flow_is_rejected(self):
        with pytest.raises(ValidationError, match="at least 1 item"):
            WorkflowConfig(
                workflow="test",
                steps={"op1": StepDefinition(service="svc", action="act")},
                flow=[],
            )

    def test_output_contract_requires_result(self):
        with pytest.raises(ValidationError, match="output_schema requires"):
            WorkflowConfig(
                workflow="test",
                output_schema={"type": "object"},
                steps={"op1": StepDefinition(service="svc", action="act")},
                flow=[FlowStep(name="done", terminal=True)],
            )


class TestStrictDeclarations:
    def test_unknown_flow_field_is_rejected(self):
        with pytest.raises(ValidationError, match="on_faliure"):
            FlowStep(
                name="step",
                op="op1",
                then="done",
                on_faliure="handler",
            )

    def test_native_boolean_field_rejects_string_coercion(self):
        with pytest.raises(ValidationError, match="valid boolean"):
            FlowStep.model_validate({"name": "done", "terminal": "true"})

    def test_native_integer_field_rejects_string_coercion(self):
        with pytest.raises(ValidationError, match="valid integer"):
            ServiceConfig.model_validate(
                {
                    "transport": "http",
                    "base_url": "https://example.test",
                    "dispatch_timeout_sec": "30",
                    "retries": 0,
                }
            )

    def test_condition_requires_false_path(self):
        with pytest.raises(ValidationError, match="condition requires"):
            FlowStep(name="step", op="op1", condition="input.ok == true")

    def test_non_terminal_requires_successor(self):
        with pytest.raises(ValidationError, match="requires a success successor"):
            FlowStep(name="step", op="op1")

    @pytest.mark.parametrize(
        ("path", "match"),
        [
            pytest.param("steps/*/input/token", "beginning with '/'", id="not-pointer"),
            pytest.param(
                "/steps/*/inpt/token",
                "must target globals, input, output, or message",
                id="unknown-step-field",
            ),
        ],
    )
    def test_redaction_paths_are_strict_json_pointers(self, path: str, match: str) -> None:
        with pytest.raises(ValidationError, match=match):
            RedactedAuditCapture(paths=[path], max_payload_bytes=1)

    def test_redaction_path_rejects_invalid_json_pointer_escape(self) -> None:
        with pytest.raises(ValidationError, match="invalid JSON Pointer escape"):
            RedactedAuditCapture(paths=["/steps/~2/input"], max_payload_bytes=1)
