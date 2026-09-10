"""Tests for config validator."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from justflow.config.diagnostics import DiagnosticCategory, DiagnosticSeverity
from justflow.config.models import (
    FlowStep,
    OnResultBranch,
    ResourceConfig,
    ResourcesConfig,
    ServiceConfig,
    ServicesConfig,
    StepDefinition,
    WorkflowConfig,
)
from justflow.config.runtime_limits import RuntimeLimits
from justflow.config.validator import (
    ConfigValidationError,
    ConfigValidator,
    ResourcesNotResolvedError,
    ServicesNotResolvedError,
    ValidationResult,
)
from justflow.engine.contracts import SchemaDeclarationError

RESOURCES = ResourcesConfig(
    resources={
        "s3": ResourceConfig(
            provider="memory_archive",
            config={"retention_policies": {"test": 60, "standard": 60}},
        ),
        "cache": ResourceConfig(provider="memory_cache"),
        "runtime_config": ResourceConfig(provider="static"),
    }
)

SERVICES = ServicesConfig(
    services={
        "source_api": ServiceConfig(
            transport="http",
            transport_config={"base_url": "https://localhost"},
            connect_timeout_sec=5,
            dispatch_timeout_sec=30,
            retries=2,
        ),
        "queue_worker": ServiceConfig(
            transport="queue",
            transport_config={
                "broker": "main",
                "destination": "worker-requests",
                "idempotency": "durable",
            },
            dispatch_timeout_sec=30,
            response_timeout_sec=120,
            retries=1,
        ),
        "processor": ServiceConfig(
            transport="grpc",
            transport_config={
                "address": "localhost:50051",
                "security": {"mode": "insecure_local"},
            },
            connect_timeout_sec=5,
            dispatch_timeout_sec=60,
            retries=2,
        ),
        "checker": ServiceConfig(
            transport="direct",
            transport_config={"class": "tests.workflow_fixtures.actions.fetch_record.FetchRecord"},
            dispatch_timeout_sec=10,
            retries=0,
        ),
    }
)

STEPS = {
    "fetch": StepDefinition(service="source_api", action="GET:/api/data"),
    "process": StepDefinition(service="processor", action="Run"),
}


def _flow(*steps: FlowStep) -> list[FlowStep]:
    return [*steps, FlowStep(name="end", terminal=True)]


def _wf(
    steps: dict[str, StepDefinition] | None = None,
    flow: list[FlowStep] | None = None,
    **kwargs,
) -> WorkflowConfig:
    return WorkflowConfig(
        workflow="case",
        steps=steps if steps is not None else STEPS,
        flow=flow
        if flow is not None
        else _flow(FlowStep(name="s1", op="fetch", output="data", then="end")),
        **kwargs,
    )


@dataclass(frozen=True, kw_only=True)
class ValidationCase:
    id: str
    workflow: WorkflowConfig
    expect_errors: tuple[str, ...] = ()  # substrings that must each appear; empty = valid


CASES = [
    ValidationCase(
        id="valid-linear-with-on-complete",
        workflow=_wf(
            on_complete={
                "resource": "s3",
                "path": "audit/${request_id}.json",
                "retention_policy": "test",
            },
            flow=_flow(
                FlowStep(name="s1", op="fetch", output="data", then="s2"),
                FlowStep(name="s2", op="process", input="s1.data", then="end"),
            ),
        ),
    ),
    ValidationCase(
        id="valid-convergence-and-alias-input",
        workflow=_wf(
            flow=_flow(
                FlowStep(
                    name="s1",
                    op="fetch",
                    output="shared",
                    on_result=[
                        OnResultBranch(when="input.cached == true", then="s2b"),
                        OnResultBranch(default="s2a"),
                    ],
                ),
                FlowStep(name="s2a", op="process", then="s3"),
                FlowStep(name="s2b", op="process", then="s3"),
                FlowStep(name="s3", op="process", input="shared", then="end"),
            ),
        ),
    ),
    ValidationCase(
        id="valid-param-reference-in-input",
        workflow=_wf(
            params={"source_id": "${source_id}"},
            flow=_flow(
                FlowStep(name="s1", op="fetch", output="data", then="s2"),
                FlowStep(
                    name="s2",
                    op="process",
                    input={"data": "s1.data", "source": "source_id"},
                    then="end",
                ),
            ),
        ),
    ),
    ValidationCase(
        id="missing-service",
        workflow=_wf(steps={"fetch": StepDefinition(service="nonexistent", action="do")}),
        expect_errors=("nonexistent",),
    ),
    ValidationCase(
        id="missing-on-complete-resource",
        workflow=_wf(
            on_complete={
                "resource": "missing_resource",
                "path": "x",
                "retention_policy": "test",
            }
        ),
        expect_errors=("missing_resource",),
    ),
    ValidationCase(
        id="invalid-http-action-format",
        workflow=_wf(steps={"fetch": StepDefinition(service="source_api", action="plain_name")}),
        expect_errors=("METHOD:/path",),
    ),
    ValidationCase(
        id="http-style-action-on-direct-service",
        workflow=_wf(steps={"fetch": StepDefinition(service="checker", action="GET:/x")}),
        expect_errors=("method name",),
    ),
    ValidationCase(
        id="http-style-action-on-queue-service",
        workflow=_wf(steps={"fetch": StepDefinition(service="queue_worker", action="GET:/x")}),
        expect_errors=("only valid for HTTP",),
    ),
    ValidationCase(
        id="invalid-identifier-action-on-grpc",
        workflow=_wf(steps={"fetch": StepDefinition(service="processor", action="Run-Now")}),
        expect_errors=("method name",),
    ),
    ValidationCase(
        id="missing-op-reference",
        workflow=_wf(flow=_flow(FlowStep(name="s1", op="ghost_op", then="end"))),
        expect_errors=("ghost_op",),
    ),
    ValidationCase(
        id="bad-then-target",
        workflow=_wf(flow=_flow(FlowStep(name="s1", op="fetch", then="nowhere"))),
        expect_errors=("nowhere",),
    ),
    ValidationCase(
        id="bad-on-result-target",
        workflow=_wf(
            flow=_flow(
                FlowStep(
                    name="s1",
                    op="fetch",
                    on_result=[
                        OnResultBranch(when="input.ok == true", then="nowhere"),
                        OnResultBranch(default="end"),
                    ],
                )
            ),
        ),
        expect_errors=("nowhere",),
    ),
    ValidationCase(
        id="circular-then",
        workflow=_wf(
            flow=[
                FlowStep(name="a", op="fetch", then="b"),
                FlowStep(name="b", op="process", then="a"),
            ],
        ),
        expect_errors=("Circular",),
    ),
    ValidationCase(
        id="circular-via-on-result",
        workflow=_wf(
            flow=_flow(
                FlowStep(
                    name="a",
                    op="fetch",
                    on_result=[
                        OnResultBranch(when="input.retry == true", then="b"),
                        OnResultBranch(default="end"),
                    ],
                ),
                FlowStep(name="b", op="process", then="a"),
            ),
        ),
        expect_errors=("Circular",),
    ),
    ValidationCase(
        id="unknown-input-root",
        workflow=_wf(
            flow=_flow(FlowStep(name="s1", op="fetch", input="ghost.data", then="end")),
        ),
        expect_errors=("'ghost' is not a flow step",),
    ),
    ValidationCase(
        id="unknown-input-root-in-named-inputs",
        workflow=_wf(
            flow=_flow(
                FlowStep(name="s1", op="fetch", output="data", then="s2"),
                FlowStep(
                    name="s2", op="process", input={"a": "s1.data", "b": "ghost.x"}, then="end"
                ),
            ),
        ),
        expect_errors=("'ghost' is not a flow step",),
    ),
    ValidationCase(
        id="for-each-input-rooted-without-input",
        workflow=_wf(
            flow=_flow(
                FlowStep(name="s1", op="fetch", for_each="input.items", as_var="item", then="end")
            ),
        ),
        expect_errors=("has no 'input'",),
    ),
    ValidationCase(
        id="for-each-unknown-root",
        workflow=_wf(
            flow=_flow(
                FlowStep(name="s1", op="fetch", for_each="ghost.items", as_var="item", then="end")
            ),
        ),
        expect_errors=("'ghost' is not a flow step",),
    ),
    ValidationCase(
        id="condition-invalid-expression",
        workflow=_wf(
            flow=_flow(FlowStep(name="s1", op="fetch", condition="input.a + 1 > 2", then="end")),
        ),
        expect_errors=("Invalid condition",),
    ),
    ValidationCase(
        id="condition-unknown-root",
        workflow=_wf(
            flow=_flow(FlowStep(name="s1", op="fetch", condition="ghost.flag == true", then="end")),
        ),
        expect_errors=("'ghost' is not a flow step",),
    ),
    ValidationCase(
        id="on-result-when-unknown-root",
        workflow=_wf(
            flow=_flow(
                FlowStep(
                    name="s1",
                    op="fetch",
                    on_result=[
                        OnResultBranch(when="ghost.status == 'ok'", then="end"),
                        OnResultBranch(default="end"),
                    ],
                )
            ),
        ),
        expect_errors=("'ghost' is not a flow step",),
    ),
    ValidationCase(
        id="valid-wait-and-sleep-steps",
        workflow=_wf(
            flow=_flow(
                FlowStep(name="s1", op="fetch", output="data", then="w"),
                FlowStep(
                    name="w",
                    wait_for={
                        "signal": "evt",
                        "timeout_sec": 60,
                        "timeout_until": "s1.data",
                        "on_timeout": "z",
                    },
                    output="payload",
                    then="z",
                ),
                FlowStep(name="z", sleep_sec=10, then="end"),
            ),
        ),
    ),
    ValidationCase(
        id="wait-on-timeout-bad-target",
        workflow=_wf(
            flow=_flow(
                FlowStep(
                    name="w",
                    wait_for={"signal": "evt", "timeout_sec": 60, "on_timeout": "nowhere"},
                    then="end",
                ),
            ),
        ),
        expect_errors=("on_timeout target 'nowhere'",),
    ),
    ValidationCase(
        id="wait-timeout-until-unknown-root",
        workflow=_wf(
            flow=_flow(
                FlowStep(
                    name="w",
                    wait_for={"signal": "evt", "timeout_until": "ghost.deadline"},
                    then="end",
                ),
            ),
        ),
        expect_errors=("timeout_until reference",),
    ),
    ValidationCase(
        id="circular-via-on-timeout",
        workflow=_wf(
            flow=_flow(
                FlowStep(name="a", op="fetch", then="w"),
                FlowStep(
                    name="w",
                    wait_for={"signal": "evt", "timeout_sec": 5, "on_timeout": "a"},
                    then="end",
                ),
            ),
        ),
        expect_errors=("Circular",),
    ),
    ValidationCase(
        id="valid-field-reference-against-output-schema",
        workflow=_wf(
            steps={
                "fetch": StepDefinition(
                    service="source_api",
                    action="GET:/x",
                    output_schema={"type": "object", "properties": {"record_id": {}, "name": {}}},
                ),
                "process": StepDefinition(service="processor", action="Run"),
            },
            flow=_flow(
                FlowStep(name="s1", op="fetch", output="data", then="s2"),
                FlowStep(name="s2", op="process", input="s1.data.record_id", then="end"),
            ),
        ),
    ),
    ValidationCase(
        id="unknown-field-against-output-schema",
        workflow=_wf(
            steps={
                "fetch": StepDefinition(
                    service="source_api",
                    action="GET:/x",
                    output_schema={"type": "object", "properties": {"record_id": {}, "name": {}}},
                ),
                "process": StepDefinition(service="processor", action="Run"),
            },
            flow=_flow(
                FlowStep(name="s1", op="fetch", output="data", then="s2"),
                FlowStep(name="s2", op="process", input="s1.data.recrod_id", then="end"),
            ),
        ),
        expect_errors=("field 'recrod_id' is not declared",),
    ),
    ValidationCase(
        id="unknown-field-via-single-producer-alias",
        workflow=_wf(
            steps={
                "fetch": StepDefinition(
                    service="source_api",
                    action="GET:/x",
                    output_schema={"type": "object", "properties": {"record_id": {}}},
                ),
                "process": StepDefinition(service="processor", action="Run"),
            },
            flow=_flow(
                FlowStep(name="s1", op="fetch", output="data", then="s2"),
                FlowStep(name="s2", op="process", input="data.ghost_field", then="end"),
            ),
        ),
        expect_errors=("field 'ghost_field' is not declared",),
    ),
    ValidationCase(
        id="valid-on-error-handler-with-error-alias",
        workflow=_wf(
            on_error={"then": "handler"},
            flow=_flow(
                FlowStep(name="s1", op="fetch", then="end"),
                FlowStep(name="handler", op="fetch", input="error", then="end"),
            ),
        ),
    ),
    ValidationCase(
        id="valid-per-step-on-failure-with-error-alias",
        workflow=_wf(
            flow=_flow(
                FlowStep(name="s1", op="fetch", on_failure="handler", then="end"),
                FlowStep(name="handler", op="fetch", input="error", then="end"),
            ),
        ),
    ),
    ValidationCase(
        id="on-failure-bad-target",
        workflow=_wf(
            flow=_flow(FlowStep(name="s1", op="fetch", on_failure="nowhere", then="end")),
        ),
        expect_errors=("on_failure target 'nowhere'",),
    ),
    ValidationCase(
        id="failure-handler-return-is-one-shot",
        workflow=_wf(
            flow=_flow(
                FlowStep(name="a", op="fetch", then="b"),
                FlowStep(name="b", op="fetch", on_failure="a", then="end"),
            ),
        ),
    ),
    ValidationCase(
        id="on-error-bad-target",
        workflow=_wf(on_error={"then": "nowhere"}),
        expect_errors=("on_error target 'nowhere'",),
    ),
    ValidationCase(
        id="cache-resource-not-found",
        workflow=_wf(
            steps={
                "fetch": StepDefinition(
                    service="source_api",
                    action="GET:/x",
                    cache={"resource": "ghost_cache", "key": "k"},
                )
            },
        ),
        expect_errors=("Cache resource 'ghost_cache'",),
    ),
    ValidationCase(
        id="until-bad-on-exhausted-target",
        workflow=_wf(
            flow=_flow(
                FlowStep(
                    name="p",
                    op="fetch",
                    until="input.ok == true",
                    max_iterations=2,
                    on_exhausted="nowhere",
                    then="end",
                ),
            ),
        ),
        expect_errors=("on_exhausted target 'nowhere'",),
    ),
    ValidationCase(
        id="until-unknown-root",
        workflow=_wf(
            flow=_flow(
                FlowStep(
                    name="p",
                    op="fetch",
                    until="ghost.ok == true",
                    max_iterations=2,
                    then="end",
                ),
            ),
        ),
        expect_errors=("'ghost' is not a flow step",),
    ),
    ValidationCase(
        id="output-collides-with-step-name",
        workflow=_wf(
            flow=_flow(
                FlowStep(name="s1", op="fetch", output="s2", then="s2"),
                FlowStep(name="s2", op="process", then="end"),
            ),
        ),
        expect_errors=("collides with flow step",),
    ),
]


def _sub_wf(name: str, child: str | None = None) -> WorkflowConfig:
    steps = (
        {"call": StepDefinition(workflow=child)}
        if child
        else {"call": StepDefinition(service="source_api", action="GET:/x")}
    )
    return WorkflowConfig(
        workflow=name,
        steps=steps,
        flow=[
            FlowStep(name="s1", op="call", output="out", then="end"),
            FlowStep(name="end", terminal=True),
        ],
        result="s1.out",
    )


class TestSubWorkflowValidation:
    def _validate(self, workflows: dict[str, WorkflowConfig]):
        return ConfigValidator(RESOURCES, SERVICES, workflows, check_imports=False).validate()

    def test_valid_composition_with_result(self):
        workflows = {"parent": _sub_wf("parent", child="leaf"), "leaf": _sub_wf("leaf")}
        result = self._validate(workflows)
        assert result.is_valid, [str(e) for e in result.errors]

    def test_unknown_subworkflow_reference(self):
        result = self._validate({"parent": _sub_wf("parent", child="ghost_flow")})
        assert any("'ghost_flow' not found" in e.message for e in result.errors)

    def test_composition_cycle_detected(self):
        workflows = {
            "a": _sub_wf("a", child="b"),
            "b": _sub_wf("b", child="a"),
        }
        result = self._validate(workflows)
        assert any("composition cycle" in e.message for e in result.errors)

    def test_result_unknown_root(self):
        wf = _sub_wf("solo")
        wf = WorkflowConfig.model_validate(
            {**wf.model_dump(mode="json", by_alias=True), "result": "ghost.thing"}
        )
        result = self._validate({"solo": wf})
        assert any("result reference" in e.message for e in result.errors)


class TestConfigValidator:
    @pytest.mark.parametrize("case", CASES, ids=lambda c: c.id)
    def test_validation(self, case: ValidationCase):
        validator = ConfigValidator(
            RESOURCES, SERVICES, {case.workflow.workflow: case.workflow}, check_imports=False
        )

        result = validator.validate()

        if not case.expect_errors:
            assert result.is_valid, [str(e) for e in result.errors]
        else:
            assert not result.is_valid
            for expected in case.expect_errors:
                assert any(expected in e.message for e in result.errors), (
                    f"expected '{expected}' in {[str(e) for e in result.errors]}"
                )

    def test_services_are_unavailable_before_validation(self) -> None:
        validator = ConfigValidator(RESOURCES, SERVICES, {}, check_imports=False)

        with pytest.raises(
            ServicesNotResolvedError,
            match="Services are available only after configuration validation",
        ):
            _ = validator.resolved_services

    def test_resources_are_unavailable_before_validation(self) -> None:
        validator = ConfigValidator(RESOURCES, SERVICES, {}, check_imports=False)

        with pytest.raises(
            ResourcesNotResolvedError,
            match="Resources are available only after configuration validation",
        ):
            _ = validator.resolved_resources

    def test_repeated_validation_rebuilds_all_derived_state(self) -> None:
        workflow = _wf(input_schema={"type": "object"})
        validator = ConfigValidator(
            RESOURCES,
            SERVICES,
            {workflow.workflow: workflow},
            check_imports=False,
        )

        first_result = validator.validate()

        assert first_result.is_valid
        assert validator.contract_identities
        assert validator.resolved_resources
        assert validator.resolved_services

        validator.resources = ResourcesConfig(resources={})
        validator.services = ServicesConfig(services={})
        validator.workflows[workflow.workflow] = _wf(
            steps={},
            flow=[FlowStep(name="end", terminal=True)],
        )

        second_result = validator.validate()

        assert second_result.is_valid
        assert validator.contract_identities == {}
        assert validator.resolved_resources == {}
        assert validator.resolved_services == {}

    def test_multiphase_diagnostics_preserve_the_public_contract(self) -> None:
        workflow = _wf(
            steps={
                "missing": StepDefinition(service="absent", action="run"),
                "unused": StepDefinition(service="source_api", action="GET:/unused"),
            },
            flow=[
                FlowStep(
                    name="start",
                    op="missing",
                    output="request_id",
                    then="missing_target",
                ),
                FlowStep(name="end", terminal=True),
            ],
            on_complete={
                "resource": "missing_archive",
                "path": "audit/${request_id}.json",
                "retention_policy": "test",
            },
        )

        result = ConfigValidator(
            RESOURCES,
            SERVICES,
            {workflow.workflow: workflow},
            check_imports=False,
        ).validate()

        assert [
            (
                diagnostic.source_file,
                diagnostic.location,
                diagnostic.category,
                diagnostic.severity,
                diagnostic.message,
            )
            for diagnostic in result.diagnostics
        ] == [
            (
                "workflows/case.yaml",
                ("workflows", "case", "flow", "end"),
                DiagnosticCategory.SEMANTIC,
                DiagnosticSeverity.ERROR,
                "Flow step 'end' is unreachable from entry step 'start' and every failure handler",
            ),
            (
                "workflows/case.yaml",
                ("workflows", "case", "flow", "start"),
                DiagnosticCategory.SEMANTIC,
                DiagnosticSeverity.ERROR,
                "'then' target 'missing_target' not found in flow",
            ),
            (
                "workflows/case.yaml",
                ("workflows", "case", "flow", "start"),
                DiagnosticCategory.SEMANTIC,
                DiagnosticSeverity.ERROR,
                (
                    "Output alias 'request_id' collides with a reserved root, workflow parameter, "
                    "or flow step"
                ),
            ),
            (
                "workflows/case.yaml",
                ("workflows", "case", "flow", "start"),
                DiagnosticCategory.SEMANTIC,
                DiagnosticSeverity.ERROR,
                "Reachable path from step 'start' has no explicit terminal successor",
            ),
            (
                "workflows/case.yaml",
                ("workflows", "case", "on_complete"),
                DiagnosticCategory.SEMANTIC,
                DiagnosticSeverity.ERROR,
                "Resource 'missing_archive' not found in resources.yaml",
            ),
            (
                "workflows/case.yaml",
                ("workflows", "case", "steps", "missing"),
                DiagnosticCategory.SEMANTIC,
                DiagnosticSeverity.ERROR,
                "Service 'absent' not found in services.yaml",
            ),
            (
                "workflows/case.yaml",
                ("workflows", "case", "steps", "unused"),
                DiagnosticCategory.LINT,
                DiagnosticSeverity.WARNING,
                "Operation definition 'unused' is not used by the workflow flow",
            ),
        ]
        assert all(diagnostic.cause is None for diagnostic in result.diagnostics)


@dataclass(frozen=True, kw_only=True)
class ValidationReturns:
    value: None = None


@dataclass(frozen=True, kw_only=True)
class ValidationRaises:
    exc: type[BaseException]
    match: str


ValidationOutcome = ValidationReturns | ValidationRaises


@dataclass(frozen=True, kw_only=True)
class LoopCacheValidationCase:
    id: str
    loop_kind: str
    parallel: bool
    cached: bool
    outcome: ValidationOutcome


LOOP_CACHE_VALIDATION_CASES = [
    LoopCacheValidationCase(
        id="cached-sequential-for-each",
        loop_kind="for_each",
        parallel=False,
        cached=True,
        outcome=ValidationRaises(
            exc=ConfigValidationError,
            match="operation 'work'.*for_each.*resource='cache'.*key='fixed'",
        ),
    ),
    LoopCacheValidationCase(
        id="cached-parallel-for-each",
        loop_kind="for_each",
        parallel=True,
        cached=True,
        outcome=ValidationRaises(
            exc=ConfigValidationError,
            match="operation 'work'.*for_each.*resource='cache'.*key='fixed'",
        ),
    ),
    LoopCacheValidationCase(
        id="cached-until",
        loop_kind="until",
        parallel=False,
        cached=True,
        outcome=ValidationRaises(
            exc=ConfigValidationError,
            match="operation 'work'.*until.*resource='cache'.*key='fixed'",
        ),
    ),
    LoopCacheValidationCase(
        id="uncached-until",
        loop_kind="until",
        parallel=False,
        cached=False,
        outcome=ValidationReturns(),
    ),
]


class TestLoopCacheValidation:
    @pytest.mark.parametrize(
        "case",
        LOOP_CACHE_VALIDATION_CASES,
        ids=lambda case: case.id,
    )
    def test_loop_cache_declaration(self, case: LoopCacheValidationCase):
        cache = {"resource": "cache", "key": "fixed"} if case.cached else None
        if case.loop_kind == "for_each":
            loop_step = FlowStep(
                name="loop",
                op="work",
                for_each="items",
                parallel=case.parallel,
                max_concurrency=2 if case.parallel else None,
                then="end",
            )
        else:
            loop_step = FlowStep(
                name="loop",
                op="work",
                until="input.ready == true",
                max_iterations=2,
                then="end",
            )
        workflow = _wf(
            steps={
                "work": StepDefinition(
                    service="source_api",
                    action="GET:/work",
                    cache=cache,
                )
            },
            flow=_flow(loop_step),
            params={"items": "${items}"},
        )
        result = ConfigValidator(
            RESOURCES,
            SERVICES,
            {workflow.workflow: workflow},
            check_imports=False,
        ).validate()

        if isinstance(case.outcome, ValidationRaises):
            with pytest.raises(case.outcome.exc, match=case.outcome.match):
                result.raise_if_invalid()
        else:
            result.raise_if_invalid()


@dataclass(frozen=True, kw_only=True)
class AliasCollisionCase:
    id: str
    workflow: WorkflowConfig
    expected_error: str


ALIAS_COLLISION_CASES = [
    AliasCollisionCase(
        id="another-producer",
        workflow=_wf(
            flow=_flow(
                FlowStep(name="s1", op="fetch", output="shared", then="s2"),
                FlowStep(name="s2", op="process", output="shared", then="end"),
            )
        ),
        expected_error="Output alias 'shared' has more than one producer",
    ),
    AliasCollisionCase(
        id="reserved-root",
        workflow=_wf(
            flow=_flow(
                FlowStep(name="s1", op="fetch", output="error", then="end"),
            )
        ),
        expected_error="Output alias 'error' collides",
    ),
    AliasCollisionCase(
        id="workflow-parameter",
        workflow=_wf(
            params={"source_id": "${source_id}"},
            flow=_flow(
                FlowStep(name="s1", op="fetch", output="source_id", then="end"),
            ),
        ),
        expected_error="Output alias 'source_id' collides",
    ),
]


class TestNamespaceValidation:
    @pytest.mark.parametrize(
        "case",
        ALIAS_COLLISION_CASES,
        ids=lambda case: case.id,
    )
    def test_output_alias_collision(self, case: AliasCollisionCase):
        result = ConfigValidator(
            RESOURCES,
            SERVICES,
            {case.workflow.workflow: case.workflow},
            check_imports=False,
        ).validate()

        assert any(case.expected_error in error.message for error in result.errors)

    def test_duplicate_flow_step_names_are_rejected(self):
        workflow = _wf(
            flow=[
                FlowStep(name="duplicate", op="fetch", then="done"),
                FlowStep(name="duplicate", op="process", then="done"),
                FlowStep(name="done", terminal=True),
            ]
        )

        result = ConfigValidator(
            RESOURCES,
            SERVICES,
            {workflow.workflow: workflow},
            check_imports=False,
        ).validate()

        assert any("Duplicate flow step names" in error.message for error in result.errors)


class TestContractDeclarationValidation:
    def test_pydantic_model_path_is_resolved_at_startup(self):
        workflow = _wf(input_schema="no.such.Model")

        result = ConfigValidator(
            RESOURCES,
            SERVICES,
            {workflow.workflow: workflow},
            check_imports=True,
        ).validate()

        assert any("Cannot import schema" in error.message for error in result.errors)

    def test_inline_remote_reference_is_rejected_at_startup(self):
        workflow = _wf(input_schema={"$ref": "https://schemas.example/trigger.json"})

        result = ConfigValidator(
            RESOURCES,
            SERVICES,
            {workflow.workflow: workflow},
            check_imports=False,
        ).validate()

        assert any("Remote JSON Schema references" in error.message for error in result.errors)

    def test_valid_contract_identity_is_available_after_startup_validation(self):
        workflow = _wf(input_schema={"type": "object"})
        validator = ConfigValidator(
            RESOURCES,
            SERVICES,
            {workflow.workflow: workflow},
            check_imports=False,
        )

        result = validator.validate()

        assert result.is_valid
        identity = validator.contract_identities["workflows.case.input_schema"]
        assert identity.startswith("sha256:")


@dataclass(frozen=True, kw_only=True)
class RuntimeBoundCase:
    id: str
    workflow: WorkflowConfig
    limits: RuntimeLimits
    expected_error: str | None


RUNTIME_BOUND_CASES = [
    RuntimeBoundCase(
        id="iteration-over-limit",
        workflow=_wf(
            flow=_flow(
                FlowStep(
                    name="loop",
                    op="fetch",
                    until="input.ready == true",
                    max_iterations=3,
                    then="end",
                )
            )
        ),
        limits=RuntimeLimits(loop_attempts=2),
        expected_error="Declared iteration bound 3",
    ),
    RuntimeBoundCase(
        id="concurrency-over-limit",
        workflow=_wf(
            params={"items": "${items}"},
            flow=_flow(
                FlowStep(
                    name="loop",
                    op="fetch",
                    for_each="items",
                    parallel=True,
                    max_concurrency=3,
                    then="end",
                )
            ),
        ),
        limits=RuntimeLimits(parallelism=2),
        expected_error="Declared concurrency bound 3",
    ),
    RuntimeBoundCase(
        id="audit-over-limit",
        workflow=_wf(
            params={"tenant_id": "${tenant_id}"},
            on_complete={
                "resource": "s3",
                "path": "audit/${request_id}.json",
                "retention_policy": "standard",
                "capture": {
                    "mode": "redacted",
                    "paths": ["/params/tenant_id"],
                    "max_payload_bytes": 3,
                },
            },
        ),
        limits=RuntimeLimits(audit_record_bytes=2),
        expected_error="Declared audit payload bound 3",
    ),
    RuntimeBoundCase(
        id="equal-limit",
        workflow=_wf(
            flow=_flow(
                FlowStep(
                    name="loop",
                    op="fetch",
                    until="input.ready == true",
                    max_iterations=2,
                    then="end",
                )
            )
        ),
        limits=RuntimeLimits(loop_attempts=2),
        expected_error=None,
    ),
]


@pytest.mark.parametrize("case", RUNTIME_BOUND_CASES, ids=lambda case: case.id)
def test_declarations_respect_effective_runtime_limits(case: RuntimeBoundCase) -> None:
    result = ConfigValidator(
        RESOURCES,
        SERVICES,
        {case.workflow.workflow: case.workflow},
        check_imports=False,
        limits=case.limits,
    ).validate()

    limit_errors = [
        diagnostic
        for diagnostic in result.errors
        if diagnostic.category is DiagnosticCategory.LIMIT
    ]
    if case.expected_error is None:
        assert not limit_errors
    else:
        assert any(case.expected_error in diagnostic.message for diagnostic in limit_errors)


NESTED_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "payload": {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                    },
                },
                "count": {"type": "integer"},
                "deadline": {"type": "object", "properties": {}},
            },
        }
    },
}


@dataclass(frozen=True, kw_only=True)
class StaticSchemaCase:
    id: str
    workflow: WorkflowConfig
    expected_error: str | None


STATIC_SCHEMA_CASES = [
    StaticSchemaCase(
        id="nested-input-valid",
        workflow=_wf(
            steps={
                "fetch": StepDefinition(
                    service="source_api",
                    action="GET:/x",
                    output_schema=NESTED_OUTPUT_SCHEMA,
                ),
                "process": StepDefinition(service="processor", action="Run"),
            },
            flow=_flow(
                FlowStep(name="produce", op="fetch", output="data", then="consume"),
                FlowStep(
                    name="consume",
                    op="process",
                    input="produce.data.payload.items.0.id",
                    then="end",
                ),
            ),
        ),
        expected_error=None,
    ),
    StaticSchemaCase(
        id="nested-input-missing",
        workflow=_wf(
            steps={
                "fetch": StepDefinition(
                    service="source_api",
                    action="GET:/x",
                    output_schema=NESTED_OUTPUT_SCHEMA,
                ),
                "process": StepDefinition(service="processor", action="Run"),
            },
            flow=_flow(
                FlowStep(name="produce", op="fetch", output="data", then="consume"),
                FlowStep(
                    name="consume",
                    op="process",
                    input="data.payload.items.0.missing",
                    then="end",
                ),
            ),
        ),
        expected_error="field 'missing' is not declared",
    ),
    StaticSchemaCase(
        id="scalar-for-each",
        workflow=_wf(
            steps={
                "fetch": StepDefinition(
                    service="source_api",
                    action="GET:/x",
                    output_schema=NESTED_OUTPUT_SCHEMA,
                ),
                "process": StepDefinition(service="processor", action="Run"),
            },
            flow=_flow(
                FlowStep(name="produce", op="fetch", output="data", then="consume"),
                FlowStep(
                    name="consume",
                    op="process",
                    for_each="data.payload.count",
                    then="end",
                ),
            ),
        ),
        expected_error="schema-known integer, expected an array or object",
    ),
    StaticSchemaCase(
        id="object-deadline",
        workflow=_wf(
            steps={
                "fetch": StepDefinition(
                    service="source_api",
                    action="GET:/x",
                    output_schema=NESTED_OUTPUT_SCHEMA,
                )
            },
            flow=_flow(
                FlowStep(name="produce", op="fetch", output="data", then="wait"),
                FlowStep(
                    name="wait",
                    wait_for={"signal": "ready", "timeout_until": "data.payload.deadline"},
                    then="end",
                ),
            ),
        ),
        expected_error="schema-known object, expected an ISO-8601 string or epoch number",
    ),
    StaticSchemaCase(
        id="condition-missing-field",
        workflow=_wf(
            steps={
                "fetch": StepDefinition(service="source_api", action="GET:/x"),
                "process": StepDefinition(
                    service="processor",
                    action="Run",
                    input_schema=NESTED_OUTPUT_SCHEMA,
                ),
            },
            flow=_flow(
                FlowStep(name="produce", op="fetch", output="data", then="consume"),
                FlowStep(
                    name="consume",
                    op="process",
                    input="data",
                    condition="input.payload.missing == true",
                    then="end",
                ),
            ),
        ),
        expected_error="field 'missing' is not declared",
    ),
    StaticSchemaCase(
        id="result-missing-field",
        workflow=_wf(
            result="data.payload.missing",
            steps={
                "fetch": StepDefinition(
                    service="source_api",
                    action="GET:/x",
                    output_schema=NESTED_OUTPUT_SCHEMA,
                )
            },
            flow=_flow(FlowStep(name="produce", op="fetch", output="data", then="end")),
        ),
        expected_error="field 'missing' is not declared",
    ),
    StaticSchemaCase(
        id="result-branch-missing-field",
        workflow=_wf(
            steps={
                "fetch": StepDefinition(
                    service="source_api",
                    action="GET:/x",
                    output_schema=NESTED_OUTPUT_SCHEMA,
                )
            },
            flow=_flow(
                FlowStep(
                    name="produce",
                    op="fetch",
                    on_result=[
                        OnResultBranch(
                            when="input.payload.missing == true",
                            then="end",
                        ),
                        OnResultBranch(default="end"),
                    ],
                )
            ),
        ),
        expected_error="field 'missing' is not declared",
    ),
    StaticSchemaCase(
        id="until-missing-field",
        workflow=_wf(
            steps={
                "fetch": StepDefinition(
                    service="source_api",
                    action="GET:/x",
                    output_schema=NESTED_OUTPUT_SCHEMA,
                )
            },
            flow=_flow(
                FlowStep(
                    name="produce",
                    op="fetch",
                    until="input.payload.missing == true",
                    max_iterations=2,
                    then="end",
                )
            ),
        ),
        expected_error="field 'missing' is not declared",
    ),
]


@pytest.mark.parametrize("case", STATIC_SCHEMA_CASES, ids=lambda case: case.id)
def test_nested_schema_semantics(case: StaticSchemaCase) -> None:
    result = ConfigValidator(
        RESOURCES,
        SERVICES,
        {case.workflow.workflow: case.workflow},
        check_imports=False,
    ).validate()

    reference_errors = [
        diagnostic.message
        for diagnostic in result.errors
        if diagnostic.category in {DiagnosticCategory.DECLARATION, DiagnosticCategory.REFERENCE}
    ]
    if case.expected_error is None:
        assert not reference_errors
    else:
        assert any(case.expected_error in message for message in reference_errors)


def test_duplicate_result_conditions_and_resource_grants_are_rejected() -> None:
    evaluator = {
        "evaluator": "tests.workflow_fixtures.evaluators.has_allowed_item",
        "resources": ["s3", "s3"],
    }
    workflow = _wf(
        steps={
            "fetch": StepDefinition(
                service="source_api",
                action="GET:/x",
                required_resources=["s3", "s3"],
            )
        },
        flow=_flow(
            FlowStep(
                name="start",
                op="fetch",
                on_result=[
                    OnResultBranch(when=evaluator, then="end"),
                    OnResultBranch(when=evaluator, then="end"),
                    OnResultBranch(default="end"),
                ],
            )
        ),
    )

    result = ConfigValidator(
        RESOURCES,
        SERVICES,
        {workflow.workflow: workflow},
        check_imports=False,
    ).validate()

    messages = [diagnostic.message for diagnostic in result.errors]
    assert any("Duplicate resource grants" in message for message in messages)
    assert any("Duplicate evaluator resource grants" in message for message in messages)
    assert any("Duplicate result conditions" in message for message in messages)


def test_archive_reference_requires_archive_capability() -> None:
    resources = ResourcesConfig(resources={"archive": ResourceConfig(provider="memory_cache")})
    workflow = _wf(
        on_complete={
            "resource": "archive",
            "path": "audit/${request_id}.json",
            "retention_policy": "standard",
        }
    )

    result = ConfigValidator(
        resources,
        SERVICES,
        {workflow.workflow: workflow},
        check_imports=False,
    ).validate()

    assert any(
        "does not provide the 'archive' capability" in error.message for error in result.errors
    )


def test_cache_reference_requires_cache_capability() -> None:
    resources = ResourcesConfig(
        resources={
            "cache": ResourceConfig(
                provider="memory_archive",
                config={"retention_policies": {"standard": 60}},
            )
        }
    )
    workflow = _wf(
        steps={
            "fetch": StepDefinition(
                service="source_api",
                action="GET:/x",
                cache={"resource": "cache", "key": "cache-key"},
            )
        }
    )

    result = ConfigValidator(
        resources,
        SERVICES,
        {workflow.workflow: workflow},
        check_imports=False,
    ).validate()

    assert any(
        "does not provide the 'cache' capability" in error.message for error in result.errors
    )


def _postgres_with_secret_reference(secret_resource: str) -> ResourceConfig:
    return ResourceConfig(
        provider="postgresql",
        config={
            "connection": {
                "endpoint": {
                    "host": "database.local",
                    "database": "application",
                },
                "credentials": {
                    "secret_resource": secret_resource,
                    "secret_alias": "database_credentials",
                },
            }
        },
    )


def test_resource_dependency_is_validated_before_startup() -> None:
    resources = ResourcesConfig(
        resources={"database": _postgres_with_secret_reference("platform_secrets")}
    )

    result = ConfigValidator(
        resources,
        ServicesConfig(services={}),
        {},
        check_imports=False,
    ).validate()

    assert any(
        "requires missing resource 'platform_secrets'" in error.message for error in result.errors
    )


def test_declared_secret_alias_satisfies_resource_dependency() -> None:
    resources = ResourcesConfig(
        resources={
            "database": _postgres_with_secret_reference("platform_secrets"),
            "platform_secrets": ResourceConfig(
                provider="aws_secrets_manager",
                config={
                    "secrets": {
                        "database_credentials": {
                            "secret_id": "configured-secret-id",
                        }
                    }
                },
            ),
        }
    )

    result = ConfigValidator(
        resources,
        ServicesConfig(services={}),
        {},
        check_imports=False,
    ).validate()

    assert not result.errors


@dataclass(frozen=True, kw_only=True)
class EvaluatorContractCase:
    id: str
    evaluator: str
    expected_messages: tuple[str, ...]


EVALUATOR_CONTRACT_CASES = [
    EvaluatorContractCase(
        id="valid",
        evaluator="tests.workflow_fixtures.evaluators.has_allowed_item",
        expected_messages=(),
    ),
    EvaluatorContractCase(
        id="not-callable",
        evaluator="tests.workflow_fixtures.evaluators.NOT_CALLABLE",
        expected_messages=(
            "Evaluator 'tests.workflow_fixtures.evaluators.NOT_CALLABLE' is not callable",
        ),
    ),
    EvaluatorContractCase(
        id="missing-resources",
        evaluator="tests.workflow_fixtures.evaluators.missing_resources",
        expected_messages=(
            (
                "Evaluator 'tests.workflow_fixtures.evaluators.missing_resources' must accept one "
                "data argument and the 'resources' keyword argument"
            ),
        ),
    ),
    EvaluatorContractCase(
        id="extra-required",
        evaluator="tests.workflow_fixtures.evaluators.extra_required_argument",
        expected_messages=(
            (
                "Evaluator 'tests.workflow_fixtures.evaluators.extra_required_argument' must "
                "accept one data argument and the 'resources' keyword argument"
            ),
        ),
    ),
]


@pytest.mark.parametrize(
    "case",
    EVALUATOR_CONTRACT_CASES,
    ids=lambda case: case.id,
)
def test_evaluator_callable_contract(case: EvaluatorContractCase) -> None:
    workflow = _wf(
        flow=_flow(
            FlowStep(
                name="start",
                op="fetch",
                on_result=[
                    OnResultBranch(
                        when={"evaluator": case.evaluator, "resources": []},
                        then="end",
                    ),
                    OnResultBranch(default="end"),
                ],
            )
        )
    )

    result = ConfigValidator(
        RESOURCES,
        SERVICES,
        {workflow.workflow: workflow},
        check_imports=True,
    ).validate()
    evaluator_errors = [
        diagnostic.message
        for diagnostic in result.errors
        if diagnostic.category is DiagnosticCategory.IMPORT and "Evaluator" in diagnostic.message
    ]

    assert tuple(evaluator_errors) == case.expected_messages


def test_unused_operations_are_sorted_nonfatal_lint_diagnostics() -> None:
    workflow = _wf(
        steps={
            "unused_z": StepDefinition(service="source_api", action="GET:/z"),
            "fetch": StepDefinition(service="source_api", action="GET:/x"),
            "unused_a": StepDefinition(service="source_api", action="GET:/a"),
        }
    )

    result = ConfigValidator(
        RESOURCES,
        SERVICES,
        {workflow.workflow: workflow},
        check_imports=False,
    ).validate()

    assert result.is_valid
    assert [diagnostic.location[-1] for diagnostic in result.warnings] == [
        "unused_a",
        "unused_z",
    ]
    assert all(
        diagnostic.category is DiagnosticCategory.LINT
        and diagnostic.severity is DiagnosticSeverity.WARNING
        for diagnostic in result.warnings
    )


def test_diagnostics_include_source_location_category_and_cause(tmp_path) -> None:
    source = tmp_path / "workflows" / "custom-name.yaml"
    workflow = _wf(input_schema={"type": "invalid"})

    result = ConfigValidator(
        RESOURCES,
        SERVICES,
        {workflow.workflow: workflow},
        check_imports=False,
        config_dir=tmp_path,
        workflow_sources={workflow.workflow: source},
    ).validate()
    diagnostic = next(
        item for item in result.errors if item.category is DiagnosticCategory.DECLARATION
    )

    assert diagnostic.source_file == str(source)
    assert diagnostic.location == ("workflows", "case", "input_schema")
    assert isinstance(diagnostic.cause, SchemaDeclarationError)
    assert diagnostic.as_dict() == {
        "category": "declaration",
        "location": ["workflows", "case", "input_schema"],
        "message": diagnostic.message,
        "severity": "error",
        "source_file": str(source),
    }


@dataclass(frozen=True, kw_only=True)
class PlaceholderGlobalsCase:
    id: str
    params: dict[str, str]
    step_params: dict[str, str]
    expected_messages: tuple[str, ...]


PLACEHOLDER_GLOBAL_CASES = [
    PlaceholderGlobalsCase(
        id="declared-trigger-global",
        params={"tenant_id": "${tenant_id}"},
        step_params={"value": "${tenant_id}"},
        expected_messages=(),
    ),
    PlaceholderGlobalsCase(
        id="engine-request-id",
        params={},
        step_params={"value": "${request_id}"},
        expected_messages=(),
    ),
    PlaceholderGlobalsCase(
        id="undeclared-global",
        params={},
        step_params={"value": "${tenant_id}"},
        expected_messages=("Placeholders require undeclared trigger globals: ['tenant_id']",),
    ),
]


@pytest.mark.parametrize(
    "case",
    PLACEHOLDER_GLOBAL_CASES,
    ids=lambda case: case.id,
)
def test_placeholder_globals_are_explicit_without_requiring_publication_values(
    case: PlaceholderGlobalsCase,
) -> None:
    workflow = _wf(
        params=case.params,
        flow=_flow(FlowStep(name="start", op="fetch", params=case.step_params, then="end")),
    )

    result = ConfigValidator(
        RESOURCES,
        SERVICES,
        {workflow.workflow: workflow},
        check_imports=False,
    ).validate()
    placeholder_errors = [
        diagnostic.message for diagnostic in result.errors if "Placeholder" in diagnostic.message
    ]

    assert tuple(placeholder_errors) == case.expected_messages


def test_diagnostic_bound_fails_closed() -> None:
    result = ValidationResult()
    for index in range(1_001):
        result.add(
            f"workflows.example.steps.unused_{index}",
            "Unused declaration",
            category=DiagnosticCategory.LINT,
            severity=DiagnosticSeverity.WARNING,
        )

    assert len(result.diagnostics) == 1_000
    assert not result.is_valid
    assert result.errors[0].location == ("diagnostics",)
    assert "omitted" in result.errors[0].message
