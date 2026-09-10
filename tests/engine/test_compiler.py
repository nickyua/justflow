"""Tests for the YAML-to-Temporal workflow compiler."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from temporalio.exceptions import ApplicationError

from justflow.config.loader import ConfigLoader
from justflow.config.models import (
    FlowStep,
    ServiceConfig,
    StepDefinition,
    WorkflowConfig,
)
from justflow.config.runtime_limits import RuntimeLimits
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    SHA256_HEX_LENGTH,
    build_definition_manifests,
    workflow_type_name,
)
from justflow.definitions.routing import WorkerDeployment
from justflow.engine import compiler as compiler_module
from justflow.engine.activities import ContractValidationInput
from justflow.engine.audit import FailurePhase
from justflow.engine.compiler import (
    PendingMessages,
    TemporalStepExecutor,
    _application_error,
    compile_workflow,
)
from justflow.engine.continuation import RunnerCheckpoint, WorkflowContinuationInput
from justflow.engine.errors import build_execution_error, correlation_identity
from justflow.engine.runner import CacheDirective, StepInvocation
from justflow.provenance import WorkerArtifactIdentity
from justflow.sdk.message_contract import ResponseStatus, WorkflowTrigger, make_signal_key
from justflow.transports.builtins import builtin_transport_registry
from tests.conftest import PRIME_STATS_CONFIG_DIR

TINY_LIMIT = 1
TEST_ARTIFACT_DIGEST = f"sha256:{'a' * 64}"
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * SHA256_HEX_LENGTH


def _worker_deployment() -> WorkerDeployment:
    return WorkerDeployment(
        artifact_identity=WorkerArtifactIdentity(
            deployment_name="justflow",
            build_id="build-1",
            artifact_digest=TEST_ARTIFACT_DIGEST,
            package_version="0.1.0",
        ),
        compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
    )


@pytest.fixture
def simple_services():
    return builtin_transport_registry().resolve_services(
        {
            "source_api": ServiceConfig(
                transport="http",
                transport_config={"base_url": "https://localhost:8080"},
                connect_timeout_sec=5,
                dispatch_timeout_sec=30,
                retries=2,
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
        }
    )


class TestCompiler:
    def test_compile_produces_workflow_class(self, simple_services):
        wf_config = WorkflowConfig(
            workflow="test_simple",
            steps={
                "fetch": StepDefinition(service="source_api", action="GET:/api/data"),
                "process": StepDefinition(service="processor", action="Process"),
            },
            flow=[
                FlowStep(name="step1", op="fetch", output="data", then="step2"),
                FlowStep(name="step2", op="process", input="step1.data", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

        wf_class = compile_workflow(wf_config, simple_services)

        assert wf_class.__name__.startswith("Workflow_test_simple_")
        assert hasattr(wf_class, "run")
        definition = getattr(wf_class, "__temporal_workflow_definition")
        assert definition.sandboxed is True

    def test_compile_all_example_workflows(self):
        loader = ConfigLoader(PRIME_STATS_CONFIG_DIR)
        _, services_config, workflow_configs = loader.load_all()
        services = builtin_transport_registry().resolve_services(services_config.services)

        for wf_config in workflow_configs.values():
            wf_class = compile_workflow(wf_config, services)
            assert wf_class is not None
            assert "Workflow_" in wf_class.__name__

    def test_compiled_workflow_has_signal_handler(self, simple_services):
        wf_config = WorkflowConfig(
            workflow="with_signal",
            steps={"op1": StepDefinition(service="source_api", action="GET:/api/x")},
            flow=[
                FlowStep(name="s1", op="op1", then="end"),
                FlowStep(name="end", terminal=True),
            ],
        )

        wf_class = compile_workflow(wf_config, simple_services)
        instance = wf_class()
        assert hasattr(instance, "handle_signal")
        assert hasattr(instance, "_signals")

    async def test_versioned_workflow_type_and_audit_record_use_selected_identities(
        self, monkeypatch
    ) -> None:
        workflow_config = WorkflowConfig(
            workflow="versioned",
            steps={},
            flow=[FlowStep(name="done", terminal=True)],
        )
        manifest = build_definition_manifests(
            {"versioned": workflow_config},
            {},
            RuntimeLimits(),
        )["versioned"]
        deployment = _worker_deployment()
        workflow_class = compile_workflow(
            workflow_config,
            {},
            manifest=manifest,
            deployment=deployment,
            environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
        )
        monkeypatch.setattr(
            compiler_module.workflow,
            "info",
            lambda: SimpleNamespace(run_id="run", workflow_id="request"),
        )
        monkeypatch.setattr(compiler_module.workflow, "time", lambda: 0.0)
        monkeypatch.setattr(compiler_module.workflow, "patched", lambda _: True)

        record = await workflow_class().run(
            {
                "request_id": "request",
                "globals": {},
                "definition_digest": manifest.definition_digest,
                "worker_deployment": deployment.name,
                "worker_build_id": deployment.build_id,
                "worker_artifact": deployment.artifact_identity.model_dump(mode="json"),
                "environment_snapshot_digest": ENVIRONMENT_SNAPSHOT_DIGEST,
            }
        )

        definition = getattr(workflow_class, "__temporal_workflow_definition")
        assert definition.name == workflow_type_name("versioned", manifest.definition_digest)
        assert record["definition_digest"] == manifest.definition_digest
        assert record["worker_deployment"] == "justflow"
        assert record["worker_build_id"] == "build-1"
        assert record["worker_artifact"] == deployment.artifact_identity.model_dump(
            mode="json", exclude_none=True
        )
        assert record["environment_snapshot_digest"] == ENVIRONMENT_SNAPSHOT_DIGEST

    @pytest.mark.parametrize(
        "selected_identity",
        [
            pytest.param({}, id="missing"),
            pytest.param(
                {
                    "definition_digest": "0" * 64,
                    "worker_deployment": "justflow",
                    "worker_build_id": "build-1",
                },
                id="mismatched",
            ),
        ],
    )
    async def test_versioned_workflow_rejects_invalid_selected_identity(
        self, monkeypatch, selected_identity: dict[str, object]
    ) -> None:
        workflow_config = WorkflowConfig(
            workflow="versioned_mismatch",
            steps={},
            flow=[FlowStep(name="done", terminal=True)],
        )
        manifest = build_definition_manifests(
            {"versioned_mismatch": workflow_config},
            {},
            RuntimeLimits(),
        )["versioned_mismatch"]
        deployment = _worker_deployment()
        workflow_class = compile_workflow(
            workflow_config,
            {},
            manifest=manifest,
            deployment=deployment,
            environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
        )
        monkeypatch.setattr(
            compiler_module.workflow,
            "info",
            lambda: SimpleNamespace(run_id="run", workflow_id="request"),
        )
        monkeypatch.setattr(compiler_module.workflow, "time", lambda: 0.0)
        monkeypatch.setattr(compiler_module.workflow, "patched", lambda _: True)

        with pytest.raises(ApplicationError) as exc_info:
            await workflow_class().run(
                {
                    "request_id": "request",
                    "globals": {},
                    **selected_identity,
                }
            )

        assert exc_info.value.type == "INVALID_TRIGGER"
        assert exc_info.value.details[0]["correlation"]["definition_digest"] == (
            manifest.definition_digest
        )

    def test_engine_failure_becomes_application_error_with_positional_record(self):
        sensitive_sentinel = "synthetic-failure-secret"
        failure = build_execution_error(
            exc=RuntimeError(sensitive_sentinel),
            correlation=correlation_identity(
                workflow="test_simple",
                request_id="request",
                run_id="run",
            ),
            phase=FailurePhase.STEP,
            audit_version=2,
            started_at="2026-07-31T00:00:00+00:00",
            failed_at="2026-07-31T00:00:01+00:00",
        )

        error = _application_error(failure)

        assert error.type == "STEP_FAILED"
        assert error.non_retryable is True
        assert error.details == (failure.record.dump(),)
        assert sensitive_sentinel not in str(error)


class TestCompiledWorkflowLimits:
    @staticmethod
    def _workflow_config() -> WorkflowConfig:
        return WorkflowConfig(
            workflow="bounded_signals",
            steps={},
            flow=[FlowStep(name="done", terminal=True)],
        )

    @staticmethod
    def _patch_workflow_context(monkeypatch) -> None:
        monkeypatch.setattr(
            compiler_module.workflow,
            "info",
            lambda: SimpleNamespace(run_id="run", workflow_id="request"),
        )
        monkeypatch.setattr(compiler_module.workflow, "time", lambda: 0.0)

    @pytest.mark.parametrize(
        ("handler_name", "payload", "queue_name"),
        [
            pytest.param(
                "handle_signal",
                {
                    "request_id": "request",
                    "step_name": "step",
                    "action": "action",
                    "status": "success",
                },
                "_signals",
                id="step-response",
            ),
            pytest.param(
                "handle_event",
                {"signal": "ready", "data": None},
                "_events",
                id="workflow-event",
            ),
        ],
    )
    async def test_queued_message_limit_rejects_without_appending(
        self,
        monkeypatch,
        handler_name: str,
        payload: dict,
        queue_name: str,
    ) -> None:
        self._patch_workflow_context(monkeypatch)
        workflow_class = compile_workflow(
            self._workflow_config(),
            {},
            limits=RuntimeLimits(queued_messages=TINY_LIMIT),
        )
        instance = workflow_class()
        handler = getattr(instance, handler_name)

        await handler(payload)
        with pytest.raises(ApplicationError) as exc_info:
            await handler(payload)

        assert exc_info.value.type == "LIMIT_EXCEEDED"
        queued = getattr(instance, queue_name)
        assert sum(len(messages) for messages in queued.values()) == TINY_LIMIT

    async def test_signal_payload_limit_rejects_before_queueing(self, monkeypatch) -> None:
        self._patch_workflow_context(monkeypatch)
        workflow_class = compile_workflow(
            self._workflow_config(),
            {},
            limits=RuntimeLimits(signal_payload_bytes=TINY_LIMIT),
        )
        instance = workflow_class()

        with pytest.raises(ApplicationError) as exc_info:
            await instance.handle_event({"signal": "ready", "data": None})

        assert exc_info.value.type == "LIMIT_EXCEEDED"
        assert instance._events == {}

    @pytest.mark.parametrize(
        ("handler_name", "payload"),
        [
            pytest.param(
                "handle_signal",
                {
                    "request_id": "request",
                    "step_name": "step",
                    "action": "action",
                    "status": "success",
                },
                id="step-response",
            ),
            pytest.param(
                "handle_event",
                {"signal": "ready", "data": None},
                id="workflow-event",
            ),
        ],
    )
    async def test_queued_message_byte_limit_rejects_without_appending(
        self,
        monkeypatch,
        handler_name: str,
        payload: dict,
    ) -> None:
        self._patch_workflow_context(monkeypatch)
        workflow_class = compile_workflow(
            self._workflow_config(),
            {},
            limits=RuntimeLimits(queued_message_bytes=TINY_LIMIT),
        )
        instance = workflow_class()

        with pytest.raises(ApplicationError) as exc_info:
            await getattr(instance, handler_name)(payload)

        assert exc_info.value.type == "LIMIT_EXCEEDED"
        assert instance._signals == {}
        assert instance._events == {}

    async def test_queue_limit_is_shared_by_signals_and_events(self, monkeypatch) -> None:
        self._patch_workflow_context(monkeypatch)
        workflow_class = compile_workflow(
            self._workflow_config(),
            {},
            limits=RuntimeLimits(queued_messages=TINY_LIMIT),
        )
        instance = workflow_class()
        await instance.handle_event({"signal": "ready", "data": None})

        with pytest.raises(ApplicationError) as exc_info:
            await instance.handle_signal(
                {
                    "request_id": "request",
                    "step_name": "step",
                    "action": "action",
                    "status": "success",
                }
            )

        assert exc_info.value.type == "LIMIT_EXCEEDED"
        assert instance._signals == {}
        assert len(instance._events["ready"]) == TINY_LIMIT

    @pytest.mark.parametrize(
        ("handler_name", "payload", "queue_name", "queue_key"),
        [
            pytest.param(
                "handle_signal",
                {
                    "request_id": "request",
                    "step_name": "step",
                    "action": "action",
                    "status": "success",
                },
                "_signals",
                make_signal_key("request", "step", "action"),
                id="step-response",
            ),
            pytest.param(
                "handle_event",
                {"signal": "ready", "data": {"value": "available"}},
                "_events",
                "ready",
                id="workflow-event",
            ),
        ],
    )
    async def test_run_preserves_messages_delivered_before_it_starts(
        self,
        monkeypatch,
        handler_name: str,
        payload: dict,
        queue_name: str,
        queue_key: str,
    ) -> None:
        self._patch_workflow_context(monkeypatch)
        monkeypatch.setattr(compiler_module.workflow, "patched", lambda _: True)
        workflow_class = compile_workflow(self._workflow_config(), {})
        instance = workflow_class()

        await getattr(instance, handler_name)(payload)
        await instance.run({"request_id": "request", "globals": {}})

        assert len(getattr(instance, queue_name)[queue_key]) == 1

    async def test_continued_run_orders_carried_events_before_new_events(self, monkeypatch) -> None:
        monkeypatch.setattr(
            compiler_module.workflow,
            "info",
            lambda: SimpleNamespace(
                run_id="continued-run",
                workflow_id="request",
                continued_run_id="previous-run",
            ),
        )
        monkeypatch.setattr(compiler_module.workflow, "time", lambda: 1.0)
        monkeypatch.setattr(compiler_module.workflow, "patched", lambda _: True)
        workflow_class = compile_workflow(self._workflow_config(), {})
        instance = workflow_class()
        await instance.handle_event({"signal": "ready", "data": "new"})
        continuation = WorkflowContinuationInput(
            sequence=1,
            previous_run_id="previous-run",
            trigger=WorkflowTrigger(request_id="request", globals={}),
            checkpoint=RunnerCheckpoint(
                started_at="1970-01-01T00:00:00+00:00",
                params={"request_id": "request"},
                step_outputs={},
                step_timings={},
                transitions=(),
                failures=(),
                next_step_name="done",
                next_seq=0,
                invocations=0,
            ),
            signals={},
            events={"ready": ["carried"]},
            observed_history_events=1,
            observed_history_bytes=1,
            server_suggested=False,
        )

        await instance.run(continuation.model_dump(mode="json"))

        assert instance._events == {"ready": ["carried", "new"]}

    async def test_event_collection_limit_rejects_before_queueing(self, monkeypatch) -> None:
        self._patch_workflow_context(monkeypatch)
        workflow_class = compile_workflow(
            self._workflow_config(),
            {},
            limits=RuntimeLimits(collection_items=TINY_LIMIT),
        )
        instance = workflow_class()

        with pytest.raises(ApplicationError) as exc_info:
            await instance.handle_event({"signal": "ready", "data": list(range(TINY_LIMIT + 1))})

        assert exc_info.value.type == "LIMIT_EXCEEDED"
        assert instance._events == {}


class TestTemporalStepExecutorContracts:
    async def test_step_schemas_are_propagated_to_activity_input(
        self, simple_services, monkeypatch
    ):
        activity_inputs = []

        async def execute_activity(name, *, arg, **kwargs):
            activity_inputs.append((name, arg))
            return {"data": {"ok": True}}

        monkeypatch.setattr(
            "justflow.engine.compiler.workflow.execute_activity",
            execute_activity,
        )
        executor = TemporalStepExecutor(
            pending_messages=PendingMessages(signals={}, events={}),
            request_id="request",
            workflow_id="workflow",
            workflow_run_id="run",
            flow_name="flow",
        )
        input_schema = {"type": "object", "required": ["id"]}
        output_schema = {"type": "object", "required": ["ok"]}

        await executor.run_step(
            StepInvocation(
                step_name="step",
                service_name="source_api",
                action="GET:/api/data",
                input={"id": "R1"},
                globals={},
                input_schema=input_schema,
                output_schema=output_schema,
            ),
            simple_services["source_api"],
        )

        assert activity_inputs[0][0] == "execute_step"
        assert activity_inputs[0][1]["input_schema"] == input_schema
        assert activity_inputs[0][1]["output_schema"] == output_schema

    @pytest.mark.parametrize(
        ("patch_active", "expected_resources"),
        [
            pytest.param(False, None, id="legacy-history"),
            pytest.param(True, ["runtime_config"], id="resource-grants-enabled"),
        ],
    )
    async def test_resource_grants_use_temporal_patch_compatibility(
        self,
        simple_services,
        monkeypatch,
        patch_active: bool,
        expected_resources: list[str] | None,
    ) -> None:
        activity_inputs = []

        async def execute_activity(name, *, arg, **kwargs):
            activity_inputs.append((name, arg))
            return {"data": {"ok": True}}

        monkeypatch.setattr(
            "justflow.engine.compiler.workflow.execute_activity",
            execute_activity,
        )
        monkeypatch.setattr(
            "justflow.engine.compiler.workflow.patched",
            lambda _patch: patch_active,
        )
        executor = TemporalStepExecutor(
            pending_messages=PendingMessages(signals={}, events={}),
            request_id="request",
            workflow_id="workflow",
            workflow_run_id="run",
            flow_name="flow",
        )

        await executor.run_step(
            StepInvocation(
                step_name="step",
                service_name="source_api",
                action="GET:/api/data",
                input={},
                globals={},
                required_resources=("runtime_config",),
            ),
            simple_services["source_api"],
        )

        activity_payload = activity_inputs[0][1]
        assert activity_payload.get("required_resources") == expected_resources

    @pytest.mark.parametrize(
        "patch_active", [False, True], ids=["old-waiting-history", "new-history"]
    )
    async def test_asynchronous_output_uses_contract_activity(
        self, simple_services, monkeypatch, patch_active
    ):
        from justflow.sdk.message_contract import make_step_invocation_id

        monkeypatch.setattr(compiler_module.workflow, "patched", lambda patch: patch_active)
        output_schema = {"type": "object", "required": ["ok"]}
        signal_key = make_signal_key("request", "step", "GET:/api/data")
        response_key = make_signal_key(
            make_step_invocation_id("workflow", "run", "step"), "step", "GET:/api/data"
        )
        activity_calls = []

        async def execute_activity(name, *, arg, **kwargs):
            activity_calls.append((name, arg))
            if name == "execute_step":
                return {
                    "data": None,
                    "awaiting_signal": True,
                    "signal_key": signal_key,
                    "response_timeout_sec": 30,
                    "cache": None,
                }
            return None

        async def wait_condition(predicate, **kwargs):
            assert predicate()

        monkeypatch.setattr(
            "justflow.engine.compiler.workflow.execute_activity",
            execute_activity,
        )
        monkeypatch.setattr(
            "justflow.engine.compiler.workflow.wait_condition",
            wait_condition,
        )
        executor = TemporalStepExecutor(
            pending_messages=PendingMessages(
                signals={
                    response_key: [
                        {
                            "request_id": "request",
                            "step_name": "step",
                            "action": "GET:/api/data",
                            "status": ResponseStatus.SUCCESS.value,
                            "step_response": {"ok": True},
                        }
                    ]
                },
                events={},
            ),
            request_id="request",
            workflow_id="workflow",
            workflow_run_id="run",
            flow_name="flow",
        )

        result = await executor.run_step(
            StepInvocation(
                step_name="step",
                service_name="source_api",
                action="GET:/api/data",
                input=None,
                globals={},
                output_schema=output_schema,
            ),
            simple_services["source_api"],
        )

        assert result.data == {"ok": True}
        assert activity_calls[1] == (
            "validate_contract",
            ContractValidationInput(
                schema=output_schema,
                payload={"ok": True},
                direction="asynchronous output",
                boundary_name="step",
            ),
        )


class TestTemporalStepExecutorDefinitionIdentity:
    async def test_cache_key_is_namespaced_by_definition_and_contract(
        self, simple_services, monkeypatch
    ) -> None:
        activity_inputs = []

        async def execute_activity(name, *, arg, **kwargs):
            activity_inputs.append(arg)
            return {"data": {"ok": True}}

        monkeypatch.setattr(
            "justflow.engine.compiler.workflow.execute_activity",
            execute_activity,
        )
        definition_digest = "a" * 64
        contract_identity = "sha256:contract"
        executor = TemporalStepExecutor(
            pending_messages=PendingMessages(signals={}, events={}),
            request_id="request",
            workflow_id="workflow",
            workflow_run_id="run",
            flow_name="flow",
            definition_digest=definition_digest,
            contract_identities={
                'json-schema:{"type":"object"}': contract_identity,
            },
        )

        await executor.run_step(
            StepInvocation(
                step_name="step",
                service_name="source_api",
                action="GET:/api/data",
                input={},
                globals={},
                cache=CacheDirective(resource="cache", key="customer-key"),
                output_schema={"type": "object"},
            ),
            simple_services["source_api"],
        )

        cache = activity_inputs[0]["cache"]
        assert cache["definition_digest"] == definition_digest
        assert cache["contract_identity"] == contract_identity
        assert cache["key"] == "customer-key"

    async def test_child_start_uses_explicit_definition_identity(self, monkeypatch) -> None:
        calls = []

        async def execute_child_workflow(name, trigger, **kwargs):
            calls.append((name, trigger, kwargs))
            return {"status": "completed"}

        monkeypatch.setattr(
            "justflow.engine.compiler.workflow.execute_child_workflow",
            execute_child_workflow,
        )
        child_digest = "b" * 64
        executor = TemporalStepExecutor(
            pending_messages=PendingMessages(signals={}, events={}),
            request_id="request",
            workflow_id="workflow",
            workflow_run_id="run",
            flow_name="parent",
            worker_deployment="justflow",
            worker_build_id="build-1",
            worker_artifact=_worker_deployment().artifact_identity,
            child_workflow_types={"child": workflow_type_name("child", child_digest)},
            child_definition_digests={"child": child_digest},
            child_environment_snapshot_digests={"child": ENVIRONMENT_SNAPSHOT_DIGEST},
        )

        await executor.run_subworkflow("child", "step", {"input": 1})

        name, trigger, kwargs = calls[0]
        assert name == workflow_type_name("child", child_digest)
        assert trigger["definition_digest"] == child_digest
        assert trigger["worker_artifact"] == _worker_deployment().artifact_identity.model_dump(
            mode="json", exclude_none=True
        )
        assert trigger["environment_snapshot_digest"] == ENVIRONMENT_SNAPSHOT_DIGEST
        assert kwargs["memo"]["justflow.definition_digest"] == child_digest
        assert kwargs["memo"]["justflow.worker_artifact_digest"] == TEST_ARTIFACT_DIGEST
        assert kwargs["memo"]["justflow.environment_snapshot_digest"] == ENVIRONMENT_SNAPSHOT_DIGEST
