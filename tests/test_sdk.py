"""Tests for workflow SDK components."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from justflow.resources.base import ResourceNotFoundError
from justflow.sdk.base_action import ActionContext, BaseAction
from justflow.sdk.message_contract import (
    PROTOCOL_VERSION,
    EventEnvelope,
    MessageKind,
    StepErrorBody,
    StepRequestEnvelope,
    StepRequestPayload,
    StepResponseEnvelope,
    StepSuccessBody,
    TriggerEnvelope,
    make_step_invocation_id,
    make_step_request_message_id,
    make_trigger_message_id,
    make_workflow_id,
    parse_async_envelope,
)
from justflow.sdk.router import Router
from justflow.sdk.service_context import ServiceCallContext


class EchoAction(BaseAction):
    async def echo(self, input: Any) -> Any:
        return {"echoed": input, "globals": self.globals}


class UpperAction(BaseAction):
    async def upper(self, input: Any) -> Any:
        return {"result": input.upper()}


class TestBaseAction:
    @pytest.mark.asyncio
    async def test_action_method(self):
        action = EchoAction(
            globals={"key": "value"},
            context=ActionContext(
                request_id="req-1", flow_name="flow", step_name="step", action="echo"
            ),
        )
        result = await action.echo({"data": 123})
        assert result == {"echoed": {"data": 123}, "globals": {"key": "value"}}

    def test_get_resource(self):
        action = EchoAction(resources={"db": "postgres_conn", "cache": "redis_conn"})
        assert action.get_resource("db") == "postgres_conn"
        assert action.get_resource("cache") == "redis_conn"

    def test_get_resource_missing(self):
        action = EchoAction(resources={"db": "conn"})
        with pytest.raises(ResourceNotFoundError, match="not included in this grant"):
            action.get_resource("cache")


@pytest.mark.parametrize("scope_digest", ["short", "A" * 64])
def test_service_call_context_rejects_invalid_scope_digest(scope_digest: str) -> None:
    with pytest.raises(ValidationError, match="scope_digest"):
        ServiceCallContext(scope_digest=scope_digest)


class TestRouter:
    @pytest.mark.asyncio
    async def test_dispatch(self):
        router = Router()
        router.register("echo", EchoAction)
        router.register("upper", UpperAction)

        result = await router.dispatch("echo", {"hello": "world"}, globals={"g": 1})
        assert result == {"echoed": {"hello": "world"}, "globals": {"g": 1}}

    @pytest.mark.asyncio
    async def test_dispatch_unknown_action(self):
        router = Router()
        with pytest.raises(ValueError, match="Unknown action"):
            await router.dispatch("nonexistent", {})

    @pytest.mark.asyncio
    async def test_dispatch_class_without_matching_method(self):
        router = Router()
        router.register("mislabeled", EchoAction)
        with pytest.raises(ValueError, match="no action method 'mislabeled'"):
            await router.dispatch("mislabeled", {})

    def test_discover_registers_action_methods(self):
        router = Router()
        router.discover("tests.workflow_fixtures.actions")
        assert "fetch_record" in router.registered_actions
        assert "classify_record" in router.registered_actions

    def test_registered_actions(self):
        router = Router()
        router.register("echo", EchoAction)
        router.register("upper", UpperAction)
        assert sorted(router.registered_actions) == ["echo", "upper"]


class TestMessageContract:
    def test_trigger_envelope_has_stable_namespaced_identity(self):
        definition_digest = "a" * 64
        workflow_id = make_workflow_id("test_flow", "abc")
        message_id = make_trigger_message_id("test_flow", definition_digest, "abc")
        msg = TriggerEnvelope(
            protocol_version=PROTOCOL_VERSION,
            message_id=message_id,
            kind=MessageKind.TRIGGER,
            workflow_name="test_flow",
            definition_digest=definition_digest,
            workflow_id=workflow_id,
            correlation_id="abc",
            trace_id="trace-1",
            business_request_id="abc",
            input={"source": "test"},
        )
        data = msg.model_dump()
        assert data["protocol_version"] == "1"
        assert data["kind"] == MessageKind.TRIGGER
        assert "workflow_run_id" not in data

    def test_step_request_envelope(self):
        invocation_id = make_step_invocation_id("workflow-1", "run-1", "step1")
        msg = StepRequestEnvelope(
            protocol_version=PROTOCOL_VERSION,
            message_id=make_step_request_message_id(invocation_id),
            kind=MessageKind.STEP_REQUEST,
            workflow_name="flow",
            definition_digest="a" * 64,
            workflow_id="workflow-1",
            correlation_id="abc",
            trace_id="trace-1",
            workflow_run_id="run-1",
            step_invocation_id=invocation_id,
            step_name="step1",
            action="fetch",
            payload=StepRequestPayload(
                globals={"timeout": 30},
                input={"record_id": "R001"},
            ),
            reply_destination="responses",
        )
        json_str = msg.model_dump_json()
        assert "abc" in json_str
        assert "fetch" in json_str

    @pytest.mark.parametrize(
        ("body", "is_success"),
        [
            pytest.param(StepSuccessBody(output={"data": [1, 2, 3]}), True, id="success"),
            pytest.param(
                StepErrorBody(code="TIMEOUT", message="timed out"),
                False,
                id="error",
            ),
        ],
    )
    def test_step_response_body_is_discriminated(self, body, is_success):
        step_name = "step1"
        action = "fetch"
        invocation_id = make_step_invocation_id("workflow-1", "run-1", step_name)
        request_message_id = make_step_request_message_id(invocation_id)
        msg = StepResponseEnvelope(
            protocol_version=PROTOCOL_VERSION,
            message_id="response-1",
            kind=MessageKind.STEP_RESPONSE,
            workflow_name="flow",
            definition_digest="a" * 64,
            workflow_id="workflow-1",
            correlation_id="abc",
            causation_id=request_message_id,
            trace_id="trace-1",
            workflow_run_id="run-1",
            in_reply_to=request_message_id,
            step_invocation_id=invocation_id,
            step_name=step_name,
            action=action,
            body=body,
        )
        assert (msg.body.status == "success") is is_success

    def test_event_requires_exact_run(self):
        event = EventEnvelope(
            protocol_version=PROTOCOL_VERSION,
            message_id="event-1",
            kind=MessageKind.EVENT,
            workflow_name="flow",
            definition_digest="a" * 64,
            workflow_id="workflow-1",
            correlation_id="abc",
            trace_id="trace-1",
            workflow_run_id="run-1",
            event_name="ready",
            payload={"value": 1},
        )
        assert event.workflow_run_id == "run-1"

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            pytest.param("protocol_version", "2", id="unsupported-version"),
            pytest.param("kind", "unknown", id="unknown-kind"),
            pytest.param("unexpected", True, id="unknown-field"),
        ],
    )
    def test_invalid_envelope_declaration_is_rejected(self, field, value):
        raw = {
            "protocol_version": "1",
            "message_id": "trigger-1",
            "kind": "trigger",
            "workflow_name": "test_flow",
            "definition_digest": "a" * 64,
            "workflow_id": make_workflow_id("test_flow", "abc"),
            "correlation_id": "abc",
            "causation_id": None,
            "trace_id": "trace-1",
            "business_request_id": "abc",
            "input": {},
        }
        raw[field] = value
        with pytest.raises(ValidationError):
            parse_async_envelope(raw)
