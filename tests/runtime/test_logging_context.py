"""Tests for correlation-context logging."""

from __future__ import annotations

import logging
from typing import Any

from justflow.config.models import ServiceConfig
from justflow.engine.activities import ActivityInput, WorkflowActivities
from justflow.sdk.base_action import BaseAction
from justflow.sdk.logging_context import (
    UNSET,
    ContextFilter,
    current_context,
    identity_log_digest,
    logging_context,
)
from justflow.transports.builtins import builtin_transport_registry


def make_record() -> logging.LogRecord:
    return logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )


class TestContextFilter:
    def test_injects_unset_markers_outside_context(self):
        record = make_record()
        assert ContextFilter().filter(record) is True
        assert record.request_id == UNSET
        assert record.flow_name == UNSET
        assert record.step_name == UNSET

    def test_injects_context_fields_inside_context(self):
        record = make_record()
        with logging_context(request_id="r1", flow_name="f1", step_name="s1"):
            ContextFilter().filter(record)
        assert record.request_id == "r1"
        assert record.flow_name == "f1"
        assert record.step_name == "s1"

    def test_context_resets_after_exit(self):
        with logging_context(request_id="r1"):
            assert current_context()["request_id"] == "r1"
        assert current_context() == {}


class ContextEchoAction(BaseAction):
    async def echo_context(self, input: Any) -> Any:
        return current_context()


class TestActivityContext:
    async def test_execute_step_sets_safe_correlation_context(self):
        registry = builtin_transport_registry()
        resolved_services = registry.resolve_services(
            {
                "svc": ServiceConfig(
                    transport="direct",
                    transport_config={"class": f"{__name__}.ContextEchoAction"},
                    dispatch_timeout_sec=5,
                    retries=0,
                )
            }
        )
        activities = WorkflowActivities(
            services=registry.configure_services(resolved_services, resources={})
        )

        result = await activities.execute_step(
            ActivityInput(
                service_name="svc",
                action="echo_context",
                input=None,
                globals={},
                request_id="req-9",
                correlation_id="req-9",
                trace_id="trace-9",
                workflow_id="workflow-9",
                workflow_run_id="run-9",
                flow_name="flow-9",
                definition_digest="a" * 64,
                step_name="step-9",
            )
        )

        assert result.data == {
            "request_id": identity_log_digest("req-9"),
            "flow_name": "flow-9",
            "step_name": "step-9",
        }
