"""Tests for the typed Temporal control-operation boundary."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.api.enums.v1 import PendingActivityState, PendingWorkflowTaskState
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.workflowservice.v1 import DescribeWorkflowExecutionResponse
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode

from justflow.definitions.manifest import SHA256_HEX_LENGTH
from justflow.definitions.routing import (
    MEMO_CONFIGURATION_RESOLUTION_DIGEST,
    MEMO_CONFIGURATION_REVISION,
)
from justflow.provenance import ExecutionConfigurationIdentity
from justflow.runtime.operations import (
    ControlErrorCode,
    ControlOperationError,
    WorkflowControlService,
    WorkflowExecutionState,
    WorkflowListQuery,
)
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope, decode_scope_cursor, scoped_identity
from justflow.sdk.message_contract import WORKFLOW_EVENT_SIGNAL

MAX_PAYLOAD_BYTES = 1_024
PENDING_WAIT_INPUT_COUNT = 21
MAX_EXPECTED_PENDING_WAITS = 20
SENSITIVE_SENTINEL = "synthetic-temporal-secret"
DATA_CONVERTER = DataConverter.default
SCOPE_A = RuntimeScope.create(
    tenant="tenant-a",
    application="orders",
    environment="production",
)
SCOPE_B = RuntimeScope.create(
    tenant="tenant-b",
    application="orders",
    environment="production",
)
EXECUTION_CONFIGURATION = ExecutionConfigurationIdentity(
    configuration_revision_id="c" * SHA256_HEX_LENGTH,
    resolution_digest=f"sha256:{'d' * SHA256_HEX_LENGTH}",
)


class EmptyWorkflowIterator:
    def __init__(self, next_page_token: bytes | None) -> None:
        self.next_page_token = next_page_token

    def __aiter__(self) -> EmptyWorkflowIterator:
        return self

    async def __anext__(self) -> object:
        raise StopAsyncIteration


class HistoryEventIterator:
    def __init__(self, events: tuple[HistoryEvent, ...]) -> None:
        self._events = iter(events)

    def __aiter__(self) -> HistoryEventIterator:
        return self

    async def __anext__(self) -> HistoryEvent:
        try:
            return next(self._events)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


def raw_description(*, first_run_id: str = "run-1") -> DescribeWorkflowExecutionResponse:
    response = DescribeWorkflowExecutionResponse()
    response.workflow_execution_info.first_run_id = first_run_id
    return response


@pytest.mark.parametrize(
    ("workflow_id", "run_id"),
    [
        pytest.param("", None, id="missing-workflow-id"),
        pytest.param("x" * 257, None, id="oversized-workflow-id"),
        pytest.param("workflow-1", "", id="missing-run-id"),
        pytest.param("workflow-1", "x" * 257, id="oversized-run-id"),
    ],
)
async def test_describe_rejects_invalid_execution_identity(
    workflow_id: str,
    run_id: str | None,
) -> None:
    client = MagicMock()
    controls = WorkflowControlService(client, max_payload_bytes=MAX_PAYLOAD_BYTES)

    with pytest.raises(ControlOperationError) as exc_info:
        await controls.describe(workflow_id, run_id=run_id)

    assert exc_info.value.code is ControlErrorCode.INVALID_REQUEST
    client.get_workflow_handle.assert_not_called()


async def test_temporal_not_found_is_a_typed_public_error() -> None:
    handle = MagicMock()
    handle.describe = AsyncMock(
        side_effect=RPCError("library detail", RPCStatusCode.NOT_FOUND, b"raw-status")
    )
    client = MagicMock()
    client.get_workflow_handle.return_value = handle
    controls = WorkflowControlService(client, max_payload_bytes=MAX_PAYLOAD_BYTES)

    with pytest.raises(ControlOperationError) as exc_info:
        await controls.describe("workflow-1")

    assert exc_info.value.code is ControlErrorCode.NOT_FOUND
    assert exc_info.value.retryable is False
    assert "library detail" not in str(exc_info.value)


async def test_describe_returns_pinned_configuration_identity() -> None:
    description = SimpleNamespace(
        id="workflow-1",
        run_id="run-1",
        workflow_type="workflow-type",
        status=None,
        task_queue="task-queue",
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        close_time=None,
        raw_description=raw_description(),
        memo=AsyncMock(
            return_value={
                MEMO_CONFIGURATION_REVISION: (EXECUTION_CONFIGURATION.configuration_revision_id),
                MEMO_CONFIGURATION_RESOLUTION_DIGEST: (EXECUTION_CONFIGURATION.resolution_digest),
            }
        ),
    )
    handle = MagicMock()
    handle.describe = AsyncMock(return_value=description)
    client = MagicMock()
    client.get_workflow_handle.return_value = handle

    result = await WorkflowControlService(
        client,
        max_payload_bytes=MAX_PAYLOAD_BYTES,
    ).describe("workflow-1")

    assert result.execution_configuration == EXECUTION_CONFIGURATION


async def test_describe_bounds_pending_waits_and_reports_continuation_identity() -> None:
    raw = raw_description(first_run_id="run-first")
    for index in range(PENDING_WAIT_INPUT_COUNT):
        activity = raw.pending_activities.add()
        activity.activity_id = f"activity-{index}"
        activity.state = PendingActivityState.PENDING_ACTIVITY_STATE_SCHEDULED
    child = raw.pending_children.add()
    child.workflow_id = "child-workflow"
    raw.pending_workflow_task.state = PendingWorkflowTaskState.PENDING_WORKFLOW_TASK_STATE_SCHEDULED
    description = SimpleNamespace(
        id="workflow-1",
        run_id="run-current",
        workflow_type="workflow-type",
        status=None,
        task_queue="task-queue",
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        close_time=None,
        memo=AsyncMock(return_value={}),
        raw_description=raw,
    )
    handle = MagicMock()
    handle.describe = AsyncMock(return_value=description)
    client = MagicMock()
    client.get_workflow_handle.return_value = handle

    result = await WorkflowControlService(
        client,
        max_payload_bytes=MAX_PAYLOAD_BYTES,
    ).describe("workflow-1")

    assert len(result.pending_waits) == MAX_EXPECTED_PENDING_WAITS
    assert result.pending_waits_truncated is True
    assert result.continuation is not None
    assert result.continuation.first_run_id == "run-first"
    assert result.continuation.current_run_id == "run-current"
    assert result.continuation.complete is False
    handle.fetch_history_events.assert_not_called()


async def test_describe_classifies_close_failure_without_exposing_failure_details() -> None:
    event = HistoryEvent()
    failure = event.workflow_execution_failed_event_attributes.failure
    failure.message = SENSITIVE_SENTINEL
    failure.application_failure_info.type = "STEP_FAILED"
    failure.application_failure_info.non_retryable = True
    description = SimpleNamespace(
        id="workflow-1",
        run_id="run-1",
        workflow_type="workflow-type",
        status=None,
        task_queue="task-queue",
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        close_time=datetime(2026, 1, 2, tzinfo=UTC),
        data_converter=DATA_CONVERTER,
        memo=AsyncMock(return_value={}),
        raw_description=raw_description(),
    )
    handle = MagicMock()
    handle.describe = AsyncMock(return_value=description)
    handle.fetch_history_events.return_value = HistoryEventIterator((event,))
    client = MagicMock()
    client.get_workflow_handle.return_value = handle

    result = await WorkflowControlService(
        client,
        max_payload_bytes=MAX_PAYLOAD_BYTES,
    ).describe("workflow-1")

    assert result.failure is not None
    assert result.failure.code == "STEP_FAILED"
    assert result.failure.retryable is False
    assert result.continuation is not None
    assert result.continuation.complete is True
    assert SENSITIVE_SENTINEL not in result.model_dump_json()


async def test_describe_projects_bounded_failure_metadata_from_temporal_details() -> None:
    event = HistoryEvent()
    failure = event.workflow_execution_failed_event_attributes.failure
    failure.application_failure_info.type = "STEP_FAILED"
    failure.application_failure_info.details.CopyFrom(
        await DATA_CONVERTER.encode_wrapper(
            [
                {
                    "audit_version": 1,
                    "serialization_version": 1,
                    "correlation": {"workflow": "example"},
                    "status": "failed",
                    "error": {
                        "code": "STEP_FAILED",
                        "cause_code": "HTTP_UNAVAILABLE",
                        "category": "execution",
                        "phase": "step",
                        "retryable": True,
                        "step": "fetch",
                    },
                    "started_at": "2026-01-01T00:00:00Z",
                    "failed_at": "2026-01-01T00:01:00Z",
                    "payload": {"secret": SENSITIVE_SENTINEL},
                }
            ]
        )
    )
    description = SimpleNamespace(
        id="workflow-1",
        run_id="run-1",
        workflow_type="workflow-type",
        status=None,
        task_queue="task-queue",
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        close_time=datetime(2026, 1, 2, tzinfo=UTC),
        data_converter=DATA_CONVERTER,
        memo=AsyncMock(return_value={}),
        raw_description=raw_description(),
    )
    handle = MagicMock()
    handle.describe = AsyncMock(return_value=description)
    handle.fetch_history_events.return_value = HistoryEventIterator((event,))
    client = MagicMock()
    client.get_workflow_handle.return_value = handle

    result = await WorkflowControlService(
        client,
        max_payload_bytes=MAX_PAYLOAD_BYTES,
    ).describe("workflow-1")

    assert result.failure is not None
    assert result.failure.cause_code == "HTTP_UNAVAILABLE"
    assert result.failure.phase == "step"
    assert result.failure.step == "fetch"
    assert SENSITIVE_SENTINEL not in result.model_dump_json()


async def test_describe_replaces_unknown_failure_types_with_a_safe_classification() -> None:
    event = HistoryEvent()
    failure = event.workflow_execution_failed_event_attributes.failure
    failure.application_failure_info.type = SENSITIVE_SENTINEL
    description = SimpleNamespace(
        id="workflow-1",
        run_id="run-1",
        workflow_type="workflow-type",
        status=None,
        task_queue="task-queue",
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        close_time=datetime(2026, 1, 2, tzinfo=UTC),
        data_converter=DATA_CONVERTER,
        memo=AsyncMock(return_value={}),
        raw_description=raw_description(),
    )
    handle = MagicMock()
    handle.describe = AsyncMock(return_value=description)
    handle.fetch_history_events.return_value = HistoryEventIterator((event,))
    client = MagicMock()
    client.get_workflow_handle.return_value = handle

    result = await WorkflowControlService(
        client,
        max_payload_bytes=MAX_PAYLOAD_BYTES,
    ).describe("workflow-1")

    assert result.failure is not None
    assert result.failure.code == "APPLICATION_FAILURE"
    assert SENSITIVE_SENTINEL not in result.model_dump_json()


@pytest.mark.parametrize("scope", [SCOPE_A, LOCAL_RUNTIME_SCOPE], ids=["tenant", "local"])
async def test_control_mutations_reject_a_foreign_scoped_workflow_identity(
    scope: RuntimeScope,
) -> None:
    client = MagicMock()
    controls = WorkflowControlService(
        client,
        scope=scope,
        max_payload_bytes=MAX_PAYLOAD_BYTES,
    )
    foreign_workflow_id = scoped_identity("workflow", SCOPE_B, "orders", "request-1")

    with pytest.raises(ControlOperationError) as raised:
        await controls.cancel(foreign_workflow_id)

    assert raised.value.code is ControlErrorCode.NOT_FOUND
    client.get_workflow_handle.assert_not_called()


async def test_signal_cancel_and_terminate_call_only_typed_operations() -> None:
    handle = MagicMock()
    handle.signal = AsyncMock()
    handle.cancel = AsyncMock()
    handle.terminate = AsyncMock()
    client = MagicMock()
    client.get_workflow_handle.return_value = handle
    controls = WorkflowControlService(client, max_payload_bytes=MAX_PAYLOAD_BYTES)

    await controls.signal_event(
        "workflow-1",
        "approved",
        {"approved": True},
        run_id="run-1",
    )
    await controls.cancel("workflow-1", run_id="run-1")
    await controls.terminate("workflow-1", run_id="run-1")

    handle.signal.assert_awaited_once()
    assert handle.signal.await_args.args[0] == WORKFLOW_EVENT_SIGNAL
    handle.cancel.assert_awaited_once()
    handle.terminate.assert_awaited_once()
    assert handle.cancel.await_args.kwargs["reason"] == (
        "Requested through the Justflow control API"
    )
    assert handle.terminate.await_args.kwargs["reason"] == (
        "Requested through the Justflow control API"
    )


async def test_workflow_listing_has_bounded_limit_and_page_token() -> None:
    client = MagicMock()
    client.list_workflows.return_value = EmptyWorkflowIterator(b"next-page")
    controls = WorkflowControlService(client, max_payload_bytes=MAX_PAYLOAD_BYTES)

    result = await controls.list(limit=10)

    assert result.workflows == ()
    assert result.next_page_token is not None
    assert bytes(decode_scope_cursor(LOCAL_RUNTIME_SCOPE, result.next_page_token)) == b"next-page"
    client.list_workflows.assert_called_once()
    assert client.list_workflows.call_args.kwargs["limit"] == 10

    with pytest.raises(ControlOperationError) as limit_error:
        await controls.list(limit=101)
    with pytest.raises(ControlOperationError) as token_error:
        await controls.list(page_token="not/base64!")

    assert limit_error.value.code is ControlErrorCode.INVALID_REQUEST
    assert token_error.value.code is ControlErrorCode.INVALID_REQUEST


async def test_tenant_workflow_listing_uses_a_scope_specific_visibility_query() -> None:
    client = MagicMock()
    client.list_workflows.return_value = EmptyWorkflowIterator(None)
    controls = WorkflowControlService(
        client,
        scope=SCOPE_A,
        max_payload_bytes=MAX_PAYLOAD_BYTES,
    )

    assert (await controls.list(limit=10)).workflows == ()

    query = client.list_workflows.call_args.kwargs["query"]
    assert SCOPE_A.digest in query
    assert "tenant-a" not in query


async def test_unbound_control_service_applies_scope_per_operation() -> None:
    client = MagicMock()
    client.list_workflows.return_value = EmptyWorkflowIterator(None)
    controls = WorkflowControlService(
        client,
        scope=None,
        max_payload_bytes=MAX_PAYLOAD_BYTES,
    )

    await controls.list(limit=10, scope=SCOPE_A)
    await controls.list(limit=10, scope=SCOPE_B)

    queries = [call.kwargs["query"] for call in client.list_workflows.call_args_list]
    assert SCOPE_A.digest in queries[0]
    assert SCOPE_B.digest in queries[1]
    assert queries[0] != queries[1]
    with pytest.raises(ValueError, match="authorized runtime scope"):
        await controls.list(limit=10)


@pytest.mark.parametrize(
    ("state", "temporal_state"),
    [
        pytest.param(WorkflowExecutionState.RUNNING, "Running", id="running"),
        pytest.param(WorkflowExecutionState.COMPLETED, "Completed", id="completed"),
        pytest.param(WorkflowExecutionState.FAILED, "Failed", id="failed"),
        pytest.param(WorkflowExecutionState.CANCELED, "Canceled", id="canceled"),
        pytest.param(WorkflowExecutionState.TERMINATED, "Terminated", id="terminated"),
        pytest.param(
            WorkflowExecutionState.CONTINUED_AS_NEW,
            "ContinuedAsNew",
            id="continued-as-new",
        ),
        pytest.param(WorkflowExecutionState.TIMED_OUT, "TimedOut", id="timed-out"),
    ],
)
async def test_execution_state_filters_are_pushed_to_temporal_visibility(
    state: WorkflowExecutionState,
    temporal_state: str,
) -> None:
    client = MagicMock()
    client.list_workflows.return_value = EmptyWorkflowIterator(None)

    await WorkflowControlService(
        client,
        scope=SCOPE_A,
        max_payload_bytes=MAX_PAYLOAD_BYTES,
    ).list(query=WorkflowListQuery(state=state))

    assert (
        f'ExecutionStatus = "{temporal_state}"' in (client.list_workflows.call_args.kwargs["query"])
    )


async def test_safe_index_filters_are_combined_with_mandatory_scope_isolation() -> None:
    client = MagicMock()
    client.list_workflows.return_value = EmptyWorkflowIterator(None)
    controls = WorkflowControlService(
        client,
        scope=SCOPE_A,
        max_payload_bytes=MAX_PAYLOAD_BYTES,
        indexed_search_attributes_enabled=True,
    )

    await controls.list(
        query=WorkflowListQuery(
            workflow="orders",
            definition_digest="d" * 64,
            trigger_source="host",
            worker_artifact=f"sha256:{'e' * 64}",
            scope="current",
        )
    )

    query = client.list_workflows.call_args.kwargs["query"]
    assert SCOPE_A.digest in query
    assert "JustflowLogicalWorkflow" in query
    assert "JustflowDefinitionDigest" in query
    assert "JustflowTriggerSource" in query
    assert "JustflowWorkerArtifact" in query
    assert "tenant-a" not in query


async def test_unregistered_custom_index_filter_fails_without_scanning() -> None:
    client = MagicMock()
    controls = WorkflowControlService(
        client,
        scope=SCOPE_A,
        max_payload_bytes=MAX_PAYLOAD_BYTES,
    )

    with pytest.raises(ControlOperationError) as raised:
        await controls.list(query=WorkflowListQuery(definition_digest="d" * 64))

    assert raised.value.code is ControlErrorCode.FILTER_UNAVAILABLE
    client.list_workflows.assert_not_called()
