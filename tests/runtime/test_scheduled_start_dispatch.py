"""One-off scheduled-start arbitration activity tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.common import Priority
from temporalio.exceptions import ApplicationError

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.config.settings import ScheduledStartWorkloadClass
from justflow.definitions.manifest import ENGINE_WORKFLOW_ABI, build_definition_manifests
from justflow.definitions.routing import WorkerDeployment, WorkflowStartTarget
from justflow.provenance import LOCAL_ARTIFACT_DIGEST, WorkerArtifactIdentity
from justflow.runtime.scheduled_start_dispatch import (
    SCHEDULED_START_ACTIVITY_INVALID_INPUT,
    SCHEDULED_START_ACTIVITY_REJECTED,
    ScheduledStartDispatchActivities,
)
from justflow.runtime.scheduled_start_service import ScheduledStartService
from justflow.runtime.scheduled_starts import (
    PendingScheduledStartRecord,
    ScheduledStartArbiterApplied,
    ScheduledStartArbiterCommandKind,
    ScheduledStartArbiterInitialInput,
    ScheduledStartArbiterPreparedDue,
    ScheduledStartArbiterTerminalInput,
    ScheduledStartAttemptAccepted,
    ScheduledStartAttemptOutcome,
    ScheduledStartAttemptRequest,
    ScheduledStartCancelRequest,
    ScheduledStartCreateRequest,
    ScheduledStartDueClaimed,
    ScheduledStartDueCommand,
    ScheduledStartDueInput,
    ScheduledStartDueSkipped,
    ScheduledStartError,
    ScheduledStartErrorCode,
    ScheduledStartResolvedTarget,
    ScheduledStartRun,
    cancel_scheduled_start,
    claim_scheduled_start,
    create_scheduled_start_record,
    describe_scheduled_start,
    make_scheduled_start_id,
)
from justflow.scope import LOCAL_RUNTIME_SCOPE

WORKFLOW_NAME = "reporting"
TRIGGER_NAME = "reporting_api"
NOW = datetime(2026, 8, 11, 8, 0, tzinfo=UTC)
START_AT = NOW + timedelta(hours=1)
REQUEST = ScheduledStartCreateRequest(
    workflow_name=WORKFLOW_NAME,
    input={"reference": "opaque-reference"},
    business_request_id="private-request",
    start_at=START_AT,
    workload_class=ScheduledStartWorkloadClass.STANDARD,
)
MANIFEST = build_definition_manifests(
    {
        WORKFLOW_NAME: WorkflowConfig(
            workflow=WORKFLOW_NAME,
            steps={},
            flow=[FlowStep.model_validate({"name": "done", "terminal": True})],
        )
    },
    {},
    DEFAULT_RUNTIME_LIMITS,
)[WORKFLOW_NAME]
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="local",
    build_id="local",
    artifact_digest=LOCAL_ARTIFACT_DIGEST,
    package_version="0.1.0",
)
TARGET = WorkflowStartTarget(
    manifest=MANIFEST,
    deployment=WorkerDeployment(
        artifact_identity=ARTIFACT,
        compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
    ),
    environment_snapshot_digest="e" * 64,
)
RUN = ScheduledStartRun(
    workflow_id="workflow-id",
    run_id="run-id",
    definition_digest=MANIFEST.definition_digest,
    artifact_identity=ARTIFACT,
    environment_snapshot_digest=TARGET.environment_snapshot_digest,
)


def pending_record() -> PendingScheduledStartRecord:
    return create_scheduled_start_record(
        REQUEST,
        scheduled_start_id=make_scheduled_start_id(
            LOCAL_RUNTIME_SCOPE,
            WORKFLOW_NAME,
            REQUEST.business_request_id,
        ),
        scope=LOCAL_RUNTIME_SCOPE,
        trigger_name=TRIGGER_NAME,
        request_digest="d" * 64,
        accepted_at=NOW,
        normalized_input=dict(REQUEST.input),
    )


def activities(service: MagicMock) -> ScheduledStartDispatchActivities:
    return ScheduledStartDispatchActivities(
        service,
        scope=LOCAL_RUNTIME_SCOPE,
        priority_provider=Priority,
    )


async def test_claim_activity_starts_the_exact_version_arbiter() -> None:
    due = ScheduledStartDueInput(record=pending_record())
    expected = ScheduledStartDueClaimed()
    service = MagicMock(spec=ScheduledStartService)
    service.claim_due = AsyncMock(return_value=expected)

    result = await activities(service).claim(due.model_dump(mode="json"))

    assert ScheduledStartDueClaimed.model_validate(result) == expected
    service.claim_due.assert_awaited_once_with(
        due.record,
        scope=LOCAL_RUNTIME_SCOPE,
        priority=Priority(),
    )


async def test_claim_activity_skips_a_terminal_schedule_action() -> None:
    record = pending_record()
    canceled, _ = cancel_scheduled_start(
        record,
        ScheduledStartCancelRequest(expected_version=record.version),
        request_digest="c" * 64,
        updated_at=NOW,
    )
    service = MagicMock(spec=ScheduledStartService)

    result = await activities(service).claim(
        ScheduledStartDueInput(record=canceled).model_dump(mode="json")
    )

    skipped = ScheduledStartDueSkipped.model_validate(result)
    assert skipped.current.state == canceled.state
    service.claim_due.assert_not_called()


async def test_prepare_activity_returns_the_pinned_due_target() -> None:
    record = pending_record()
    claimed = claim_scheduled_start(record, expected_version=1, claimed_at=NOW)
    assert claimed is not None
    initial = ScheduledStartArbiterInitialInput(
        record=record,
        command=ScheduledStartDueCommand(nominal_time=record.start_at),
    )
    expected = ScheduledStartArbiterPreparedDue(
        record=claimed,
        target=ScheduledStartResolvedTarget.from_target(TARGET),
    )
    service = MagicMock(spec=ScheduledStartService)
    service.prepare_arbiter = AsyncMock(return_value=expected)

    result = await activities(service).prepare(initial.model_dump(mode="json"))

    assert ScheduledStartArbiterPreparedDue.model_validate(result) == expected
    service.prepare_arbiter.assert_awaited_once_with(initial, scope=LOCAL_RUNTIME_SCOPE)


async def test_attempt_activity_returns_a_classified_single_attempt() -> None:
    claimed = claim_scheduled_start(pending_record(), expected_version=1, claimed_at=NOW)
    assert claimed is not None
    request = ScheduledStartAttemptRequest(
        record=claimed,
        target=ScheduledStartResolvedTarget.from_target(TARGET),
    )
    expected = ScheduledStartAttemptAccepted(
        outcome=ScheduledStartAttemptOutcome.ACCEPTED,
        run=RUN,
    )
    service = MagicMock(spec=ScheduledStartService)
    service.attempt_dispatch = AsyncMock(return_value=expected)

    result = await activities(service).attempt(request.model_dump(mode="json"))

    assert ScheduledStartAttemptAccepted.model_validate(result) == expected
    service.attempt_dispatch.assert_awaited_once_with(
        request,
        scope=LOCAL_RUNTIME_SCOPE,
        priority=Priority(),
    )


async def test_commit_activity_returns_the_authoritative_projection() -> None:
    canceled, _ = cancel_scheduled_start(
        pending_record(),
        ScheduledStartCancelRequest(expected_version=1),
        request_digest="c" * 64,
        updated_at=NOW,
    )
    terminal = ScheduledStartArbiterTerminalInput(
        projection=canceled,
        consumed_version=1,
        winner=ScheduledStartArbiterCommandKind.CANCEL,
        request_digest="c" * 64,
    )
    expected = ScheduledStartArbiterApplied(
        consumed_version=1,
        winner=ScheduledStartArbiterCommandKind.CANCEL,
        request_digest="c" * 64,
        scheduled_start=describe_scheduled_start(canceled),
    )
    service = MagicMock(spec=ScheduledStartService)
    service.commit_arbiter = AsyncMock(return_value=expected)

    result = await activities(service).commit(terminal.model_dump(mode="json"))

    assert ScheduledStartArbiterApplied.model_validate(result) == expected
    service.commit_arbiter.assert_awaited_once_with(terminal, scope=LOCAL_RUNTIME_SCOPE)


@pytest.mark.parametrize(
    "retryable",
    [
        pytest.param(False, id="non-retryable"),
        pytest.param(True, id="retryable"),
    ],
)
async def test_claim_activity_preserves_retryability_without_leaking_input(
    retryable: bool,
) -> None:
    service = MagicMock(spec=ScheduledStartService)
    service.claim_due = AsyncMock(
        side_effect=ScheduledStartError(
            ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
            "opaque-reference",
            retryable=retryable,
        )
    )

    with pytest.raises(ApplicationError) as raised:
        await activities(service).claim(
            ScheduledStartDueInput(record=pending_record()).model_dump(mode="json")
        )

    assert raised.value.type == SCHEDULED_START_ACTIVITY_REJECTED
    assert raised.value.non_retryable is not retryable
    assert "opaque-reference" not in str(raised.value)


async def test_claim_activity_rejects_oversized_input_before_calling_service() -> None:
    service = MagicMock(spec=ScheduledStartService)
    handler = ScheduledStartDispatchActivities(
        service,
        limits=RuntimeLimits(activity_input_bytes=1),
        priority_provider=Priority,
    )

    with pytest.raises(ApplicationError) as raised:
        await handler.claim(ScheduledStartDueInput(record=pending_record()).model_dump(mode="json"))

    assert raised.value.type == SCHEDULED_START_ACTIVITY_INVALID_INPUT
    assert raised.value.non_retryable is True
    service.claim_due.assert_not_called()


@pytest.mark.parametrize(
    "activity_name",
    [
        pytest.param("prepare", id="prepare"),
        pytest.param("attempt", id="attempt"),
        pytest.param("commit", id="commit"),
    ],
)
async def test_dispatch_activity_rejects_malformed_input(activity_name: str) -> None:
    service = MagicMock(spec=ScheduledStartService)
    handler = activities(service)

    with pytest.raises(ApplicationError) as raised:
        await getattr(handler, activity_name)({})

    assert raised.value.type == SCHEDULED_START_ACTIVITY_INVALID_INPUT
    assert raised.value.non_retryable is True
