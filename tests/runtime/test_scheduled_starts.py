"""One-off scheduled-start contracts and lifecycle behavior."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import TypeAlias

import pytest
from pydantic import ValidationError

from justflow.config.settings import ScheduledStartWorkloadClass
from justflow.provenance import LOCAL_ARTIFACT_DIGEST, WorkerArtifactIdentity
from justflow.runtime.scheduled_starts import (
    CanceledScheduledStartRecord,
    DispatchingScheduledStartRecord,
    PendingScheduledStartRecord,
    ScheduledStartArbiterInitialInput,
    ScheduledStartCancelCommand,
    ScheduledStartCancelRequest,
    ScheduledStartCreateRequest,
    ScheduledStartDueCommand,
    ScheduledStartError,
    ScheduledStartErrorCode,
    ScheduledStartFailureCode,
    ScheduledStartRescheduleRequest,
    ScheduledStartRun,
    ScheduledStartState,
    StartedScheduledStartRecord,
    cancel_scheduled_start,
    claim_scheduled_start,
    complete_scheduled_start,
    create_scheduled_start_record,
    describe_scheduled_start,
    fail_scheduled_start,
    make_scheduled_start_arbiter_id,
    make_scheduled_start_due_id,
    make_scheduled_start_id,
    make_scheduled_start_schedule_id,
    reschedule_scheduled_start,
    scheduled_start_request_digest,
)
from justflow.scope import LOCAL_RUNTIME_SCOPE

WORKFLOW_NAME = "reporting"
BUSINESS_REQUEST_ID = "appointment-private-42"
TRIGGER_NAME = "reporting_api"
ACCEPTED_AT = datetime(2026, 8, 11, 8, 0, tzinfo=UTC)
START_AT = ACCEPTED_AT + timedelta(hours=2)
RESCHEDULED_AT = START_AT + timedelta(hours=1)
UPDATED_AT = ACCEPTED_AT + timedelta(minutes=5)
CREATE_DIGEST = "a" * 64
RESCHEDULE_DIGEST = "b" * 64
CANCEL_DIGEST = "c" * 64
INPUT_VALUE = "private-input"
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="local",
    build_id="local",
    artifact_digest=LOCAL_ARTIFACT_DIGEST,
    package_version="0.1.0",
)
RUN = ScheduledStartRun(
    workflow_id="workflow-id",
    run_id="run-id",
    definition_digest="d" * 64,
    artifact_identity=ARTIFACT,
    environment_snapshot_digest="e" * 64,
)


def create_request(*, start_at: datetime = START_AT) -> ScheduledStartCreateRequest:
    return ScheduledStartCreateRequest(
        workflow_name=WORKFLOW_NAME,
        input={"reference": INPUT_VALUE},
        business_request_id=BUSINESS_REQUEST_ID,
        start_at=start_at,
        workload_class=ScheduledStartWorkloadClass.INTERACTIVE,
    )


def pending_record() -> PendingScheduledStartRecord:
    request = create_request()
    return create_scheduled_start_record(
        request,
        scheduled_start_id=make_scheduled_start_id(
            LOCAL_RUNTIME_SCOPE,
            request.workflow_name,
            request.business_request_id,
        ),
        scope=LOCAL_RUNTIME_SCOPE,
        trigger_name=TRIGGER_NAME,
        request_digest=CREATE_DIGEST,
        accepted_at=ACCEPTED_AT,
        normalized_input=dict(request.input),
    )


def rescheduled_record() -> PendingScheduledStartRecord:
    record, _ = reschedule_scheduled_start(
        pending_record(),
        ScheduledStartRescheduleRequest(start_at=RESCHEDULED_AT, expected_version=1),
        request_digest=RESCHEDULE_DIGEST,
        updated_at=UPDATED_AT,
    )
    return record


def dispatching_record() -> DispatchingScheduledStartRecord:
    record = claim_scheduled_start(
        pending_record(),
        expected_version=1,
        claimed_at=UPDATED_AT,
    )
    assert record is not None
    return record


@dataclass(frozen=True, kw_only=True)
class Returns:
    state: ScheduledStartState
    version: int
    duplicate: bool
    start_at: datetime


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


RescheduleOutcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class RescheduleCase:
    id: str
    record: PendingScheduledStartRecord | DispatchingScheduledStartRecord
    expected_version: int
    request_digest: str
    outcome: RescheduleOutcome


RESCHEDULE_CASES = [
    RescheduleCase(
        id="pending",
        record=pending_record(),
        expected_version=1,
        request_digest=RESCHEDULE_DIGEST,
        outcome=Returns(
            state=ScheduledStartState.SCHEDULED,
            version=2,
            duplicate=False,
            start_at=RESCHEDULED_AT,
        ),
    ),
    RescheduleCase(
        id="exact-retry",
        record=rescheduled_record(),
        expected_version=1,
        request_digest=RESCHEDULE_DIGEST,
        outcome=Returns(
            state=ScheduledStartState.SCHEDULED,
            version=2,
            duplicate=True,
            start_at=RESCHEDULED_AT,
        ),
    ),
    RescheduleCase(
        id="stale-version",
        record=rescheduled_record(),
        expected_version=1,
        request_digest="f" * 64,
        outcome=Raises(exc=ScheduledStartError, match="expected version"),
    ),
    RescheduleCase(
        id="dispatch-won",
        record=dispatching_record(),
        expected_version=1,
        request_digest=RESCHEDULE_DIGEST,
        outcome=Raises(exc=ScheduledStartError, match="expected version"),
    ),
]


@pytest.mark.parametrize("case", RESCHEDULE_CASES, ids=lambda case: case.id)
def test_reschedule_transition(case: RescheduleCase) -> None:
    request = ScheduledStartRescheduleRequest(
        start_at=RESCHEDULED_AT,
        expected_version=case.expected_version,
    )
    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            reschedule_scheduled_start(
                case.record,
                request,
                request_digest=case.request_digest,
                updated_at=UPDATED_AT,
            )
        return

    record, duplicate = reschedule_scheduled_start(
        case.record,
        request,
        request_digest=case.request_digest,
        updated_at=UPDATED_AT,
    )
    assert (record.state, record.version, duplicate, record.start_at) == (
        case.outcome.state,
        case.outcome.version,
        case.outcome.duplicate,
        case.outcome.start_at,
    )


def test_cancel_is_versioned_and_exact_retry_is_idempotent() -> None:
    request = ScheduledStartCancelRequest(expected_version=1)

    canceled, duplicate = cancel_scheduled_start(
        pending_record(),
        request,
        request_digest=CANCEL_DIGEST,
        updated_at=UPDATED_AT,
    )
    retried, retry_duplicate = cancel_scheduled_start(
        canceled,
        request,
        request_digest=CANCEL_DIGEST,
        updated_at=UPDATED_AT,
    )

    assert isinstance(canceled, CanceledScheduledStartRecord)
    assert canceled.version == 1
    assert canceled.completed_at == UPDATED_AT
    assert duplicate is False
    assert retried == canceled
    assert retry_duplicate is True


def test_cancel_rejects_a_competing_request_digest() -> None:
    canceled, _ = cancel_scheduled_start(
        pending_record(),
        ScheduledStartCancelRequest(expected_version=1),
        request_digest=CANCEL_DIGEST,
        updated_at=UPDATED_AT,
    )

    with pytest.raises(ScheduledStartError, match="expected version") as raised:
        cancel_scheduled_start(
            canceled,
            ScheduledStartCancelRequest(expected_version=1),
            request_digest="f" * 64,
            updated_at=UPDATED_AT,
        )

    assert raised.value.code is ScheduledStartErrorCode.CONFLICT


@pytest.mark.parametrize(
    ("record", "expected_version", "claimed"),
    [
        pytest.param(pending_record(), 1, True, id="pending-claim"),
        pytest.param(pending_record(), 2, False, id="rescheduled-version-won"),
        pytest.param(
            cancel_scheduled_start(
                pending_record(),
                ScheduledStartCancelRequest(expected_version=1),
                request_digest=CANCEL_DIGEST,
                updated_at=UPDATED_AT,
            )[0],
            1,
            False,
            id="cancel-won",
        ),
    ],
)
def test_due_time_claim_has_one_versioned_winner(
    record: PendingScheduledStartRecord | CanceledScheduledStartRecord,
    expected_version: int,
    claimed: bool,
) -> None:
    result = claim_scheduled_start(
        record,
        expected_version=expected_version,
        claimed_at=UPDATED_AT,
    )

    assert (result is not None) is claimed
    if result is not None:
        assert result.state is ScheduledStartState.DISPATCHING
        assert result.claimed_version == expected_version
        assert result.version == expected_version
        assert (
            claim_scheduled_start(
                result,
                expected_version=expected_version,
                claimed_at=UPDATED_AT,
            )
            == result
        )


def test_dispatch_completion_and_failure_are_terminal_and_safe() -> None:
    dispatching = claim_scheduled_start(
        pending_record(),
        expected_version=1,
        claimed_at=UPDATED_AT,
    )
    assert dispatching is not None

    started = complete_scheduled_start(
        dispatching,
        RUN,
        completed_at=UPDATED_AT + timedelta(seconds=1),
    )
    failed = fail_scheduled_start(
        dispatching,
        ScheduledStartFailureCode.CONTRACT_DRIFT,
        completed_at=UPDATED_AT + timedelta(seconds=1),
    )

    assert isinstance(started, StartedScheduledStartRecord)
    assert started.version == 1
    assert describe_scheduled_start(started).run == RUN
    assert failed.state is ScheduledStartState.FAILED
    assert describe_scheduled_start(failed).failure_code is (
        ScheduledStartFailureCode.CONTRACT_DRIFT
    )


def test_scheduled_start_identities_are_stable_opaque_and_namespaced() -> None:
    scheduled_start_id = make_scheduled_start_id(
        LOCAL_RUNTIME_SCOPE,
        WORKFLOW_NAME,
        BUSINESS_REQUEST_ID,
    )

    assert scheduled_start_id == make_scheduled_start_id(
        LOCAL_RUNTIME_SCOPE,
        WORKFLOW_NAME,
        BUSINESS_REQUEST_ID,
    )
    assert BUSINESS_REQUEST_ID not in scheduled_start_id
    assert WORKFLOW_NAME not in scheduled_start_id
    assert make_scheduled_start_schedule_id(
        LOCAL_RUNTIME_SCOPE,
        scheduled_start_id,
    ).startswith("jf1.scheduled-start-schedule.")
    assert make_scheduled_start_arbiter_id(
        LOCAL_RUNTIME_SCOPE,
        scheduled_start_id,
        1,
    ) != make_scheduled_start_arbiter_id(
        LOCAL_RUNTIME_SCOPE,
        scheduled_start_id,
        2,
    )
    assert make_scheduled_start_due_id(
        LOCAL_RUNTIME_SCOPE,
        scheduled_start_id,
        1,
    ) != make_scheduled_start_due_id(
        LOCAL_RUNTIME_SCOPE,
        scheduled_start_id,
        2,
    )


@pytest.mark.parametrize(
    "command",
    [
        pytest.param(
            ScheduledStartCancelCommand(
                expected_version=2,
                request_digest=CANCEL_DIGEST,
            ),
            id="future-mutation-version",
        ),
        pytest.param(
            ScheduledStartDueCommand(nominal_time=RESCHEDULED_AT),
            id="wrong-nominal-time",
        ),
    ],
)
def test_arbiter_input_rejects_a_command_for_another_version(
    command: ScheduledStartCancelCommand | ScheduledStartDueCommand,
) -> None:
    with pytest.raises(ValidationError, match="retained"):
        ScheduledStartArbiterInitialInput(record=pending_record(), command=command)


def test_sensitive_logical_request_fields_are_hidden_from_representations() -> None:
    request = create_request()
    record = pending_record()

    assert BUSINESS_REQUEST_ID not in repr(request)
    assert INPUT_VALUE not in repr(request)
    assert BUSINESS_REQUEST_ID not in repr(record)
    assert INPUT_VALUE not in repr(record)


def test_request_digest_is_canonical_and_sensitive_values_are_not_exposed() -> None:
    digest = scheduled_start_request_digest(
        "create",
        create_request().model_dump(mode="json"),
        "idempotency-key",
    )

    assert digest == scheduled_start_request_digest(
        "create",
        create_request().model_dump(mode="json"),
        "idempotency-key",
    )
    assert BUSINESS_REQUEST_ID not in digest
    assert len(digest) == 64


@pytest.mark.parametrize(
    ("start_at", "match"),
    [
        pytest.param(
            datetime(2026, 8, 11, 10, 0, tzinfo=UTC).replace(tzinfo=None),
            "timezone",
            id="naive",
        ),
        pytest.param(
            datetime(2026, 8, 11, 10, 0, 0, 1, tzinfo=UTC),
            "whole-second",
            id="subsecond",
        ),
    ],
)
def test_create_request_rejects_ambiguous_timestamps(
    start_at: datetime,
    match: str,
) -> None:
    with pytest.raises(ValidationError, match=match):
        create_request(start_at=start_at)


def test_create_request_normalizes_timezone_to_utc() -> None:
    offset = timezone(timedelta(hours=2))
    request = create_request(start_at=datetime(2026, 8, 11, 12, 0, tzinfo=offset))

    assert request.start_at.tzinfo is UTC
    assert request.start_at == START_AT
