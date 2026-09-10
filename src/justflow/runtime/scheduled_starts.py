"""Typed one-off scheduled-start contracts and deterministic lifecycle transitions."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import Enum
from typing import Annotated, Literal, TypeAlias, TypeVar

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)

from justflow.config.grammar import TriggerName, WorkflowName
from justflow.config.settings import (
    DEFAULT_SCHEDULE_DISPATCH_ATTEMPTS,
    DEFAULT_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
    ScheduledStartWorkloadClass,
)
from justflow.definitions.manifest import DefinitionManifest
from justflow.definitions.routing import (
    WorkerDeployment,
    WorkflowStartTarget,
)
from justflow.provenance import ExecutionConfigurationIdentity, WorkerArtifactIdentity
from justflow.scope import RuntimeScope, scoped_identity_from_digest
from justflow.sdk.message_contract import DEFINITION_DIGEST_LENGTH, MAX_IDENTIFIER_LENGTH

SCHEDULED_START_FORMAT_VERSION = 1
SCHEDULED_START_OWNER = "justflow.scheduled-start"
SCHEDULED_START_MAINTENANCE_OWNER = "justflow.scheduled-start-maintenance"
SCHEDULED_START_ARBITER_WORKFLOW_TYPE = "justflow.scheduled-start-arbiter.v1"
SCHEDULED_START_DUE_WORKFLOW_TYPE = "justflow.scheduled-start-due.v1"
SCHEDULED_START_CLAIM_ACTIVITY_TYPE = "justflow.claim-scheduled-start.v1"
SCHEDULED_START_PREPARE_ACTIVITY_TYPE = "justflow.prepare-scheduled-start.v1"
SCHEDULED_START_ATTEMPT_ACTIVITY_TYPE = "justflow.attempt-scheduled-start.v1"
SCHEDULED_START_COMMIT_ACTIVITY_TYPE = "justflow.commit-scheduled-start.v1"
SCHEDULED_START_CLEANUP_WORKFLOW_TYPE = "justflow.scheduled-start-cleanup.v1"
SCHEDULED_START_CLEANUP_ACTIVITY_TYPE = "justflow.cleanup-scheduled-starts.v1"
SCHEDULED_START_SCHEDULE_ID_PREFIX = "jf1.scheduled-start-schedule."
SCHEDULED_START_MAINTENANCE_ID_PREFIX = "justflow-scheduled-start-maintenance-"
MEMO_SCHEDULED_START_OWNER = "justflow.scheduled_start_owner"
MEMO_SCHEDULED_START_ID = "justflow.scheduled_start_id"
MEMO_SCHEDULED_START_FORMAT_VERSION = "justflow.scheduled_start_format_version"
MEMO_SCHEDULED_START_STATE = "justflow.scheduled_start_state"
MEMO_SCHEDULED_START_REQUEST_DIGEST = "justflow.scheduled_start_request_digest"
MEMO_SCHEDULED_START_ARBITER_COMMAND = "justflow.scheduled_start_arbiter_command"
MEMO_SCHEDULED_START_ARBITER_REQUEST_DIGEST = "justflow.scheduled_start_arbiter_request_digest"
MEMO_SCHEDULED_START_ARBITER_VERSION = "justflow.scheduled_start_arbiter_version"
MAX_SCHEDULED_START_FAILURE_CODE_LENGTH = 64
MAX_SCHEDULED_START_CURSOR_LENGTH = 16_384
SCHEDULED_START_DIGEST_LENGTH = 64


class StrictScheduledStartModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class ScheduledStartState(str, Enum):
    SCHEDULED = "scheduled"
    DISPATCHING = "dispatching"
    STARTED = "started"
    CANCELED = "canceled"
    FAILED = "failed"


class ScheduledStartMutationKind(str, Enum):
    CREATE = "create"
    RESCHEDULE = "reschedule"
    CANCEL = "cancel"
    DISPATCH = "dispatch"
    START = "start"
    FAIL = "fail"


class ScheduledStartMutationStatus(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    RESCHEDULED = "rescheduled"
    CANCELED = "canceled"
    IN_PROGRESS = "in_progress"


class ScheduledStartArbiterCommandKind(str, Enum):
    DUE = "due"
    CANCEL = "cancel"
    RESCHEDULE = "reschedule"


class ScheduledStartArbiterPhase(str, Enum):
    INITIAL = "initial"
    RESOLVED_DUE = "resolved_due"
    TERMINAL_PENDING_PROJECTION = "terminal_pending_projection"


class ScheduledStartAttemptOutcome(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    AUTHORITATIVE_REJECTION = "authoritative_rejection"
    AMBIGUOUS = "ambiguous"


class ScheduledStartDecisionOutcome(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    RESCHEDULED = "rescheduled"
    CANCELED = "canceled"
    DUE = "due"
    STARTED = "started"
    FAILED = "failed"


class ScheduledStartFailureCode(str, Enum):
    TARGET_UNAVAILABLE = "target_unavailable"
    CONTRACT_DRIFT = "contract_drift"
    INCOMPATIBLE_WORKER = "incompatible_worker"
    DISPATCH_EXHAUSTED = "dispatch_exhausted"


class ScheduledStartErrorCode(str, Enum):
    INVALID_REQUEST = "invalid_request"
    TRIGGER_UNAVAILABLE = "trigger_unavailable"
    TRIGGER_PAUSED = "trigger_paused"
    INPUT_REJECTED = "input_rejected"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    QUOTA_EXCEEDED = "quota_exceeded"
    QUOTA_UNAVAILABLE = "quota_unavailable"
    PRIORITY_UNSUPPORTED = "priority_unsupported"
    TEMPORAL_UNAVAILABLE = "temporal_unavailable"


class ScheduledStartError(Exception):
    def __init__(
        self,
        code: ScheduledStartErrorCode,
        message: str,
        *,
        retryable: bool,
    ) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(message)


class ScheduledStartCreateRequest(StrictScheduledStartModel):
    workflow_name: WorkflowName
    input: dict[str, object] = Field(default_factory=dict, repr=False)
    business_request_id: str = Field(
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
        repr=False,
    )
    start_at: AwareDatetime
    workload_class: ScheduledStartWorkloadClass = ScheduledStartWorkloadClass.STANDARD

    @field_validator("start_at")
    @classmethod
    def normalize_start_at(cls, value: datetime) -> datetime:
        if value.microsecond:
            raise ValueError("Scheduled-start timestamps require whole-second precision")
        return value.astimezone(UTC)


class ScheduledStartRescheduleRequest(StrictScheduledStartModel):
    start_at: AwareDatetime
    expected_version: int = Field(ge=1)

    @field_validator("start_at")
    @classmethod
    def normalize_start_at(cls, value: datetime) -> datetime:
        if value.microsecond:
            raise ValueError("Scheduled-start timestamps require whole-second precision")
        return value.astimezone(UTC)


class ScheduledStartCancelRequest(StrictScheduledStartModel):
    expected_version: int = Field(ge=1)


class ScheduledStartDueCommand(StrictScheduledStartModel):
    kind: Literal[ScheduledStartArbiterCommandKind.DUE] = ScheduledStartArbiterCommandKind.DUE
    nominal_time: AwareDatetime


class ScheduledStartCancelCommand(StrictScheduledStartModel):
    kind: Literal[ScheduledStartArbiterCommandKind.CANCEL] = ScheduledStartArbiterCommandKind.CANCEL
    expected_version: int = Field(ge=1)
    request_digest: str = Field(
        min_length=SCHEDULED_START_DIGEST_LENGTH,
        max_length=SCHEDULED_START_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCHEDULED_START_DIGEST_LENGTH}}}$",
        repr=False,
    )


class ScheduledStartRescheduleCommand(StrictScheduledStartModel):
    kind: Literal[ScheduledStartArbiterCommandKind.RESCHEDULE] = (
        ScheduledStartArbiterCommandKind.RESCHEDULE
    )
    expected_version: int = Field(ge=1)
    start_at: AwareDatetime
    request_digest: str = Field(
        min_length=SCHEDULED_START_DIGEST_LENGTH,
        max_length=SCHEDULED_START_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCHEDULED_START_DIGEST_LENGTH}}}$",
        repr=False,
    )


ScheduledStartArbiterCommand: TypeAlias = Annotated[
    ScheduledStartDueCommand | ScheduledStartCancelCommand | ScheduledStartRescheduleCommand,
    Field(discriminator="kind"),
]


class ScheduledStartRun(StrictScheduledStartModel):
    workflow_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    run_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    definition_digest: str = Field(
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{DEFINITION_DIGEST_LENGTH}}}$",
    )
    artifact_identity: WorkerArtifactIdentity
    environment_snapshot_digest: str = Field(
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{DEFINITION_DIGEST_LENGTH}}}$",
    )
    execution_configuration: ExecutionConfigurationIdentity | None = None


class ScheduledStartRecordBase(StrictScheduledStartModel):
    format_version: Literal[1] = 1
    scheduled_start_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH * 2)
    scope_digest: str = Field(
        min_length=SCHEDULED_START_DIGEST_LENGTH,
        max_length=SCHEDULED_START_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCHEDULED_START_DIGEST_LENGTH}}}$",
    )
    workflow_name: WorkflowName
    input: dict[str, object] = Field(default_factory=dict, repr=False)
    business_request_id: str = Field(
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
        repr=False,
    )
    start_at: AwareDatetime
    workload_class: ScheduledStartWorkloadClass
    trigger_name: TriggerName
    source: Literal["control_api"] = "control_api"
    create_request_digest: str = Field(
        min_length=SCHEDULED_START_DIGEST_LENGTH,
        max_length=SCHEDULED_START_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCHEDULED_START_DIGEST_LENGTH}}}$",
        repr=False,
    )
    version: int = Field(ge=1)
    accepted_at: AwareDatetime
    updated_at: AwareDatetime
    last_mutation_kind: ScheduledStartMutationKind
    last_mutation_digest: str = Field(
        min_length=SCHEDULED_START_DIGEST_LENGTH,
        max_length=SCHEDULED_START_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCHEDULED_START_DIGEST_LENGTH}}}$",
        repr=False,
    )
    dispatch_timeout_seconds: int = Field(
        default=DEFAULT_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
        ge=1,
    )
    dispatch_attempts: int = Field(
        default=DEFAULT_SCHEDULE_DISPATCH_ATTEMPTS,
        ge=1,
    )

    @model_validator(mode="after")
    def validate_timestamps(self) -> ScheduledStartRecordBase:
        timestamps = (self.start_at, self.accepted_at, self.updated_at)
        if any(value.microsecond for value in timestamps):
            raise ValueError("Scheduled-start record timestamps require whole-second precision")
        if self.updated_at < self.accepted_at:
            raise ValueError("Scheduled-start update time precedes acceptance")
        return self


class PendingScheduledStartRecord(ScheduledStartRecordBase):
    state: Literal[ScheduledStartState.SCHEDULED] = ScheduledStartState.SCHEDULED


class DispatchingScheduledStartRecord(ScheduledStartRecordBase):
    state: Literal[ScheduledStartState.DISPATCHING] = ScheduledStartState.DISPATCHING
    claimed_version: int = Field(ge=1)
    dispatch_started_at: AwareDatetime


class StartedScheduledStartRecord(ScheduledStartRecordBase):
    state: Literal[ScheduledStartState.STARTED] = ScheduledStartState.STARTED
    claimed_version: int = Field(ge=1)
    dispatch_started_at: AwareDatetime
    completed_at: AwareDatetime
    run: ScheduledStartRun


class CanceledScheduledStartRecord(ScheduledStartRecordBase):
    state: Literal[ScheduledStartState.CANCELED] = ScheduledStartState.CANCELED
    completed_at: AwareDatetime


class FailedScheduledStartRecord(ScheduledStartRecordBase):
    state: Literal[ScheduledStartState.FAILED] = ScheduledStartState.FAILED
    claimed_version: int = Field(ge=1)
    dispatch_started_at: AwareDatetime
    completed_at: AwareDatetime
    failure_code: ScheduledStartFailureCode


ScheduledStartRecord: TypeAlias = Annotated[
    PendingScheduledStartRecord
    | DispatchingScheduledStartRecord
    | StartedScheduledStartRecord
    | CanceledScheduledStartRecord
    | FailedScheduledStartRecord,
    Field(discriminator="state"),
]
SCHEDULED_START_RECORD_ADAPTER: TypeAdapter[ScheduledStartRecord] = TypeAdapter(
    ScheduledStartRecord
)
ScheduledStartProjectionRecord: TypeAlias = Annotated[
    PendingScheduledStartRecord
    | StartedScheduledStartRecord
    | CanceledScheduledStartRecord
    | FailedScheduledStartRecord,
    Field(discriminator="state"),
]
SCHEDULED_START_PROJECTION_RECORD_ADAPTER: TypeAdapter[ScheduledStartProjectionRecord] = (
    TypeAdapter(ScheduledStartProjectionRecord)
)
ScheduledStartRecordT = TypeVar("ScheduledStartRecordT", bound=ScheduledStartRecordBase)


class ScheduledStartDescription(StrictScheduledStartModel):
    scheduled_start_id: str
    workflow_name: WorkflowName
    trigger_name: TriggerName
    start_at: AwareDatetime
    workload_class: ScheduledStartWorkloadClass
    state: ScheduledStartState
    version: int = Field(ge=1)
    accepted_at: AwareDatetime
    updated_at: AwareDatetime
    dispatch_started_at: AwareDatetime | None = None
    completed_at: AwareDatetime | None = None
    failure_code: ScheduledStartFailureCode | None = None
    run: ScheduledStartRun | None = None


class ScheduledStartDecision(StrictScheduledStartModel):
    outcome: ScheduledStartDecisionOutcome
    scheduled_start_identity_digest: str = Field(
        min_length=SCHEDULED_START_DIGEST_LENGTH,
        max_length=SCHEDULED_START_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCHEDULED_START_DIGEST_LENGTH}}}$",
    )
    scope_digest: str = Field(
        min_length=SCHEDULED_START_DIGEST_LENGTH,
        max_length=SCHEDULED_START_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCHEDULED_START_DIGEST_LENGTH}}}$",
    )
    workflow_name: WorkflowName
    trigger_name: TriggerName
    source: Literal["control_api"] = "control_api"
    start_at: AwareDatetime
    workload_class: ScheduledStartWorkloadClass
    state: ScheduledStartState
    version: int = Field(ge=1)
    failure_code: ScheduledStartFailureCode | None = None


class ScheduledStartMutationResult(StrictScheduledStartModel):
    status: ScheduledStartMutationStatus
    scheduled_start: ScheduledStartDescription
    expected_version: int | None = Field(default=None, ge=1)


class ScheduledStartPage(StrictScheduledStartModel):
    scheduled_starts: tuple[ScheduledStartDescription, ...]
    next_cursor: str | None = Field(
        default=None,
        max_length=MAX_SCHEDULED_START_CURSOR_LENGTH,
        repr=False,
    )


class ScheduledStartResolvedTarget(StrictScheduledStartModel):
    manifest: DefinitionManifest
    artifact_identity: WorkerArtifactIdentity
    environment_snapshot_digest: str = Field(
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{DEFINITION_DIGEST_LENGTH}}}$",
    )
    scope_digest: str | None = Field(
        default=None,
        min_length=SCHEDULED_START_DIGEST_LENGTH,
        max_length=SCHEDULED_START_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCHEDULED_START_DIGEST_LENGTH}}}$",
    )
    execution_configuration: ExecutionConfigurationIdentity | None = None

    @classmethod
    def from_target(cls, target: WorkflowStartTarget) -> ScheduledStartResolvedTarget:
        return cls(
            manifest=target.manifest,
            artifact_identity=target.deployment.artifact_identity,
            environment_snapshot_digest=target.environment_snapshot_digest,
            scope_digest=target.scope_digest,
            execution_configuration=target.execution_configuration,
        )

    def to_target(self) -> WorkflowStartTarget:
        return WorkflowStartTarget(
            manifest=self.manifest,
            deployment=WorkerDeployment(
                artifact_identity=self.artifact_identity,
                compatible_engine_workflow_abis=frozenset(
                    {self.manifest.required_engine_workflow_abi}
                ),
            ),
            environment_snapshot_digest=self.environment_snapshot_digest,
            scope_digest=self.scope_digest,
            execution_configuration=self.execution_configuration,
        )


class ScheduledStartDueInput(StrictScheduledStartModel):
    record: ScheduledStartProjectionRecord


class ScheduledStartDueClaimed(StrictScheduledStartModel):
    status: Literal["claimed"] = "claimed"
    duplicate: bool = False


class ScheduledStartDueSkipped(StrictScheduledStartModel):
    status: Literal["skipped"] = "skipped"
    current: ScheduledStartDescription


ScheduledStartDueClaimResult: TypeAlias = Annotated[
    ScheduledStartDueClaimed | ScheduledStartDueSkipped,
    Field(discriminator="status"),
]
SCHEDULED_START_DUE_CLAIM_RESULT_ADAPTER: TypeAdapter[ScheduledStartDueClaimResult] = TypeAdapter(
    ScheduledStartDueClaimResult
)


class ScheduledStartArbiterInitialInput(StrictScheduledStartModel):
    phase: Literal[ScheduledStartArbiterPhase.INITIAL] = ScheduledStartArbiterPhase.INITIAL
    record: PendingScheduledStartRecord
    command: ScheduledStartArbiterCommand

    @model_validator(mode="after")
    def validate_command_version(self) -> ScheduledStartArbiterInitialInput:
        command = self.command
        if isinstance(command, ScheduledStartDueCommand):
            if command.nominal_time != self.record.start_at:
                raise ValueError("Due command does not match the retained nominal time")
        elif command.expected_version != self.record.version:
            raise ValueError("Mutation command does not match the retained version")
        return self


class ScheduledStartArbiterResolvedDueInput(StrictScheduledStartModel):
    phase: Literal[ScheduledStartArbiterPhase.RESOLVED_DUE] = (
        ScheduledStartArbiterPhase.RESOLVED_DUE
    )
    record: DispatchingScheduledStartRecord
    target: ScheduledStartResolvedTarget
    saw_ambiguous: bool = False
    authoritative_rejections: int = Field(default=0, ge=0)


class ScheduledStartArbiterTerminalInput(StrictScheduledStartModel):
    phase: Literal[ScheduledStartArbiterPhase.TERMINAL_PENDING_PROJECTION] = (
        ScheduledStartArbiterPhase.TERMINAL_PENDING_PROJECTION
    )
    projection: ScheduledStartProjectionRecord
    consumed_version: int = Field(ge=1)
    winner: ScheduledStartArbiterCommandKind
    request_digest: str | None = Field(
        default=None,
        min_length=SCHEDULED_START_DIGEST_LENGTH,
        max_length=SCHEDULED_START_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCHEDULED_START_DIGEST_LENGTH}}}$",
        repr=False,
    )

    @model_validator(mode="after")
    def validate_projection_version(self) -> ScheduledStartArbiterTerminalInput:
        expected_projection_version = self.consumed_version
        if self.winner is ScheduledStartArbiterCommandKind.DUE:
            valid_projection = isinstance(
                self.projection,
                StartedScheduledStartRecord | FailedScheduledStartRecord,
            )
        elif self.winner is ScheduledStartArbiterCommandKind.CANCEL:
            valid_projection = isinstance(
                self.projection,
                CanceledScheduledStartRecord | StartedScheduledStartRecord,
            )
        else:
            valid_projection = isinstance(
                self.projection,
                PendingScheduledStartRecord | StartedScheduledStartRecord,
            )
            if isinstance(self.projection, PendingScheduledStartRecord):
                expected_projection_version += 1
        if not valid_projection:
            raise ValueError("Terminal projection does not match the winner kind")
        if self.projection.version != expected_projection_version:
            raise ValueError("Terminal projection does not match the consumed version")
        if (self.winner is ScheduledStartArbiterCommandKind.DUE) != (self.request_digest is None):
            raise ValueError("Arbiter request digest does not match the winner kind")
        return self


ScheduledStartArbiterInput: TypeAlias = Annotated[
    ScheduledStartArbiterInitialInput
    | ScheduledStartArbiterResolvedDueInput
    | ScheduledStartArbiterTerminalInput,
    Field(discriminator="phase"),
]
SCHEDULED_START_ARBITER_INPUT_ADAPTER: TypeAdapter[ScheduledStartArbiterInput] = TypeAdapter(
    ScheduledStartArbiterInput
)


class ScheduledStartArbiterPreparedDue(StrictScheduledStartModel):
    status: Literal["due"] = "due"
    record: DispatchingScheduledStartRecord
    target: ScheduledStartResolvedTarget


class ScheduledStartArbiterPreparedProjection(StrictScheduledStartModel):
    status: Literal["projection"] = "projection"
    terminal: ScheduledStartArbiterTerminalInput


class ScheduledStartArbiterPreparedStale(StrictScheduledStartModel):
    status: Literal["stale"] = "stale"
    current: ScheduledStartDescription


ScheduledStartArbiterPreparation: TypeAlias = Annotated[
    ScheduledStartArbiterPreparedDue
    | ScheduledStartArbiterPreparedProjection
    | ScheduledStartArbiterPreparedStale,
    Field(discriminator="status"),
]
SCHEDULED_START_ARBITER_PREPARATION_ADAPTER: TypeAdapter[ScheduledStartArbiterPreparation] = (
    TypeAdapter(ScheduledStartArbiterPreparation)
)


class ScheduledStartAttemptRequest(StrictScheduledStartModel):
    record: DispatchingScheduledStartRecord
    target: ScheduledStartResolvedTarget


class ScheduledStartAttemptAccepted(StrictScheduledStartModel):
    outcome: Literal[
        ScheduledStartAttemptOutcome.ACCEPTED,
        ScheduledStartAttemptOutcome.DUPLICATE,
    ]
    run: ScheduledStartRun


class ScheduledStartAttemptRejected(StrictScheduledStartModel):
    outcome: Literal[ScheduledStartAttemptOutcome.AUTHORITATIVE_REJECTION] = (
        ScheduledStartAttemptOutcome.AUTHORITATIVE_REJECTION
    )


class ScheduledStartAttemptAmbiguous(StrictScheduledStartModel):
    outcome: Literal[ScheduledStartAttemptOutcome.AMBIGUOUS] = (
        ScheduledStartAttemptOutcome.AMBIGUOUS
    )


ScheduledStartAttemptResult: TypeAlias = Annotated[
    ScheduledStartAttemptAccepted | ScheduledStartAttemptRejected | ScheduledStartAttemptAmbiguous,
    Field(discriminator="outcome"),
]
SCHEDULED_START_ATTEMPT_RESULT_ADAPTER: TypeAdapter[ScheduledStartAttemptResult] = TypeAdapter(
    ScheduledStartAttemptResult
)


class ScheduledStartArbiterApplied(StrictScheduledStartModel):
    status: Literal["applied"] = "applied"
    consumed_version: int = Field(ge=1)
    winner: ScheduledStartArbiterCommandKind
    request_digest: str | None = Field(default=None, repr=False)
    scheduled_start: ScheduledStartDescription


class ScheduledStartArbiterStale(StrictScheduledStartModel):
    status: Literal["stale"] = "stale"
    consumed_version: int = Field(ge=1)
    current: ScheduledStartDescription


ScheduledStartArbiterResult: TypeAlias = Annotated[
    ScheduledStartArbiterApplied | ScheduledStartArbiterStale,
    Field(discriminator="status"),
]
SCHEDULED_START_ARBITER_RESULT_ADAPTER: TypeAdapter[ScheduledStartArbiterResult] = TypeAdapter(
    ScheduledStartArbiterResult
)


def create_scheduled_start_record(
    request: ScheduledStartCreateRequest,
    *,
    scheduled_start_id: str,
    scope: RuntimeScope,
    trigger_name: TriggerName,
    request_digest: str,
    accepted_at: datetime,
    normalized_input: dict[str, object],
    dispatch_timeout_seconds: int = DEFAULT_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
    dispatch_attempts: int = DEFAULT_SCHEDULE_DISPATCH_ATTEMPTS,
) -> PendingScheduledStartRecord:
    return PendingScheduledStartRecord(
        scheduled_start_id=scheduled_start_id,
        scope_digest=scope.digest,
        workflow_name=request.workflow_name,
        input=normalized_input,
        business_request_id=request.business_request_id,
        start_at=request.start_at,
        workload_class=request.workload_class,
        trigger_name=trigger_name,
        create_request_digest=request_digest,
        version=1,
        accepted_at=accepted_at,
        updated_at=accepted_at,
        last_mutation_kind=ScheduledStartMutationKind.CREATE,
        last_mutation_digest=request_digest,
        dispatch_timeout_seconds=dispatch_timeout_seconds,
        dispatch_attempts=dispatch_attempts,
    )


def reschedule_scheduled_start(
    record: ScheduledStartRecord,
    request: ScheduledStartRescheduleRequest,
    *,
    request_digest: str,
    updated_at: datetime,
) -> tuple[PendingScheduledStartRecord, bool]:
    if (
        isinstance(record, PendingScheduledStartRecord)
        and record.version == request.expected_version + 1
        and record.last_mutation_kind is ScheduledStartMutationKind.RESCHEDULE
        and record.last_mutation_digest == request_digest
    ):
        return record, True
    if not isinstance(record, PendingScheduledStartRecord) or (
        record.version != request.expected_version
    ):
        raise ScheduledStartError(
            ScheduledStartErrorCode.CONFLICT,
            "Scheduled start is no longer pending at the expected version",
            retryable=False,
        )
    return _transition_record(
        PendingScheduledStartRecord,
        record,
        {
            "state": ScheduledStartState.SCHEDULED,
            "start_at": request.start_at,
            "version": record.version + 1,
            "updated_at": updated_at,
            "last_mutation_kind": ScheduledStartMutationKind.RESCHEDULE,
            "last_mutation_digest": request_digest,
        },
    ), False


def cancel_scheduled_start(
    record: ScheduledStartRecord,
    request: ScheduledStartCancelRequest,
    *,
    request_digest: str,
    updated_at: datetime,
) -> tuple[CanceledScheduledStartRecord, bool]:
    if (
        isinstance(record, CanceledScheduledStartRecord)
        and record.version == request.expected_version
        and record.last_mutation_kind is ScheduledStartMutationKind.CANCEL
        and record.last_mutation_digest == request_digest
    ):
        return record, True
    if not isinstance(record, PendingScheduledStartRecord) or (
        record.version != request.expected_version
    ):
        raise ScheduledStartError(
            ScheduledStartErrorCode.CONFLICT,
            "Scheduled start is no longer pending at the expected version",
            retryable=False,
        )
    return _transition_record(
        CanceledScheduledStartRecord,
        record,
        {
            "state": ScheduledStartState.CANCELED,
            "updated_at": updated_at,
            "last_mutation_kind": ScheduledStartMutationKind.CANCEL,
            "last_mutation_digest": request_digest,
            "completed_at": updated_at,
        },
    ), False


def claim_scheduled_start(
    record: ScheduledStartRecord,
    *,
    expected_version: int,
    claimed_at: datetime,
) -> DispatchingScheduledStartRecord | None:
    if (
        isinstance(record, DispatchingScheduledStartRecord)
        and record.claimed_version == expected_version
    ):
        return record
    if not isinstance(record, PendingScheduledStartRecord) or record.version != expected_version:
        return None
    claim_digest = scheduled_start_request_digest(
        "dispatch",
        record.scheduled_start_id,
        expected_version,
    )
    return _transition_record(
        DispatchingScheduledStartRecord,
        record,
        {
            "state": ScheduledStartState.DISPATCHING,
            "updated_at": claimed_at,
            "last_mutation_kind": ScheduledStartMutationKind.DISPATCH,
            "last_mutation_digest": claim_digest,
            "claimed_version": expected_version,
            "dispatch_started_at": claimed_at,
        },
    )


def complete_scheduled_start(
    record: DispatchingScheduledStartRecord,
    run: ScheduledStartRun,
    *,
    completed_at: datetime,
) -> StartedScheduledStartRecord:
    completion_digest = scheduled_start_request_digest(
        "start",
        record.scheduled_start_id,
        record.claimed_version,
        run.workflow_id,
        run.run_id,
    )
    return _transition_record(
        StartedScheduledStartRecord,
        record,
        {
            "state": ScheduledStartState.STARTED,
            "updated_at": completed_at,
            "last_mutation_kind": ScheduledStartMutationKind.START,
            "last_mutation_digest": completion_digest,
            "completed_at": completed_at,
            "run": run,
        },
    )


def fail_scheduled_start(
    record: DispatchingScheduledStartRecord,
    failure_code: ScheduledStartFailureCode,
    *,
    completed_at: datetime,
) -> FailedScheduledStartRecord:
    failure_digest = scheduled_start_request_digest(
        "fail",
        record.scheduled_start_id,
        record.claimed_version,
        failure_code.value,
    )
    return _transition_record(
        FailedScheduledStartRecord,
        record,
        {
            "state": ScheduledStartState.FAILED,
            "updated_at": completed_at,
            "last_mutation_kind": ScheduledStartMutationKind.FAIL,
            "last_mutation_digest": failure_digest,
            "completed_at": completed_at,
            "failure_code": failure_code,
        },
    )


def describe_scheduled_start(record: ScheduledStartRecord) -> ScheduledStartDescription:
    return ScheduledStartDescription(
        scheduled_start_id=record.scheduled_start_id,
        workflow_name=record.workflow_name,
        trigger_name=record.trigger_name,
        start_at=record.start_at,
        workload_class=record.workload_class,
        state=record.state,
        version=record.version,
        accepted_at=record.accepted_at,
        updated_at=record.updated_at,
        dispatch_started_at=(
            record.dispatch_started_at
            if isinstance(
                record,
                (
                    DispatchingScheduledStartRecord,
                    StartedScheduledStartRecord,
                    FailedScheduledStartRecord,
                ),
            )
            else None
        ),
        completed_at=(
            record.completed_at
            if isinstance(
                record,
                (
                    StartedScheduledStartRecord,
                    CanceledScheduledStartRecord,
                    FailedScheduledStartRecord,
                ),
            )
            else None
        ),
        failure_code=(
            record.failure_code if isinstance(record, FailedScheduledStartRecord) else None
        ),
        run=record.run if isinstance(record, StartedScheduledStartRecord) else None,
    )


def make_scheduled_start_id(
    scope: RuntimeScope,
    workflow_name: str,
    business_request_id: str,
) -> str:
    return scoped_identity_from_digest(
        "scheduled-start",
        scope.digest,
        workflow_name,
        business_request_id,
    )


def make_scheduled_start_schedule_id(scope: RuntimeScope, scheduled_start_id: str) -> str:
    return scoped_identity_from_digest(
        "scheduled-start-schedule",
        scope.digest,
        scheduled_start_id,
    )


def make_scheduled_start_arbiter_id(
    scope: RuntimeScope,
    scheduled_start_id: str,
    version: int,
) -> str:
    return scoped_identity_from_digest(
        "scheduled-start-arbiter",
        scope.digest,
        scheduled_start_id,
        str(version),
    )


def make_scheduled_start_due_id(
    scope: RuntimeScope,
    scheduled_start_id: str,
    version: int,
) -> str:
    return scoped_identity_from_digest(
        "scheduled-start-due",
        scope.digest,
        scheduled_start_id,
        str(version),
    )


def scheduled_start_request_digest(*parts: object) -> str:
    payload = json.dumps(
        parts,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _transition_record(
    model: type[ScheduledStartRecordT],
    record: ScheduledStartRecordBase,
    updates: dict[str, object],
) -> ScheduledStartRecordT:
    payload = record.model_dump(mode="python")
    payload.update(updates)
    return model.model_validate(payload)
