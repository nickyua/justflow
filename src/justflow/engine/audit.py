"""Typed models for the audit record and runner-internal payloads.

The audit record crosses the Temporal serialization boundary and is archived
as JSON, so models are dumped to plain dicts at the boundary — but every
shape is constructed through these models, never as ad-hoc dicts.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from justflow.provenance import ExecutionConfigurationIdentity, WorkerArtifactIdentity


class TransitionRecord(BaseModel):
    """One routing decision: which branch/handler fired and where it led."""

    step: str
    matched: str  # when-expression | "default" | "timeout" | "exhausted" | "error"
    target: str


class StepFailure(BaseModel):
    """Stable engine and underlying cause identity exposed to handlers."""

    step: str
    code: str
    cause_code: str
    message: str


class FailureCategory(str, Enum):
    INPUT = "input"
    DEFINITION = "definition"
    CONTRACT = "contract"
    EXECUTION = "execution"
    INFRASTRUCTURE = "infrastructure"
    INTERNAL = "internal"


class FailurePhase(str, Enum):
    TRIGGER = "trigger"
    SIGNAL = "signal"
    WORKFLOW_INPUT = "workflow_input"
    PARAMETERS = "parameters"
    STEP = "step"
    HANDLER = "handler"
    RESULT = "result"
    ARCHIVE = "archive"
    INTERNAL = "internal"


class FailureDetail(BaseModel):
    code: str
    cause_code: str
    category: FailureCategory
    phase: FailurePhase
    message: str
    retryable: bool
    step: str | None = None


class FailureMetadata(BaseModel):
    code: str
    cause_code: str
    category: FailureCategory
    phase: FailurePhase
    retryable: bool
    step: str | None = None

    @classmethod
    def from_detail(cls, detail: FailureDetail) -> FailureMetadata:
        return cls(
            code=detail.code,
            cause_code=detail.cause_code,
            category=detail.category,
            phase=detail.phase,
            retryable=detail.retryable,
            step=detail.step,
        )


class CorrelationIdentity(BaseModel):
    workflow: str
    request_id: str | None = None
    run_id: str | None = None
    definition_digest: str | None = None
    worker_deployment: str | None = None
    worker_build_id: str | None = None
    worker_artifact: WorkerArtifactIdentity | None = None
    environment_snapshot_digest: str | None = None
    trigger_source: str | None = None
    trigger_name: str | None = None
    source_identity_digest: str | None = None
    correlation_identity_digest: str | None = None
    scope_digest: str | None = None
    execution_configuration: ExecutionConfigurationIdentity | None = None


class FailureStepMetadata(BaseModel):
    name: str
    seq: int
    status: str
    code: str | None = None


class FailureTransitionMetadata(BaseModel):
    step: str
    matched: str
    target: str


class FailureRecord(BaseModel):
    """Bounded metadata retained in Temporal failure details."""

    audit_version: int
    serialization_version: int
    correlation: CorrelationIdentity
    status: Literal["failed"] = "failed"
    error: FailureMetadata
    secondary: list[FailureMetadata] = Field(default_factory=list)
    steps: list[FailureStepMetadata] = Field(default_factory=list)
    transitions: list[FailureTransitionMetadata] = Field(default_factory=list)
    started_at: str
    failed_at: str

    def dump(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


class IterationFailure(BaseModel):
    """A collected per-item failure (on_iteration_fail: collect)."""

    model_config = ConfigDict(populate_by_name=True)

    error: bool = Field(default=True, serialization_alias="_error")
    code: str
    message: str


class StepAuditEntry(BaseModel):
    """One step's audit entry; populated fields depend on the step's status.

    Dump with exclude_unset so each status keeps exactly its own shape.
    """

    seq: int
    status: str
    globals: dict[str, Any] | None = None
    input: Any = None
    output: Any = None
    started_at: str | None = None
    duration_ms: int | None = None
    condition: str | None = None
    then: str | None = None
    signal: str | None = None
    sleep_sec: int | None = None
    attempts: int | None = None
    cache: str | None = None
    items_total: int | None = None
    items_failed: int | None = None
    code: str | None = None
    message: str | None = None

    def dump(self) -> dict[str, Any]:
        return self.model_dump(exclude_unset=True)


class AuditRecord(BaseModel):
    """The complete run record; `error`/`result` appear only when set."""

    audit_version: int
    serialization_version: int
    request_id: str
    workflow: str
    definition_digest: str | None = None
    worker_deployment: str | None = None
    worker_build_id: str | None = None
    worker_artifact: WorkerArtifactIdentity | None = None
    environment_snapshot_digest: str | None = None
    trigger_source: str | None = None
    trigger_name: str | None = None
    source_identity_digest: str | None = None
    correlation_identity_digest: str | None = None
    scope_digest: str | None = None
    execution_configuration: ExecutionConfigurationIdentity | None = None
    status: str
    reason: str | None
    params: dict[str, Any]
    steps: dict[str, dict[str, Any]]
    transitions: list[TransitionRecord]
    started_at: str
    completed_at: str
    error: StepFailure | None = None
    failures: list[StepFailure] | None = None
    result: Any = None

    def dump(self) -> dict[str, Any]:
        return self.model_dump(exclude_unset=True)
