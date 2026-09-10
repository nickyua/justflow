"""Strict, versioned contracts for asynchronous workflow messages."""

from __future__ import annotations

import hashlib
import json
from enum import Enum
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from justflow.provenance import ExecutionConfigurationIdentity, WorkerArtifactIdentity
from justflow.scope import (
    SCOPE_DIGEST_LENGTH,
    RuntimeScope,
    scoped_identity,
    scoped_identity_from_digest,
)

PROTOCOL_VERSION: Literal["1"] = "1"
STEP_RESPONSE_SIGNAL = "step_response"
WORKFLOW_EVENT_SIGNAL = "workflow_event"
MAX_IDENTIFIER_LENGTH = 256
MAX_ACTION_LENGTH = 512
DEFINITION_DIGEST_LENGTH = 64


class StrictEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MessageKind(str, Enum):
    TRIGGER = "trigger"
    STEP_REQUEST = "step_request"
    STEP_RESPONSE = "step_response"
    EVENT = "event"


class ResponseStatus(str, Enum):
    SUCCESS = "success"
    ERROR = "error"


def _stable_identity(*parts: str) -> str:
    encoded = json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def make_workflow_id(
    workflow_name: str,
    business_request_id: str,
    *,
    scope: RuntimeScope | None = None,
) -> str:
    """Namespace a business request identity by its logical workflow."""
    if scope is None:
        return _stable_identity("workflow", workflow_name, business_request_id)
    return scoped_identity("workflow", scope, workflow_name, business_request_id)


def make_trigger_message_id(
    workflow_name: str,
    definition_digest: str,
    business_request_id: str,
    *,
    scope: RuntimeScope | None = None,
) -> str:
    if scope is None:
        return _stable_identity("trigger", workflow_name, definition_digest, business_request_id)
    return scoped_identity(
        "trigger",
        scope,
        workflow_name,
        definition_digest,
        business_request_id,
    )


def make_step_invocation_id(
    workflow_id: str,
    workflow_run_id: str,
    step_name: str,
) -> str:
    return _stable_identity("step", workflow_id, workflow_run_id, step_name)


def make_step_request_message_id(step_invocation_id: str) -> str:
    return _stable_identity("step_request", step_invocation_id)


def make_signal_key(request_id: str, step_name: str, action: str) -> str:
    return f"{request_id}:{step_name}:{action}"


class CommonEnvelope(StrictEnvelope):
    protocol_version: Literal["1"]
    message_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    kind: MessageKind
    workflow_name: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    definition_digest: str = Field(
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
    )
    workflow_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    correlation_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    causation_id: str | None = Field(None, min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    trace_id: str | None = Field(None, min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    scope_digest: str | None = Field(
        default=None,
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$",
    )


class TriggerEnvelope(CommonEnvelope):
    """External request to start one immutable workflow definition."""

    kind: Literal[MessageKind.TRIGGER]
    business_request_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    input: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_workflow_identity(self) -> TriggerEnvelope:
        expected = (
            _stable_identity("workflow", self.workflow_name, self.business_request_id)
            if self.scope_digest is None
            else scoped_identity_from_digest(
                "workflow",
                self.scope_digest,
                self.workflow_name,
                self.business_request_id,
            )
        )
        if self.workflow_id != expected:
            raise ValueError("workflow_id does not match workflow and business request identity")
        return self


class StepRequestPayload(StrictEnvelope):
    input: Any = None
    globals: dict[str, Any] = Field(default_factory=dict)


class StepRequestEnvelope(CommonEnvelope):
    """Request for a service to execute one exact workflow step invocation."""

    kind: Literal[MessageKind.STEP_REQUEST]
    workflow_run_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    step_invocation_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    step_name: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    action: str = Field(min_length=1, max_length=MAX_ACTION_LENGTH)
    payload: StepRequestPayload
    reply_destination: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)

    @model_validator(mode="after")
    def validate_step_identity(self) -> StepRequestEnvelope:
        expected_invocation_id = make_step_invocation_id(
            self.workflow_id,
            self.workflow_run_id,
            self.step_name,
        )
        if self.step_invocation_id != expected_invocation_id:
            raise ValueError("step_invocation_id does not match its workflow execution")
        expected_message_id = make_step_request_message_id(expected_invocation_id)
        if self.message_id != expected_message_id:
            raise ValueError("message_id does not match its step invocation")
        return self


class StepSuccessBody(StrictEnvelope):
    status: Literal[ResponseStatus.SUCCESS] = ResponseStatus.SUCCESS
    output: Any = None


class StepErrorBody(StrictEnvelope):
    status: Literal[ResponseStatus.ERROR] = ResponseStatus.ERROR
    code: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    message: str = Field(min_length=1, max_length=MAX_ACTION_LENGTH)
    retryable: bool = False


StepResponseBody: TypeAlias = Annotated[
    StepSuccessBody | StepErrorBody,
    Field(discriminator="status"),
]


class StepResponseEnvelope(CommonEnvelope):
    """Response for one exact step request and Temporal execution."""

    kind: Literal[MessageKind.STEP_RESPONSE]
    workflow_run_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    in_reply_to: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    step_invocation_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    step_name: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    action: str = Field(min_length=1, max_length=MAX_ACTION_LENGTH)
    body: StepResponseBody

    @model_validator(mode="after")
    def validate_reply_identity(self) -> StepResponseEnvelope:
        expected_invocation_id = make_step_invocation_id(
            self.workflow_id,
            self.workflow_run_id,
            self.step_name,
        )
        if self.step_invocation_id != expected_invocation_id:
            raise ValueError("step_invocation_id does not match its workflow execution")
        expected_request_id = make_step_request_message_id(expected_invocation_id)
        if self.in_reply_to != expected_request_id:
            raise ValueError("in_reply_to does not match the step invocation")
        if self.causation_id != self.in_reply_to:
            raise ValueError("causation_id must identify the step request")
        return self


class EventEnvelope(CommonEnvelope):
    """External event targeted at one exact Temporal workflow execution."""

    kind: Literal[MessageKind.EVENT]
    workflow_run_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    event_name: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    payload: Any = None


AsyncEnvelope: TypeAlias = Annotated[
    TriggerEnvelope | StepRequestEnvelope | StepResponseEnvelope | EventEnvelope,
    Field(discriminator="kind"),
]
ASYNC_ENVELOPE_ADAPTER: TypeAdapter[AsyncEnvelope] = TypeAdapter(AsyncEnvelope)


def parse_async_envelope(value: Any) -> AsyncEnvelope:
    return ASYNC_ENVELOPE_ADAPTER.validate_python(value)


class WorkflowTrigger(StrictEnvelope):
    """The argument every compiled workflow's run method receives."""

    request_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    globals: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = Field(None, min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    trace_id: str | None = Field(None, min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    definition_digest: str | None = Field(
        None,
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
    )
    worker_deployment: str | None = Field(None, min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    worker_build_id: str | None = Field(None, min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    worker_artifact: WorkerArtifactIdentity | None = None
    environment_snapshot_digest: str | None = Field(
        None,
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{DEFINITION_DIGEST_LENGTH}}}$",
    )
    execution_configuration: ExecutionConfigurationIdentity | None = None
    trigger_source: (
        Literal[
            "broker",
            "cloud_event",
            "control_api",
            "host",
            "schedule",
            "webhook",
        ]
        | None
    ) = Field(default=None, min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    trigger_name: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
    )
    source_identity_digest: str | None = Field(
        default=None,
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{DEFINITION_DIGEST_LENGTH}}}$",
    )
    correlation_identity_digest: str | None = Field(
        default=None,
        min_length=DEFINITION_DIGEST_LENGTH,
        max_length=DEFINITION_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{DEFINITION_DIGEST_LENGTH}}}$",
    )
    scope_digest: str | None = Field(
        default=None,
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$",
    )

    @model_validator(mode="after")
    def validate_worker_artifact(self) -> WorkflowTrigger:
        if self.worker_artifact is None:
            return self
        if self.worker_deployment != self.worker_artifact.deployment_name:
            raise ValueError("worker_deployment does not match worker_artifact")
        if self.worker_build_id != self.worker_artifact.build_id:
            raise ValueError("worker_build_id does not match worker_artifact")
        return self


class SignalPayload(StrictEnvelope):
    """Payload of the step-response Temporal signal."""

    request_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    step_name: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    action: str = Field(min_length=1, max_length=MAX_ACTION_LENGTH)
    status: ResponseStatus
    step_response: Any = None
    error: dict[str, Any] | None = None


class EventPayload(StrictEnvelope):
    """Payload of the workflow-event Temporal signal."""

    signal: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    data: Any = None
