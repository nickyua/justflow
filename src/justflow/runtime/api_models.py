"""Typed public HTTP request and response models."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from justflow.config.grammar import SignalName, TriggerName, WorkflowName
from justflow.configuration.activation import (
    ActivationCheckpointKind,
    ActivationState,
)
from justflow.configuration.local_authoring import LOCAL_AUTHORING_VERSION_BYTES
from justflow.runtime.api_compatibility import ApiCompatibility
from justflow.runtime.schedule_reconciler import (
    ScheduleApplyErrorCode,
    ScheduleApplyStatus,
)
from justflow.runtime.scheduled_starts import ScheduledStartMutationResult
from justflow.runtime.schedules import ScheduleChangeKind
from justflow.runtime.starter import StartWorkflowResult
from justflow.scope import RuntimeScope

MAX_API_STATUS_LENGTH = 64
MAX_API_ERROR_CODE_LENGTH = 128
MAX_API_ERROR_MESSAGE_LENGTH = 1_024
MAX_API_STATUS_URL_LENGTH = 1_024
MAX_API_FRAGMENT_BYTES = 1_048_576
MAX_API_FRAGMENT_VERSION = 2 ** (8 * LOCAL_AUTHORING_VERSION_BYTES)
MAX_API_APPLY_ITEMS = 1_000


class StrictApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class StartApiRequest(StrictApiModel):
    workflow_name: WorkflowName
    business_request_id: str = Field(min_length=1, max_length=128, repr=False)
    input: dict[str, object] = Field(default_factory=dict, repr=False)
    definition_digest: str | None = None
    correlation_id: str | None = Field(default=None, min_length=1, max_length=128, repr=False)
    trace_id: str | None = Field(default=None, min_length=1, max_length=128, repr=False)


class SignalApiRequest(StrictApiModel):
    event_name: SignalName
    payload: object = Field(repr=False)


class SignalEventApiRequest(StrictApiModel):
    payload: object = Field(repr=False)


class TriggerPathApiRequest(StrictApiModel):
    trigger_name: TriggerName


class TriggerDeleteApiRequest(StrictApiModel):
    confirmation: str = Field(min_length=1, max_length=128, repr=False)


class ApiErrorDetail(StrictApiModel):
    code: str = Field(min_length=1, max_length=MAX_API_ERROR_CODE_LENGTH)
    message: str = Field(min_length=1, max_length=MAX_API_ERROR_MESSAGE_LENGTH)


class ApiErrorResponse(StrictApiModel):
    error: ApiErrorDetail


class ProbeApiResponse(StrictApiModel):
    status: Literal["live", "ready", "unavailable"]


class StatusApiResponse(StrictApiModel):
    status: str = Field(min_length=1, max_length=MAX_API_STATUS_LENGTH)


class StartWorkflowApiResponse(StartWorkflowResult):
    status_url: str = Field(min_length=1, max_length=MAX_API_STATUS_URL_LENGTH)


class ScheduledStartCreateApiResponse(ScheduledStartMutationResult):
    status_url: str = Field(min_length=1, max_length=MAX_API_STATUS_URL_LENGTH)


class ApiCompatibilityResponse(StrictApiModel):
    current_version: int = Field(ge=1)
    minimum_supported_client_version: int = Field(ge=1)
    maximum_supported_client_version: int = Field(ge=1)

    @classmethod
    def from_contract(cls, compatibility: ApiCompatibility) -> ApiCompatibilityResponse:
        return cls(**compatibility.public_dict())


class CapabilitiesApiResponse(StrictApiModel):
    api_compatibility: ApiCompatibilityResponse
    operations_view: Literal[True] = True
    scope: RuntimeScope
    configuration_mode: Literal["managed", "local_source", "unavailable"]
    configuration_view: bool
    configuration_edit: bool
    configuration_validate: bool
    configuration_apply: bool
    configuration_discard: bool
    configuration_publish: bool
    configuration_activate: bool
    configuration_rollback: bool
    workflow_start: bool
    workflow_signal: bool
    workflow_cancel: bool
    workflow_terminate: bool
    trigger_pause: bool
    trigger_resume: bool
    trigger_run: bool
    trigger_delete: bool
    scheduled_start_create: bool
    scheduled_start_view: bool
    scheduled_start_reschedule: bool
    scheduled_start_cancel: bool
    trigger_apply: bool


class TriggerApplyItemApiResponse(StrictApiModel):
    schedule_id: str = Field(min_length=1, max_length=1_000)
    trigger_name: str | None = Field(default=None, max_length=128)
    change: ScheduleChangeKind
    status: ScheduleApplyStatus
    error_code: ScheduleApplyErrorCode | None = None


class TriggerApplyApiResponse(StrictApiModel):
    plan_digest: str = Field(min_length=1, max_length=128)
    successful: bool
    items: tuple[TriggerApplyItemApiResponse, ...] = Field(max_length=MAX_API_APPLY_ITEMS)


class ActivationReadinessApiResponse(StrictApiModel):
    activation_id: str = Field(min_length=1, max_length=128)
    ready: bool
    state: ActivationState
    target_revision_id: str = Field(min_length=1, max_length=128)
    worker_readiness_registered: bool
    completed_checkpoints: tuple[ActivationCheckpointKind, ...]


class TriggersFragmentApiResponse(StrictApiModel):
    scope_digest: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    version: int = Field(ge=1, le=MAX_API_FRAGMENT_VERSION)
    document: str = Field(max_length=MAX_API_FRAGMENT_BYTES)


class WorkflowFragmentApiResponse(TriggersFragmentApiResponse):
    workflow: WorkflowName
