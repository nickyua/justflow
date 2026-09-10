"""Typed state carried between Temporal workflow runs."""

from __future__ import annotations

from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from justflow.engine.audit import FailureDetail, StepFailure, TransitionRecord
from justflow.sdk.message_contract import MAX_IDENTIFIER_LENGTH, WorkflowTrigger

CONTINUATION_VERSION: Literal[1] = 1
CONTINUATION_KIND: Literal["justflow-continuation"] = "justflow-continuation"


class FrozenContinuationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class FailureEpisodeCheckpoint(FrozenContinuationModel):
    primary: FailureDetail
    handler: str = Field(min_length=1)
    handler_steps: frozenset[str]


class IncludedIterationResult(FrozenContinuationModel):
    kind: Literal["included"] = "included"
    key: int | str
    value: Any


class OmittedIterationResult(FrozenContinuationModel):
    kind: Literal["omitted"] = "omitted"
    key: int | str


IterationResultCheckpoint = Annotated[
    IncludedIterationResult | OmittedIterationResult,
    Field(discriminator="kind"),
]


class IterationCheckpoint(FrozenContinuationModel):
    kind: Literal["for_each"] = "for_each"
    step_name: str = Field(min_length=1)
    started_at_epoch: float = Field(allow_inf_nan=False)
    next_offset: int = Field(ge=0)
    results: tuple[IterationResultCheckpoint, ...]
    failed: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_progress(self) -> Self:
        if len(self.results) != self.next_offset:
            raise ValueError("Iteration continuation results do not match its offset")
        if self.failed > self.next_offset:
            raise ValueError("Iteration continuation failure count exceeds processed items")
        return self


class UntilCheckpoint(FrozenContinuationModel):
    kind: Literal["until"] = "until"
    step_name: str = Field(min_length=1)
    started_at_epoch: float = Field(allow_inf_nan=False)
    attempts_completed: int = Field(ge=1)
    last_result: Any


LoopCheckpoint = Annotated[
    IterationCheckpoint | UntilCheckpoint,
    Field(discriminator="kind"),
]


class RunnerCheckpoint(FrozenContinuationModel):
    version: Literal[1] = CONTINUATION_VERSION
    started_at: str = Field(min_length=1)
    params: dict[str, Any]
    step_outputs: dict[str, Any]
    step_timings: dict[str, dict[str, Any]]
    transitions: tuple[TransitionRecord, ...]
    failures: tuple[StepFailure, ...]
    active_episode: FailureEpisodeCheckpoint | None = None
    next_step_name: str = Field(min_length=1)
    next_seq: int = Field(ge=0)
    invocations: int = Field(ge=0)
    loop: LoopCheckpoint | None = None


class WorkflowContinuationInput(FrozenContinuationModel):
    kind: Literal["justflow-continuation"] = CONTINUATION_KIND
    version: Literal[1] = CONTINUATION_VERSION
    sequence: int = Field(ge=1)
    previous_run_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    trigger: WorkflowTrigger
    checkpoint: RunnerCheckpoint
    signals: dict[str, list[dict[str, Any]]]
    events: dict[str, list[Any]]
    observed_history_events: int = Field(ge=0)
    observed_history_bytes: int = Field(ge=0)
    server_suggested: bool

    @model_validator(mode="after")
    def validate_request_identity(self) -> Self:
        request_id = self.checkpoint.params.get("request_id")
        if request_id != self.trigger.request_id:
            raise ValueError("Continuation checkpoint request identity does not match its trigger")
        return self
