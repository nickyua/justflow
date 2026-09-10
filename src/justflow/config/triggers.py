"""Strict authored trigger declarations."""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Literal, Self, TypeAlias
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, StrictBool, StrictInt, StrictStr, model_validator

from justflow.config.grammar import ProviderName, TriggerName, WorkflowName
from justflow.config.models import StrictDeclarationModel
from justflow.config.schedules import (
    BackfillPolicy,
    IntervalScheduleSpec,
    ScheduleOverlapPolicy,
    ScheduleSpec,
)

DEFINITION_DIGEST_PATTERN = r"^[0-9a-f]{64}$"
MIN_CATCH_UP_WINDOW_SECONDS = 10
MAX_CATCH_UP_WINDOW_SECONDS = 31_536_000
DEFAULT_CATCH_UP_WINDOW_SECONDS = 60
UTC_TIMEZONE = "UTC"


class TriggerKind(str, Enum):
    API = "api"
    SCHEDULE = "schedule"
    WEBHOOK = "webhook"
    EVENT = "event"
    BROKER = "broker"
    HOST = "host"


class TriggerDeclarationBase(StrictDeclarationModel):
    workflow: WorkflowName
    paused: StrictBool = False


class ApiTriggerDeclaration(TriggerDeclarationBase):
    kind: Literal[TriggerKind.API] = TriggerKind.API


class ScheduleTriggerDeclaration(TriggerDeclarationBase):
    kind: Literal[TriggerKind.SCHEDULE] = TriggerKind.SCHEDULE
    definition_digest: (
        Annotated[
            StrictStr,
            Field(pattern=DEFINITION_DIGEST_PATTERN),
        ]
        | None
    ) = None
    input: dict[str, Any] = Field(default_factory=dict)
    spec: ScheduleSpec
    timezone: StrictStr = Field(min_length=1, max_length=128, default=UTC_TIMEZONE)
    overlap_policy: ScheduleOverlapPolicy = ScheduleOverlapPolicy.SKIP
    catch_up_window_seconds: StrictInt = Field(
        default=DEFAULT_CATCH_UP_WINDOW_SECONDS,
        ge=MIN_CATCH_UP_WINDOW_SECONDS,
        le=MAX_CATCH_UP_WINDOW_SECONDS,
    )
    backfill: BackfillPolicy = Field(default_factory=BackfillPolicy)

    @model_validator(mode="after")
    def validate_timezone_semantics(self) -> Self:
        try:
            ZoneInfo(self.timezone)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise ValueError("schedule timezone must be an installed IANA timezone") from exc
        if isinstance(self.spec, IntervalScheduleSpec) and self.timezone != UTC_TIMEZONE:
            raise ValueError("interval schedules use absolute time and require timezone 'UTC'")
        return self


class WebhookTriggerDeclaration(TriggerDeclarationBase):
    kind: Literal[TriggerKind.WEBHOOK] = TriggerKind.WEBHOOK
    source: ProviderName


class EventTriggerDeclaration(TriggerDeclarationBase):
    kind: Literal[TriggerKind.EVENT] = TriggerKind.EVENT
    mapping: ProviderName


class BrokerTriggerDeclaration(TriggerDeclarationBase):
    kind: Literal[TriggerKind.BROKER] = TriggerKind.BROKER
    broker: ProviderName


class HostTriggerDeclaration(TriggerDeclarationBase):
    kind: Literal[TriggerKind.HOST] = TriggerKind.HOST
    adapter: ProviderName


TriggerDeclaration: TypeAlias = Annotated[
    ApiTriggerDeclaration
    | ScheduleTriggerDeclaration
    | WebhookTriggerDeclaration
    | EventTriggerDeclaration
    | BrokerTriggerDeclaration
    | HostTriggerDeclaration,
    Field(discriminator="kind"),
]


class TriggersConfig(StrictDeclarationModel):
    triggers: dict[TriggerName, TriggerDeclaration]

    @model_validator(mode="after")
    def validate_api_uniqueness(self) -> Self:
        api_workflows = [
            declaration.workflow
            for declaration in self.triggers.values()
            if isinstance(declaration, ApiTriggerDeclaration)
        ]
        if len(api_workflows) != len(set(api_workflows)):
            raise ValueError("a workflow may declare at most one api trigger")
        source_bindings = [
            (declaration.workflow, declaration.kind, binding)
            for declaration in self.triggers.values()
            if (binding := _source_binding(declaration)) is not None
        ]
        if len(source_bindings) != len(set(source_bindings)):
            raise ValueError("a workflow trigger source binding must be unique")
        return self

    @property
    def schedules(self) -> dict[TriggerName, ScheduleTriggerDeclaration]:
        return {
            name: declaration
            for name, declaration in self.triggers.items()
            if isinstance(declaration, ScheduleTriggerDeclaration)
        }


def _source_binding(declaration: TriggerDeclaration) -> ProviderName | None:
    if isinstance(declaration, WebhookTriggerDeclaration):
        return declaration.source
    if isinstance(declaration, EventTriggerDeclaration):
        return declaration.mapping
    if isinstance(declaration, BrokerTriggerDeclaration):
        return declaration.broker
    if isinstance(declaration, HostTriggerDeclaration):
        return declaration.adapter
    return None
