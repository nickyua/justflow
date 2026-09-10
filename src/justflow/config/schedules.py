"""Strict authored schedule declarations."""

from __future__ import annotations

import re
from enum import Enum
from typing import Annotated, Literal, Self

from pydantic import Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator

from justflow.config.models import StrictDeclarationModel

MAX_CRON_EXPRESSIONS = 10
MAX_CRON_EXPRESSION_LENGTH = 512
MAX_CRON_LIST_ITEMS = 64
MAX_CALENDAR_RANGES = 10
MIN_SCHEDULE_YEAR = 2000
MAX_SCHEDULE_YEAR = 2100
MAX_INTERVAL_SECONDS = 31_536_000
MAX_BACKFILL_WINDOW_SECONDS = 31_536_000
MAX_BACKFILL_ACTIONS = 10_000
DEFAULT_BACKFILL_WINDOW_SECONDS = 86_400
DEFAULT_BACKFILL_ACTIONS = 1_000
CRON_FIELD_BOUNDS = (
    ("minute", 0, 59),
    ("hour", 0, 23),
    ("day of month", 1, 31),
    ("month", 1, 12),
    ("day of week", 0, 6),
)
CRON_ATOM_PATTERN = re.compile(r"^(?P<start>\*|[0-9]+)(?:-(?P<end>[0-9]+))?(?:/(?P<step>[0-9]+))?$")


class ScheduleOverlapPolicy(str, Enum):
    SKIP = "skip"
    BUFFER_ONE = "buffer_one"
    BUFFER_ALL = "buffer_all"
    CANCEL_OTHER = "cancel_other"
    TERMINATE_OTHER = "terminate_other"
    ALLOW_ALL = "allow_all"


class CalendarRange(StrictDeclarationModel):
    start: StrictInt
    end: StrictInt | None = None
    step: StrictInt = Field(default=1, ge=1)

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if self.end is not None and self.end < self.start:
            raise ValueError("calendar range end cannot be less than start")
        if self.end is None and self.step != 1:
            raise ValueError("calendar range step requires an explicit end")
        return self


CalendarRanges = Annotated[
    tuple[CalendarRange, ...],
    Field(min_length=1, max_length=MAX_CALENDAR_RANGES),
]


class CronScheduleSpec(StrictDeclarationModel):
    kind: Literal["cron"] = "cron"
    expressions: Annotated[
        tuple[StrictStr, ...],
        Field(min_length=1, max_length=MAX_CRON_EXPRESSIONS),
    ]

    @field_validator("expressions")
    @classmethod
    def validate_expressions(cls, expressions: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(expressions)) != len(expressions):
            raise ValueError("cron expressions must be unique")
        for expression in expressions:
            if not expression or len(expression.encode("utf-8")) > MAX_CRON_EXPRESSION_LENGTH:
                raise ValueError("cron expressions must be non-empty and byte-bounded")
            if expression != expression.strip():
                raise ValueError("cron expressions cannot contain surrounding whitespace")
            if expression.startswith(("TZ=", "CRON_TZ=")):
                raise ValueError("cron timezone must use the schedule timezone field")
            _validate_cron_expression(expression)
        return expressions


class IntervalScheduleSpec(StrictDeclarationModel):
    kind: Literal["interval"] = "interval"
    every_seconds: StrictInt = Field(ge=1, le=MAX_INTERVAL_SECONDS)
    offset_seconds: StrictInt = Field(default=0, ge=0, le=MAX_INTERVAL_SECONDS)

    @model_validator(mode="after")
    def validate_offset(self) -> Self:
        if self.offset_seconds >= self.every_seconds:
            raise ValueError("interval offset must be less than its interval")
        return self


class CalendarScheduleSpec(StrictDeclarationModel):
    kind: Literal["calendar"] = "calendar"
    second: CalendarRanges = (CalendarRange(start=0),)
    minute: CalendarRanges = (CalendarRange(start=0),)
    hour: CalendarRanges = (CalendarRange(start=0),)
    day_of_month: CalendarRanges = (CalendarRange(start=1, end=31),)
    month: CalendarRanges = (CalendarRange(start=1, end=12),)
    year: Annotated[
        tuple[CalendarRange, ...],
        Field(max_length=MAX_CALENDAR_RANGES),
    ] = ()
    day_of_week: CalendarRanges = (CalendarRange(start=0, end=6),)

    @model_validator(mode="after")
    def validate_field_bounds(self) -> Self:
        bounds = {
            "second": (0, 59),
            "minute": (0, 59),
            "hour": (0, 23),
            "day_of_month": (1, 31),
            "month": (1, 12),
            "year": (MIN_SCHEDULE_YEAR, MAX_SCHEDULE_YEAR),
            "day_of_week": (0, 6),
        }
        for field_name, (minimum, maximum) in bounds.items():
            ranges = getattr(self, field_name)
            seen: set[int] = set()
            for value_range in ranges:
                end = value_range.start if value_range.end is None else value_range.end
                if value_range.start < minimum or end > maximum:
                    raise ValueError(
                        f"calendar {field_name} ranges must be within {minimum}..{maximum}"
                    )
                values = set(range(value_range.start, end + 1, value_range.step))
                if seen.intersection(values):
                    raise ValueError(f"calendar {field_name} ranges cannot overlap")
                seen.update(values)
        return self

    @property
    def maximum_daily_actions(self) -> int:
        return (
            _calendar_value_count(self.second)
            * _calendar_value_count(self.minute)
            * _calendar_value_count(self.hour)
        )


ScheduleSpec = Annotated[
    CronScheduleSpec | IntervalScheduleSpec | CalendarScheduleSpec,
    Field(discriminator="kind"),
]


class BackfillPolicy(StrictDeclarationModel):
    enabled: StrictBool = False
    max_window_seconds: StrictInt = Field(
        default=DEFAULT_BACKFILL_WINDOW_SECONDS,
        ge=1,
        le=MAX_BACKFILL_WINDOW_SECONDS,
    )
    max_actions: StrictInt = Field(
        default=DEFAULT_BACKFILL_ACTIONS,
        ge=1,
        le=MAX_BACKFILL_ACTIONS,
    )


def _validate_cron_expression(expression: str) -> None:
    fields = expression.split()
    if len(fields) != len(CRON_FIELD_BOUNDS):
        raise ValueError("cron expressions must contain exactly five numeric fields")
    for value, (field_name, minimum, maximum) in zip(fields, CRON_FIELD_BOUNDS, strict=True):
        _validate_cron_field(
            value,
            field_name=field_name,
            minimum=minimum,
            maximum=maximum,
        )
    if fields[2] != "*" and fields[4] != "*":
        raise ValueError("cron day-of-month and day-of-week cannot both be constrained")


def _validate_cron_field(
    value: str,
    *,
    field_name: str,
    minimum: int,
    maximum: int,
) -> None:
    atoms = value.split(",")
    if not atoms or len(atoms) > MAX_CRON_LIST_ITEMS:
        raise ValueError(f"cron {field_name} has too many list items")
    for atom in atoms:
        match = CRON_ATOM_PATTERN.fullmatch(atom)
        if match is None:
            raise ValueError(f"cron {field_name} contains unsupported syntax")
        start_text = match.group("start")
        end_text = match.group("end")
        step_text = match.group("step")
        if start_text == "*":
            if end_text is not None:
                raise ValueError(f"cron {field_name} wildcard cannot start a range")
        else:
            start = int(start_text)
            end = start if end_text is None else int(end_text)
            if start < minimum or end > maximum or end < start:
                raise ValueError(
                    f"cron {field_name} values must be ordered within {minimum}..{maximum}"
                )
        if step_text is not None:
            step = int(step_text)
            if step < 1 or step > maximum - minimum + 1:
                raise ValueError(f"cron {field_name} step is outside its supported bound")


def _calendar_value_count(ranges: tuple[CalendarRange, ...]) -> int:
    return sum(
        ((value.end if value.end is not None else value.start) - value.start) // value.step + 1
        for value in ranges
    )
