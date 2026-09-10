"""Declared trigger loading and schedule-kind contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

import pytest
from pydantic import ValidationError

from justflow.config.loader import ConfigLoader, ConfigLoadError
from justflow.config.schedules import (
    CalendarScheduleSpec,
    CronScheduleSpec,
    IntervalScheduleSpec,
    ScheduleOverlapPolicy,
)
from justflow.config.triggers import ScheduleTriggerDeclaration, TriggersConfig


@dataclass(frozen=True, kw_only=True)
class Returns:
    spec_type: type[object]


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


DeclarationOutcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class DeclarationCase:
    id: str
    declaration: dict[str, object]
    outcome: DeclarationOutcome


DECLARATION_CASES = [
    DeclarationCase(
        id="cron-timezone",
        declaration={
            "workflow": "daily_report",
            "spec": {"kind": "cron", "expressions": ["0 9 * * 1-5"]},
            "timezone": "Europe/Zurich",
            "overlap_policy": "buffer_one",
        },
        outcome=Returns(spec_type=CronScheduleSpec),
    ),
    DeclarationCase(
        id="calendar",
        declaration={
            "workflow": "month_end",
            "spec": {
                "kind": "calendar",
                "hour": [{"start": 23}],
                "day_of_month": [{"start": 28, "end": 31}],
            },
        },
        outcome=Returns(spec_type=CalendarScheduleSpec),
    ),
    DeclarationCase(
        id="interval",
        declaration={
            "workflow": "heartbeat",
            "spec": {"kind": "interval", "every_seconds": 300, "offset_seconds": 30},
        },
        outcome=Returns(spec_type=IntervalScheduleSpec),
    ),
    DeclarationCase(
        id="ambiguous-interval-timezone",
        declaration={
            "workflow": "heartbeat",
            "spec": {"kind": "interval", "every_seconds": 300},
            "timezone": "Europe/Zurich",
        },
        outcome=Raises(exc=ValidationError, match="absolute time.*UTC"),
    ),
    DeclarationCase(
        id="embedded-cron-timezone",
        declaration={
            "workflow": "daily_report",
            "spec": {"kind": "cron", "expressions": ["CRON_TZ=UTC 0 9 * * *"]},
        },
        outcome=Raises(exc=ValidationError, match="timezone field"),
    ),
    DeclarationCase(
        id="cron-field-count",
        declaration={
            "workflow": "daily_report",
            "spec": {"kind": "cron", "expressions": ["0 0 9 * * *"]},
        },
        outcome=Raises(exc=ValidationError, match="exactly five"),
    ),
    DeclarationCase(
        id="cron-field-bound",
        declaration={
            "workflow": "daily_report",
            "spec": {"kind": "cron", "expressions": ["60 9 * * *"]},
        },
        outcome=Raises(exc=ValidationError, match=r"minute values.*0\.\.59"),
    ),
    DeclarationCase(
        id="cron-unsupported-name",
        declaration={
            "workflow": "daily_report",
            "spec": {"kind": "cron", "expressions": ["0 9 * * MON"]},
        },
        outcome=Raises(exc=ValidationError, match="unsupported syntax"),
    ),
    DeclarationCase(
        id="cron-ambiguous-day-selection",
        declaration={
            "workflow": "daily_report",
            "spec": {"kind": "cron", "expressions": ["0 9 1 * 1"]},
        },
        outcome=Raises(exc=ValidationError, match="cannot both be constrained"),
    ),
    DeclarationCase(
        id="cron-zero-step",
        declaration={
            "workflow": "daily_report",
            "spec": {"kind": "cron", "expressions": ["*/0 9 * * *"]},
        },
        outcome=Raises(exc=ValidationError, match="step.*supported bound"),
    ),
    DeclarationCase(
        id="duplicate-cron-expression",
        declaration={
            "workflow": "daily_report",
            "spec": {"kind": "cron", "expressions": ["0 9 * * *", "0 9 * * *"]},
        },
        outcome=Raises(exc=ValidationError, match="expressions must be unique"),
    ),
    DeclarationCase(
        id="invalid-calendar-range",
        declaration={
            "workflow": "daily_report",
            "spec": {"kind": "calendar", "hour": [{"start": 24}]},
        },
        outcome=Raises(exc=ValidationError, match=r"hour ranges.*0\.\.23"),
    ),
    DeclarationCase(
        id="overlapping-calendar-ranges",
        declaration={
            "workflow": "daily_report",
            "spec": {
                "kind": "calendar",
                "minute": [{"start": 0, "end": 30}, {"start": 15, "end": 45}],
            },
        },
        outcome=Raises(exc=ValidationError, match="minute ranges cannot overlap"),
    ),
    DeclarationCase(
        id="calendar-step-without-range",
        declaration={
            "workflow": "daily_report",
            "spec": {"kind": "calendar", "minute": [{"start": 0, "step": 5}]},
        },
        outcome=Raises(exc=ValidationError, match="step requires an explicit end"),
    ),
    DeclarationCase(
        id="unsupported-calendar-year",
        declaration={
            "workflow": "daily_report",
            "spec": {"kind": "calendar", "year": [{"start": 2101}]},
        },
        outcome=Raises(exc=ValidationError, match=r"year ranges.*2000\.\.2100"),
    ),
    DeclarationCase(
        id="invalid-timezone",
        declaration={
            "workflow": "daily_report",
            "spec": {"kind": "cron", "expressions": ["0 9 * * *"]},
            "timezone": "Synthetic/Nowhere",
        },
        outcome=Raises(exc=ValidationError, match="installed IANA timezone"),
    ),
]


@pytest.mark.parametrize("case", DECLARATION_CASES, ids=lambda case: case.id)
def test_schedule_declaration_matrix(case: DeclarationCase) -> None:
    value = {"kind": "schedule", **case.declaration}
    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            ScheduleTriggerDeclaration.model_validate(value)
        return

    declaration = ScheduleTriggerDeclaration.model_validate(value)
    assert isinstance(declaration.spec, case.outcome.spec_type)


def test_schedule_config_rejects_unknown_fields_and_invalid_names() -> None:
    with pytest.raises(ValidationError):
        TriggersConfig.model_validate(
            {
                "triggers": {
                    "invalid-name": {
                        "kind": "schedule",
                        "workflow": "daily_report",
                        "spec": {"kind": "interval", "every_seconds": 60},
                        "credential": "synthetic",
                    }
                }
            }
        )


@pytest.mark.parametrize(
    ("triggers", "message"),
    [
        pytest.param(
            {
                "first": {"kind": "api", "workflow": "orders"},
                "second": {"kind": "api", "workflow": "orders"},
            },
            "at most one api trigger",
            id="duplicate-api",
        ),
        pytest.param(
            {
                "first": {"kind": "webhook", "workflow": "orders", "source": "stripe"},
                "second": {"kind": "webhook", "workflow": "orders", "source": "stripe"},
            },
            "source binding must be unique",
            id="duplicate-source-binding",
        ),
    ],
)
def test_trigger_config_rejects_ambiguous_ingress_bindings(
    triggers: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        TriggersConfig.model_validate({"triggers": triggers})


def test_schedule_defaults_are_explicit_and_bounded() -> None:
    declaration = ScheduleTriggerDeclaration.model_validate(
        {
            "kind": "schedule",
            "workflow": "daily_report",
            "spec": {"kind": "interval", "every_seconds": 60},
        }
    )

    assert declaration.timezone == "UTC"
    assert declaration.overlap_policy is ScheduleOverlapPolicy.SKIP
    assert declaration.catch_up_window_seconds == 60
    assert not declaration.paused
    assert not declaration.backfill.enabled


def test_loader_requires_trigger_file(tmp_path) -> None:
    with pytest.raises(ConfigLoadError, match="configuration file is required"):
        ConfigLoader(tmp_path).load_triggers()


def test_loader_rejects_legacy_schedule_file_with_mechanical_migration(tmp_path) -> None:
    (tmp_path / "schedules.yaml").write_text("schedules: {}\n")

    with pytest.raises(
        ConfigLoadError,
        match=(
            "rename the file, rename its top-level schedules key to triggers, "
            "and add 'kind: schedule'"
        ),
    ):
        ConfigLoader(tmp_path).load_triggers()


def test_loader_reads_schedule_declarations(tmp_path) -> None:
    (tmp_path / "triggers.yaml").write_text(
        """
triggers:
  daily_report:
    kind: schedule
    workflow: sample_flow
    spec:
      kind: cron
      expressions: ["0 9 * * *"]
    timezone: Europe/Zurich
"""
    )

    triggers = ConfigLoader(tmp_path).load_triggers()

    assert triggers.schedules["daily_report"].workflow == "sample_flow"
