"""The example's two-hour reminder policy uses elapsed time and explicit late handling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest
from scheduled_reporting.customer_client import AppointmentTooLateError, reminder_start_at

NOW = datetime(2026, 1, 1, tzinfo=UTC)
ZURICH = ZoneInfo("Europe/Zurich")


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: datetime


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


@dataclass(frozen=True, kw_only=True)
class ReminderCase:
    id: str
    appointment_at: datetime
    outcome: Returns | Raises
    now: datetime = NOW


REMINDER_CASES = [
    ReminderCase(
        id="normal-offset",
        appointment_at=datetime(2026, 9, 10, 9, tzinfo=ZURICH),
        outcome=Returns(value=datetime(2026, 9, 10, 5, tzinfo=UTC)),
    ),
    ReminderCase(
        id="spring-dst",
        appointment_at=datetime(2026, 3, 29, 3, 30, tzinfo=ZURICH),
        outcome=Returns(value=datetime(2026, 3, 28, 23, 30, tzinfo=UTC)),
    ),
    ReminderCase(
        id="autumn-dst-later-fold",
        appointment_at=datetime(2026, 10, 25, 2, 30, tzinfo=ZURICH, fold=1),
        outcome=Returns(value=datetime(2026, 10, 24, 23, 30, tzinfo=UTC)),
    ),
    ReminderCase(
        id="naive-appointment",
        appointment_at=datetime(2026, 9, 10, 9, tzinfo=UTC).replace(tzinfo=None),
        outcome=Raises(exc=ValueError, match="UTC offset"),
    ),
    ReminderCase(
        id="already-due",
        appointment_at=datetime(2026, 1, 1, 2, tzinfo=UTC),
        outcome=Raises(exc=AppointmentTooLateError, match="already due"),
    ),
]


@pytest.mark.parametrize("case", REMINDER_CASES, ids=lambda c: c.id)
def test_reminder_time_policy(case: ReminderCase) -> None:
    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            reminder_start_at(case.appointment_at, now=case.now)
    else:
        assert reminder_start_at(case.appointment_at, now=case.now) == case.outcome.value
