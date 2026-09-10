"""Behavior checks for the scheduled-reporting example package."""

from __future__ import annotations

from pathlib import Path

from scheduled_reporting.actions import LocalReportingService

from justflow.config.loader import ConfigLoader
from justflow.config.schedules import CronScheduleSpec, ScheduleOverlapPolicy
from justflow.config.triggers import ScheduleTriggerDeclaration
from justflow.resources.memory import StaticConfig

CONFIG_DIR = Path("examples/scheduled_reporting/configs")


async def test_reporting_service_uses_injected_date_and_read_only_configuration() -> None:
    service = LocalReportingService(
        resources={"reporting_config": StaticConfig(report_title="Daily synthetic activity report")}
    )

    report = await service.create_report({"report_date": "2026-08-10"})
    delivery = await service.deliver_report(report)

    assert report == {
        "report_id": "daily-2026-08-10",
        "title": "Daily synthetic activity report",
        "row_count": 10,
    }
    assert delivery == {
        "report_id": "daily-2026-08-10",
        "delivery": "recorded-locally",
    }


def test_reporting_schedule_pins_timezone_input_and_safe_initial_state() -> None:
    declaration = ConfigLoader(CONFIG_DIR).load_triggers().triggers["daily_reporting"]

    assert isinstance(declaration, ScheduleTriggerDeclaration)
    assert isinstance(declaration.spec, CronScheduleSpec)
    assert declaration.spec.expressions == ("0 9 * * *",)
    assert declaration.timezone == "Europe/Zurich"
    assert declaration.input == {"report_date": "2026-08-10"}
    assert declaration.paused is True
    assert declaration.overlap_policy is ScheduleOverlapPolicy.BUFFER_ONE
    assert declaration.backfill.enabled is True
    assert declaration.backfill.max_actions == 7
