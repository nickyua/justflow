"""Credential-free reporting service with injected read-only configuration."""

from __future__ import annotations

from justflow.resources import ConfigReader, ResourceError
from justflow.sdk.base_action import BaseAction
from scheduled_reporting.contracts import ReportArtifact, ReportRequest

REPORTING_CONFIG_RESOURCE = "reporting_config"
REPORT_TITLE_KEY = "report_title"


class LocalReportingService(BaseAction):
    async def create_report(self, input: object) -> dict[str, object]:
        request = ReportRequest.model_validate(input)
        resource = self.resources[REPORTING_CONFIG_RESOURCE]
        if not isinstance(resource, ConfigReader):
            raise ResourceError("Reporting configuration is unavailable")
        title = resource.get(REPORT_TITLE_KEY)
        if not isinstance(title, str) or not title:
            raise ResourceError("Report title is unavailable")
        artifact = ReportArtifact(
            report_id=f"daily-{request.report_date.isoformat()}",
            title=title,
            row_count=request.report_date.day,
        )
        return artifact.model_dump(mode="json")

    async def deliver_report(self, input: object) -> dict[str, object]:
        artifact = ReportArtifact.model_validate(input)
        return {
            "report_id": artifact.report_id,
            "delivery": "recorded-locally",
        }
