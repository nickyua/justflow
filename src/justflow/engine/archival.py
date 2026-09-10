"""Archival activity - snapshots workflow state on completion."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from temporalio import activity

from justflow.config.models import AuditCaptureMode
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.engine.limits import enforce_payload_bytes, enforce_utf8_bytes
from justflow.engine.payload_protection import (
    PayloadProtectionBinding,
    PayloadProtectionError,
    encode_archive_record,
)
from justflow.engine.serialization import StrictJsonError, StrictJsonLayout, dumps_strict_json
from justflow.resources.base import (
    ArchiveStore,
    ResourceCapability,
    ResourceCapabilityError,
    ResourceNotFoundError,
)
from justflow.resources.registry import ResourceCollection
from justflow.sdk.logging_context import identity_log_digest, logging_context

logger = logging.getLogger(__name__)


@dataclass
class ArchiveRequest:
    resource: str
    path: str
    retention_policy: str
    capture_mode: AuditCaptureMode
    record: dict[str, Any]


class ArchivalError(Exception):
    """The workflow audit record could not be archived."""


class ArchivalActivity:
    """Temporal activity that archives workflow state to a configured resource.

    The resource must implement
    ``async write(path: str, data: str, *, retention_policy: str)`` (see
    S3Resource / MemoryStore). Failures raise so the workflow fails loudly
    instead of silently losing its audit record.
    """

    def __init__(
        self,
        resources: ResourceCollection | Mapping[str, object],
        limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
        payload_protection: PayloadProtectionBinding | None = None,
    ):
        self._resources = (
            resources
            if isinstance(resources, ResourceCollection)
            else ResourceCollection.from_instances(resources)
        )
        self._limits = limits
        self._payload_protection = payload_protection

    @activity.defn(name="archive_workflow")
    async def archive_workflow(self, request: ArchiveRequest) -> None:
        enforce_payload_bytes(
            request.record,
            boundary="activities.archive.record",
            limit=self._limits.audit_record_bytes,
        )
        with logging_context(
            request_id=_audit_log_identity(request.record),
            flow_name=request.record.get("workflow", "-"),
        ):
            await self._archive(request)

    async def _archive(self, request: ArchiveRequest) -> None:
        try:
            resource = self._resources.require(request.resource, ResourceCapability.ARCHIVE)
        except ResourceNotFoundError as exc:
            raise ArchivalError(f"Archive resource '{request.resource}' was not found") from exc
        except ResourceCapabilityError as exc:
            raise ArchivalError(
                f"Archive resource '{request.resource}' does not implement archive capability"
            ) from exc
        if not isinstance(resource, ArchiveStore):
            raise TypeError("Validated archive resource does not implement ArchiveStore")

        if (
            request.capture_mode is AuditCaptureMode.APPROVED_FULL
            and self._payload_protection is None
        ):
            raise ArchivalError("Approved-full audit capture requires payload encryption")
        try:
            data = (
                await encode_archive_record(request.record, self._payload_protection)
                if self._payload_protection is not None
                else dumps_strict_json(request.record, layout=StrictJsonLayout.PRETTY)
            )
        except (PayloadProtectionError, StrictJsonError):
            raise ArchivalError("Archive record serialization failed") from None
        enforce_utf8_bytes(
            data,
            boundary="activities.archive.output",
            limit=self._limits.audit_record_bytes,
        )
        try:
            await resource.write(
                request.path,
                data,
                retention_policy=request.retention_policy,
            )
        except Exception as exc:  # noqa: BLE001 - archive resource boundary
            logger.error(
                "Archive resource write failed",
                extra={
                    "resource": request.resource,
                    "retention_policy": request.retention_policy,
                    "exception_type": type(exc).__name__,
                },
            )
            raise ArchivalError(
                f"Archive resource '{request.resource}' failed to store the record"
            ) from None
        logger.info(
            "Archived workflow record",
            extra={
                "resource": request.resource,
                "retention_policy": request.retention_policy,
            },
        )


def _audit_log_identity(record: Mapping[str, Any]) -> str:
    digest = record.get("correlation_identity_digest")
    if isinstance(digest, str):
        return digest
    request_id = record.get("request_id")
    return identity_log_digest(request_id) if isinstance(request_id, str) else "-"
