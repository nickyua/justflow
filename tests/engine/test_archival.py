"""Tests for the archival activity."""

from __future__ import annotations

import json
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from justflow.config.models import AuditCaptureMode
from justflow.config.runtime_limits import RuntimeLimits
from justflow.engine.archival import ArchivalActivity, ArchivalError, ArchiveRequest
from justflow.engine.limits import LimitExceededError
from justflow.engine.payload_protection import PayloadProtectionBinding
from justflow.resources.memory import MemoryStore

RECORD = {"request_id": "r1", "status": "completed"}
TINY_BYTE_LIMIT = 1
RETENTION_POLICY = "test"
RETENTION_SECONDS = 60.0


def archive_params(resource: str = "store") -> ArchiveRequest:
    return ArchiveRequest(
        resource=resource,
        path="audit/r1.json",
        retention_policy=RETENTION_POLICY,
        capture_mode=AuditCaptureMode.METADATA_ONLY,
        record=RECORD,
    )


class TestArchivalActivity:
    async def test_writes_record_as_json(self):
        store = MemoryStore(retention_policies={RETENTION_POLICY: RETENTION_SECONDS})
        activity = ArchivalActivity(resources={"store": store})

        await activity.archive_workflow(archive_params())

        assert json.loads(store.records["audit/r1.json"]) == RECORD

    async def test_missing_resource_raises(self):
        activity = ArchivalActivity(resources={})

        with pytest.raises(ArchivalError, match="not found"):
            await activity.archive_workflow(archive_params())

    async def test_resource_without_write_raises(self):
        activity = ArchivalActivity(resources={"store": object()})

        with pytest.raises(ArchivalError, match="does not implement"):
            await activity.archive_workflow(archive_params())

    async def test_oversized_record_is_rejected_before_write(self):
        store = MemoryStore(retention_policies={RETENTION_POLICY: RETENTION_SECONDS})
        activity = ArchivalActivity(
            resources={"store": store},
            limits=RuntimeLimits(audit_record_bytes=TINY_BYTE_LIMIT),
        )

        with pytest.raises(LimitExceededError, match="activities.archive.record"):
            await activity.archive_workflow(archive_params())

        assert store.records == {}

    async def test_approved_full_capture_requires_encryption(self):
        store = MemoryStore(retention_policies={RETENTION_POLICY: RETENTION_SECONDS})
        activity = ArchivalActivity(resources={"store": store})
        request = replace(
            archive_params(),
            capture_mode=AuditCaptureMode.APPROVED_FULL,
        )

        with pytest.raises(ArchivalError, match="requires payload encryption"):
            await activity.archive_workflow(request)

        assert store.records == {}

    async def test_payload_binding_encrypts_archive_bytes(self):
        store = MemoryStore(retention_policies={RETENTION_POLICY: RETENTION_SECONDS})
        cipher = AsyncMock()
        cipher.encrypt_authenticated.return_value = b"encrypted-record"
        binding = PayloadProtectionBinding(
            cipher=cipher,
            active_key_id="current",
            readable_key_ids=frozenset({"current"}),
        )
        activity = ArchivalActivity(
            resources={"store": store},
            payload_protection=binding,
        )

        await activity.archive_workflow(archive_params())

        stored = json.loads(store.records["audit/r1.json"])
        assert stored["format"] == "justflow-encrypted-json"
        assert "status" not in stored

    async def test_resource_exception_text_is_contained(self, caplog):
        sensitive_sentinel = "synthetic-archive-secret"

        class FailingStore:
            async def write(self, path: str, data: str, *, retention_policy: str) -> None:
                raise RuntimeError(sensitive_sentinel)

        activity = ArchivalActivity(resources={"store": FailingStore()})

        with pytest.raises(ArchivalError) as exc_info:
            await activity.archive_workflow(archive_params())

        assert sensitive_sentinel not in str(exc_info.value)
        assert sensitive_sentinel not in caplog.text
