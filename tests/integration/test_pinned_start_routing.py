"""Integration coverage for Temporal's pinned-start rejection contract."""

from __future__ import annotations

import pytest
from temporalio.common import PinnedVersioningOverride
from temporalio.service import RPCError, RPCStatusCode

from justflow.definitions.manifest import ENGINE_WORKFLOW_ABI, SHA256_HEX_LENGTH
from justflow.definitions.routing import WorkerDeployment, retry_pinned_workflow_start
from justflow.engine.local_temporal import start_local_environment
from justflow.provenance import WorkerArtifactIdentity

TASK_QUEUE = "test-missing-pinned-version"
WORKFLOW_ID = "missing-pinned-version"
WORKFLOW_TYPE = "missing_pinned_version"
RETRY_ATTEMPTS = 2
DEPLOYMENT = WorkerDeployment(
    artifact_identity=WorkerArtifactIdentity(
        deployment_name="justflow",
        build_id="missing-pinned-version",
        artifact_digest=f"sha256:{'a' * SHA256_HEX_LENGTH}",
        package_version="0.1.0",
    ),
    compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
)
VERSIONING_OVERRIDE = PinnedVersioningOverride(DEPLOYMENT.temporal_version)


async def test_temporal_missing_pinned_version_rejection_is_retried() -> None:
    calls = 0

    async with await start_local_environment() as environment:

        async def start():
            nonlocal calls
            calls += 1
            return await environment.client.start_workflow(
                WORKFLOW_TYPE,
                {},
                id=WORKFLOW_ID,
                task_queue=TASK_QUEUE,
                versioning_override=VERSIONING_OVERRIDE,
            )

        with pytest.raises(RPCError) as exc_info:
            await retry_pinned_workflow_start(
                start,
                DEPLOYMENT,
                TASK_QUEUE,
                attempts=RETRY_ATTEMPTS,
                interval_seconds=0,
            )

    assert exc_info.value.status is RPCStatusCode.FAILED_PRECONDITION
    assert calls == RETRY_ATTEMPTS
