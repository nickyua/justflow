"""Pinned Temporal dev-server environment used by local runs and integration tests."""

from __future__ import annotations

from temporalio.testing import WorkflowEnvironment

TEMPORAL_DEV_SERVER_VERSION = "v1.8.1"


async def start_local_environment(*, identity: str | None = None) -> WorkflowEnvironment:
    return await WorkflowEnvironment.start_local(
        dev_server_download_version=TEMPORAL_DEV_SERVER_VERSION,
        identity=identity,
    )
