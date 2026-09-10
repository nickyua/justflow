"""Append real baseline histories without replacing previously recorded evidence."""

from __future__ import annotations

import asyncio
import json
from functools import partial
from pathlib import Path
from typing import Any

from temporalio.worker import Worker

from justflow.definitions.replay import ReplayFixture, ReplayFixtureIndex
from justflow.definitions.routing import (
    retry_pinned_workflow_start,
    wait_for_worker_deployment,
    worker_deployment_config,
)
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.scope import LOCAL_RUNTIME_SCOPE
from tests.beta_replay_cases import (
    BASELINE_FAMILIES,
    BASELINE_STEPS,
    BaselineActivities,
    prepare_baseline_definitions,
)
from tests.replay_cases import REPLAY_DEPLOYMENT

REPLAY_ROOT = Path("tests/replay")
INDEX_PATH = REPLAY_ROOT / "index.json"
TASK_QUEUE = "justflow-beta-baseline"
SCENARIO_TIMEOUT_SECONDS = 90


async def record_baselines() -> list[tuple[ReplayFixture, dict[str, Any]]]:
    prepared = prepare_baseline_definitions()
    activities = BaselineActivities()
    recordings: list[tuple[ReplayFixture, dict[str, Any]]] = []
    async with (
        asyncio.timeout(SCENARIO_TIMEOUT_SECONDS),
        await start_local_environment() as environment,
        Worker(
            environment.client,
            task_queue=TASK_QUEUE,
            workflows=list(prepared.workflow_classes.values()),
            activities=[
                activities.execute_step,
                activities.validate_contract,
                activities.archive_workflow,
            ],
            workflow_runner=workflow_sandbox_runner(),
            deployment_config=worker_deployment_config(REPLAY_DEPLOYMENT),
        ),
    ):
        await wait_for_worker_deployment(environment.client, REPLAY_DEPLOYMENT, TASK_QUEUE)
        for logical_name, family in BASELINE_FAMILIES.items():
            target = prepared.start_targets[logical_name]
            workflow_id = f"replay-{family}"
            activities.invocations.clear()
            start = partial(
                environment.client.start_workflow,
                target.workflow_type,
                {
                    "request_id": workflow_id,
                    "globals": {},
                    "definition_digest": target.manifest.definition_digest,
                    "worker_deployment": REPLAY_DEPLOYMENT.name,
                    "worker_build_id": REPLAY_DEPLOYMENT.build_id,
                    "worker_artifact": REPLAY_DEPLOYMENT.artifact_identity.model_dump(mode="json"),
                    "environment_snapshot_digest": target.environment_snapshot_digest,
                    "scope_digest": LOCAL_RUNTIME_SCOPE.digest,
                },
                id=workflow_id,
                task_queue=TASK_QUEUE,
                memo=target.memo,
                versioning_override=target.versioning_override,
            )
            handle = await retry_pinned_workflow_start(start, REPLAY_DEPLOYMENT, TASK_QUEUE)
            result = await handle.result()
            if logical_name == "baseline_continuation":
                assert activities.invocations == [f"step{index}" for index in range(BASELINE_STEPS)]
                assert result["result"] == {"value": "step0"}
            if logical_name == "baseline_archive":
                assert len(activities.archives) == 1
                assert activities.archives[0].path == "baseline/record.json"
            run_id = handle.first_execution_run_id
            run_index = 0
            while run_id is not None:
                history = await environment.client.get_workflow_handle(
                    workflow_id, run_id=run_id
                ).fetch_history()
                fixture_family = (
                    "continuation-restored-baseline"
                    if logical_name == "baseline_continuation" and run_index > 0
                    else family
                )
                fixture_id = family if run_index == 0 else f"{family}-run-{run_index}"
                recordings.append(
                    (
                        ReplayFixture(
                            id=fixture_id,
                            behavior_family=fixture_family,
                            workflow_id=workflow_id,
                            workflow_type=target.workflow_type,
                            definition_digest=target.manifest.definition_digest,
                            worker_deployment=REPLAY_DEPLOYMENT.name,
                            worker_build_id=REPLAY_DEPLOYMENT.build_id,
                            history=str(REPLAY_ROOT / "histories" / f"{fixture_id}.json"),
                        ),
                        json.loads(history.to_json()),
                    )
                )
                next_run = history.events[-1].workflow_execution_continued_as_new_event_attributes
                run_id = next_run.new_execution_run_id or None
                run_index += 1
            if logical_name == "baseline_continuation":
                assert run_index > 1
    return recordings


async def generate() -> None:
    index = ReplayFixtureIndex.load(INDEX_PATH)
    existing_ids = {fixture.id for fixture in index.fixtures}
    if existing_ids.intersection(BASELINE_FAMILIES.values()):
        raise ValueError("Baseline recordings already exist; retain them as immutable evidence")
    recordings = await record_baselines()
    await asyncio.to_thread(_save_recordings, index, recordings)


def _save_recordings(
    index: ReplayFixtureIndex, recordings: list[tuple[ReplayFixture, dict[str, Any]]]
) -> None:
    if any(Path(fixture.history).exists() for fixture, _ in recordings):
        raise ValueError("A recording path already exists; refusing to replace baseline evidence")
    for fixture, history in recordings:
        with Path(fixture.history).open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(history, indent=2, sort_keys=True) + "\n")
    updated = ReplayFixtureIndex(fixtures=(*index.fixtures, *(item[0] for item in recordings)))
    INDEX_PATH.write_text(
        json.dumps(updated.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    asyncio.run(generate())
