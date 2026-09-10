"""Replay committed histories against the currently compatible worker build."""

from __future__ import annotations

from pathlib import Path

from justflow.definitions.replay import ReplayFixtureIndex, replay_fixtures
from justflow.runtime.schedule_dispatch import ScheduleDispatchWorkflow
from justflow.runtime.scheduled_start_dispatch import (
    ScheduledStartArbiterWorkflow,
    ScheduledStartDueWorkflow,
)
from justflow.runtime.scheduled_starts import (
    SCHEDULED_START_ARBITER_WORKFLOW_TYPE,
    SCHEDULED_START_DUE_WORKFLOW_TYPE,
)
from justflow.runtime.schedules import SCHEDULE_DISPATCH_WORKFLOW_TYPE
from tests.beta_replay_cases import prepare_baseline_definitions
from tests.replay_cases import prepare_replay_definitions

REPLAY_INDEX = Path(__file__).parent.parent / "replay" / "index.json"
RELEASED_BEHAVIOR_FAMILIES = frozenset(
    {
        "terminal",
        "activity",
        "branching",
        "parallel-fanout",
        "until-loop",
        "durable-wait",
        "durable-sleep",
        "failure-handler",
        "child-workflow",
        "schedule-dispatch",
        "scheduled-start-due",
        "scheduled-start-arbiter-initial",
        "scheduled-start-arbiter-resolved",
        "scheduled-start-arbiter-terminal",
        "contract-validation-baseline",
        "archival-baseline",
        "continuation-baseline",
        "continuation-restored-baseline",
    }
)


async def test_released_behavior_histories_replay() -> None:
    index = ReplayFixtureIndex.load(REPLAY_INDEX)
    index.require_behavior_families(RELEASED_BEHAVIOR_FAMILIES)
    prepared = prepare_replay_definitions()

    workflow_classes = {
        **prepared.workflow_classes,
        **prepare_baseline_definitions().workflow_classes,
        SCHEDULE_DISPATCH_WORKFLOW_TYPE: ScheduleDispatchWorkflow,
        SCHEDULED_START_DUE_WORKFLOW_TYPE: ScheduledStartDueWorkflow,
        SCHEDULED_START_ARBITER_WORKFLOW_TYPE: ScheduledStartArbiterWorkflow,
    }
    await replay_fixtures(index, workflow_classes)
