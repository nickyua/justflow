"""Replay-fixture metadata and Temporal history compatibility checks."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path

from pydantic import BaseModel, ConfigDict
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from justflow.engine.sandbox import workflow_sandbox_runner

REPLAY_INDEX_FORMAT_VERSION = 1


class ReplayFixtureError(Exception):
    """Replay fixture metadata or history is incomplete or invalid."""


class ReplayFixture(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    behavior_family: str
    workflow_id: str
    workflow_type: str
    definition_digest: str
    worker_deployment: str
    worker_build_id: str
    history: str


class ReplayFixtureIndex(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    format_version: int = REPLAY_INDEX_FORMAT_VERSION
    fixtures: tuple[ReplayFixture, ...]

    @classmethod
    def load(cls, path: str | Path) -> ReplayFixtureIndex:
        index_path = Path(path)
        try:
            index = cls.model_validate_json(index_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError) as exc:
            raise ReplayFixtureError(f"Cannot load replay index '{index_path}': {exc}") from exc
        if index.format_version != REPLAY_INDEX_FORMAT_VERSION:
            raise ReplayFixtureError(
                f"Unsupported replay index format version {index.format_version}"
            )
        ids = [fixture.id for fixture in index.fixtures]
        if len(ids) != len(set(ids)):
            raise ReplayFixtureError("Replay fixture ids must be unique")
        return index

    def require_behavior_families(self, required: Iterable[str]) -> None:
        actual = {fixture.behavior_family for fixture in self.fixtures}
        missing = sorted(set(required) - actual)
        if missing:
            raise ReplayFixtureError(f"Replay fixtures are missing behavior families: {missing}")

    @property
    def definition_digests(self) -> frozenset[str]:
        return frozenset(fixture.definition_digest for fixture in self.fixtures)

    @property
    def worker_deployment_versions(self) -> frozenset[tuple[str, str]]:
        return frozenset(
            (fixture.worker_deployment, fixture.worker_build_id) for fixture in self.fixtures
        )


async def replay_fixtures(
    index: ReplayFixtureIndex,
    workflow_classes: Mapping[str, type],
) -> None:
    missing_types = sorted(
        {fixture.workflow_type for fixture in index.fixtures} - set(workflow_classes)
    )
    if missing_types:
        raise ReplayFixtureError(f"Replay workflow types are not registered: {missing_types}")
    replayer = Replayer(
        workflows=list(workflow_classes.values()),
        workflow_runner=workflow_sandbox_runner(),
    )
    for fixture in index.fixtures:
        history_path = Path(fixture.history)
        try:
            raw_history = json.loads(history_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ReplayFixtureError(f"Cannot load replay history '{history_path}': {exc}") from exc
        history = WorkflowHistory.from_json(fixture.workflow_id, raw_history)
        await replayer.replay_workflow(history)
