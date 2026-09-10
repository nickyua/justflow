"""Tests for replay fixture coverage metadata."""

from __future__ import annotations

import json

import pytest

from justflow.definitions.replay import ReplayFixtureError, ReplayFixtureIndex


def _write_index(tmp_path, fixtures):
    path = tmp_path / "index.json"
    path.write_text(json.dumps({"format_version": 1, "fixtures": fixtures}))
    return path


def _fixture(identifier: str, behavior_family: str):
    return {
        "id": identifier,
        "behavior_family": behavior_family,
        "workflow_id": f"workflow-{identifier}",
        "workflow_type": f"type-{identifier}",
        "definition_digest": "a" * 64,
        "worker_deployment": "justflow",
        "worker_build_id": "build-1",
        "history": f"{identifier}.json",
    }


def test_index_requires_released_behavior_families(tmp_path) -> None:
    index = ReplayFixtureIndex.load(_write_index(tmp_path, [_fixture("terminal", "terminal")]))

    with pytest.raises(ReplayFixtureError, match="branching"):
        index.require_behavior_families(["terminal", "branching"])


def test_index_rejects_duplicate_fixture_ids(tmp_path) -> None:
    path = _write_index(
        tmp_path,
        [_fixture("same", "terminal"), _fixture("same", "branching")],
    )

    with pytest.raises(ReplayFixtureError, match="ids must be unique"):
        ReplayFixtureIndex.load(path)
