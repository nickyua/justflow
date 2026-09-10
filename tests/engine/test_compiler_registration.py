"""Compiled contexts remain stable when the real Temporal sandbox imports them."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from temporalio.worker import UnsandboxedWorkflowRunner, WorkflowInstanceDetails

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.definitions.manifest import build_definition_manifests
from justflow.engine.compiler import compile_workflow
from justflow.engine.sandbox import workflow_sandbox_runner
from tests.replay_cases import REPLAY_DEPLOYMENT

SCOPE_A = "a" * 64
SCOPE_B = "b" * 64
SNAPSHOT_A = "c" * 64
SNAPSHOT_B = "d" * 64


@dataclass(frozen=True, kw_only=True)
class RegistrationCase:
    id: str
    first_scope: str
    second_scope: str
    first_snapshot: str
    second_snapshot: str
    same_registration: bool


CASES = [
    RegistrationCase(
        id="scope-a-then-b",
        first_scope=SCOPE_A,
        second_scope=SCOPE_B,
        first_snapshot=SNAPSHOT_A,
        second_snapshot=SNAPSHOT_A,
        same_registration=False,
    ),
    RegistrationCase(
        id="scope-b-then-a",
        first_scope=SCOPE_B,
        second_scope=SCOPE_A,
        first_snapshot=SNAPSHOT_A,
        second_snapshot=SNAPSHOT_A,
        same_registration=False,
    ),
    RegistrationCase(
        id="environment-change",
        first_scope=SCOPE_A,
        second_scope=SCOPE_A,
        first_snapshot=SNAPSHOT_A,
        second_snapshot=SNAPSHOT_B,
        same_registration=False,
    ),
    RegistrationCase(
        id="equivalent-context",
        first_scope=SCOPE_A,
        second_scope=SCOPE_A,
        first_snapshot=SNAPSHOT_A,
        second_snapshot=SNAPSHOT_A,
        same_registration=True,
    ),
]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.id)
async def test_sandbox_imports_the_requested_compilation(
    case: RegistrationCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = WorkflowConfig(
        workflow="registration", steps={}, flow=[FlowStep(name="done", terminal=True)]
    )
    manifest = build_definition_manifests({config.workflow: config}, {}, RuntimeLimits())[
        config.workflow
    ]
    first = compile_workflow(
        config,
        {},
        manifest=manifest,
        deployment=REPLAY_DEPLOYMENT,
        runtime_scope_digest=case.first_scope,
        environment_snapshot_digest=case.first_snapshot,
    )
    second = compile_workflow(
        config,
        {},
        manifest=manifest,
        deployment=REPLAY_DEPLOYMENT,
        runtime_scope_digest=case.second_scope,
        environment_snapshot_digest=case.second_snapshot,
    )
    selected: list[type] = []
    create = UnsandboxedWorkflowRunner.create_instance

    def observe(runner: UnsandboxedWorkflowRunner, details: WorkflowInstanceDetails):
        selected.append(details.defn.cls)
        return create(runner, details)

    monkeypatch.setattr(UnsandboxedWorkflowRunner, "create_instance", observe)
    workflow_sandbox_runner().prepare_workflow(getattr(first, "__temporal_workflow_definition"))
    assert selected == [first]
    assert (first is second) is case.same_registration


def test_legacy_compilations_capture_config_and_do_not_collide() -> None:
    config = WorkflowConfig(
        workflow="legacy_registration", steps={}, flow=[FlowStep(name="done", terminal=True)]
    )
    first = compile_workflow(config, {})
    config.flow[0].reason = "changed"
    second = compile_workflow(config, {})
    assert first.__name__ != second.__name__
