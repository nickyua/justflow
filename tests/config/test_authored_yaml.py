"""Tests for authored-style YAML rendering."""

from __future__ import annotations

import yaml

from justflow.config.authored_yaml import authored_dump, render_authored_yaml
from justflow.config.models import WorkflowConfig
from justflow.config.triggers import ScheduleTriggerDeclaration

WORKFLOW_DECLARATION = {
    "workflow": "orders",
    "description": "First line.\nSecond line.\n",
    "params": {"n": "${n}"},
    "steps": {"charge": {"service": "billing", "action": "charge"}},
    "flow": [
        {"name": "charge", "op": "charge", "parallel": False, "params": {}, "then": "done"},
        {"name": "done", "terminal": True},
    ],
}


def test_workflow_dump_omits_defaults_and_keeps_declaration_order() -> None:
    workflow = WorkflowConfig.model_validate(WORKFLOW_DECLARATION)

    payload = authored_dump(workflow)

    assert list(payload) == ["workflow", "description", "params", "steps", "flow"]
    steps = payload["steps"]
    assert isinstance(steps, dict)
    assert steps["charge"] == {
        "target": {"kind": "service", "service": "billing", "action": "charge"},
    }
    flow = payload["flow"]
    assert isinstance(flow, list)
    assert flow[0] == {"name": "charge", "op": "charge", "then": "done"}
    assert WorkflowConfig.model_validate(payload) == workflow


def test_dump_keeps_defaults_that_are_required_to_reparse() -> None:
    schedule = ScheduleTriggerDeclaration.model_validate(
        {
            "kind": "schedule",
            "workflow": "orders",
            "input": {},
            "spec": {"kind": "interval", "every_seconds": 3600},
            "timezone": "UTC",
        }
    )

    payload = authored_dump(schedule)

    spec = payload["spec"]
    assert isinstance(spec, dict)
    assert spec["kind"] == "interval"
    assert ScheduleTriggerDeclaration.model_validate(payload) == schedule


def test_render_uses_block_literals_for_multiline_strings_and_round_trips() -> None:
    workflow = WorkflowConfig.model_validate(WORKFLOW_DECLARATION)

    rendered = render_authored_yaml(authored_dump(workflow))

    assert "description: |\n" in rendered
    assert WorkflowConfig.model_validate(yaml.safe_load(rendered)) == workflow
