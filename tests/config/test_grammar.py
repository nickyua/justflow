"""Contracts for authored identifier, reference, and placeholder grammar."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

import pytest
from pydantic import ValidationError

from justflow.config.grammar import (
    MAX_REFERENCE_COMPONENTS,
    Placeholder,
    PlaceholderSyntaxError,
    ReferencePath,
    ReferenceSyntaxError,
    parse_placeholders,
)
from justflow.config.models import (
    FlowStep,
    ResourcesConfig,
    ServicesConfig,
    WorkflowConfig,
)


@dataclass(frozen=True, kw_only=True)
class ReferenceReturns:
    components: tuple[str | int, ...]


@dataclass(frozen=True, kw_only=True)
class ReferenceRaises:
    match: str


ReferenceOutcome: TypeAlias = ReferenceReturns | ReferenceRaises


@dataclass(frozen=True, kw_only=True)
class ReferenceCase:
    id: str
    value: object
    outcome: ReferenceOutcome


REFERENCE_CASES = [
    ReferenceCase(
        id="root",
        value="record",
        outcome=ReferenceReturns(components=("record",)),
    ),
    ReferenceCase(
        id="nested-array",
        value="record.items.2.value",
        outcome=ReferenceReturns(components=("record", "items", 2, "value")),
    ),
    ReferenceCase(
        id="empty-component",
        value="record..value",
        outcome=ReferenceRaises(match="must not be empty"),
    ),
    ReferenceCase(
        id="path-character",
        value="record/items",
        outcome=ReferenceRaises(match="identifier or array index"),
    ),
    ReferenceCase(
        id="numeric-root",
        value="1.value",
        outcome=ReferenceRaises(match="identifier or array index"),
    ),
    ReferenceCase(
        id="leading-zero-index",
        value="record.items.01",
        outcome=ReferenceRaises(match="leading zeroes"),
    ),
    ReferenceCase(
        id="too-many-components",
        value=".".join(["root", *(["item"] * MAX_REFERENCE_COMPONENTS)]),
        outcome=ReferenceRaises(match="more than"),
    ),
    ReferenceCase(
        id="native-number",
        value=12,
        outcome=ReferenceRaises(match="must be a string"),
    ),
]


@pytest.mark.parametrize("case", REFERENCE_CASES, ids=lambda case: case.id)
def test_reference_path_grammar(case: ReferenceCase) -> None:
    if isinstance(case.outcome, ReferenceReturns):
        assert ReferencePath.parse(case.value).components == case.outcome.components
    else:
        with pytest.raises(ReferenceSyntaxError, match=case.outcome.match):
            ReferencePath.parse(case.value)


@dataclass(frozen=True, kw_only=True)
class PlaceholderReturns:
    names: tuple[str, ...]


@dataclass(frozen=True, kw_only=True)
class PlaceholderRaises:
    match: str


PlaceholderOutcome: TypeAlias = PlaceholderReturns | PlaceholderRaises


@dataclass(frozen=True, kw_only=True)
class PlaceholderCase:
    id: str
    value: str
    outcome: PlaceholderOutcome


PLACEHOLDER_CASES = [
    PlaceholderCase(
        id="none",
        value="literal value",
        outcome=PlaceholderReturns(names=()),
    ),
    PlaceholderCase(
        id="multiple",
        value="audit/${tenant_id}/${request_id}.json",
        outcome=PlaceholderReturns(names=("tenant_id", "request_id")),
    ),
    PlaceholderCase(
        id="unclosed",
        value="${tenant_id",
        outcome=PlaceholderRaises(match="unclosed"),
    ),
    PlaceholderCase(
        id="empty",
        value="${}",
        outcome=PlaceholderRaises(match="invalid name"),
    ),
    PlaceholderCase(
        id="reference-separator",
        value="${tenant.id}",
        outcome=PlaceholderRaises(match="invalid name"),
    ),
    PlaceholderCase(
        id="catalog-character",
        value="${tenant/id}",
        outcome=PlaceholderRaises(match="invalid name"),
    ),
]


@pytest.mark.parametrize("case", PLACEHOLDER_CASES, ids=lambda case: case.id)
def test_placeholder_grammar(case: PlaceholderCase) -> None:
    if isinstance(case.outcome, PlaceholderReturns):
        placeholders = parse_placeholders(case.value)
        assert tuple(placeholder.name for placeholder in placeholders) == case.outcome.names
        assert all(isinstance(placeholder, Placeholder) for placeholder in placeholders)
    else:
        with pytest.raises(PlaceholderSyntaxError, match=case.outcome.match):
            parse_placeholders(case.value)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("workflow", "invalid/name"),
        ("step", "invalid.name"),
        ("operation", "invalid-name"),
        ("resource", "invalid name"),
    ],
    ids=["workflow", "step", "operation", "resource"],
)
def test_data_identifiers_reject_ambiguous_characters(field: str, value: str) -> None:
    if field == "resource":
        with pytest.raises(ValidationError, match="string_pattern_mismatch"):
            ResourcesConfig.model_validate({"resources": {value: {"class": "package.Resource"}}})
        return

    declaration = {
        "workflow": value if field == "workflow" else "example",
        "steps": {
            value if field == "operation" else "operation": {
                "service": "service",
                "action": "run",
            }
        },
        "flow": [
            {
                "name": value if field == "step" else "start",
                "op": value if field == "operation" else "operation",
                "then": "done",
            },
            {"name": "done", "terminal": True},
        ],
    }
    with pytest.raises(ValidationError, match="string_pattern_mismatch"):
        WorkflowConfig.model_validate(declaration)


def test_reference_paths_serialize_as_authored_strings() -> None:
    workflow = WorkflowConfig(
        workflow="example",
        result="start.output.value",
        steps={"operation": {"service": "service", "action": "run"}},
        flow=[
            FlowStep(
                name="start",
                op="operation",
                input="request.items.0",
                output="output",
                then="done",
            ),
            FlowStep(name="done", terminal=True),
        ],
    )

    dumped = workflow.model_dump(mode="json", by_alias=True)

    assert dumped["result"] == "start.output.value"
    assert dumped["flow"][0]["input"] == "request.items.0"


@pytest.mark.parametrize(
    "declaration",
    [
        {
            "workflow": "example",
            "description": "${broken",
            "steps": {},
            "flow": [{"name": "done", "terminal": True}],
        },
        {
            "workflow": "example",
            "params": {"tenant_id": "${tenant.id}"},
            "steps": {},
            "flow": [{"name": "done", "terminal": True}],
        },
    ],
    ids=["workflow-field", "parameter-default"],
)
def test_workflow_rejects_malformed_placeholders(declaration: dict[str, object]) -> None:
    with pytest.raises(ValidationError, match="placeholder"):
        WorkflowConfig.model_validate(declaration)


@pytest.mark.parametrize(
    "declaration",
    [
        {
            "workflow": "example",
            "on_complete": {
                "resource": "store",
                "path": "${broken",
                "retention_policy": "standard",
            },
            "steps": {},
            "flow": [{"name": "done", "terminal": True}],
        },
        {
            "workflow": "example",
            "steps": {
                "operation": {
                    "service": "service",
                    "action": "run",
                    "cache": {"resource": "store", "key": "${broken"},
                }
            },
            "flow": [
                {"name": "start", "op": "operation", "then": "done"},
                {"name": "done", "terminal": True},
            ],
        },
        {
            "workflow": "example",
            "steps": {"operation": {"service": "service", "action": "run"}},
            "flow": [
                {
                    "name": "start",
                    "op": "operation",
                    "params": {"value": "${broken"},
                    "then": "done",
                },
                {"name": "done", "terminal": True},
            ],
        },
    ],
    ids=["archival", "cache", "step"],
)
def test_nested_workflow_fields_reject_malformed_placeholders(
    declaration: dict[str, object],
) -> None:
    with pytest.raises(ValidationError, match="placeholder"):
        WorkflowConfig.model_validate(declaration)


def test_service_fields_reject_malformed_placeholders() -> None:
    with pytest.raises(ValidationError, match="placeholder"):
        ServicesConfig.model_validate(
            {
                "services": {
                    "example": {
                        "transport": "direct",
                        "dispatch_timeout_sec": 10,
                        "retries": 0,
                        "params": {"value": "${broken"},
                    }
                }
            }
        )
