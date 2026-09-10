"""Pure control-flow and path-sensitive dataflow analysis."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from justflow.config.grammar import ReferencePath
from justflow.config.models import FlowStep, WorkflowConfig
from justflow.engine.evaluator import ExpressionSyntaxError, condition_reference_roots

INPUT_ROOT = "input"
ERROR_ROOT = "error"
REQUEST_ID_ROOT = "request_id"


class EdgeKind(str, Enum):
    SUCCESS = "success"
    CONDITION_SKIP = "condition_skip"
    RESULT_BRANCH = "result_branch"
    RESULT_DEFAULT = "result_default"
    WAIT_TIMEOUT = "wait_timeout"
    LOOP_EXHAUSTION = "loop_exhaustion"
    FAILURE = "failure"


@dataclass(frozen=True)
class FlowEdge:
    source: str
    target: str
    kind: EdgeKind
    produced: frozenset[str] = frozenset()


@dataclass(frozen=True)
class AnalysisIssue:
    location: str
    message: str


def analyze_workflow(workflow: WorkflowConfig) -> list[AnalysisIssue]:
    steps = {step.name: step for step in workflow.flow}
    edges = _build_edges(workflow, steps)
    execution_edges = [edge for edge in edges if edge.kind is not EdgeKind.FAILURE]
    cycle = _find_cycle(steps, execution_edges)
    if cycle is not None:
        return [
            AnalysisIssue(
                location="flow",
                message=f"Circular control flow detected: {' -> '.join(cycle)}",
            )
        ]

    normal_reachable = _reachable(
        entry=workflow.flow[0].name,
        edges=execution_edges,
    )
    handler_entries = {
        edge.target
        for edge in edges
        if edge.kind is EdgeKind.FAILURE and edge.source in normal_reachable
    }
    handler_reachable = _reachable(entries=handler_entries, edges=execution_edges)
    issues = _reachability_issues(workflow, steps, normal_reachable | handler_reachable)
    issues.extend(
        _sink_issues(
            workflow,
            normal_reachable | handler_reachable,
            execution_edges,
        )
    )

    normal_available = _must_available(
        steps,
        execution_edges,
        reachable=normal_reachable,
        entry_available={workflow.flow[0].name: _workflow_roots(workflow)},
    )
    handler_entry_available = _handler_entry_available(edges, normal_reachable, normal_available)
    handler_available = _must_available(
        steps,
        execution_edges,
        reachable=handler_reachable,
        entry_available=handler_entry_available,
    )
    issues.extend(_reference_issues(workflow, normal_available, handler_available))
    issues.extend(_result_issues(workflow, normal_available, handler_available))
    return issues


def _build_edges(workflow: WorkflowConfig, steps: dict[str, FlowStep]) -> list[FlowEdge]:
    edges: list[FlowEdge] = []
    for step in workflow.flow:
        if step.terminal:
            continue

        produced = _produced_symbols(step)
        if step.condition is not None and step.then in steps:
            edges.append(
                FlowEdge(
                    source=step.name,
                    target=step.then,
                    kind=EdgeKind.CONDITION_SKIP,
                )
            )

        if step.on_result:
            for branch in step.on_result:
                target = branch.then or branch.default
                if target not in steps:
                    continue
                kind = (
                    EdgeKind.RESULT_DEFAULT
                    if branch.default is not None
                    else EdgeKind.RESULT_BRANCH
                )
                edges.append(
                    FlowEdge(
                        source=step.name,
                        target=target,
                        kind=kind,
                        produced=produced,
                    )
                )
        elif step.then in steps:
            edges.append(
                FlowEdge(
                    source=step.name,
                    target=step.then,
                    kind=EdgeKind.SUCCESS,
                    produced=produced,
                )
            )

        if step.wait_for is not None and step.wait_for.on_timeout in steps:
            edges.append(
                FlowEdge(
                    source=step.name,
                    target=step.wait_for.on_timeout,
                    kind=EdgeKind.WAIT_TIMEOUT,
                )
            )
        if step.until is not None and step.on_exhausted in steps:
            edges.append(
                FlowEdge(
                    source=step.name,
                    target=step.on_exhausted,
                    kind=EdgeKind.LOOP_EXHAUSTION,
                )
            )

        if step.op is not None:
            handler = step.on_failure
            if handler is None and workflow.on_error is not None:
                handler = workflow.on_error.then
            if handler in steps:
                edges.append(
                    FlowEdge(
                        source=step.name,
                        target=handler,
                        kind=EdgeKind.FAILURE,
                        produced=frozenset({ERROR_ROOT}),
                    )
                )
    return edges


def _produced_symbols(step: FlowStep) -> frozenset[str]:
    if step.sleep_sec is not None:
        return frozenset()
    symbols = {step.name}
    if step.output is not None:
        symbols.add(step.output)
    return frozenset(symbols)


def _reachable(
    *,
    edges: list[FlowEdge],
    entry: str | None = None,
    entries: set[str] | None = None,
) -> set[str]:
    outgoing = _group_outgoing(edges)
    reachable: set[str] = set()
    pending = list(entries or ())
    if entry is not None:
        pending.append(entry)
    while pending:
        current = pending.pop()
        if current in reachable:
            continue
        reachable.add(current)
        pending.extend(edge.target for edge in outgoing.get(current, []))
    return reachable


def _reachability_issues(
    workflow: WorkflowConfig,
    steps: dict[str, FlowStep],
    reachable: set[str],
) -> list[AnalysisIssue]:
    entry = workflow.flow[0].name
    return [
        AnalysisIssue(
            location=f"flow.{name}",
            message=(
                f"Flow step '{name}' is unreachable from entry step '{entry}' "
                f"and every failure handler"
            ),
        )
        for name in steps
        if name not in reachable
    ]


def _sink_issues(
    workflow: WorkflowConfig,
    reachable: set[str],
    execution_edges: list[FlowEdge],
) -> list[AnalysisIssue]:
    outgoing = _group_outgoing(execution_edges)
    return [
        AnalysisIssue(
            location=f"flow.{step.name}",
            message=(f"Reachable path from step '{step.name}' has no explicit terminal successor"),
        )
        for step in workflow.flow
        if step.name in reachable and not step.terminal and not outgoing.get(step.name)
    ]


def _find_cycle(steps: dict[str, FlowStep], edges: list[FlowEdge]) -> list[str] | None:
    outgoing = _group_outgoing(edges)
    visiting: set[str] = set()
    visited: set[str] = set()
    stack: list[str] = []

    def visit(name: str) -> list[str] | None:
        visiting.add(name)
        stack.append(name)
        for edge in outgoing.get(name, []):
            if edge.target in visiting:
                return stack[stack.index(edge.target) :] + [edge.target]
            if edge.target not in visited:
                cycle = visit(edge.target)
                if cycle is not None:
                    return cycle
        stack.pop()
        visiting.remove(name)
        visited.add(name)
        return None

    for name in steps:
        if name not in visited:
            cycle = visit(name)
            if cycle is not None:
                return cycle
    return None


def _must_available(
    steps: dict[str, FlowStep],
    edges: list[FlowEdge],
    *,
    reachable: set[str],
    entry_available: dict[str, frozenset[str]],
) -> dict[str, frozenset[str]]:
    incoming: dict[str, list[FlowEdge]] = {name: [] for name in steps}
    outgoing = _group_outgoing(edges)
    indegree = dict.fromkeys(steps, 0)
    for edge in edges:
        incoming[edge.target].append(edge)
        indegree[edge.target] += 1

    pending = [name for name in steps if indegree[name] == 0]
    order: list[str] = []
    while pending:
        current = pending.pop(0)
        order.append(current)
        for edge in outgoing.get(current, []):
            indegree[edge.target] -= 1
            if indegree[edge.target] == 0:
                pending.append(edge.target)

    available: dict[str, frozenset[str]] = {}
    for name in order:
        if name not in reachable:
            continue
        predecessor_sets: list[frozenset[str]] = []
        if name in entry_available:
            predecessor_sets.append(entry_available[name])
        predecessor_sets.extend(
            available[edge.source] | edge.produced
            for edge in incoming[name]
            if edge.source in available
        )
        if predecessor_sets:
            available[name] = frozenset.intersection(*predecessor_sets)
    return available


def _workflow_roots(workflow: WorkflowConfig) -> frozenset[str]:
    return frozenset({*workflow.params, REQUEST_ID_ROOT})


def _handler_entry_available(
    edges: list[FlowEdge],
    normal_reachable: set[str],
    normal_available: dict[str, frozenset[str]],
) -> dict[str, frozenset[str]]:
    candidates: dict[str, list[frozenset[str]]] = {}
    for edge in edges:
        if (
            edge.kind is not EdgeKind.FAILURE
            or edge.source not in normal_reachable
            or edge.source not in normal_available
        ):
            continue
        candidates.setdefault(edge.target, []).append(normal_available[edge.source] | edge.produced)
    return {
        target: frozenset.intersection(*source_sets) for target, source_sets in candidates.items()
    }


def _reference_issues(
    workflow: WorkflowConfig,
    normal_available: dict[str, frozenset[str]],
    handler_available: dict[str, frozenset[str]],
) -> list[AnalysisIssue]:
    issues: list[AnalysisIssue] = []
    for step in workflow.flow:
        if step.terminal:
            continue
        availability = [
            available[step.name]
            for available in (normal_available, handler_available)
            if step.name in available
        ]
        if not availability:
            continue
        guaranteed = frozenset.intersection(*availability)
        roots = _step_reference_roots(step)
        missing = sorted(root for root in roots if root not in guaranteed)
        for root in missing:
            issues.append(
                AnalysisIssue(
                    location=f"flow.{step.name}",
                    message=(
                        f"Reference root '{root}' is not available on every path "
                        f"to step '{step.name}'"
                    ),
                )
            )
    return issues


def _step_reference_roots(step: FlowStep) -> set[str]:
    roots: set[str] = set()
    if isinstance(step.input, ReferencePath):
        roots.add(step.input.root)
    elif isinstance(step.input, dict):
        roots.update(reference.root for reference in step.input.values())
    elif isinstance(step.input, list):
        roots.update(reference.root for reference in step.input)

    if step.for_each is not None and step.for_each.root != INPUT_ROOT:
        roots.add(step.for_each.root)
    if step.wait_for is not None and step.wait_for.timeout_until is not None:
        roots.add(step.wait_for.timeout_until.root)

    expressions = [step.condition, step.until]
    expressions.extend(
        branch.when for branch in step.on_result or [] if isinstance(branch.when, str)
    )
    for expression in expressions:
        if expression is None:
            continue
        try:
            expression_roots = condition_reference_roots(expression)
        except ExpressionSyntaxError:
            continue
        roots.update(root for root in expression_roots if root != INPUT_ROOT)
    return roots


def _result_issues(
    workflow: WorkflowConfig,
    normal_available: dict[str, frozenset[str]],
    handler_available: dict[str, frozenset[str]],
) -> list[AnalysisIssue]:
    if workflow.result is None:
        return []
    root = workflow.result.root
    issues: list[AnalysisIssue] = []
    for step in workflow.flow:
        if not step.terminal or step.reason is not None:
            continue
        availability = [
            available[step.name]
            for available in (normal_available, handler_available)
            if step.name in available
        ]
        if availability and root not in frozenset.intersection(*availability):
            issues.append(
                AnalysisIssue(
                    location=f"flow.{step.name}",
                    message=(
                        f"Workflow result '{workflow.result}' is not available at "
                        f"normal terminal '{step.name}'"
                    ),
                )
            )
    return issues


def _group_outgoing(edges: list[FlowEdge]) -> dict[str, list[FlowEdge]]:
    outgoing: dict[str, list[FlowEdge]] = {}
    for edge in edges:
        outgoing.setdefault(edge.source, []).append(edge)
    return outgoing
