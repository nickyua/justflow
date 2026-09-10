"""Graph model and builder for workflow visualization."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from justflow.config.grammar import ReferencePath
from justflow.config.models import (
    ChildWorkflowTarget,
    ResourceConfig,
    ServiceConfig,
    ServiceOperationTarget,
    StepDefinition,
    WorkflowConfig,
)

# Node-id prefix for data-plane (resource) nodes, keeping them collision-free
# from flow-step ids.
RESOURCE_NODE_PREFIX = "res__"
INLINE_CONDITION_PREFIX = "input."
INLINE_CONDITION_DISPLAY_PREFIXES = ("flags.",)


def _serialize_references(value: Any) -> Any:
    if isinstance(value, ReferencePath):
        return value.value
    if isinstance(value, dict):
        return {key: _serialize_references(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_references(item) for item in value]
    return value


def _shorten_condition(text: str) -> str:
    """Shorten condition text for display in diagram nodes."""
    if text.startswith(INLINE_CONDITION_PREFIX):
        text = text[len(INLINE_CONDITION_PREFIX) :]
        for prefix in INLINE_CONDITION_DISPLAY_PREFIXES:
            text = text.removeprefix(prefix)
    return text


def _human_duration(seconds: int) -> str:
    for unit_seconds, suffix in ((86400, "d"), (3600, "h"), (60, "m")):
        if seconds % unit_seconds == 0 and seconds >= unit_seconds:
            return f"{seconds // unit_seconds}{suffix}"
    return f"{seconds}s"


@dataclass
class GraphNode:
    id: str
    label: str
    # "step" | "terminal" | "iteration" | "decision" | "action" | "wait" | "sleep"
    node_type: str
    metadata: dict[str, Any] = field(default_factory=dict)
    # Sub-workflow name when this node belongs to an embedded child graph.
    group: str | None = None


@dataclass
class GraphEdge:
    source: str
    target: str
    label: str | None = None
    style: str | None = None  # "dashed" for bypass/fallback edges


@dataclass
class WorkflowGraph:
    name: str
    description: str
    nodes: list[GraphNode] = field(default_factory=list)
    edges: list[GraphEdge] = field(default_factory=list)


def build_graph(
    config: WorkflowConfig,
    subworkflows: dict[str, WorkflowConfig] | None = None,
    services: dict[str, ServiceConfig] | None = None,
    resources: dict[str, ResourceConfig] | None = None,
) -> WorkflowGraph:
    """Build a WorkflowGraph from a WorkflowConfig.

    Conditional steps are split into a decision diamond + action rectangle
    with a "skip" bypass edge for clarity. When `subworkflows` is provided,
    referenced child workflows are embedded as grouped subgraphs connected
    by dashed edges from their calling steps. When `services` is provided,
    nodes are annotated with their service's transport. When `resources` is
    provided, the data plane is rendered: resource cylinders with dotted
    uses/cache/audit edges from the steps that touch them.
    """
    graph = _build_single(config, services)
    if subworkflows:
        _attach_subworkflows(graph, config, subworkflows, services, prefix="", attached=set())
    if resources is not None:
        _attach_resources(graph, config, subworkflows or {}, resources)
    return graph


def _attach_resources(
    graph: WorkflowGraph,
    config: WorkflowConfig,
    subworkflows: dict[str, WorkflowConfig],
    resources: dict[str, ResourceConfig],
) -> None:
    """Add resource cylinders + dotted usage edges for every touched resource."""
    node_ids = {n.id for n in graph.nodes}
    used: dict[str, ResourceConfig | None] = {}

    def resource_node_id(name: str) -> str:
        if name not in used:
            used[name] = resources.get(name)
        return RESOURCE_NODE_PREFIX + name

    def usage_edge(source: str, resource_name: str, kind: str) -> None:
        if source not in node_ids:
            return
        graph.edges.append(
            GraphEdge(
                source=source,
                target=resource_node_id(resource_name),
                label=kind,
                style="dotted",
            )
        )

    def attach_for(wf: WorkflowConfig, prefix: str) -> None:
        terminals = [s.name for s in wf.flow if s.terminal]
        if wf.on_complete:
            for terminal in terminals:
                usage_edge(prefix + terminal, wf.on_complete.resource, "audit")
        for step in wf.flow:
            if step.terminal or step.op is None:
                continue
            step_def = wf.steps.get(step.op)
            if step_def is None:
                continue
            for resource_name in step_def.required_resources:
                usage_edge(prefix + step.name, resource_name, "uses")
            if step_def.cache is not None:
                usage_edge(prefix + step.name, step_def.cache.resource, "cache")

    attach_for(config, "")
    for name, child in subworkflows.items():
        # Only embedded children have prefixed nodes in this graph
        if any(node.group == name for node in graph.nodes):
            attach_for(child, f"{name}__")

    for name, resource_config in used.items():
        metadata: dict[str, Any] = {"resource": name}
        if resource_config is not None:
            metadata["provider"] = resource_config.provider or resource_config.class_path
        graph.nodes.append(
            GraphNode(
                id=RESOURCE_NODE_PREFIX + name,
                label=name,
                node_type="resource",
                metadata=metadata,
            )
        )


def _transport_of(
    op_def: StepDefinition | None, services: dict[str, ServiceConfig] | None
) -> str | None:
    if op_def is None or not isinstance(op_def.target, ServiceOperationTarget) or services is None:
        return None
    service = services.get(op_def.target.service)
    return service.transport if service else None


# Keep node labels compact: long HTTP paths show METHOD + trailing segments.
HTTP_ACTION_LABEL_SEGMENTS = 2


def _short_action(action: str) -> str:
    if ":" not in action:
        return action
    method, path = action.split(":", 1)
    segments = [s for s in path.split("/") if s]
    if len(segments) > HTTP_ACTION_LABEL_SEGMENTS:
        path = "/…/" + "/".join(segments[-HTTP_ACTION_LABEL_SEGMENTS:])
    return f"{method} {path}"


def _call_line(
    op_def: StepDefinition | None, services: dict[str, ServiceConfig] | None
) -> str | None:
    """The 'service · action [transport]' label line for a service-backed step."""
    if op_def is None or not isinstance(op_def.target, ServiceOperationTarget):
        return None
    line = f"{op_def.target.service} · {_short_action(op_def.target.action)}"
    transport = _transport_of(op_def, services)
    return f"{line} [{transport}]" if transport else line


def _entry_node_id(config: WorkflowConfig) -> str:
    first = config.flow[0]
    if first.condition and not first.terminal:
        return f"{first.name}_check"
    return first.name


def _attach_subworkflows(
    graph: WorkflowGraph,
    config: WorkflowConfig,
    subworkflows: dict[str, WorkflowConfig],
    services: dict[str, ServiceConfig] | None,
    prefix: str,
    attached: set[str],
) -> None:
    for step in config.flow:
        if step.terminal or step.op is None:
            continue
        op_def = config.steps.get(step.op)
        if op_def is None or not isinstance(op_def.target, ChildWorkflowTarget):
            continue
        child_config = subworkflows.get(op_def.target.workflow)
        if child_config is None:
            continue

        child_name = op_def.target.workflow
        child_prefix = f"{child_name}__"
        if child_name not in attached:
            attached.add(child_name)
            child_graph = _build_single(child_config, services)
            for node in child_graph.nodes:
                node.id = child_prefix + node.id
                node.group = child_name
                graph.nodes.append(node)
            for edge in child_graph.edges:
                edge.source = child_prefix + edge.source
                edge.target = child_prefix + edge.target
                graph.edges.append(edge)
            _attach_subworkflows(
                graph,
                child_config,
                subworkflows,
                services,
                prefix=child_prefix,
                attached=attached,
            )

        graph.edges.append(
            GraphEdge(
                source=prefix + step.name,
                target=child_prefix + _entry_node_id(child_config),
                label="per item" if step.for_each else "calls",
                style="dashed",
            )
        )


def _build_single(
    config: WorkflowConfig, services: dict[str, ServiceConfig] | None = None
) -> WorkflowGraph:
    graph = WorkflowGraph(name=config.workflow, description=config.description)
    step_defs = config.steps

    # Map step names to their "entry" node ID (for rewiring edges to conditional steps)
    entry_node_ids: dict[str, str] = {}

    for step in config.flow:
        metadata: dict[str, Any] = {}

        if step.output:
            metadata["output"] = step.output
        if step.input:
            metadata["input"] = _serialize_references(step.input)
        if step.params:
            metadata["params"] = step.params

        if step.terminal:
            entry_node_ids[step.name] = step.name
            node_metadata = dict(metadata)
            if step.reason:
                node_metadata["reason"] = step.reason
            graph.nodes.append(
                GraphNode(
                    id=step.name,
                    label=step.reason or step.name,
                    node_type="terminal",
                    metadata=node_metadata,
                )
            )

        elif step.condition:
            # Split into decision node + action node
            decision_id = f"{step.name}_check"
            action_id = step.name

            entry_node_ids[step.name] = decision_id

            op_def = step_defs.get(step.op) if step.op else None
            decision_meta = {"condition": step.condition}
            action_meta = dict(metadata)
            action_meta["op"] = step.op
            action_label = step.name
            if op_def:
                if isinstance(op_def.target, ServiceOperationTarget):
                    action_meta["service"] = op_def.target.service
                    action_meta["action"] = op_def.target.action
                else:
                    action_meta["workflow"] = op_def.target.workflow
            transport = _transport_of(op_def, services)
            if transport:
                action_meta["transport"] = transport
            call_line = _call_line(op_def, services)
            if call_line:
                action_label = f"{step.name}<br/>{call_line}"

            graph.nodes.append(
                GraphNode(
                    id=decision_id,
                    label=_shorten_condition(step.condition),
                    node_type="decision",
                    metadata=decision_meta,
                )
            )
            graph.nodes.append(
                GraphNode(
                    id=action_id,
                    label=action_label,
                    node_type="action",
                    metadata=action_meta,
                )
            )

            # Decision → action (yes path)
            graph.edges.append(GraphEdge(source=decision_id, target=action_id, label="yes"))

            # Decision → skip to then target (no path) — deferred, resolved below
            # Action → then target — deferred, resolved below
            if step.then:
                graph.edges.append(
                    GraphEdge(
                        source=decision_id,
                        target=f"__resolve__{step.then}",
                        label="skip",
                        style="dashed",
                    )
                )
                graph.edges.append(GraphEdge(source=action_id, target=f"__resolve__{step.then}"))
            if step.on_failure:
                graph.edges.append(
                    GraphEdge(
                        source=action_id,
                        target=f"__resolve__{step.on_failure}",
                        label="on failure",
                        style="dashed",
                    )
                )

        elif step.wait_for is not None:
            entry_node_ids[step.name] = step.name
            metadata["signal"] = step.wait_for.signal
            bounds = []
            if step.wait_for.timeout_sec is not None:
                metadata["timeout_sec"] = step.wait_for.timeout_sec
                bounds.append(_human_duration(step.wait_for.timeout_sec))
            if step.wait_for.timeout_until is not None:
                metadata["timeout_until"] = step.wait_for.timeout_until.value
                bounds.append(f"until {step.wait_for.timeout_until}")

            graph.nodes.append(
                GraphNode(
                    id=step.name,
                    label=f"{step.name}<br/>wait: {step.wait_for.signal} ({', '.join(bounds)})",
                    node_type="wait",
                    metadata=metadata,
                )
            )

            if step.wait_for.on_timeout:
                graph.edges.append(
                    GraphEdge(
                        source=step.name,
                        target=f"__resolve__{step.wait_for.on_timeout}",
                        label="timeout",
                        style="dashed",
                    )
                )
            if step.then:
                graph.edges.append(GraphEdge(source=step.name, target=f"__resolve__{step.then}"))
            _append_on_result_edges(graph, step)

        elif step.sleep_sec is not None:
            entry_node_ids[step.name] = step.name
            metadata["sleep_sec"] = step.sleep_sec
            graph.nodes.append(
                GraphNode(
                    id=step.name,
                    label=f"{step.name}<br/>sleep {_human_duration(step.sleep_sec)}",
                    node_type="sleep",
                    metadata=metadata,
                )
            )
            if step.then:
                graph.edges.append(GraphEdge(source=step.name, target=f"__resolve__{step.then}"))

        elif step.for_each:
            entry_node_ids[step.name] = step.name
            op_def = step_defs.get(step.op) if step.op else None
            metadata["op"] = step.op
            metadata["for_each"] = step.for_each.value
            metadata["parallel"] = step.parallel
            label = step.name
            if step.max_concurrency:
                metadata["max_concurrency"] = step.max_concurrency
            if step.as_var:
                metadata["as"] = step.as_var
            if step.on_iteration_fail:
                metadata["on_iteration_fail"] = step.on_iteration_fail.value
            if op_def:
                if isinstance(op_def.target, ChildWorkflowTarget):
                    metadata["workflow"] = op_def.target.workflow
                    label = f"{step.name}<br/>⊂ {op_def.target.workflow} (per item)"
                else:
                    metadata["service"] = op_def.target.service
                    metadata["action"] = op_def.target.action
                    transport = _transport_of(op_def, services)
                    if transport:
                        metadata["transport"] = transport
                    call_line = _call_line(op_def, services)
                    if call_line:
                        label = f"{step.name}<br/>{call_line}"

            graph.nodes.append(
                GraphNode(id=step.name, label=label, node_type="iteration", metadata=metadata)
            )

            if step.then:
                graph.edges.append(GraphEdge(source=step.name, target=f"__resolve__{step.then}"))
            if step.on_failure:
                graph.edges.append(
                    GraphEdge(
                        source=step.name,
                        target=f"__resolve__{step.on_failure}",
                        label="on failure",
                        style="dashed",
                    )
                )

        else:
            entry_node_ids[step.name] = step.name
            op_def = step_defs.get(step.op) if step.op else None
            metadata["op"] = step.op
            label = step.name
            if op_def:
                if isinstance(op_def.target, ChildWorkflowTarget):
                    metadata["workflow"] = op_def.target.workflow
                    label = f"{step.name}<br/>⊂ {op_def.target.workflow}"
                else:
                    metadata["service"] = op_def.target.service
                    metadata["action"] = op_def.target.action
                    transport = _transport_of(op_def, services)
                    if transport:
                        metadata["transport"] = transport
                    call_line = _call_line(op_def, services)
                    if call_line:
                        label = f"{step.name}<br/>{call_line}"
            if step.until:
                metadata["until"] = step.until
                metadata["max_iterations"] = step.max_iterations
                label = f"{label}<br/>↻ until {_shorten_condition(step.until)} (≤{step.max_iterations}x)"

            graph.nodes.append(
                GraphNode(id=step.name, label=label, node_type="step", metadata=metadata)
            )

            if step.on_exhausted:
                graph.edges.append(
                    GraphEdge(
                        source=step.name,
                        target=f"__resolve__{step.on_exhausted}",
                        label="exhausted",
                        style="dashed",
                    )
                )
            if step.on_failure:
                graph.edges.append(
                    GraphEdge(
                        source=step.name,
                        target=f"__resolve__{step.on_failure}",
                        label="on failure",
                        style="dashed",
                    )
                )
            if step.then:
                graph.edges.append(GraphEdge(source=step.name, target=f"__resolve__{step.then}"))

            _append_on_result_edges(graph, step)

    # Resolve edge targets — point to the entry node of each step
    for edge in graph.edges:
        if edge.target.startswith("__resolve__"):
            step_name = edge.target[len("__resolve__") :]
            edge.target = entry_node_ids.get(step_name, step_name)

    return graph


def _append_on_result_edges(graph: WorkflowGraph, step: Any) -> None:
    for branch in step.on_result or []:
        if branch.default:
            graph.edges.append(
                GraphEdge(source=step.name, target=f"__resolve__{branch.default}", label="default")
            )
        elif branch.when and branch.then:
            when_str = branch.when if isinstance(branch.when, str) else branch.when.evaluator
            graph.edges.append(
                GraphEdge(source=step.name, target=f"__resolve__{branch.then}", label=when_str)
            )
