"""Tests for workflow visualization."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from justflow.config.models import (
    FlowStep,
    OnResultBranch,
    StepDefinition,
    WorkflowConfig,
)
from justflow.visualization.graph import GraphNode, WorkflowGraph, build_graph
from justflow.visualization.renderer import (
    VisualizationRenderError,
    _vendored_mermaid_source,
    render_html,
    render_mermaid,
)


@dataclass(frozen=True, kw_only=True)
class MermaidEscapingCase:
    id: str
    label: str
    expected_fragment: str
    forbidden_fragment: str | None


MERMAID_ESCAPING_CASES = [
    MermaidEscapingCase(
        id="allowed-line-break",
        label="first<br/>second",
        expected_fragment='n0["first<br/>second"]',
        forbidden_fragment=None,
    ),
    MermaidEscapingCase(
        id="script-tag",
        label="<script>alert(1)</script>",
        expected_fragment="&lt;script&gt;alert(1)&lt;/script&gt;",
        forbidden_fragment="<script>",
    ),
    MermaidEscapingCase(
        id="tag-with-event-handler",
        label='x<img src=x onerror="alert(1)">y',
        expected_fragment="x&lt;img src=x onerror=&quot;alert(1)&quot;&gt;y",
        forbidden_fragment="<img",
    ),
    MermaidEscapingCase(
        id="line-break-with-attribute",
        label='first<br onclick="alert(1)">second',
        expected_fragment="first&lt;br onclick=&quot;alert(1)&quot;&gt;second",
        forbidden_fragment="<br onclick",
    ),
]


def _make_workflow(**overrides) -> WorkflowConfig:
    defaults = {
        "workflow": "test_wf",
        "description": "Test workflow",
        "steps": {"op1": StepDefinition(service="svc", action="do_thing")},
        "flow": [
            FlowStep(name="s1", op="op1", then="done"),
            FlowStep(name="done", terminal=True),
        ],
    }
    defaults.update(overrides)
    return WorkflowConfig(**defaults)


class TestBuildGraph:
    @pytest.mark.parametrize(
        "flow,expected_node_types",
        [
            (
                [FlowStep(name="s1", op="op1", then="done"), FlowStep(name="done", terminal=True)],
                {"s1": "step", "done": "terminal"},
            ),
            (
                [
                    FlowStep(name="s1", op="op1", then="s2"),
                    FlowStep(name="s2", op="op1", condition="input.x == true", then="done"),
                    FlowStep(name="done", terminal=True),
                ],
                {"s1": "step", "s2_check": "decision", "s2": "action", "done": "terminal"},
            ),
            (
                [
                    FlowStep(
                        name="iter",
                        op="op1",
                        for_each="input.items",
                        **{"as": "item"},
                        then="done",
                    ),
                    FlowStep(name="done", terminal=True),
                ],
                {"iter": "iteration", "done": "terminal"},
            ),
        ],
        ids=["linear", "conditional", "iteration"],
    )
    def test_node_types(self, flow, expected_node_types):
        wf = _make_workflow(flow=flow)
        graph = build_graph(wf)
        node_types = {n.id: n.node_type for n in graph.nodes}
        assert node_types == expected_node_types

    def test_branching_edges(self):
        wf = _make_workflow(
            steps={
                "op1": StepDefinition(service="svc", action="check"),
                "op2": StepDefinition(service="svc", action="do_a"),
            },
            flow=[
                FlowStep(
                    name="check",
                    op="op1",
                    on_result=[
                        OnResultBranch(when="input.ok == true", then="proceed"),
                        OnResultBranch(default="fail"),
                    ],
                ),
                FlowStep(name="proceed", op="op2", then="done"),
                FlowStep(name="fail", terminal=True, reason="failed"),
                FlowStep(name="done", terminal=True),
            ],
        )
        graph = build_graph(wf)
        edges = [(e.source, e.target, e.label) for e in graph.edges]
        assert ("check", "proceed", "input.ok == true") in edges
        assert ("check", "fail", "default") in edges

    def test_terminal_with_reason(self):
        wf = _make_workflow(
            flow=[
                FlowStep(name="s1", op="op1", then="end"),
                FlowStep(name="end", terminal=True, reason="all_done"),
            ],
        )
        graph = build_graph(wf)
        terminal = next(n for n in graph.nodes if n.id == "end")
        assert terminal.node_type == "terminal"
        assert "all_done" in terminal.label
        assert terminal.metadata["reason"] == "all_done"

    def test_graph_metadata(self):
        wf = _make_workflow()
        graph = build_graph(wf)
        assert graph.name == "test_wf"
        assert graph.description == "Test workflow"


class TestRenderMermaid:
    @pytest.mark.parametrize(
        "case",
        MERMAID_ESCAPING_CASES,
        ids=lambda case: case.id,
    )
    def test_label_html_allowlist(self, case: MermaidEscapingCase) -> None:
        graph = WorkflowGraph(
            name="escaping",
            description="",
            nodes=[GraphNode(id="node", label=case.label, node_type="step")],
        )

        mermaid = render_mermaid(graph)

        assert case.expected_fragment in mermaid
        if case.forbidden_fragment is not None:
            assert case.forbidden_fragment not in mermaid

    def test_contains_node_shapes(self):
        wf = _make_workflow(
            steps={
                "op1": StepDefinition(service="svc", action="act"),
            },
            flow=[
                FlowStep(name="step", op="op1", then="iter"),
                FlowStep(name="iter", op="op1", for_each="input.items", **{"as": "i"}, then="cond"),
                FlowStep(name="cond", op="op1", condition="input.x == true", then="end"),
                FlowStep(name="end", terminal=True),
            ],
        )
        graph = build_graph(wf)
        mermaid = render_mermaid(graph)
        # Op nodes carry a compact 'service · action' line in the label
        assert 'n0["step<br/>svc · act"]' in mermaid
        assert 'n1[["iter<br/>svc · act"]]' in mermaid
        # Conditional splits into decision diamond (shortened label) + action trapezoid
        assert 'n2{"x == true"}' in mermaid
        assert 'n3[/"cond<br/>svc · act"\\]' in mermaid
        assert 'n4(["end"])' in mermaid

    def test_contains_edges(self):
        wf = _make_workflow()
        graph = build_graph(wf)
        mermaid = render_mermaid(graph)
        assert "n0 --> n1" in mermaid

    def test_class_defs(self):
        wf = _make_workflow(
            flow=[
                FlowStep(name="s1", op="op1", condition="input.x", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )
        graph = build_graph(wf)
        mermaid = render_mermaid(graph)
        assert "classDef terminal" in mermaid
        assert "classDef decision" in mermaid
        assert "classDef action" in mermaid

    def test_conditional_skip_edge(self):
        wf = _make_workflow(
            flow=[
                FlowStep(name="s1", op="op1", condition="input.x", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )
        graph = build_graph(wf)
        mermaid = render_mermaid(graph)
        # Dashed skip edge from decision to target
        assert '-.->|"skip"|' in mermaid
        # Solid yes edge from decision to action
        assert '-->|"yes"|' in mermaid

    def test_uses_opaque_ids_and_escapes_labels(self):
        graph = WorkflowGraph(
            name="unsafe",
            description="",
            nodes=[
                GraphNode(
                    id='node; click node "javascript:alert(1)"',
                    label='value"<&',
                    node_type="step",
                )
            ],
        )

        mermaid = render_mermaid(graph)

        assert 'n0["value&quot;&lt;&amp;"]' in mermaid
        assert "javascript:alert" not in mermaid


class TestRenderHtml:
    def test_contains_mermaid_script(self):
        html = render_html("graph TD\n    A --> B", title="Test", description="desc")
        assert "mermaid" in html
        assert "<title>Test</title>" in html
        assert "desc" in html
        assert "graph TD" in html

    def test_self_contained(self):
        html = render_html("graph TD\n    A --> B", title="T")
        assert "<!DOCTYPE html>" in html
        # mermaid.js is inlined (vendored) — no external requests needed
        assert "cdn.jsdelivr.net" not in html
        assert "mermaid" in html
        assert "securityLevel: 'strict'" in html

    def test_tabs_present(self):
        html = render_html("graph TD\n    A --> B", title="T")
        assert 'data-tab="flow"' in html
        assert 'data-tab="services"' in html
        assert 'data-tab="resources"' in html

    def test_services_data_embedded(self):
        services = {
            "my_svc": {
                "transport": "direct",
                "class_": "path.to.Class",
                "dispatch_timeout_sec": 10,
            }
        }
        html = render_html("graph TD", title="T", services=services)
        assert "my_svc" in html
        assert "path.to.Class" in html

    def test_unsupported_metadata_is_rejected_instead_of_stringified(self):
        with pytest.raises(VisualizationRenderError, match="not strict JSON"):
            render_html(
                "graph TD",
                title="T",
                services={"service": {"unsupported": object()}},
            )

    def test_resources_data_embedded(self):
        resources = {"postgres": {"provider": "postgresql", "config": {"connection": "primary"}}}
        html = render_html("graph TD", title="T", resources=resources)
        assert "postgres" in html
        assert "postgresql" in html

    def test_node_metadata_embedded(self):
        wf = _make_workflow()
        graph = build_graph(wf)
        mermaid = render_mermaid(graph)
        html = render_html(mermaid, title="T", graph=graph)
        assert "nodeMetadata" in html
        assert '"s1"' in html

    def test_edge_metadata_embedded(self):
        wf = _make_workflow(
            steps={
                "op1": StepDefinition(service="svc", action="check"),
                "op2": StepDefinition(service="svc", action="do_a"),
            },
            flow=[
                FlowStep(
                    name="check",
                    op="op1",
                    on_result=[
                        OnResultBranch(when="input.flags.is_valid == true", then="proceed"),
                        OnResultBranch(default="fail"),
                    ],
                ),
                FlowStep(name="proceed", op="op2", then="done"),
                FlowStep(name="fail", terminal=True, reason="failed"),
                FlowStep(name="done", terminal=True),
            ],
        )
        graph = build_graph(wf)
        mermaid = render_mermaid(graph)
        html = render_html(mermaid, title="T", graph=graph)
        assert "edgeMetadata" in html
        assert "input.flags.is_valid == true" in html
        assert "is_valid == true" in html

    def test_html_and_script_contexts_escape_untrusted_values(self):
        sentinel = "</script><script>window.rendererSentinel = true</script>\u2028&"
        html = render_html(
            f'graph TD\n    A["{sentinel}"]',
            title=sentinel,
            description=sentinel,
            services={sentinel: {"transport": "direct", "params": {"value": sentinel}}},
            resources={sentinel: {"provider": sentinel}},
        )

        assert "<script>window.rendererSentinel" not in html
        assert "&lt;/script&gt;" in html
        assert "\\u003c/script\\u003e" in html
        assert "\\u2028" in html

    def test_missing_vendored_asset_fails_without_network_fallback(self, monkeypatch, tmp_path):
        from justflow.visualization import renderer

        _vendored_mermaid_source.cache_clear()
        monkeypatch.setattr(renderer, "VENDORED_MERMAID_JS", tmp_path / "missing.js")

        with pytest.raises(VisualizationRenderError, match="unavailable"):
            render_html("graph TD", title="T")

        _vendored_mermaid_source.cache_clear()


class TestGConstructVisualization:
    def _wait_workflow(self) -> WorkflowConfig:
        return WorkflowConfig(
            workflow="wait_wf",
            steps={"notify": StepDefinition(service="svc", action="notify")},
            flow=[
                FlowStep(
                    name="wait_form",
                    wait_for={
                        "signal": "form_submitted",
                        "timeout_sec": 259200,
                        "on_timeout": "fallback",
                    },
                    output="form",
                    then="pause",
                ),
                FlowStep(name="fallback", op="notify", then="pause"),
                FlowStep(name="pause", sleep_sec=300, then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

    def test_wait_step_node_and_timeout_edge(self):
        graph = build_graph(self._wait_workflow())

        wait_node = next(n for n in graph.nodes if n.id == "wait_form")
        assert wait_node.node_type == "wait"
        assert wait_node.metadata["signal"] == "form_submitted"
        assert "3d" in wait_node.label

        timeout_edge = next(e for e in graph.edges if e.label == "timeout")
        assert timeout_edge.source == "wait_form"
        assert timeout_edge.target == "fallback"
        assert timeout_edge.style == "dashed"

    def test_sleep_node(self):
        graph = build_graph(self._wait_workflow())
        sleep_node = next(n for n in graph.nodes if n.id == "pause")
        assert sleep_node.node_type == "sleep"
        assert "5m" in sleep_node.label

    def test_mermaid_renders_hexagons_and_dashed_timeout(self):
        mermaid = render_mermaid(build_graph(self._wait_workflow()))
        assert 'n0{{"' in mermaid
        assert 'n0 -.->|"timeout"| n1' in mermaid
        assert "classDef wait" in mermaid

    def test_until_loop_annotation_and_exhausted_edge(self):
        config = WorkflowConfig(
            workflow="until_wf",
            steps={"check": StepDefinition(service="svc", action="check")},
            flow=[
                FlowStep(
                    name="poll",
                    op="check",
                    until="input.ready == true",
                    max_iterations=10,
                    on_exhausted="give_up",
                    then="done",
                ),
                FlowStep(name="give_up", terminal=True, reason="never_ready"),
                FlowStep(name="done", terminal=True),
            ],
        )

        graph = build_graph(config)
        poll = next(n for n in graph.nodes if n.id == "poll")
        assert "≤10x" in poll.label

        exhausted = next(e for e in graph.edges if e.label == "exhausted")
        assert exhausted.style == "dashed"
        assert exhausted.target == "give_up"

    def test_subworkflow_step_label(self):
        config = WorkflowConfig(
            workflow="parent_wf",
            steps={"child": StepDefinition(workflow="child_flow")},
            flow=[
                FlowStep(name="delegate", op="child", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )

        graph = build_graph(config)
        node = next(n for n in graph.nodes if n.id == "delegate")
        assert node.metadata["workflow"] == "child_flow"
        assert "child_flow" in node.label


class TestCompositeSubworkflowGraph:
    def _workflows(self) -> dict[str, WorkflowConfig]:
        child = WorkflowConfig(
            workflow="child_flow",
            steps={"work": StepDefinition(service="svc", action="work")},
            flow=[
                FlowStep(name="do_work", op="work", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )
        parent = WorkflowConfig(
            workflow="parent_flow",
            steps={"call_child": StepDefinition(workflow="child_flow")},
            flow=[
                FlowStep(
                    name="fan",
                    op="call_child",
                    input="items",
                    for_each="input",
                    as_var="item",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
            params={"items": "${items}"},
        )
        return {"parent_flow": parent, "child_flow": child}

    def test_child_embedded_as_grouped_subgraph_with_link_edge(self):
        workflows = self._workflows()
        graph = build_graph(workflows["parent_flow"], subworkflows=workflows)

        child_nodes = [n for n in graph.nodes if n.group == "child_flow"]
        assert {n.id for n in child_nodes} == {"child_flow__do_work", "child_flow__done"}

        link = next(e for e in graph.edges if e.label == "per item")
        assert link.source == "fan"
        assert link.target == "child_flow__do_work"
        assert link.style == "dashed"

    def test_mermaid_renders_subgraph_block(self):
        workflows = self._workflows()
        mermaid = render_mermaid(build_graph(workflows["parent_flow"], subworkflows=workflows))

        assert 'subgraph g0["sub-workflow: child_flow"]' in mermaid
        assert 'n0 -.->|"per item"| n2' in mermaid

    def test_iteration_node_shows_subworkflow(self):
        workflows = self._workflows()
        graph = build_graph(workflows["parent_flow"], subworkflows=workflows)
        fan = next(n for n in graph.nodes if n.id == "fan")
        assert fan.metadata["workflow"] == "child_flow"
        assert "child_flow" in fan.label


class TestTransportAnnotation:
    def test_nodes_carry_transport_when_services_provided(self):
        from justflow.config.models import ServiceConfig

        services = {
            "queue_service": ServiceConfig(
                transport="queue",
                transport_config={
                    "broker": "main",
                    "destination": "requests",
                    "idempotency": "durable",
                },
                dispatch_timeout_sec=10,
                response_timeout_sec=60,
                retries=1,
            ),
        }
        config = WorkflowConfig(
            workflow="t_wf",
            steps={"process": StepDefinition(service="queue_service", action="process_item")},
            flow=[
                FlowStep(name="s1", op="process", output="data", then="maybe"),
                FlowStep(
                    name="maybe",
                    op="process",
                    input="s1.data",
                    condition="input.ok == true",
                    then="loop",
                ),
                FlowStep(
                    name="loop",
                    op="process",
                    input="s1.data",
                    for_each="input",
                    as_var="x",
                    then="end",
                ),
                FlowStep(name="end", terminal=True),
            ],
        )

        graph = build_graph(config, services=services)

        plain = next(n for n in graph.nodes if n.id == "s1")
        guarded = next(n for n in graph.nodes if n.id == "maybe")
        loop = next(n for n in graph.nodes if n.id == "loop")
        for node in (plain, guarded, loop):
            assert node.metadata["transport"] == "queue"
            assert "[queue]" in node.label

    def test_no_services_means_no_transport(self):
        config = WorkflowConfig(
            workflow="t_wf",
            steps={"process": StepDefinition(service="queue_service", action="process_item")},
            flow=[
                FlowStep(name="s1", op="process", then="end"),
                FlowStep(name="end", terminal=True),
            ],
        )
        graph = build_graph(config)
        assert "transport" not in next(n for n in graph.nodes if n.id == "s1").metadata


class TestTransportEdgeColoring:
    def test_edges_into_service_nodes_get_transport_link_styles(self):
        from justflow.config.models import ServiceConfig

        services = {
            "queue_service": ServiceConfig(
                transport="queue",
                transport_config={
                    "broker": "main",
                    "destination": "requests",
                    "idempotency": "durable",
                },
                dispatch_timeout_sec=10,
                response_timeout_sec=60,
                retries=1,
            ),
            "source_api": ServiceConfig(
                transport="http",
                transport_config={"base_url": "https://x"},
                connect_timeout_sec=5,
                dispatch_timeout_sec=30,
                retries=1,
            ),
        }
        config = WorkflowConfig(
            workflow="edge_wf",
            steps={
                "process": StepDefinition(service="queue_service", action="process_item"),
                "update": StepDefinition(service="source_api", action="PUT:/api/x"),
            },
            flow=[
                FlowStep(name="s1", op="process", output="data", then="s2"),
                FlowStep(name="s2", op="update", input="s1.data", then="end"),
                FlowStep(name="end", terminal=True),
            ],
        )

        mermaid = render_mermaid(build_graph(config, services=services))

        # edge 0: s1 -> s2 (target http), edge 1: s2 -> end (terminal, uncolored)
        assert "linkStyle 0 stroke:#0284c7,stroke-width:2px" in mermaid
        assert "linkStyle 1" not in mermaid


class TestDataPlaneVisualization:
    def _config_and_resources(self):
        from justflow.config.models import ResourceConfig

        resources = {
            "audit_store": ResourceConfig(provider="memory_archive"),
            "verification_db": ResourceConfig(provider="postgresql"),
        }
        config = WorkflowConfig(
            workflow="dp_wf",
            on_complete={
                "resource": "audit_store",
                "path": "a/${request_id}.json",
                "retention_policy": "test",
            },
            steps={
                "persist": StepDefinition(
                    service="svc", action="persist", required_resources=["verification_db"]
                ),
                "process": StepDefinition(
                    service="svc",
                    action="process",
                    cache={"resource": "verification_db", "key": "k"},
                ),
            },
            flow=[
                FlowStep(name="s1", op="process", output="data", then="s2"),
                FlowStep(name="s2", op="persist", input="s1.data", then="end"),
                FlowStep(name="end", terminal=True),
            ],
        )
        return config, resources

    def test_resource_nodes_and_usage_edges(self):
        config, resources = self._config_and_resources()
        graph = build_graph(config, resources=resources)

        db = next(n for n in graph.nodes if n.id == "res__verification_db")
        assert db.node_type == "resource"
        assert db.metadata["provider"] == "postgresql"

        labels = {(e.source, e.target): e.label for e in graph.edges if e.style == "dotted"}
        assert labels[("s2", "res__verification_db")] == "uses"
        assert labels[("s1", "res__verification_db")] == "cache"
        assert labels[("end", "res__audit_store")] == "audit"

    def test_mermaid_renders_cylinders_and_dotted_styles(self):
        config, resources = self._config_and_resources()
        mermaid = render_mermaid(build_graph(config, resources=resources))

        assert 'n4[("verification_db")]' in mermaid
        assert "classDef resource" in mermaid
        assert "stroke-dasharray:2 3" in mermaid

    def test_no_resources_means_no_data_plane(self):
        config, _ = self._config_and_resources()
        graph = build_graph(config)
        assert not any(n.node_type == "resource" for n in graph.nodes)


class TestStepStylingAndFailureEdges:
    def test_plain_steps_get_explicit_class(self):
        wf = _make_workflow()
        mermaid = render_mermaid(build_graph(wf))
        assert "classDef step fill:#ffffff" in mermaid
        assert "class n0 step" in mermaid

    def test_on_failure_edge_rendered_dashed(self):
        wf = _make_workflow(
            steps={
                "op1": StepDefinition(service="svc", action="do_thing"),
                "alert": StepDefinition(service="svc", action="alert"),
            },
            flow=[
                FlowStep(name="s1", op="op1", on_failure="warn", then="done"),
                FlowStep(name="warn", op="alert", then="done"),
                FlowStep(name="done", terminal=True),
            ],
        )
        graph = build_graph(wf)
        failure_edge = next(e for e in graph.edges if e.label == "on failure")
        assert failure_edge.source == "s1"
        assert failure_edge.target == "warn"
        assert failure_edge.style == "dashed"
        assert 'n0 -.->|"on failure"| n1' in render_mermaid(graph)
