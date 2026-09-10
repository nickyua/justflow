"""Mermaid diagram and HTML rendering for workflow graphs."""

from __future__ import annotations

import hashlib
from functools import lru_cache
from html import escape as escape_html
from pathlib import Path
from typing import Any

from justflow.engine.serialization import StrictJsonError, StrictJsonLayout, dumps_strict_json
from justflow.visualization.graph import (
    INLINE_CONDITION_DISPLAY_PREFIXES,
    INLINE_CONDITION_PREFIX,
    GraphNode,
    WorkflowGraph,
)

VENDORED_MERMAID_JS = Path(__file__).parent / "static" / "mermaid.min.js"
VENDORED_MERMAID_VERSION = "11.16.0"
VENDORED_MERMAID_SHA256 = "74d7c46dabca328c2294733910a8aa1ed0c37451776e8d5295da38a2b758fb9b"
MERMAID_LABEL_LINE_BREAK = "<br/>"
SHORT_EDGE_LABELS = frozenset({"default", "yes", "skip"})
EVALUATOR_PATH_SEPARATOR = "."


class VisualizationRenderError(Exception):
    pass


@lru_cache(maxsize=1)
def _vendored_mermaid_source() -> str:
    try:
        source = VENDORED_MERMAID_JS.read_text(encoding="utf-8")
    except OSError as exc:
        raise VisualizationRenderError(
            f"Vendored Mermaid {VENDORED_MERMAID_VERSION} is unavailable"
        ) from exc
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
    if digest != VENDORED_MERMAID_SHA256:
        raise VisualizationRenderError("Vendored Mermaid asset failed its integrity check")
    return source


def _vendored_tooltip_source() -> str:
    return (VENDORED_MERMAID_JS.parent / "graph-tooltips.js").read_text(encoding="utf-8")


def _vendored_tooltip_styles() -> str:
    return (VENDORED_MERMAID_JS.parent / "graph-tooltips.css").read_text(encoding="utf-8")


def _escape_mermaid(text: str) -> str:
    """Escape label content while preserving the sole allowed Mermaid HTML token."""
    return MERMAID_LABEL_LINE_BREAK.join(
        escape_html(part, quote=True) for part in text.split(MERMAID_LABEL_LINE_BREAK)
    )


def _script_safe_json(value: Any) -> str:
    try:
        serialized = dumps_strict_json(value, layout=StrictJsonLayout.SCRIPT)
    except StrictJsonError as exc:
        raise VisualizationRenderError("Visualization metadata is not strict JSON") from exc
    return serialized.replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")


def _shorten_edge_label(text: str) -> str:
    """Shorten long edge labels for readable display in the diagram."""
    if text in SHORT_EDGE_LABELS:
        return text

    # Evaluator path like "src.workflows.conditions.is_priority" → "is_priority"
    if EVALUATOR_PATH_SEPARATOR in text and "==" not in text and " " not in text:
        return text.rsplit(EVALUATOR_PATH_SEPARATOR, 1)[-1]

    # Inline condition like "input.flags.needs_review == true" → "needs_review == true"
    if text.startswith(INLINE_CONDITION_PREFIX):
        text = text[len(INLINE_CONDITION_PREFIX) :]
        # Strip common prefixes like "flags."
        for prefix in INLINE_CONDITION_DISPLAY_PREFIXES:
            text = text.removeprefix(prefix)

    return text


# Edge color = transport of the step the edge leads into (the gateway→service
# hop happens at the target node). Dash pattern independently means
# fallback/skip/per-item.
TRANSPORT_COLORS: dict[str, str] = {
    "direct": "#64748b",
    "http": "#0284c7",
    "grpc": "#16a34a",
    "queue": "#f59e0b",
    "lambda": "#d946ef",
}


def _node_line(node: GraphNode, node_id: str) -> str:
    label = _escape_mermaid(node.label)
    match node.node_type:
        case "terminal":
            return f'{node_id}(["{label}"])'
        case "iteration":
            return f'{node_id}[["{label}"]]'
        case "decision":
            return f'{node_id}{{"{label}"}}'
        case "action":
            return f'{node_id}[/"{label}"\\]'
        case "wait" | "sleep":
            return f'{node_id}{{{{"{label}"}}}}'
        case "resource":
            return f'{node_id}[("{label}")]'
        case _:
            return f'{node_id}["{label}"]'


def _opaque_node_ids(graph: WorkflowGraph) -> dict[str, str]:
    node_ids: dict[str, str] = {}
    for index, node in enumerate(graph.nodes):
        if node.id in node_ids:
            raise VisualizationRenderError(f"Duplicate graph node ID: {node.id}")
        node_ids[node.id] = f"n{index}"
    return node_ids


def render_mermaid(graph: WorkflowGraph) -> str:
    """Render a WorkflowGraph as a Mermaid flowchart definition."""
    lines = ["graph TD"]
    node_ids = _opaque_node_ids(graph)

    for node in graph.nodes:
        if node.group is None:
            lines.append(f"    {_node_line(node, node_ids[node.id])}")

    # Embedded sub-workflows render as boxed subgraphs
    groups: dict[str, list[GraphNode]] = {}
    for node in graph.nodes:
        if node.group is not None:
            groups.setdefault(node.group, []).append(node)
    for group_index, (group_name, nodes) in enumerate(groups.items()):
        group_label = _escape_mermaid(f"sub-workflow: {group_name}")
        lines.append(f'    subgraph g{group_index}["{group_label}"]')
        for node in nodes:
            lines.append(f"        {_node_line(node, node_ids[node.id])}")
        lines.append("    end")

    lines.append("")

    transport_by_node = {
        n.id: n.metadata.get("transport") for n in graph.nodes if n.metadata.get("transport")
    }
    link_styles: list[str] = []
    for index, edge in enumerate(graph.edges):
        try:
            source_id = node_ids[edge.source]
            target_id = node_ids[edge.target]
        except KeyError as exc:
            raise VisualizationRenderError(
                f"Graph edge references unknown node: {exc.args[0]}"
            ) from exc
        arrow = "-.->" if edge.style == "dashed" else "-->"
        if edge.label:
            short = _escape_mermaid(_shorten_edge_label(edge.label))
            lines.append(f'    {source_id} {arrow}|"{short}"| {target_id}')
        else:
            lines.append(f"    {source_id} {arrow} {target_id}")

        if edge.style == "dotted":
            # Data-plane (resource) edges: thin grey, tight dash
            link_styles.append(
                f"    linkStyle {index} stroke:#94a3b8,stroke-width:1.5px,stroke-dasharray:2 3"
            )
            continue
        transport = transport_by_node.get(edge.target)
        if transport in TRANSPORT_COLORS:
            link_styles.append(
                f"    linkStyle {index} stroke:{TRANSPORT_COLORS[transport]},stroke-width:2px"
            )

    lines.extend(link_styles)
    lines.append("")

    terminal_ids = [node_ids[n.id] for n in graph.nodes if n.node_type == "terminal"]
    iteration_ids = [node_ids[n.id] for n in graph.nodes if n.node_type == "iteration"]
    decision_ids = [node_ids[n.id] for n in graph.nodes if n.node_type == "decision"]
    action_ids = [node_ids[n.id] for n in graph.nodes if n.node_type == "action"]
    wait_ids = [node_ids[n.id] for n in graph.nodes if n.node_type in ("wait", "sleep")]
    resource_ids = [node_ids[n.id] for n in graph.nodes if n.node_type == "resource"]
    step_ids = [node_ids[n.id] for n in graph.nodes if n.node_type == "step"]

    # Explicit style for plain steps — without it Mermaid's default theme
    # renders them lavender, which reads as the wait/sleep purple.
    if step_ids:
        lines.append("    classDef step fill:#ffffff,stroke:#333,color:#111")
        lines.append(f"    class {','.join(step_ids)} step")
    if resource_ids:
        lines.append("    classDef resource fill:#f1f5f9,stroke:#94a3b8,color:#334155")
        lines.append(f"    class {','.join(resource_ids)} resource")
    if wait_ids:
        lines.append("    classDef wait fill:#ddd6fe,stroke:#7c3aed,color:#4c1d95")
        lines.append(f"    class {','.join(wait_ids)} wait")
    if terminal_ids:
        lines.append("    classDef terminal fill:#fa8072,stroke:#333,color:#000")
        lines.append(f"    class {','.join(terminal_ids)} terminal")
    if iteration_ids:
        lines.append("    classDef iteration fill:#6495ed,stroke:#333,color:#fff")
        lines.append(f"    class {','.join(iteration_ids)} iteration")
    if decision_ids:
        lines.append("    classDef decision fill:#fef3c7,stroke:#d97706,color:#92400e")
        lines.append(f"    class {','.join(decision_ids)} decision")
    if action_ids:
        lines.append("    classDef action fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e")
        lines.append(f"    class {','.join(action_ids)} action")

    return "\n".join(lines)


def render_html(
    mermaid_def: str,
    title: str,
    description: str = "",
    graph: WorkflowGraph | None = None,
    services: dict[str, Any] | None = None,
    resources: dict[str, Any] | None = None,
) -> str:
    """Render a self-contained HTML page with tabs, diagram, and hover overlays."""
    node_metadata: dict[str, dict[str, Any]] = {}
    edge_metadata: list[dict[str, Any]] = []
    if graph:
        opaque_ids = _opaque_node_ids(graph)
        for node in graph.nodes:
            node_metadata[opaque_ids[node.id]] = {
                **node.metadata,
                "id": node.id,
                "type": node.node_type,
            }
        for edge in graph.edges:
            entry: dict[str, Any] = {
                "source": edge.source,
                "target": edge.target,
                "kind": "resource" if edge.style == "dotted" else "flow",
            }
            if edge.label:
                entry["short_label"] = _shorten_edge_label(edge.label)
                entry["full_label"] = edge.label
            edge_metadata.append(entry)

    services_data = services or {}
    resources_data = resources or {}

    metadata_json = _script_safe_json(node_metadata)
    edge_metadata_json = _script_safe_json(edge_metadata)
    services_json = _script_safe_json(services_data)
    resources_json = _script_safe_json(resources_data)
    safe_title = escape_html(title)
    safe_description = escape_html(description)
    safe_mermaid = escape_html(mermaid_def, quote=False)
    mermaid_source = _vendored_mermaid_source()
    tooltip_source = _vendored_tooltip_source()
    tooltip_styles = _vendored_tooltip_styles()

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{safe_title}</title>
    <style>
        :root {{
            --bg: #f8f9fb;
            --surface: #ffffff;
            --border: #e2e8f0;
            --text: #1e293b;
            --text-secondary: #64748b;
            --accent: #3b82f6;
            --accent-hover: #2563eb;
            --radius: 10px;
            --shadow: 0 1px 3px rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.04);
            --shadow-lg: 0 4px 16px rgba(0,0,0,0.12), 0 8px 32px rgba(0,0,0,0.06);
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Inter, Roboto, sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.6;
        }}
        .header {{
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            padding: 1.5rem 2rem;
        }}
        .header h1 {{
            font-size: 1.5rem;
            font-weight: 700;
            letter-spacing: -0.02em;
        }}
        .header .description {{
            color: var(--text-secondary);
            margin-top: 0.25rem;
            font-size: 0.95rem;
        }}
        .tabs {{
            display: flex;
            gap: 0;
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            padding: 0 2rem;
        }}
        .tab {{
            padding: 0.75rem 1.25rem;
            cursor: pointer;
            font-size: 0.9rem;
            font-weight: 500;
            color: var(--text-secondary);
            border-bottom: 2px solid transparent;
            transition: all 0.15s ease;
            user-select: none;
        }}
        .tab:hover {{ color: var(--text); }}
        .tab.active {{
            color: var(--accent);
            border-bottom-color: var(--accent);
        }}
        .content {{
            padding: 1.5rem 2rem;
            max-width: 1400px;
            margin: 0 auto;
        }}
        .tab-panel {{ display: none; }}
        .tab-panel.active {{ display: block; }}

        /* Flow tab */
        .flow-container {{
            position: relative;
            background: var(--surface);
            border-radius: var(--radius);
            border: 1px solid var(--border);
            box-shadow: var(--shadow);
            padding: 2rem;
            overflow-x: auto;
        }}
        .mermaid {{
            display: flex;
            justify-content: center;
        }}
        .mermaid svg {{
            max-width: 100%;
            height: auto;
        }}

        /* Tooltip overlay */
{tooltip_styles}
        .edgeLabel:hover rect {{
            fill: #ede9fe !important;
        }}

        /* Legend */
        .legend {{
            display: flex;
            gap: 1.25rem;
            margin-top: 1.25rem;
            padding-top: 1rem;
            border-top: 1px solid var(--border);
            flex-wrap: wrap;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 0.4rem;
            font-size: 0.8rem;
            color: var(--text-secondary);
        }}
        .legend-swatch {{
            width: 14px;
            height: 14px;
            border-radius: 3px;
            border: 1px solid rgba(0,0,0,0.15);
        }}

        /* Tables for services/resources */
        .data-table {{
            width: 100%;
            border-collapse: collapse;
            background: var(--surface);
            border-radius: var(--radius);
            border: 1px solid var(--border);
            box-shadow: var(--shadow);
            overflow: hidden;
        }}
        .data-table th {{
            background: #f1f5f9;
            padding: 0.75rem 1rem;
            text-align: left;
            font-weight: 600;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            color: var(--text-secondary);
            border-bottom: 1px solid var(--border);
        }}
        .data-table td {{
            padding: 0.75rem 1rem;
            border-bottom: 1px solid var(--border);
            font-size: 0.875rem;
            vertical-align: top;
        }}
        .data-table tr:last-child td {{ border-bottom: none; }}
        .data-table tr:hover td {{ background: #f8fafc; }}
        .data-table .mono {{
            font-family: 'SF Mono', 'Fira Code', monospace;
            font-size: 0.8rem;
            color: #6366f1;
        }}
        .data-table .badge {{
            display: inline-block;
            padding: 0.15rem 0.5rem;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
        }}
        .badge-http {{ background: #dcfce7; color: #166534; }}
        .badge-grpc {{ background: #e0e7ff; color: #3730a3; }}
        .badge-queue {{ background: #fef3c7; color: #92400e; }}
        .badge-direct {{ background: #f3e8ff; color: #6b21a8; }}
        .badge-lambda {{ background: #ffe4e6; color: #9f1239; }}
        .params-block {{
            font-family: 'SF Mono', 'Fira Code', monospace;
            font-size: 0.75rem;
            background: #f8fafc;
            padding: 0.4rem 0.6rem;
            border-radius: 4px;
            border: 1px solid var(--border);
            white-space: pre-wrap;
            margin-top: 0.25rem;
        }}
        .empty-state {{
            text-align: center;
            padding: 3rem;
            color: var(--text-secondary);
            font-size: 0.95rem;
        }}

        /* Filter bar */
        .filter-bar {{
            margin-bottom: 1rem;
        }}
        .filter-group {{
            display: flex;
            gap: 0.5rem;
            flex-wrap: wrap;
        }}
        .filter-btn {{
            padding: 0.4rem 0.9rem;
            border: 1px solid var(--border);
            border-radius: 6px;
            background: var(--surface);
            color: var(--text-secondary);
            font-size: 0.82rem;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.15s ease;
            text-transform: capitalize;
        }}
        .filter-btn:hover {{
            border-color: var(--accent);
            color: var(--accent);
        }}
        .filter-btn.active {{
            background: var(--accent);
            border-color: var(--accent);
            color: #fff;
        }}

    </style>
</head>
<body>
    <div class="header">
        <h1>{safe_title}</h1>
        <p class="description">{safe_description}</p>
    </div>
    <div class="tabs">
        <div class="tab active" data-tab="flow">Flow</div>
        <div class="tab" data-tab="services">Services</div>
        <div class="tab" data-tab="resources">Resources</div>
    </div>
    <div class="content">
        <div class="tab-panel active" id="panel-flow">
            <div class="flow-container">
                <div class="mermaid">
{safe_mermaid}
                </div>
            </div>
            <div class="legend">
                <div class="legend-item"><div class="legend-swatch" style="background:#fff;border:2px solid #333"></div>Step</div>
                <div class="legend-item"><div class="legend-swatch" style="background:#fa8072"></div>Terminal</div>
                <div class="legend-item"><div class="legend-swatch" style="background:#6495ed"></div>Iteration</div>
                <div class="legend-item"><div class="legend-swatch" style="background:#fef3c7;border:1px solid #d97706"></div>Condition check</div>
                <div class="legend-item"><div class="legend-swatch" style="background:#e0f2fe;border:1px solid #0284c7"></div>Guarded action</div>
                <div class="legend-item"><div class="legend-swatch" style="background:#ddd6fe;border:1px solid #7c3aed"></div>Wait / sleep</div>
                <div class="legend-item" style="margin-left:1rem"><span style="color:var(--text-secondary);font-size:0.8rem">--- dashed = skip / timeout / per-item</span></div>
                <div class="legend-item" style="margin-left:1rem"><span style="color:var(--text-secondary);font-size:0.8rem">edge color = transport into the step:</span></div>
                <div class="legend-item"><span style="display:inline-block;width:22px;height:0;border-top:3px solid #64748b;margin-right:4px"></span>direct</div>
                <div class="legend-item"><span style="display:inline-block;width:22px;height:0;border-top:3px solid #0284c7;margin-right:4px"></span>http</div>
                <div class="legend-item"><span style="display:inline-block;width:22px;height:0;border-top:3px solid #16a34a;margin-right:4px"></span>grpc</div>
                <div class="legend-item"><span style="display:inline-block;width:22px;height:0;border-top:3px solid #f59e0b;margin-right:4px"></span>queue</div>
                <div class="legend-item"><span style="display:inline-block;width:22px;height:0;border-top:3px solid #d946ef;margin-right:4px"></span>lambda</div>
                <div class="legend-item"><div class="legend-swatch" style="background:#f1f5f9;border:1px solid #94a3b8;border-radius:8px"></div>Resource</div>
                <div class="legend-item" style="margin-left:0.5rem">
                    <label style="color:var(--text-secondary);font-size:0.8rem;cursor:pointer">
                        <input type="checkbox" id="toggle-data-plane" checked> show data plane (··· uses/cache/audit)
                    </label>
                </div>
            </div>
        </div>
        <div class="tab-panel" id="panel-services">
            <div class="filter-bar" id="services-filter"></div>
            <div id="services-table"></div>
        </div>
        <div class="tab-panel" id="panel-resources">
            <div id="resources-table"></div>
        </div>
    </div>
    <div class="tooltip" id="tooltip"></div>

    <script>{mermaid_source}</script>
    <script>{tooltip_source}</script>
    <script>
    (function() {{
        const nodeMetadata = {metadata_json};
        const edgeMetadata = {edge_metadata_json};
        const servicesData = {services_json};
        const resourcesData = {resources_json};
        const knownNodeIds = Object.keys(nodeMetadata);

        // --- Mermaid init (manual render so we know when it's done) ---
        mermaid.initialize({{
            startOnLoad: false,
            securityLevel: 'strict',
            theme: 'default',
            flowchart: {{curve: 'basis'}},
        }});
        mermaid.run({{querySelector: '.mermaid'}}).then(() => graphTooltips.attach({{
            svg: document.querySelector('.mermaid svg'),
            tooltipElement: document.getElementById('tooltip'),
            nodeMetadata: nodeMetadata,
            edgeMetadata: edgeMetadata,
            dataPlaneToggle: document.getElementById('toggle-data-plane'),
        }}));

        // --- Tabs ---
        document.querySelectorAll('.tab').forEach(tab => {{
            tab.addEventListener('click', () => {{
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
                tab.classList.add('active');
                document.getElementById('panel-' + tab.dataset.tab).classList.add('active');
            }});
        }});

        const graphTooltips = window.JustflowGraphTooltips;
        const appendTextElement = graphTooltips.appendTextElement;
        const renderEmptyState = graphTooltips.renderEmptyState;

        // --- Services table with transport filter ---
        let activeTransportFilter = 'all';

        function renderServicesFilter() {{
            const container = document.getElementById('services-filter');
            const entries = Object.entries(servicesData);
            if (entries.length === 0) return;

            const transports = [...new Set(entries.map(([, svc]) => svc.transport || 'unknown'))].sort();

            const group = document.createElement('div');
            group.className = 'filter-group';
            for (const transport of ['all', ...transports]) {{
                const button = appendTextElement(
                    group,
                    'button',
                    transport === 'all' ? 'filter-btn active' : 'filter-btn',
                    transport === 'all' ? 'All' : transport,
                );
                button.dataset.transport = transport;
            }}
            container.replaceChildren(group);

            container.querySelectorAll('.filter-btn').forEach(btn => {{
                btn.addEventListener('click', () => {{
                    container.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    activeTransportFilter = btn.dataset.transport;
                    renderServicesTable();
                }});
            }});
        }}

        function renderServicesTable() {{
            const container = document.getElementById('services-table');
            const entries = Object.entries(servicesData);
            if (entries.length === 0) {{
                renderEmptyState(
                    container,
                    'No services data provided. Use --config-dir to include services.',
                );
                return;
            }}

            const filtered = activeTransportFilter === 'all'
                ? entries
                : entries.filter(([, svc]) => svc.transport === activeTransportFilter);

            const table = document.createElement('table');
            table.className = 'data-table';
            const head = table.createTHead();
            const headingRow = head.insertRow();
            for (const heading of ['Name', 'Transport', 'Endpoint', 'Timeout', 'Retries', 'Params']) {{
                appendTextElement(headingRow, 'th', '', heading);
            }}
            const body = table.createTBody();
            for (const [name, svc] of filtered) {{
                const transport = svc.transport || '';
                const config = svc.transport_config || {{}};
                const endpoint = config['class'] || config.base_url || config.address || config.destination || config.function_name || '';
                const row = body.insertRow();
                const nameCell = row.insertCell();
                appendTextElement(nameCell, 'strong', '', name);
                const transportCell = row.insertCell();
                appendTextElement(transportCell, 'span', 'badge', transport);
                appendTextElement(row, 'td', 'mono', endpoint);
                const timeout = svc.dispatch_timeout_sec === undefined
                    ? '-'
                    : svc.dispatch_timeout_sec + 's';
                appendTextElement(row, 'td', '', timeout);
                appendTextElement(row, 'td', '', svc.retries ?? '-');
                const paramsCell = row.insertCell();
                if (svc.params && Object.keys(svc.params).length > 0) {{
                    appendTextElement(
                        paramsCell,
                        'div',
                        'params-block',
                        JSON.stringify(svc.params, null, 2),
                    );
                }} else {{
                    paramsCell.textContent = '-';
                }}
            }}
            container.replaceChildren(table);
        }}

        // --- Resources table ---
        function renderResources() {{
            const container = document.getElementById('resources-table');
            const entries = Object.entries(resourcesData);
            if (entries.length === 0) {{
                renderEmptyState(
                    container,
                    'No resources data provided. Use --config-dir to include resources.',
                );
                return;
            }}
            const table = document.createElement('table');
            table.className = 'data-table';
            const head = table.createTHead();
            const headingRow = head.insertRow();
            for (const heading of ['Name', 'Provider', 'Config']) {{
                appendTextElement(headingRow, 'th', '', heading);
            }}
            const body = table.createTBody();
            for (const [name, res] of entries) {{
                const provider = res.provider || '';
                const row = body.insertRow();
                const nameCell = row.insertCell();
                appendTextElement(nameCell, 'strong', '', name);
                appendTextElement(row, 'td', 'mono', provider);
                const configCell = row.insertCell();
                if (res.config && Object.keys(res.config).length > 0) {{
                    appendTextElement(
                        configCell,
                        'div',
                        'params-block',
                        JSON.stringify(res.config, null, 2),
                    );
                }} else {{
                    configCell.textContent = '-';
                }}
            }}
            container.replaceChildren(table);
        }}

        renderServicesFilter();
        renderServicesTable();
        renderResources();
    }})();
    </script>
</body>
</html>"""
    return html
