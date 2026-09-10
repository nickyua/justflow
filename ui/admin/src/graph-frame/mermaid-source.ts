/**
 * Builds Mermaid flowchart source from a GraphViewDocument, mirroring the CLI
 * renderer's semantics (`justflow/visualization/renderer.py`): HTML-escaped
 * quoted labels, one shape per node kind, opaque node identities, sub-workflow
 * groups as subgraphs, and per-kind class styling. Every label is escaped
 * before it reaches Mermaid syntax; Mermaid additionally runs with
 * `securityLevel: "strict"` in the frame.
 */

import type { GraphViewDocument, GraphViewNode } from "./protocol";

const EDGE_LABEL_LIMIT = 28;
const NODE_CLASS_STYLES: readonly (readonly [string, string])[] = [
  ["step", "fill:#ffffff,stroke:#333,color:#111"],
  ["resource", "fill:#f1f5f9,stroke:#94a3b8,color:#334155"],
  ["wait", "fill:#ddd6fe,stroke:#7c3aed,color:#4c1d95"],
  ["terminal", "fill:#fa8072,stroke:#333,color:#000"],
  ["iteration", "fill:#6495ed,stroke:#333,color:#fff"],
  ["decision", "fill:#fef3c7,stroke:#d97706,color:#92400e"],
  ["action", "fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e"],
  ["failed", "fill:#fff0f1,stroke:#c63f4b,color:#a92f3a,stroke-width:2.5px"],
];

export function escapeMermaidLabel(text: string): string {
  return text
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#x27;");
}

export function opaqueNodeIds(document: GraphViewDocument): Map<string, string> {
  const identities = new Map<string, string>();
  document.nodes.forEach((node, index) => {
    if (identities.has(node.id)) {
      throw new Error(`Duplicate graph node ID: ${node.id}`);
    }
    identities.set(node.id, `n${index}`);
  });
  return identities;
}

function nodeLine(node: GraphViewNode, nodeId: string): string {
  const label = escapeMermaidLabel(node.label);
  switch (node.kind) {
    case "terminal":
      return `${nodeId}(["${label}"])`;
    case "iteration":
      return `${nodeId}[["${label}"]]`;
    case "decision":
      return `${nodeId}{"${label}"}`;
    case "action":
      return `${nodeId}[/"${label}"\\]`;
    case "wait":
    case "sleep":
      return `${nodeId}{{"${label}"}}`;
    case "resource":
      return `${nodeId}[("${label}")]`;
    default:
      return `${nodeId}["${label}"]`;
  }
}

function shortenEdgeLabel(text: string): string {
  return text.length <= EDGE_LABEL_LIMIT ? text : `${text.slice(0, EDGE_LABEL_LIMIT - 1)}…`;
}

export function buildMermaidSource(document: GraphViewDocument): string {
  const lines = ["graph TD"];
  const identities = opaqueNodeIds(document);

  for (const node of document.nodes) {
    if (node.group === null) lines.push(`    ${nodeLine(node, identities.get(node.id) ?? "")}`);
  }
  const groups = new Map<string, GraphViewNode[]>();
  for (const node of document.nodes) {
    if (node.group !== null) {
      const members = groups.get(node.group) ?? [];
      members.push(node);
      groups.set(node.group, members);
    }
  }
  let groupIndex = 0;
  for (const [groupName, members] of groups) {
    const groupLabel = escapeMermaidLabel(`sub-workflow: ${groupName}`);
    lines.push(`    subgraph g${groupIndex}["${groupLabel}"]`);
    for (const node of members) {
      lines.push(`        ${nodeLine(node, identities.get(node.id) ?? "")}`);
    }
    lines.push("    end");
    groupIndex += 1;
  }
  lines.push("");

  for (const edge of document.edges) {
    const source = identities.get(edge.source);
    const target = identities.get(edge.target);
    if (source === undefined || target === undefined) {
      throw new Error("Graph edge references an unknown node");
    }
    const arrow = edge.dashed ? "-.->" : "-->";
    if (edge.label !== null && edge.label.length > 0) {
      const label = escapeMermaidLabel(shortenEdgeLabel(edge.label));
      lines.push(`    ${source} ${arrow}|"${label}"| ${target}`);
    } else {
      lines.push(`    ${source} ${arrow} ${target}`);
    }
  }
  lines.push("");

  for (const [kind, style] of NODE_CLASS_STYLES) {
    const members = document.nodes.filter((node) =>
      kind === "failed"
        ? node.failed
        : !node.failed && (node.kind === kind || (kind === "wait" && node.kind === "sleep")),
    );
    if (members.length === 0) continue;
    lines.push(`    classDef ${kind} ${style}`);
    lines.push(`    class ${members.map((node) => identities.get(node.id)).join(",")} ${kind}`);
  }
  return lines.join("\n");
}
