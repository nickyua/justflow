/**
 * The versioned message contract between the panel and the opaque-origin graph
 * frame. The transferred MessagePort — not origin checks — is the capability
 * that authenticates this channel (an `allow-scripts` sandbox has an opaque
 * origin, so ordinary origin comparison cannot identify the parent).
 */

export const GRAPH_PROTOCOL_VERSION = 1;
export const GRAPH_BOOTSTRAP_KIND = "justflow-graph-bootstrap";

export interface GraphViewNode {
  id: string;
  label: string;
  kind: string;
  group: string | null;
  failed: boolean;
  metadata: Record<string, string>;
}

export interface GraphViewEdge {
  source: string;
  target: string;
  label: string | null;
  dashed: boolean;
}

export interface GraphViewDocument {
  version: typeof GRAPH_PROTOCOL_VERSION;
  title: string;
  nodes: GraphViewNode[];
  edges: GraphViewEdge[];
}

export interface RenderGraphMessage {
  kind: "render";
  version: typeof GRAPH_PROTOCOL_VERSION;
  graph: GraphViewDocument;
}

export function isRenderGraphMessage(value: unknown): value is RenderGraphMessage {
  if (typeof value !== "object" || value === null) return false;
  const record = value as Record<string, unknown>;
  if (record.kind !== "render" || record.version !== GRAPH_PROTOCOL_VERSION) return false;
  const graph = record.graph;
  if (typeof graph !== "object" || graph === null) return false;
  const view = graph as Record<string, unknown>;
  return (
    view.version === GRAPH_PROTOCOL_VERSION &&
    typeof view.title === "string" &&
    Array.isArray(view.nodes) &&
    Array.isArray(view.edges) &&
    (view.nodes as unknown[]).every(isGraphViewNode) &&
    (view.edges as unknown[]).every(isGraphViewEdge)
  );
}

function isGraphViewNode(value: unknown): value is GraphViewNode {
  if (typeof value !== "object" || value === null) return false;
  const node = value as Record<string, unknown>;
  const metadata = node.metadata;
  return (
    typeof node.id === "string" &&
    typeof node.label === "string" &&
    typeof node.kind === "string" &&
    (node.group === null || typeof node.group === "string") &&
    typeof node.failed === "boolean" &&
    typeof metadata === "object" &&
    metadata !== null &&
    Object.values(metadata).every((value) => typeof value === "string")
  );
}

function isGraphViewEdge(value: unknown): value is GraphViewEdge {
  if (typeof value !== "object" || value === null) return false;
  const edge = value as Record<string, unknown>;
  return (
    typeof edge.source === "string" &&
    typeof edge.target === "string" &&
    (edge.label === null || typeof edge.label === "string") &&
    typeof edge.dashed === "boolean"
  );
}
