/**
 * Entry for the opaque-origin graph frame. Trust model: the frame accepts one
 * bootstrap message — matching kind/version, carrying exactly one transferred
 * MessagePort, and sent from this document's own URL origin — then discards
 * all window-level message traffic. Graph documents arrive only over the
 * private port and are re-validated here before rendering.
 */

import { buildMermaidSource } from "./mermaid-source";
import {
  GRAPH_BOOTSTRAP_KIND,
  GRAPH_PROTOCOL_VERSION,
  type GraphViewDocument,
  isRenderGraphMessage,
} from "./protocol";

interface MermaidApi {
  initialize(config: object): void;
  render(id: string, source: string): Promise<{ svg: string }>;
}

interface GraphTooltipsApi {
  attach(config: {
    svg: Element | null;
    tooltipElement: HTMLElement | null;
    nodeMetadata: Record<string, Record<string, string>>;
    edgeMetadata: unknown[];
    dataPlaneToggle: HTMLElement | null;
  }): void;
}

declare global {
  var __esbuild_esm_mermaid_nm: { mermaid?: { default?: MermaidApi } & MermaidApi } | undefined;
  var JustflowGraphTooltips: GraphTooltipsApi | undefined;
}

const EXPECTED_ORIGIN = new URL(document.URL).origin;
let renderSequence = 0;

function mermaidApi(): MermaidApi | null {
  const bundle = globalThis.__esbuild_esm_mermaid_nm?.mermaid;
  if (bundle === undefined) return null;
  return bundle.default ?? bundle;
}

function showMessage(text: string): void {
  const target = document.getElementById("graph");
  if (target === null) return;
  target.replaceChildren();
  const paragraph = document.createElement("p");
  paragraph.className = "frame-message";
  paragraph.textContent = text;
  target.appendChild(paragraph);
}

function tooltipMetadata(graph: GraphViewDocument): Record<string, Record<string, string>> {
  const metadata: Record<string, Record<string, string>> = {};
  for (const node of graph.nodes) {
    metadata[node.id] = { id: node.label, type: node.kind, ...node.metadata };
  }
  return metadata;
}

async function renderGraph(source: string, graph: GraphViewDocument): Promise<void> {
  const api = mermaidApi();
  const target = document.getElementById("graph");
  if (api === null || target === null) {
    showMessage("The graph renderer is unavailable.");
    return;
  }
  renderSequence += 1;
  const { svg } = await api.render(`graph-render-${renderSequence}`, source);
  // Mermaid's strict securityLevel escapes labels; the SVG is renderer output,
  // built exclusively from escaped source above.
  target.innerHTML = svg;
  const rendered = target.querySelector("svg");
  rendered?.setAttribute("role", "img");
  rendered?.setAttribute("aria-label", graph.title);
  globalThis.JustflowGraphTooltips?.attach({
    svg: rendered,
    tooltipElement: document.getElementById("tooltip"),
    nodeMetadata: tooltipMetadata(graph),
    edgeMetadata: [],
    dataPlaneToggle: null,
  });
}

function adoptPort(port: MessagePort): void {
  port.onmessage = (event: MessageEvent<unknown>) => {
    if (!isRenderGraphMessage(event.data)) return;
    const source = (() => {
      try {
        return buildMermaidSource(event.data.graph);
      } catch {
        showMessage("The graph document is invalid.");
        return null;
      }
    })();
    if (source !== null) {
      void renderGraph(source, event.data.graph).catch(() => {
        showMessage("The graph could not be rendered.");
      });
    }
  };
}

function bootstrap(): void {
  const api = mermaidApi();
  if (api !== null) {
    api.initialize({
      startOnLoad: false,
      securityLevel: "strict",
      theme: "default",
      flowchart: { curve: "basis" },
    });
  }
  let adopted = false;
  window.addEventListener("message", (event: MessageEvent<unknown>) => {
    if (adopted) return;
    if (event.origin !== EXPECTED_ORIGIN) return;
    const data = event.data;
    if (typeof data !== "object" || data === null) return;
    const record = data as Record<string, unknown>;
    if (record.kind !== GRAPH_BOOTSTRAP_KIND || record.version !== GRAPH_PROTOCOL_VERSION) return;
    const port = event.ports[0];
    if (port === undefined || event.ports.length !== 1) return;
    adopted = true;
    adoptPort(port);
    port.postMessage({ kind: "ready", version: GRAPH_PROTOCOL_VERSION });
  });
}

bootstrap();
