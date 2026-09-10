/**
 * Parent-side controller for the sandboxed graph frame. The frame runs with
 * `sandbox="allow-scripts"` only — an opaque origin with no same-origin
 * authority, no forms, no popups, no navigation. Authentication of the
 * channel is the one-time transferred MessagePort.
 */

import {
  GRAPH_BOOTSTRAP_KIND,
  GRAPH_PROTOCOL_VERSION,
  type GraphViewDocument,
} from "../graph-frame/protocol";

const GRAPH_FRAME_PATH = "/admin/graph-frame";

export interface GraphFrameHandle {
  render(graph: GraphViewDocument): void;
  destroy(): void;
}

export function mountGraphFrame(container: HTMLElement): GraphFrameHandle {
  const iframe = document.createElement("iframe");
  iframe.className = "graph-frame";
  iframe.setAttribute("sandbox", "allow-scripts");
  iframe.setAttribute("title", "Workflow graph");
  iframe.src = GRAPH_FRAME_PATH;

  let port: MessagePort | null = null;
  let pending: GraphViewDocument | null = null;
  let destroyed = false;

  iframe.addEventListener("load", () => {
    if (destroyed || iframe.contentWindow === null) return;
    const channel = new MessageChannel();
    channel.port1.onmessage = (event: MessageEvent<unknown>) => {
      const data = event.data;
      if (
        typeof data === "object" &&
        data !== null &&
        (data as Record<string, unknown>).kind === "ready"
      ) {
        port = channel.port1;
        if (pending !== null) {
          send(pending);
          pending = null;
        }
      }
    };
    // The sandboxed frame has an opaque origin, so the initial target origin
    // is necessarily "*"; the exact contentWindow plus the transferred port —
    // unforgeable by other contexts — are the security capability here.
    iframe.contentWindow.postMessage(
      { kind: GRAPH_BOOTSTRAP_KIND, version: GRAPH_PROTOCOL_VERSION },
      "*",
      [channel.port2],
    );
  });
  container.replaceChildren(iframe);

  function send(graph: GraphViewDocument): void {
    port?.postMessage({ kind: "render", version: GRAPH_PROTOCOL_VERSION, graph });
  }

  return {
    render(graph: GraphViewDocument): void {
      if (destroyed) return;
      if (port === null) {
        pending = graph;
        return;
      }
      send(graph);
    },
    destroy(): void {
      destroyed = true;
      port?.close();
      port = null;
      iframe.remove();
    },
  };
}
