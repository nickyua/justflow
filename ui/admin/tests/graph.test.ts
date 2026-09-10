import { describe, expect, it } from "vitest";

import {
  buildMermaidSource,
  escapeMermaidLabel,
  opaqueNodeIds,
} from "../src/graph-frame/mermaid-source";
import {
  GRAPH_PROTOCOL_VERSION,
  type GraphViewDocument,
  isRenderGraphMessage,
} from "../src/graph-frame/protocol";

function graphOf(partial: Partial<GraphViewDocument>): GraphViewDocument {
  return {
    version: GRAPH_PROTOCOL_VERSION,
    title: "example workflow steps",
    nodes: [],
    edges: [],
    ...partial,
  };
}

describe("graph frame contract", () => {
  it("escapes hostile labels before they reach Mermaid syntax", () => {
    const hostile = 'click "x" <script>alert(1)</script> & `pwn` --> n0';
    const escaped = escapeMermaidLabel(hostile);
    expect(escaped).not.toContain("<script>");
    expect(escaped).not.toContain('"');
    expect(escaped).toContain("&lt;script&gt;");

    const source = buildMermaidSource(
      graphOf({
        nodes: [
          { id: "a", label: hostile, kind: "step", group: null, failed: false, metadata: {} },
          { id: "b", label: "done", kind: "terminal", group: null, failed: true, metadata: {} },
        ],
        edges: [{ source: "a", target: "b", label: '"quoted" label', dashed: false }],
      }),
    );
    expect(source).not.toContain("<script>");
    expect(source).not.toContain('""quoted');
    expect(source).toContain("graph TD");
    expect(source).toContain('n0["');
    expect(source).toContain('n1(["done"])');
    expect(source).toContain("classDef failed");
  });

  it("renders kinds, groups, and dashed edges with opaque node identities", () => {
    const source = buildMermaidSource(
      graphOf({
        nodes: [
          { id: "start", label: "start", kind: "step", group: null, failed: false, metadata: {} },
          {
            id: "loop",
            label: "loop",
            kind: "iteration",
            group: "child",
            failed: false,
            metadata: {},
          },
          {
            id: "choice",
            label: "choice",
            kind: "decision",
            group: null,
            failed: false,
            metadata: {},
          },
        ],
        edges: [
          { source: "start", target: "loop", label: null, dashed: false },
          { source: "loop", target: "choice", label: "done", dashed: true },
        ],
      }),
    );
    expect(source).toContain('subgraph g0["sub-workflow: child"]');
    expect(source).toContain('n1[["loop"]]');
    expect(source).toContain('n2{"choice"}');
    expect(source).toContain("n0 --> n1");
    expect(source).toContain('n1 -.->|"done"| n2');
  });

  it("rejects duplicate node identities and unknown edge references", () => {
    expect(() =>
      opaqueNodeIds(
        graphOf({
          nodes: [
            { id: "a", label: "a", kind: "step", group: null, failed: false, metadata: {} },
            { id: "a", label: "b", kind: "step", group: null, failed: false, metadata: {} },
          ],
        }),
      ),
    ).toThrow("Duplicate graph node ID");
    expect(() =>
      buildMermaidSource(
        graphOf({
          nodes: [{ id: "a", label: "a", kind: "step", group: null, failed: false, metadata: {} }],
          edges: [{ source: "a", target: "ghost", label: null, dashed: false }],
        }),
      ),
    ).toThrow("unknown node");
  });

  it("accepts only the strict versioned render message", () => {
    const valid = {
      kind: "render",
      version: GRAPH_PROTOCOL_VERSION,
      graph: graphOf({
        nodes: [{ id: "a", label: "a", kind: "step", group: null, failed: false, metadata: {} }],
      }),
    };
    expect(isRenderGraphMessage(valid)).toBe(true);
    expect(isRenderGraphMessage({ ...valid, version: 2 })).toBe(false);
    expect(isRenderGraphMessage({ ...valid, kind: "renderer" })).toBe(false);
    expect(
      isRenderGraphMessage({
        ...valid,
        graph: { ...valid.graph, nodes: [{ id: 1 }] },
      }),
    ).toBe(false);
    expect(isRenderGraphMessage(null)).toBe(false);
  });
});
