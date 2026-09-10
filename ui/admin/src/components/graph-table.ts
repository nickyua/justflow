import { clear, textNode } from "../app/dom";
import type { GraphViewDocument } from "../graph-frame/protocol";

/**
 * Accessible parent-rendered alternative to the graph frame: the same
 * validated document as a semantic list of steps and transitions.
 */
export function renderGraphTable(container: HTMLElement, graph: GraphViewDocument): void {
  clear(container);
  if (graph.nodes.length === 0) {
    container.appendChild(
      textNode("p", "This definition has no displayable steps.", "empty-state"),
    );
    return;
  }
  const list = document.createElement("ul");
  list.className = "graph-node-list";
  for (const node of graph.nodes) {
    const item = document.createElement("li");
    const heading = document.createElement("div");
    heading.append(
      textNode("strong", node.label),
      textNode("span", node.failed ? `${node.kind} · failed` : node.kind, "graph-node-kind"),
    );
    if (node.group !== null) {
      heading.appendChild(textNode("span", `sub-workflow: ${node.group}`, "graph-node-kind"));
    }
    item.appendChild(heading);
    const outgoing = graph.edges.filter((edge) => edge.source === node.id);
    if (outgoing.length > 0) {
      const transitions = document.createElement("ul");
      for (const edge of outgoing) {
        const target = graph.nodes.find((candidate) => candidate.id === edge.target);
        const label = edge.label === null || edge.label.length === 0 ? "" : ` when ${edge.label}`;
        transitions.appendChild(textNode("li", `then ${target?.label ?? edge.target}${label}`));
      }
      item.appendChild(transitions);
    }
    list.appendChild(item);
  }
  container.appendChild(list);
}
