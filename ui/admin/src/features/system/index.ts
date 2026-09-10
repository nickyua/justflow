import type { ComponentHealth } from "../../api/contracts";
import { clear, element, textNode } from "../../app/dom";
import { statusBadge } from "../../components/status";

const NOT_CONFIGURED_LABEL = "Not configured (optional)";

let triggerConsumerConfigured = false;

export function isTriggerConsumerConfigured(): boolean {
  return triggerConsumerConfigured;
}

export function renderSystem(ready: boolean, components: ComponentHealth[]): void {
  const runtimeStatus = element("system-runtime-status");
  runtimeStatus.textContent = ready ? "Runtime ready" : "Needs attention";
  runtimeStatus.className = ready ? "status-badge status-ready" : "status-badge status-unavailable";

  triggerConsumerConfigured = components.some(
    (component) => component.component === "trigger_consumer" && component.status === "ready",
  );
  const healthTarget = element("health-components");
  clear(healthTarget);
  if (components.length === 0) {
    healthTarget.appendChild(textNode("p", "No component health is available.", "empty-state"));
  }
  for (const component of components) {
    const notConfigured = !component.required && component.status === "unavailable";
    const row = document.createElement("div");
    row.className = `health-row is-${notConfigured ? "optional" : component.status}`;
    row.appendChild(
      textNode(
        "span",
        component.status === "ready" ? "✓" : notConfigured ? "–" : "!",
        "health-icon",
      ),
    );
    const copy = document.createElement("div");
    copy.append(
      textNode("strong", component.component.replaceAll("_", " ")),
      textNode("span", component.required ? "Required component" : "Optional component"),
    );
    row.append(
      copy,
      notConfigured ? statusBadge(NOT_CONFIGURED_LABEL, "unknown") : statusBadge(component.status),
    );
    healthTarget.appendChild(row);
  }
}
