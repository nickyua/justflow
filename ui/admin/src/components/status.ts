import { element, textNode } from "../app/dom";
import { normalizedStatus } from "../app/format";

export type StatusKind = "" | "is-error" | "is-success";

export function statusBadge(value: string | null, classOverride?: string): HTMLElement {
  const normalized = classOverride ?? normalizedStatus(value);
  return textNode("span", value ?? "Unknown", `status-badge status-${normalized}`);
}

export function showStatus(message: string, kind: StatusKind = ""): void {
  showStatusIn("action-status", message, kind);
}

export function showStatusIn(id: string, message: string, kind: StatusKind = ""): void {
  const node = element(id);
  node.textContent = message;
  node.className = `action-status ${kind}`.trim();
}

export function setConnection(connected: boolean): void {
  element("connection-state").textContent = connected ? "Connected" : "Unavailable";
  element("connection-dot").className = connected
    ? "connection-dot is-connected"
    : "connection-dot is-unavailable";
}
