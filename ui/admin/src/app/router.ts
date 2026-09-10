export type View =
  | "overview"
  | "workflows"
  | "workflow"
  | "workflow-editor"
  | "runs"
  | "triggers"
  | "editor"
  | "components"
  | "releases"
  | "system";

const VIEWS: readonly View[] = [
  "overview",
  "workflows",
  "workflow",
  "workflow-editor",
  "runs",
  "triggers",
  "editor",
  "components",
  "releases",
  "system",
];
export const WORKFLOW_ROUTE_PREFIX = "/admin/workflows/";
export const WORKFLOW_EDITOR_ROUTE_PREFIX = "/admin/editor/workflows/";

export type WorkflowTab = "overview" | "definition" | "runs" | "triggers";

const WORKFLOW_TABS: readonly WorkflowTab[] = ["overview", "definition", "runs", "triggers"];

export function requestedWorkflowTab(): WorkflowTab {
  const candidate = new URLSearchParams(window.location.search).get("tab") ?? "overview";
  return WORKFLOW_TABS.some((tab) => tab === candidate) ? (candidate as WorkflowTab) : "overview";
}

export function isView(value: string): value is View {
  return VIEWS.some((view) => view === value);
}

export function setView(
  view: View,
  updateLocation = true,
  workflowName: string | null = null,
): void {
  for (const panel of document.querySelectorAll<HTMLElement>("[data-panel]")) {
    panel.hidden = panel.dataset.panel !== view;
  }
  for (const item of document.querySelectorAll<HTMLButtonElement>("[data-view]")) {
    const active =
      item.dataset.view === view ||
      (view === "workflow" && item.dataset.view === "workflows") ||
      (view === "workflow-editor" && item.dataset.view === "editor");
    item.classList.toggle("is-active", active);
    if (active) {
      item.setAttribute("aria-current", "page");
    } else {
      item.removeAttribute("aria-current");
    }
  }
  const label = document.getElementById("current-view-label");
  if (label !== null) {
    label.textContent =
      (view === "workflow" || view === "workflow-editor") && workflowName !== null
        ? workflowName
        : `${view[0]?.toUpperCase()}${view.slice(1)}`;
  }
  if (updateLocation) {
    let target = `/admin/#${view}`;
    if (view === "workflow" && workflowName !== null) {
      target = `${WORKFLOW_ROUTE_PREFIX}${encodeURIComponent(workflowName)}`;
      const tab = requestedWorkflowTab();
      if (tab !== "overview" && window.location.pathname === target) {
        target = `${target}?tab=${tab}`;
      }
    } else if (view === "workflow-editor" && workflowName !== null) {
      target = `${WORKFLOW_EDITOR_ROUTE_PREFIX}${encodeURIComponent(workflowName)}`;
    }
    window.history.pushState(null, "", target);
  }
}

export function requestedView(): View {
  if (window.location.pathname.startsWith(WORKFLOW_EDITOR_ROUTE_PREFIX)) return "workflow-editor";
  if (window.location.pathname.startsWith(WORKFLOW_ROUTE_PREFIX)) return "workflow";
  const candidate = window.location.hash.slice(1);
  return isView(candidate) ? candidate : "workflows";
}

export function setWorkflowTabLocation(workflowName: string, tab: WorkflowTab): void {
  const base = `${WORKFLOW_ROUTE_PREFIX}${encodeURIComponent(workflowName)}`;
  window.history.pushState(null, "", tab === "overview" ? base : `${base}?tab=${tab}`);
}

export function requestedWorkflow(): string | null {
  const pathname = window.location.pathname;
  const prefix = pathname.startsWith(WORKFLOW_EDITOR_ROUTE_PREFIX)
    ? WORKFLOW_EDITOR_ROUTE_PREFIX
    : pathname.startsWith(WORKFLOW_ROUTE_PREFIX)
      ? WORKFLOW_ROUTE_PREFIX
      : null;
  if (prefix === null) return null;
  const encoded = pathname.slice(prefix.length);
  if (encoded.length === 0 || encoded.includes("/")) return null;
  try {
    return decodeURIComponent(encoded);
  } catch {
    return null;
  }
}
