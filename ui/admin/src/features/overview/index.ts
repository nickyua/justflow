import type { ComponentHealth, MetricsLink } from "../../api/contracts";
import { clear, element, textNode } from "../../app/dom";
import { formatDate, runKey } from "../../app/format";
import { setView } from "../../app/router";
import { statusBadge } from "../../components/status";
import {
  getFailedCount,
  getFailedRuns,
  getRunDetail,
  getRunningCount,
  loadRunDetail,
} from "../runs";
import { getScheduleCount } from "../schedules";
import { getWorkflowCount } from "../workflows";

export function renderMetricsLinks(links: MetricsLink[]): void {
  const target = element("metrics-links");
  clear(target);
  for (const link of links) {
    const anchor = document.createElement("a");
    anchor.href = link.url;
    anchor.rel = "noopener noreferrer";
    anchor.target = "_blank";
    anchor.textContent = link.label;
    target.appendChild(anchor);
  }
}

export function renderOverview(ready: boolean, components: ComponentHealth[]): void {
  element("workflow-count").textContent = String(getWorkflowCount());
  element("live-run-count").textContent = String(getRunningCount());
  element("failed-run-count").textContent = String(getFailedCount());
  element("schedule-count").textContent = String(getScheduleCount());
  element("nav-run-count").textContent = String(getRunningCount());

  const runtimeStatus = element("runtime-status");
  runtimeStatus.textContent = ready ? "Runtime ready" : "Needs attention";
  runtimeStatus.className = ready ? "status-badge status-ready" : "status-badge status-unavailable";

  const summary = element("overview-runtime-summary");
  clear(summary);
  const required = components.filter((component) => component.required);
  const readyCount = required.filter((component) => component.status === "ready").length;
  summary.append(
    textNode(
      "p",
      required.length === 0
        ? "No component health is available."
        : `${readyCount} of ${required.length} required components are ready.`,
      "empty-state",
    ),
  );
  renderFailureList();
}

function renderFailureList(): void {
  const target = element("failure-list");
  clear(target);
  const failedRuns = getFailedRuns();
  if (failedRuns.length === 0) {
    target.appendChild(textNode("p", "No failed runs in the loaded results.", "empty-state"));
    return;
  }
  for (const run of failedRuns) {
    const inspect = document.createElement("button");
    inspect.className = "compact-row";
    inspect.type = "button";
    inspect.addEventListener("click", () => {
      setView("runs");
      void loadRunDetail(run);
    });
    const copy = document.createElement("div");
    const detail = getRunDetail(runKey(run));
    copy.append(
      textNode("strong", run.workflowId),
      textNode(
        "span",
        detail?.failedStep === null || detail?.failedStep === undefined
          ? formatDate(run.closeTime)
          : `Failed at ${detail.failedStep} · ${formatDate(run.closeTime)}`,
      ),
    );
    inspect.append(copy, statusBadge(run.status));
    target.appendChild(inspect);
  }
}
