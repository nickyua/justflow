import type { RunStateQuery } from "../../api/client";
import type { WorkflowRun, WorkflowRunDetail } from "../../api/contracts";
import { client } from "../../app/api";
import { getCapabilities } from "../../app/capabilities";
import {
  appendCell,
  appendNodeCell,
  button,
  clear,
  element,
  emptyTable,
  input,
  textNode,
} from "../../app/dom";
import {
  formatDate,
  normalizedStatus,
  runDuration,
  runKey,
  safeError,
  short,
} from "../../app/format";
import { refreshDashboard } from "../../app/refresh";
import { PagedCollection, STALE_LIST_NOTICE } from "../../components/pagination";
import { statusBadge } from "../../components/status";

export type RunFilter = "all" | "running" | "completed" | "failed";

export const RUN_FILTERS: readonly RunFilter[] = ["all", "running", "completed", "failed"];
const RUN_QUERY_PAGE_LIMIT = 30;
const WATCH_RUN_INTERVAL_MS = 5_000;
const WATCH_RUN_MAX_MS = 600_000;

let runningRuns: WorkflowRun[] = [];
let failedRuns: WorkflowRun[] = [];
let runFilter: RunFilter = "all";
let runSelection: string | null = null;
let currentRun: WorkflowRun | null = null;
let watchTimer: ReturnType<typeof setInterval> | null = null;
let watchStartedAt = 0;
const runDetails = new Map<string, WorkflowRunDetail>();
const runDetailLoadedListeners: (() => void)[] = [];

const runView = new PagedCollection<WorkflowRun>(async (cursor) => {
  const page = await client.runs(runFilterState(runFilter), RUN_QUERY_PAGE_LIMIT, cursor);
  return { items: page.runs, nextCursor: page.nextCursor };
});

function runFilterState(filter: RunFilter): RunStateQuery | null {
  return filter === "all" ? null : filter;
}

export function setRuns(running: WorkflowRun[], failed: WorkflowRun[]): void {
  runningRuns = running;
  failedRuns = failed;
}

export function getRunningCount(): number {
  return runningRuns.length;
}

export function getFailedCount(): number {
  return failedRuns.length;
}

export function getFailedRuns(): WorkflowRun[] {
  return failedRuns;
}

export function getRunDetail(key: string): WorkflowRunDetail | undefined {
  return runDetails.get(key);
}

const WORKFLOW_TYPE_VERSION_SEPARATOR = "__";

/** The logical workflow name embedded in the versioned Temporal workflow type. */
export function logicalWorkflowName(run: WorkflowRun): string {
  const separator = run.workflowType.indexOf(WORKFLOW_TYPE_VERSION_SEPARATOR);
  return separator === -1 ? run.workflowType : run.workflowType.slice(0, separator);
}

export function getAllRuns(): WorkflowRun[] {
  return [...runningRuns, ...failedRuns].sort(
    (left, right) => new Date(right.startTime).getTime() - new Date(left.startTime).getTime(),
  );
}

export function onRunDetailLoaded(listener: () => void): void {
  runDetailLoadedListeners.push(listener);
}

export async function loadFailureDetails(): Promise<void> {
  const results = await Promise.allSettled(
    failedRuns.map((run) => client.runDetail(run.workflowId, run.runId)),
  );
  for (const result of results) {
    if (result.status === "fulfilled") {
      runDetails.set(runKey(result.value), result.value);
    }
  }
}

export function setRunFilter(filter: RunFilter): void {
  runFilter = filter;
  void reloadRunView();
}

export async function reloadRunView(): Promise<void> {
  await runView.reload();
  renderRuns();
}

export async function loadMoreRuns(): Promise<void> {
  const control = button("load-more-runs");
  control.disabled = true;
  try {
    const outcome = await runView.loadMore();
    if (outcome === "reset") window.alert(STALE_LIST_NOTICE);
    renderRuns();
  } catch (error) {
    window.alert(safeError(error));
  } finally {
    control.disabled = false;
  }
}

export function renderRuns(): void {
  const target = element("run-list");
  const query = input("run-search").value.trim().toLowerCase();
  const runs = runView.items.filter((run) => {
    const matchesQuery =
      query.length === 0 ||
      run.workflowId.toLowerCase().includes(query) ||
      run.runId.toLowerCase().includes(query);
    return matchesQuery;
  });
  button("load-more-runs").hidden = !runView.hasMore;
  element("run-result-count").textContent =
    `${runs.length} run${runs.length === 1 ? "" : "s"} on this page`;
  clear(target);
  if (runs.length === 0) {
    emptyTable(target, RUN_TABLE_COLUMNS, "No runs match the selected filters.");
    return;
  }
  for (const run of runs) {
    const row = document.createElement("tr");
    row.className = "interactive-row";
    row.tabIndex = 0;
    row.addEventListener("click", () => void loadRunDetail(run));
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        void loadRunDetail(run);
      }
    });
    appendNodeCell(row, statusBadge(run.status));
    appendCell(row, logicalWorkflowName(run), "cell-primary");
    appendCell(row, short(run.workflowId), "mono");
    appendCell(row, short(run.runId), "mono");
    appendCell(row, formatDate(run.startTime));
    appendCell(row, runDuration(run));
    appendCell(row, runDetails.get(runKey(run))?.failedStep ?? "—");
    target.appendChild(row);
  }
}

const RUN_TABLE_COLUMNS = 7;

export async function refreshRunDetail(): Promise<void> {
  if (currentRun !== null) await loadRunDetail(currentRun);
}

function watchCheckbox(): HTMLInputElement {
  return input("watch-run-toggle");
}

export function setWatchRun(enabled: boolean): void {
  if (!enabled) {
    stopWatch();
    return;
  }
  if (watchTimer !== null || currentRun === null) return;
  watchStartedAt = Date.now();
  watchTimer = setInterval(() => {
    if (document.hidden) return;
    if (Date.now() - watchStartedAt > WATCH_RUN_MAX_MS) {
      stopWatch();
      return;
    }
    void refreshRunDetail();
  }, WATCH_RUN_INTERVAL_MS);
}

function stopWatch(): void {
  if (watchTimer !== null) {
    clearInterval(watchTimer);
    watchTimer = null;
  }
  watchCheckbox().checked = false;
}

function stopWatchAtTerminalState(detail: WorkflowRunDetail): void {
  if (watchTimer !== null && normalizedStatus(detail.status) !== "running") stopWatch();
}

export async function loadRunDetail(run: WorkflowRun): Promise<void> {
  const selection = runKey(run);
  runSelection = selection;
  currentRun = run;
  element("run-inspector-title").textContent = run.workflowId;
  const inspector = element("run-inspector");
  const scrim = element("inspector-scrim");
  inspector.hidden = false;
  scrim.hidden = false;
  const detailTarget = element("run-detail");
  clear(detailTarget);
  detailTarget.appendChild(textNode("p", "Loading run details…", "empty-state"));
  try {
    const detail = await client.runDetail(run.workflowId, run.runId);
    runDetails.set(runKey(detail), detail);
    if (runSelection === selection) {
      renderRunDetail(detail);
      stopWatchAtTerminalState(detail);
    }
    renderRuns();
    for (const listener of runDetailLoadedListeners) listener();
  } catch (error) {
    if (runSelection === selection) {
      clear(detailTarget);
      detailTarget.appendChild(textNode("p", safeError(error), "empty-state"));
    }
  }
}

export function closeRunInspector(): void {
  runSelection = null;
  currentRun = null;
  stopWatch();
  // Write-only payload discipline: no event input survives a closed inspector.
  clear(element("run-detail"));
  element("run-inspector").hidden = true;
  element("inspector-scrim").hidden = true;
}

export function isRunInspectorOpen(): boolean {
  return !element("run-inspector").hidden;
}

function renderRunDetail(detail: WorkflowRunDetail): void {
  element("run-inspector-title").textContent = detail.logicalWorkflow ?? detail.workflowId;
  const target = element("run-detail");
  clear(target);
  const summary = document.createElement("div");
  summary.className = "run-summary";

  const titleBlock = document.createElement("div");
  titleBlock.className = "run-title-block";
  const title = document.createElement("div");
  title.append(textNode("h3", detail.workflowId), textNode("code", `Run ${detail.runId}`));
  titleBlock.append(title, statusBadge(detail.status));
  summary.appendChild(titleBlock);

  const controls = document.createElement("div");
  controls.className = "run-controls";
  const running = normalizedStatus(detail.status) === "running";
  if (running && getCapabilities()?.workflow_cancel) {
    controls.appendChild(
      runControlButton("Request cancellation", "button-secondary", detail, "cancel"),
    );
  }
  if (running && getCapabilities()?.workflow_terminate) {
    controls.appendChild(runControlButton("Terminate", "button-danger", detail, "terminate"));
  }
  if (controls.childElementCount > 0) {
    summary.appendChild(controls);
    summary.appendChild(
      textNode(
        "p",
        "Cancellation lets the workflow handle cleanup. Termination stops it immediately and cannot be undone.",
        "run-control-note",
      ),
    );
  }
  if (running && getCapabilities()?.workflow_signal) {
    summary.appendChild(sendEventForm(detail));
  }

  summary.appendChild(
    detailSection("Execution", [
      ["Workflow name", detail.logicalWorkflow ?? "Unavailable"],
      ["Workflow ID", detail.workflowId],
      ["Run ID", detail.runId],
      ["Started", formatDate(detail.startTime)],
      ["Closed", formatDate(detail.closeTime)],
      ["Duration", runDuration(detail)],
      ["Trigger", detail.triggerSource ?? "Unavailable"],
      ["Pending waits", pendingWaits(detail)],
      ["Continuation", continuation(detail)],
    ]),
  );

  const provenance = document.createElement("section");
  provenance.className = "provenance-panel";
  provenance.appendChild(textNode("h4", "Execution versions"));
  const rail = document.createElement("div");
  rail.className = "provenance-rail";
  const provenanceFields: readonly [string, string][] = [
    ["Definition", short(detail.definitionDigest)],
    ["Configuration", short(detail.configurationRevisionId)],
    ["Worker artifact", short(detail.artifactDigest)],
  ];
  for (const [label, value] of provenanceFields) {
    const node = document.createElement("div");
    node.className = "provenance-node";
    node.append(textNode("span", label), textNode("code", value));
    rail.appendChild(node);
  }
  provenance.appendChild(rail);
  summary.appendChild(provenance);

  summary.appendChild(
    detailSection("Deployment and failure", [
      ["Deployment", detail.deploymentName ?? "Unavailable"],
      ["Build", detail.buildId ?? "Unavailable"],
      ["Failure code", detail.failureCode ?? "None"],
      ["Cause code", detail.failureCauseCode ?? "None"],
      ["Failure category", detail.failureCategory ?? "None"],
      ["Failure phase", detail.failurePhase ?? "None"],
      ["Failed step", detail.failedStep ?? "None"],
      [
        "Retryable",
        detail.failureRetryable === null
          ? "Not applicable"
          : detail.failureRetryable
            ? "Yes"
            : "No",
      ],
      ["Wait result", detail.pendingWaitsTruncated ? "Bounded result truncated" : "Complete"],
    ]),
  );
  target.appendChild(summary);
}

function sendEventForm(detail: WorkflowRunDetail): HTMLElement {
  const form = document.createElement("div");
  form.className = "send-event-form";
  form.appendChild(textNode("h4", "Send event"));
  const nameInput = document.createElement("input");
  nameInput.placeholder = "Event name";
  nameInput.setAttribute("aria-label", "Event name");
  const signalWait = detail.pendingWaits.find((wait) => wait.kind === "signal");
  if (signalWait !== undefined) nameInput.value = signalWait.state;
  const payloadInput = document.createElement("textarea");
  payloadInput.value = "{}";
  payloadInput.spellcheck = false;
  payloadInput.setAttribute("aria-label", "Event payload (JSON)");
  const note = textNode(
    "p",
    "The payload is sent once and never stored by the panel.",
    "run-control-note",
  );
  const send = document.createElement("button");
  send.className = "button button-secondary";
  send.type = "button";
  send.textContent = "Send event";
  send.addEventListener("click", () => void sendEvent(detail, nameInput, payloadInput, send));
  form.append(nameInput, payloadInput, note, send);
  return form;
}

async function sendEvent(
  detail: WorkflowRunDetail,
  nameInput: HTMLInputElement,
  payloadInput: HTMLTextAreaElement,
  control: HTMLButtonElement,
): Promise<void> {
  const eventName = nameInput.value.trim();
  if (eventName.length === 0) {
    window.alert("An event name is required.");
    return;
  }
  let payload: unknown;
  try {
    payload = JSON.parse(payloadInput.value);
  } catch {
    window.alert("The event payload must be valid JSON.");
    return;
  }
  control.disabled = true;
  try {
    await client.signalWorkflow(detail.workflowId, detail.runId, eventName, payload);
    payloadInput.value = "{}";
    control.textContent = "Accepted";
    await refreshRunDetail();
  } catch (error) {
    control.disabled = false;
    window.alert(safeError(error));
  }
}

function runControlButton(
  label: string,
  style: string,
  detail: WorkflowRunDetail,
  operation: "cancel" | "terminate",
): HTMLButtonElement {
  const control = document.createElement("button");
  control.className = `button ${style}`;
  control.type = "button";
  control.textContent = label;
  control.addEventListener("click", () => void controlRun(detail, operation, control));
  return control;
}

async function controlRun(
  detail: WorkflowRunDetail,
  operation: "cancel" | "terminate",
  control: HTMLButtonElement,
): Promise<void> {
  const verb = operation === "cancel" ? "request cancellation for" : "terminate";
  if (!window.confirm(`Are you sure you want to ${verb} this workflow run?`)) return;
  control.disabled = true;
  try {
    if (operation === "cancel") {
      await client.cancelRun(detail.workflowId, detail.runId);
    } else {
      await client.terminateRun(detail.workflowId, detail.runId);
    }
    control.textContent = "Accepted";
    await refreshDashboard();
  } catch (error) {
    control.disabled = false;
    control.textContent = operation === "cancel" ? "Request cancellation" : "Terminate";
    window.alert(safeError(error));
  }
}

function detailSection(title: string, fields: [string, string][]): HTMLElement {
  const section = document.createElement("section");
  section.className = "detail-section";
  section.appendChild(textNode("h4", title));
  const grid = document.createElement("div");
  grid.className = "detail-grid";
  for (const [label, value] of fields) {
    const field = document.createElement("div");
    field.className = "detail-field";
    field.append(textNode("span", label), textNode("strong", value));
    grid.appendChild(field);
  }
  section.appendChild(grid);
  return section;
}

function pendingWaits(detail: WorkflowRunDetail): string {
  if (detail.pendingWaits.length === 0) return "None reported";
  return detail.pendingWaits.map((wait) => `${wait.kind}: ${wait.state}`).join(", ");
}

function continuation(detail: WorkflowRunDetail): string {
  const identities = [detail.firstRunId, detail.nextRunId].filter(
    (value): value is string => value !== null,
  );
  return identities.length === 0 ? "None reported" : identities.map(short).join(" → ");
}
