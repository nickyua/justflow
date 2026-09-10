import type { ScheduledStart, TriggerSummary } from "../../api/contracts";
import { client } from "../../app/api";
import { getCapabilities } from "../../app/capabilities";
import {
  actionButton,
  appendCell,
  appendNodeCell,
  button,
  clear,
  element,
  emptyTable,
} from "../../app/dom";
import { formatDate, safeError, short } from "../../app/format";
import { WORKFLOW_ROUTE_PREFIX } from "../../app/router";
import { PagedCollection, STALE_LIST_NOTICE } from "../../components/pagination";
import { statusBadge } from "../../components/status";
import { editConfiguration, scheduleStateControl } from "../editor";

const SCHEDULE_LIST_COLUMNS = 7;
const SCHEDULED_START_LIST_COLUMNS = 6;

type WorkflowNavigator = (logicalWorkflow: string) => Promise<void>;

let schedules: TriggerSummary[] = [];
let navigateToWorkflow: WorkflowNavigator = () => Promise.resolve();
const scheduledStartPage = new PagedCollection<ScheduledStart>(async (cursor) => {
  if (getCapabilities()?.scheduled_start_view !== true) {
    return { items: [], nextCursor: null };
  }
  const page = await client.scheduledStarts([], cursor);
  return { items: page.scheduledStarts, nextCursor: page.nextCursor };
});

export function setSchedules(value: TriggerSummary[]): void {
  schedules = value;
}

export async function reloadSchedules(): Promise<void> {
  const scheduledStartsVisible = getCapabilities()?.scheduled_start_view === true;
  const [triggers] = await Promise.all([
    client.triggers(),
    scheduledStartsVisible ? scheduledStartPage.reload() : Promise.resolve(),
  ]);
  schedules = triggers;
  await renderSchedules();
}

export async function loadMoreScheduledStarts(): Promise<void> {
  const control = button("load-more-scheduled-starts");
  control.disabled = true;
  try {
    const outcome = await scheduledStartPage.loadMore();
    if (outcome === "reset") window.alert(STALE_LIST_NOTICE);
    renderScheduledStarts();
  } catch (error) {
    window.alert(safeError(error));
  } finally {
    control.disabled = false;
  }
}

export function getScheduleCount(): number {
  return schedules.length;
}

export function getSchedulesFor(logicalWorkflow: string): TriggerSummary[] {
  return schedules.filter(
    (trigger) => trigger.kind === "schedule" && trigger.workflowName === logicalWorkflow,
  );
}

export function getTriggersFor(logicalWorkflow: string): TriggerSummary[] {
  return schedules.filter((trigger) => trigger.workflowName === logicalWorkflow);
}

/** A schedule declaration from the configuration draft — authoring intent that
 * may not be materialized as a managed runtime schedule yet. */
export interface DeclaredScheduleRecord {
  name: string;
  workflow: string;
  paused: boolean;
}

export async function declaredSchedules(): Promise<DeclaredScheduleRecord[]> {
  return schedules
    .filter((trigger) => trigger.kind === "schedule")
    .map((trigger) => ({
      name: trigger.name,
      workflow: trigger.workflowName,
      paused: trigger.state === "inactive",
    }));
}

export function setWorkflowNavigator(navigator: WorkflowNavigator): void {
  navigateToWorkflow = navigator;
}

export async function renderSchedules(): Promise<void> {
  const target = element("schedule-list");
  const note = element("schedule-declared-note");
  clear(target);
  const activeCount = schedules.filter((trigger) => trigger.state === "active").length;
  const inactiveCount = schedules.length - activeCount;
  element("schedule-result-count").textContent =
    `${activeCount} active · ${inactiveCount} inactive`;
  note.hidden = true;
  note.textContent = "";
  renderScheduledStarts();
  if (schedules.length === 0) {
    emptyTable(target, SCHEDULE_LIST_COLUMNS, "No triggers are declared in this scope.");
    return;
  }
  for (const trigger of schedules) {
    const row = document.createElement("tr");
    appendCell(row, trigger.name, "cell-primary");
    appendNodeCell(row, workflowLink(trigger.workflowName));
    appendNodeCell(
      row,
      trigger.kind === "schedule"
        ? scheduleStateControl(trigger.name, trigger.state === "active")
        : effectiveStateCell(trigger.state === "active"),
    );
    appendCell(row, trigger.kind);
    appendCell(
      row,
      trigger.lastAction === null
        ? "—"
        : `${trigger.lastAction.outcome} · ${formatDate(trigger.lastAction.startedAt)}`,
    );
    appendCell(row, formatDate(trigger.nextRunTimes[0] ?? null));
    appendNodeCell(row, scheduleRowActions(trigger.name));
    target.appendChild(row);
  }
}

function renderScheduledStarts(): void {
  const target = element("scheduled-start-list");
  const visible = getCapabilities()?.scheduled_start_view === true;
  element("scheduled-starts-surface").hidden = !visible;
  if (!visible) return;
  clear(target);
  const scheduledStarts = scheduledStartPage.items;
  element("scheduled-start-result-count").textContent =
    `${scheduledStarts.length} scheduled start${scheduledStarts.length === 1 ? "" : "s"} on this page`;
  button("load-more-scheduled-starts").hidden = !scheduledStartPage.hasMore;
  if (scheduledStarts.length === 0) {
    emptyTable(
      target,
      SCHEDULED_START_LIST_COLUMNS,
      "No one-off scheduled starts are visible in this scope.",
    );
    return;
  }
  for (const scheduledStart of scheduledStarts) {
    const row = document.createElement("tr");
    appendNodeCell(row, workflowLink(scheduledStart.workflowName));
    appendCell(row, formatDate(scheduledStart.startAt));
    appendCell(row, scheduledStart.workloadClass);
    appendNodeCell(row, statusBadge(scheduledStart.state));
    appendCell(
      row,
      scheduledStart.workflowId === null
        ? "—"
        : `${short(scheduledStart.workflowId)} / ${short(scheduledStart.runId)}`,
      "mono",
    );
    appendNodeCell(row, scheduledStartRowActions(scheduledStart));
    target.appendChild(row);
  }
}

function workflowLink(workflowName: string): Node {
  const link = document.createElement("a");
  link.className = "workflow-link";
  link.href = `${WORKFLOW_ROUTE_PREFIX}${encodeURIComponent(workflowName)}`;
  link.textContent = workflowName;
  link.addEventListener("click", (event) => {
    event.preventDefault();
    void navigateToWorkflow(workflowName);
  });
  return link;
}

/** Active means the declaration is enabled; provider readiness remains separate. */
export function effectiveStateCell(active: boolean): Node {
  return statusBadge(active ? "Active" : "Inactive", active ? "active" : "none");
}

function scheduleRowActions(name: string): Node {
  const actions = document.createElement("div");
  actions.className = "row-actions";
  const edit = actionButton("Edit", "row-action");
  edit.addEventListener("click", () => void editConfiguration("schedule", name));
  actions.appendChild(edit);
  return actions;
}

function scheduledStartRowActions(scheduledStart: ScheduledStart): Node {
  const actions = document.createElement("div");
  actions.className = "row-actions";
  const details = actionButton("Details", "row-action");
  details.addEventListener("click", () => void showScheduledStartDetails(scheduledStart));
  actions.appendChild(details);
  if (
    scheduledStart.state === "scheduled" &&
    getCapabilities()?.scheduled_start_reschedule === true
  ) {
    const reschedule = actionButton("Reschedule", "row-action");
    reschedule.addEventListener("click", () => void rescheduleStart(scheduledStart));
    actions.appendChild(reschedule);
  }
  if (scheduledStart.state === "scheduled" && getCapabilities()?.scheduled_start_cancel === true) {
    const cancel = actionButton("Cancel", "row-action is-danger");
    cancel.addEventListener("click", () => void cancelStart(scheduledStart));
    actions.appendChild(cancel);
  }
  return actions;
}

async function showScheduledStartDetails(scheduledStart: ScheduledStart): Promise<void> {
  try {
    const current = await client.scheduledStart(scheduledStart.scheduledStartId);
    window.alert(
      [
        `Workflow: ${current.workflowName}`,
        `Due: ${formatDate(current.startAt)}`,
        `State: ${current.state}`,
        `Workload: ${current.workloadClass}`,
        `Version: ${current.version}`,
        `Definition: ${short(current.definitionDigest)}`,
        `Worker build: ${current.artifactBuildId ?? "—"}`,
        `Run: ${current.workflowId === null ? "—" : `${short(current.workflowId)} / ${short(current.runId)}`}`,
        `Failure: ${current.failureCode ?? "—"}`,
      ].join("\n"),
    );
  } catch (error) {
    window.alert(safeError(error));
  }
}

async function rescheduleStart(scheduledStart: ScheduledStart): Promise<void> {
  const requested = window.prompt("New due time (ISO 8601 with timezone)", scheduledStart.startAt);
  if (requested === null) return;
  const due = new Date(requested);
  if (Number.isNaN(due.getTime())) {
    window.alert("Enter a valid due time with a timezone.");
    return;
  }
  try {
    const result = await client.rescheduleScheduledStart(
      scheduledStart.scheduledStartId,
      due.toISOString(),
      scheduledStart.version,
      crypto.randomUUID(),
    );
    if (result.status === "in_progress") {
      window.alert("The reschedule is durably owned and still completing. Refresh to follow it.");
    }
    await reloadSchedules();
  } catch (error) {
    window.alert(safeError(error));
  }
}

async function cancelStart(scheduledStart: ScheduledStart): Promise<void> {
  if (!window.confirm(`Cancel the scheduled start for “${scheduledStart.workflowName}”?`)) return;
  try {
    const result = await client.cancelScheduledStart(
      scheduledStart.scheduledStartId,
      scheduledStart.version,
      crypto.randomUUID(),
    );
    if (result.status === "in_progress") {
      window.alert("The cancellation is durably owned and still completing. Refresh to follow it.");
    }
    await reloadSchedules();
  } catch (error) {
    window.alert(safeError(error));
  }
}
