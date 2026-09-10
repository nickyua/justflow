import type {
  DefinitionSummary,
  ScheduledStartWorkloadClass,
  WorkflowDetail,
  WorkflowRegistration,
} from "../../api/contracts";
import { client } from "../../app/api";
import { getCapabilities } from "../../app/capabilities";
import {
  actionButton,
  appendCell,
  appendNodeCell,
  button,
  clear,
  dialog,
  element,
  emptyTable,
  input,
  selectElement,
  textarea,
  textNode,
} from "../../app/dom";
import { formatDate, runDuration, runKey, safeError, short } from "../../app/format";
import {
  requestedWorkflowTab,
  setView,
  setWorkflowTabLocation,
  WORKFLOW_ROUTE_PREFIX,
  type WorkflowTab,
} from "../../app/router";
import {
  type CodeEditorControl,
  textareaControl,
  upgradeTextarea,
} from "../../components/code-editor";
import { type GraphFrameHandle, mountGraphFrame } from "../../components/graph-frame";
import { renderGraphTable } from "../../components/graph-table";
import { PagedCollection, STALE_LIST_NOTICE } from "../../components/pagination";
import { statusBadge } from "../../components/status";
import { GRAPH_PROTOCOL_VERSION, type GraphViewDocument } from "../../graph-frame/protocol";
import { retireWorkflow, scheduleStateControl } from "../editor";
import { openWorkflowEditor } from "../editor/fragment";
import { getAllRuns, getRunDetail, loadRunDetail, logicalWorkflowName } from "../runs";
import {
  type DeclaredScheduleRecord,
  declaredSchedules,
  effectiveStateCell,
  getSchedulesFor,
  getTriggersFor,
  reloadSchedules,
} from "../schedules";

const WORKFLOW_TYPE_SEPARATOR = "__";
const WORKFLOW_LIST_COLUMNS = 5;
const DEFINITION_LIST_COLUMNS = 2;
const WORKFLOW_RUN_COLUMNS = 7;
const WORKFLOW_SCHEDULE_COLUMNS = 3;

const WORKFLOW_RUNS_PAGE_LIMIT = 20;

let selectedWorkflow: string | null = null;
let workflowDetail: WorkflowDetail | null = null;
let graphFrame: GraphFrameHandle | null = null;
let activeTab: WorkflowTab = "overview";
let declarationLoadedFor: string | null = null;

const workflowRunPage = new PagedCollection((cursor: string | null) => {
  const workflow = selectedWorkflow;
  if (workflow === null) return Promise.resolve({ items: [], nextCursor: null });
  return client
    .runs(null, WORKFLOW_RUNS_PAGE_LIMIT, cursor, workflow)
    .then((page) => ({ items: page.runs, nextCursor: page.nextCursor }));
});

const workflowPage = new PagedCollection<WorkflowRegistration>(async (cursor) => {
  const page = await client.workflowRegistrations(cursor);
  return { items: page.workflows, nextCursor: page.nextCursor };
});
let draftOnlyWorkflows: string[] = [];

/** Workflow names declared in the configuration draft but not registered in the
 * catalog — they participate in nothing until published/activated. */
async function loadDraftOnlyWorkflows(): Promise<void> {
  if (getCapabilities()?.configuration_view !== true) {
    draftOnlyWorkflows = [];
    return;
  }
  try {
    const draft = await client.draft();
    const declarations = draft.bundle.workflows;
    if (typeof declarations !== "object" || declarations === null || Array.isArray(declarations)) {
      draftOnlyWorkflows = [];
      return;
    }
    const registered = new Set(workflowPage.items.map((workflow) => workflow.logicalWorkflow));
    draftOnlyWorkflows = Object.keys(declarations)
      .filter((name) => !registered.has(name))
      .sort();
  } catch {
    // The registered list stays authoritative; draft visibility is best-effort.
    draftOnlyWorkflows = [];
  }
}

export async function reloadWorkflowCollections(): Promise<void> {
  await workflowPage.reload();
  await loadDraftOnlyWorkflows();
  renderWorkflows();
}

export async function loadMoreWorkflows(): Promise<void> {
  await loadMoreInto(workflowPage, "load-more-workflows", renderWorkflows);
}

async function loadMoreInto(
  collection: PagedCollection<unknown>,
  buttonId: string,
  render: () => void,
): Promise<void> {
  const control = button(buttonId);
  control.disabled = true;
  try {
    const outcome = await collection.loadMore();
    if (outcome === "reset") window.alert(STALE_LIST_NOTICE);
    render();
  } catch (error) {
    window.alert(safeError(error));
  } finally {
    control.disabled = false;
  }
}

export function getWorkflowCount(): number {
  return workflowPage.items.length;
}

export function getRegisteredWorkflowNames(): string[] {
  return workflowPage.items.map((workflow) => workflow.logicalWorkflow);
}

export function getSelectedWorkflow(): string | null {
  return selectedWorkflow;
}

export function setSelectedWorkflow(name: string | null): void {
  selectedWorkflow = name;
}

export function rerenderDetailIfOpen(): void {
  if (workflowDetail !== null) renderWorkflowDetail(workflowDetail);
}

export function renderWorkflows(): void {
  const target = element("workflow-list");
  const note = element("workflow-draft-note");
  const query = input("workflow-search").value.trim().toLowerCase();
  const visible = workflowPage.items.filter((workflow) =>
    workflow.logicalWorkflow.toLowerCase().includes(query),
  );
  const visibleDrafts = draftOnlyWorkflows.filter((name) => name.toLowerCase().includes(query));
  button("load-more-workflows").hidden = !workflowPage.hasMore;
  element("workflow-result-count").textContent =
    visibleDrafts.length > 0
      ? `${visible.length} active · ${visibleDrafts.length} inactive`
      : `${visible.length} workflow${visible.length === 1 ? "" : "s"} on this page`;
  note.hidden = visibleDrafts.length === 0;
  note.textContent =
    visibleDrafts.length > 0
      ? "Inactive workflows are saved but not yet activated; restart Justflow to activate them."
      : "";
  clear(target);
  if (visible.length === 0 && visibleDrafts.length === 0) {
    emptyTable(
      target,
      WORKFLOW_LIST_COLUMNS,
      query.length > 0 ? "No workflows match this filter." : "No workflows are registered.",
    );
    return;
  }
  for (const workflow of visible) {
    const row = document.createElement("tr");
    row.className = "interactive-row";
    const workflowLink = document.createElement("a");
    workflowLink.className = "workflow-link";
    workflowLink.href = `${WORKFLOW_ROUTE_PREFIX}${encodeURIComponent(workflow.logicalWorkflow)}`;
    workflowLink.textContent = workflow.logicalWorkflow;
    workflowLink.addEventListener("click", (event) => {
      event.preventDefault();
      void loadWorkflowDetail(workflow.logicalWorkflow);
    });
    appendNodeCell(row, workflowLink);
    appendCell(row, short(workflow.activeDefinitionDigest), "mono");
    appendCell(row, String(workflow.retainedDefinitionCount));
    appendNodeCell(row, effectiveStateCell(workflow.activeDefinitionDigest.length > 0));

    const actions = document.createElement("div");
    actions.className = "row-actions";
    const edit = actionButton("Edit draft", "row-action");
    edit.addEventListener("click", (event) => {
      event.stopPropagation();
      void openWorkflowEditor(workflow.logicalWorkflow);
    });
    const retire = actionButton("Retire", "row-action is-danger");
    retire.addEventListener("click", (event) => {
      event.stopPropagation();
      void retireWorkflow(workflow.logicalWorkflow);
    });
    actions.append(edit, retire);
    appendNodeCell(row, actions);
    target.appendChild(row);
  }
  for (const name of visibleDrafts) {
    const row = document.createElement("tr");
    row.className = "interactive-row";
    const workflowLink = document.createElement("a");
    workflowLink.className = "workflow-link";
    workflowLink.href = "#";
    workflowLink.textContent = name;
    workflowLink.addEventListener("click", (event) => {
      event.preventDefault();
      void openWorkflowEditor(name);
    });
    appendNodeCell(row, workflowLink);
    appendCell(row, "—", "mono");
    appendCell(row, "—");
    appendNodeCell(row, effectiveStateCell(false));
    const actions = document.createElement("div");
    actions.className = "row-actions";
    const edit = actionButton("Edit draft", "row-action");
    edit.addEventListener("click", (event) => {
      event.stopPropagation();
      void openWorkflowEditor(name);
    });
    const remove = actionButton("Remove", "row-action is-danger");
    remove.addEventListener("click", (event) => {
      event.stopPropagation();
      void retireWorkflow(name);
    });
    actions.append(edit, remove);
    appendNodeCell(row, actions);
    target.appendChild(row);
  }
}

export function setWorkflowTab(tab: WorkflowTab, updateLocation = true): void {
  activeTab = tab;
  for (const panel of document.querySelectorAll<HTMLElement>("[data-workflow-panel]")) {
    panel.hidden = panel.dataset.workflowPanel !== tab;
  }
  for (const button of document.querySelectorAll<HTMLButtonElement>("[data-workflow-tab]")) {
    const active = button.dataset.workflowTab === tab;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-pressed", String(active));
  }
  if (updateLocation && selectedWorkflow !== null) {
    setWorkflowTabLocation(selectedWorkflow, tab);
  }
  void loadActiveTabData();
}

async function loadActiveTabData(): Promise<void> {
  if (selectedWorkflow === null) return;
  if (activeTab === "definition") {
    await renderDefinitionTab();
    await loadDeclaration(selectedWorkflow);
  } else if (activeTab === "runs") {
    await reloadWorkflowRuns();
  } else if (activeTab === "triggers" && workflowDetail !== null) {
    await renderWorkflowSchedules(workflowDetail.logicalWorkflow);
  }
}

async function reloadWorkflowRuns(): Promise<void> {
  await workflowRunPage.reload();
  renderWorkflowRuns();
}

export async function loadMoreWorkflowRuns(): Promise<void> {
  const control = button("load-more-workflow-runs");
  control.disabled = true;
  try {
    const outcome = await workflowRunPage.loadMore();
    if (outcome === "reset") window.alert(STALE_LIST_NOTICE);
    renderWorkflowRuns();
  } catch (error) {
    window.alert(safeError(error));
  } finally {
    control.disabled = false;
  }
}

export async function loadWorkflowDetail(
  logicalWorkflow: string,
  updateLocation = true,
): Promise<void> {
  selectedWorkflow = logicalWorkflow;
  workflowDetail = null;
  declarationLoadedFor = null;
  declarationDocument = null;
  setView("workflow", updateLocation, logicalWorkflow);
  setWorkflowTab(requestedWorkflowTab(), false);
  element("workflow-detail-name").textContent = logicalWorkflow;
  element("workflow-detail-description").textContent = "Loading active definition…";
  try {
    const detail = await client.workflowDetail(logicalWorkflow);
    if (selectedWorkflow !== logicalWorkflow) return;
    workflowDetail = detail;
    renderWorkflowDetail(detail);
    await loadActiveTabData();
  } catch (error) {
    if (selectedWorkflow !== logicalWorkflow) return;
    element("workflow-detail-description").textContent = safeError(error);
    clear(element("workflow-graph-table"));
    element("workflow-graph-table").appendChild(
      textNode("p", "The active workflow definition could not be loaded.", "empty-state"),
    );
  }
}

export function openStartRunDialog(): void {
  const detail = workflowDetail;
  const capabilities = getCapabilities();
  if (
    detail === null ||
    (capabilities?.workflow_start !== true && capabilities?.scheduled_start_create !== true)
  )
    return;
  element("start-run-now-option").hidden = capabilities.workflow_start !== true;
  element("start-run-later-option").hidden = capabilities.scheduled_start_create !== true;
  selectElement("start-run-mode").value = capabilities.workflow_start ? "now" : "later";
  const globalsNote =
    detail.referencedGlobals.length > 0
      ? ` Referenced trigger globals: ${detail.referencedGlobals.join(", ")}.`
      : "";
  element("start-run-contract").textContent =
    (detail.hasInputContract
      ? detail.inputSchema !== null
        ? "The input is validated against the inline schema shown on the Definition tab."
        : "The input is validated against the workflow's declared contract."
      : "This workflow declares no input contract.") + globalsNote;
  textarea("start-run-input").value = "{}";
  updateStartMode();
  dialog("start-run-dialog").showModal();
  textarea("start-run-input").focus();
}

export function updateStartMode(): void {
  const detail = workflowDetail;
  if (detail === null) return;
  const scheduled = selectElement("start-run-mode").value === "later";
  const scope = getCapabilities()?.scope;
  element("start-run-schedule-fields").hidden = !scheduled;
  element("start-run-workload-field").hidden = !scheduled;
  input("start-run-at").required = scheduled;
  button("start-run-submit").textContent = scheduled ? "Schedule start" : "Start run";
  element("start-run-dialog-title").textContent = scheduled ? "Schedule start" : "Start run";
  element("start-run-context").textContent = scheduled
    ? `Schedules one future invocation of “${detail.logicalWorkflow}” in ${scope?.tenant ?? "?"} / ` +
      `${scope?.application ?? "?"} / ${scope?.environment ?? "?"}. The active definition is resolved when dispatch begins.`
    : `Starts a real run of “${detail.logicalWorkflow}” against active definition ` +
      `${short(detail.activeDefinitionDigest)} in ${scope?.tenant ?? "?"} / ` +
      `${scope?.application ?? "?"} / ${scope?.environment ?? "?"}.`;
}

export async function startRun(): Promise<void> {
  const detail = workflowDetail;
  if (detail === null) return;
  let parsed: unknown;
  try {
    parsed = JSON.parse(textarea("start-run-input").value);
  } catch {
    window.alert("The workflow input must be valid JSON.");
    return;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    window.alert("The workflow input must be a JSON object.");
    return;
  }
  try {
    const requestId = crypto.randomUUID();
    if (selectElement("start-run-mode").value === "later") {
      const due = new Date(input("start-run-at").value);
      if (Number.isNaN(due.getTime())) {
        window.alert("Choose a valid future due time.");
        return;
      }
      const workload = selectElement("start-run-workload").value;
      if (!isScheduledStartWorkloadClass(workload)) {
        window.alert("Choose a supported workload class.");
        return;
      }
      await client.scheduleWorkflow(
        detail.logicalWorkflow,
        requestId,
        parsed as Record<string, unknown>,
        due.toISOString(),
        workload,
        requestId,
      );
      textarea("start-run-input").value = "{}";
      dialog("start-run-dialog").close();
      setView("triggers");
      await reloadSchedules();
      return;
    }
    const result = await client.startWorkflow(
      detail.logicalWorkflow,
      requestId,
      parsed as Record<string, unknown>,
    );
    // Write-only input discipline: the dialog never retains a submitted payload.
    textarea("start-run-input").value = "{}";
    dialog("start-run-dialog").close();
    if (result.duplicate) {
      window.alert("This request identity had already started a run; showing the existing run.");
    }
    if (result.runId !== null) {
      setView("runs");
      await loadRunDetail({
        workflowId: result.workflowId,
        runId: result.runId,
        workflowType: detail.logicalWorkflow,
        status: null,
        startTime: new Date().toISOString(),
        closeTime: null,
      });
    }
  } catch (error) {
    window.alert(safeError(error));
  }
}

function renderWorkflowDetail(detail: WorkflowDetail): void {
  const capabilities = getCapabilities();
  button("workflow-detail-start").hidden =
    capabilities?.workflow_start !== true && capabilities?.scheduled_start_create !== true;
  element("workflow-detail-name").textContent = detail.logicalWorkflow;
  element("workflow-detail-description").textContent =
    detail.description.length === 0 ? "No description is configured." : detail.description;
  element("workflow-detail-digest").textContent = short(detail.activeDefinitionDigest);
  renderWorkflowGraph(detail);

  const metadata = element("workflow-detail-metadata");
  clear(metadata);
  const metadataRows: readonly (readonly [string, string])[] = [
    ["Workflow name", detail.logicalWorkflow],
    [
      "Input parameters",
      detail.referencedGlobals.length > 0 ? detail.referencedGlobals.join(", ") : "—",
    ],
    ["Active definition", short(detail.activeDefinitionDigest)],
    ["Retained versions", String(detail.retainedDefinitionCount)],
    ["Engine ABI", detail.requiredEngineWorkflowAbi],
    ["Configured steps", String(detail.graphNodes.length)],
  ];
  for (const [label, value] of metadataRows) {
    const field = document.createElement("div");
    field.className = "metadata-field";
    field.append(textNode("span", label), textNode("strong", value));
    metadata.appendChild(field);
  }
  void renderWorkflowSchedules(detail.logicalWorkflow);
}

function isScheduledStartWorkloadClass(value: string): value is ScheduledStartWorkloadClass {
  return value === "interactive" || value === "standard" || value === "batch";
}

async function renderDefinitionTab(): Promise<void> {
  const detail = workflowDetail;
  if (detail === null) return;
  renderInputParameters(detail);
  let declared: DeclaredScheduleRecord[] = [];
  try {
    declared = await declaredSchedulesFor(detail.logicalWorkflow);
  } catch {
    // The start-sources row falls back to managed schedules only; the
    // The Triggers tab surfaces the load error itself.
  }
  renderStartSources(detail, declared);
  renderStepBindings(detail);
  renderWorkflowDefinitions(detail.definitions);
}

const INPUT_LIST_COLUMNS = 3;
const STEP_LIST_COLUMNS = 4;

function renderInputParameters(detail: WorkflowDetail): void {
  const target = element("workflow-input-list");
  clear(target);
  if (detail.referencedGlobals.length === 0) {
    emptyTable(target, INPUT_LIST_COLUMNS, "This workflow references no input parameters.");
  }
  for (const name of detail.referencedGlobals) {
    const row = document.createElement("tr");
    appendCell(row, name, "cell-primary");
    appendCell(row, "trigger global");
    appendCell(row, `\${${name}}`, "mono");
    target.appendChild(row);
  }
  const contract = element("workflow-input-contract");
  contract.textContent =
    detail.inputSchema !== null
      ? "Inputs are validated against the inline schema below."
      : detail.hasInputContract
        ? "Inputs are validated against the workflow's declared contract."
        : "No input contract is declared; parameters above come from ${…} references.";
  const schema = element("workflow-input-schema");
  schema.hidden = detail.inputSchema === null;
  schema.textContent =
    detail.inputSchema === null ? "" : JSON.stringify(detail.inputSchema, null, 2);
}

function renderStartSources(detail: WorkflowDetail, declared: DeclaredScheduleRecord[]): void {
  const target = element("workflow-start-sources");
  clear(target);

  const api = document.createElement("tr");
  appendCell(api, "Control API", "cell-primary");
  const startPermitted = getCapabilities()?.workflow_start === true;
  const apiTrigger = getTriggersFor(detail.logicalWorkflow).find(
    (trigger) => trigger.kind === "api",
  );
  const apiActive = apiTrigger?.state === "active";
  appendNodeCell(
    api,
    apiTrigger === undefined
      ? statusBadge("Not declared", "none")
      : !apiActive
        ? statusBadge("Inactive", "none")
        : startPermitted
          ? statusBadge("Active", "active")
          : statusBadge("Not permitted", "none"),
  );
  appendCell(
    api,
    apiTrigger === undefined
      ? "This workflow does not declare an API trigger."
      : !apiActive
        ? "The API trigger is disabled in the declared configuration."
        : startPermitted
          ? "Start by name with a JSON input matching the parameters above."
          : "This session lacks the workflow_start capability.",
  );
  target.appendChild(api);

  const managed = getSchedulesFor(detail.logicalWorkflow);
  const managedNames = new Set(managed.map((schedule) => schedule.name));
  const declaredOnly = declared.filter((schedule) => !managedNames.has(schedule.name));
  const scheduled = document.createElement("tr");
  appendCell(scheduled, "Schedule triggers", "cell-primary");
  const activeSchedules = managed.filter((schedule) => schedule.state === "active");
  appendNodeCell(
    scheduled,
    activeSchedules.length > 0
      ? statusBadge(`${activeSchedules.length} active`, "active")
      : managed.length + declaredOnly.length > 0
        ? statusBadge("Inactive", "none")
        : statusBadge("None", "none"),
  );
  const names = [
    ...managed.map((schedule) =>
      schedule.state === "inactive" ? `${schedule.name} (inactive)` : schedule.name,
    ),
    ...declaredOnly.map((schedule) => `${schedule.name} (inactive)`),
  ];
  appendCell(
    scheduled,
    names.length > 0 ? names.join(", ") : "No schedule trigger targets this workflow.",
  );
  target.appendChild(scheduled);

  for (const trigger of getTriggersFor(detail.logicalWorkflow).filter(
    (candidate) => candidate.kind !== "api" && candidate.kind !== "schedule",
  )) {
    const row = document.createElement("tr");
    appendCell(row, `${trigger.kind} · ${trigger.name}`, "cell-primary");
    appendNodeCell(row, effectiveStateCell(trigger.state === "active"));
    appendCell(
      row,
      "State reflects the declared trigger; provider readiness remains an independent runtime concern.",
    );
    target.appendChild(row);
  }
}

function renderStepBindings(detail: WorkflowDetail): void {
  const target = element("workflow-step-list");
  clear(target);
  if (detail.stepBindings.length === 0) {
    emptyTable(target, STEP_LIST_COLUMNS, "This definition declares no step bindings.");
  }
  for (const binding of detail.stepBindings) {
    const row = document.createElement("tr");
    appendCell(row, binding.operation, "cell-primary");
    appendCell(
      row,
      binding.service ?? (binding.subworkflow !== null ? `workflow: ${binding.subworkflow}` : "—"),
      binding.service !== null ? "mono" : undefined,
    );
    appendCell(row, binding.action ?? "—", binding.action !== null ? "mono" : undefined);
    if (binding.resources.length === 0) {
      appendCell(row, "—");
    } else {
      const chips = document.createElement("span");
      chips.className = "capability-chips";
      for (const resource of binding.resources) {
        chips.appendChild(textNode("span", resource, "capability-chip"));
      }
      appendNodeCell(row, chips);
    }
    target.appendChild(row);
  }
  const note = element("workflow-archival-note");
  note.hidden = detail.archival === null;
  note.textContent =
    detail.archival === null
      ? ""
      : `On completion, results are archived to ${detail.archival.resource} (retention: ${detail.archival.retentionPolicy}).`;
}

let declarationControl: CodeEditorControl | null = null;

function declarationSurface(): CodeEditorControl {
  declarationControl ??= textareaControl(textarea("workflow-declaration"));
  return declarationControl;
}

export async function initDeclarationSurface(): Promise<void> {
  declarationControl = await upgradeTextarea("workflow-declaration", () => {});
  declarationControl.setReadOnly(true);
}

async function loadDeclaration(logicalWorkflow: string): Promise<void> {
  const note = element("workflow-declaration-note");
  const view = element("workflow-declaration-editor");
  const tabs = element("workflow-declaration-tabs");
  const copy = button("workflow-declaration-copy");
  if (getCapabilities()?.configuration_view !== true) {
    note.textContent = "Viewing the declaration requires configuration view permission.";
    view.hidden = true;
    tabs.hidden = true;
    copy.hidden = true;
    return;
  }
  if (declarationLoadedFor === logicalWorkflow) return;
  note.textContent = "Loading the active declaration…";
  try {
    const definition = await client.workflowDefinition(logicalWorkflow);
    if (selectedWorkflow !== logicalWorkflow) return;
    declarationLoadedFor = logicalWorkflow;
    declarationDocument = definition.document;
    renderDeclarationTabs(DECLARATION_GENERAL_TAB);
    view.hidden = false;
    copy.hidden = false;
    note.textContent = `Read-only YAML from immutable definition ${short(definition.definitionDigest)} — never the draft.`;
  } catch (error) {
    note.textContent = safeError(error);
    view.hidden = true;
    tabs.hidden = true;
    copy.hidden = true;
  }
}

/** Top-level keys large enough to earn their own declaration tab; everything
 * else (name, description, params, archival, …) reads best together. */
const LARGE_DECLARATION_KEYS: ReadonlySet<string> = new Set(["steps", "flow"]);
const DECLARATION_GENERAL_TAB = "general";
const DECLARATION_FULL_TAB = "full YAML";

let declarationDocument: string | null = null;

interface DeclarationSection {
  key: string;
  text: string;
}

/** Split rendered YAML at column-0 keys; each section keeps its exact source text. */
function splitTopLevelSections(yamlDocument: string): DeclarationSection[] {
  const sections: DeclarationSection[] = [];
  let current: DeclarationSection | null = null;
  for (const line of yamlDocument.split("\n")) {
    const key = /^([A-Za-z_][A-Za-z0-9_]*):/.exec(line)?.[1];
    if (key !== undefined) {
      if (current !== null) sections.push(current);
      current = { key, text: line };
    } else if (current !== null) {
      current.text += `\n${line}`;
    }
  }
  if (current !== null) sections.push(current);
  return sections;
}

function declarationTabText(tab: string): string {
  const yamlDocument = declarationDocument ?? "";
  if (tab === DECLARATION_FULL_TAB) return yamlDocument;
  const sections = splitTopLevelSections(yamlDocument);
  const selected =
    tab === DECLARATION_GENERAL_TAB
      ? sections.filter((section) => !LARGE_DECLARATION_KEYS.has(section.key))
      : sections.filter((section) => section.key === tab);
  return selected
    .map((section) => section.text)
    .join("\n")
    .trimEnd();
}

function renderDeclarationTabs(activeTabName: string): void {
  const host = element("workflow-declaration-tabs");
  clear(host);
  const legend = document.createElement("legend");
  legend.className = "sr-only";
  legend.textContent = "Declaration sections";
  host.appendChild(legend);
  const present = splitTopLevelSections(declarationDocument ?? "").map((section) => section.key);
  const tabNames = [
    DECLARATION_GENERAL_TAB,
    ...present.filter((key) => LARGE_DECLARATION_KEYS.has(key)),
    DECLARATION_FULL_TAB,
  ];
  for (const tabName of tabNames) {
    const tab = document.createElement("button");
    tab.type = "button";
    tab.className = tabName === activeTabName ? "filter-tab is-active" : "filter-tab";
    tab.setAttribute("aria-pressed", String(tabName === activeTabName));
    tab.textContent = tabName;
    tab.addEventListener("click", () => renderDeclarationTabs(tabName));
    host.appendChild(tab);
  }
  host.hidden = false;
  declarationSurface().setValue(declarationTabText(activeTabName));
}

export async function copyDeclaration(): Promise<void> {
  if (declarationDocument === null) return;
  await navigator.clipboard.writeText(declarationDocument);
}

function renderWorkflowDefinitions(definitions: readonly DefinitionSummary[]): void {
  const target = element("workflow-definition-list");
  clear(target);
  if (definitions.length === 0) {
    emptyTable(target, DEFINITION_LIST_COLUMNS, "No retained definitions are available.");
    return;
  }
  for (const definition of definitions) {
    const row = document.createElement("tr");
    appendCell(row, short(definition.definitionDigest), "mono");
    appendNodeCell(row, statusBadge(definition.active ? "Active" : "Retained"));
    target.appendChild(row);
  }
}

function buildGraphDocument(detail: WorkflowDetail): GraphViewDocument {
  const failedStep = latestFailureStep(detail.logicalWorkflow);
  return {
    version: GRAPH_PROTOCOL_VERSION,
    title: `${detail.logicalWorkflow} workflow steps`,
    nodes: detail.graphNodes.map((node) => ({
      id: node.nodeId,
      label: node.label,
      kind: node.kind,
      group: node.group,
      failed: node.label === failedStep || node.nodeId === failedStep,
      metadata: node.metadata,
    })),
    edges: detail.graphEdges.map((edge) => ({
      source: edge.source,
      target: edge.target,
      label: edge.label,
      dashed: edge.dashed,
    })),
  };
}

function renderWorkflowGraph(detail: WorkflowDetail): void {
  const graph = buildGraphDocument(detail);
  if (graphFrame === null) {
    graphFrame = mountGraphFrame(element("workflow-graph"));
  }
  graphFrame.render(graph);
  renderGraphTable(element("workflow-graph-table"), graph);
}

function renderWorkflowRuns(): void {
  const target = element("workflow-run-list");
  const runs = workflowRunPage.items;
  button("load-more-workflow-runs").hidden = !workflowRunPage.hasMore;
  clear(target);
  if (runs.length === 0) {
    emptyTable(target, WORKFLOW_RUN_COLUMNS, "No visible runs target this workflow.");
    return;
  }
  for (const run of runs) {
    const row = document.createElement("tr");
    row.className = "interactive-row";
    row.addEventListener("click", () => void loadRunDetail(run));
    appendNodeCell(row, statusBadge(run.status));
    appendCell(row, logicalWorkflowName(run), "cell-primary");
    appendCell(row, short(run.workflowId), "mono");
    appendCell(row, short(run.runId), "mono");
    appendCell(row, formatDate(run.startTime));
    appendCell(row, runDuration(run));
    appendCell(row, getRunDetail(runKey(run))?.failedStep ?? "—");
    target.appendChild(row);
  }
}

async function declaredSchedulesFor(logicalWorkflow: string): Promise<DeclaredScheduleRecord[]> {
  return (await declaredSchedules()).filter(
    (declaration) => declaration.workflow === logicalWorkflow,
  );
}

async function renderWorkflowSchedules(logicalWorkflow: string): Promise<void> {
  const target = element("workflow-schedule-list");
  const note = element("workflow-schedule-note");
  const managed = getSchedulesFor(logicalWorkflow);
  let declared: DeclaredScheduleRecord[] = [];
  let declaredError: string | null = null;
  try {
    declared = await declaredSchedulesFor(logicalWorkflow);
  } catch (error) {
    declaredError = safeError(error);
  }
  const managedNames = new Set(managed.map((schedule) => schedule.name));
  const declaredOnly = declared.filter((schedule) => !managedNames.has(schedule.name));
  clear(target);
  if (managed.length === 0 && declaredOnly.length === 0) {
    emptyTable(target, WORKFLOW_SCHEDULE_COLUMNS, "No schedule trigger targets this workflow.");
  }
  for (const schedule of managed) {
    const row = document.createElement("tr");
    appendCell(row, schedule.name, "cell-primary");
    appendNodeCell(row, scheduleStateControl(schedule.name, schedule.state === "active"));
    appendCell(row, formatDate(schedule.nextRunTimes[0] ?? null));
    target.appendChild(row);
  }
  for (const schedule of declaredOnly) {
    const row = document.createElement("tr");
    appendCell(row, schedule.name, "cell-primary");
    appendNodeCell(row, scheduleStateControl(schedule.name, false));
    appendCell(row, "—");
    target.appendChild(row);
  }
  note.hidden = declaredError === null;
  note.textContent =
    declaredError !== null ? `Declared triggers could not be loaded: ${declaredError}` : "";
}

function workflowRuns(logicalWorkflow: string) {
  const versionedPrefix = `${logicalWorkflow}${WORKFLOW_TYPE_SEPARATOR}`;
  return getAllRuns().filter(
    (run) => run.workflowType === logicalWorkflow || run.workflowType.startsWith(versionedPrefix),
  );
}

function latestFailureStep(logicalWorkflow: string): string | null {
  for (const run of workflowRuns(logicalWorkflow)) {
    const failedStep = getRunDetail(runKey(run))?.failedStep;
    if (failedStep !== null && failedStep !== undefined) return failedStep;
  }
  return null;
}
