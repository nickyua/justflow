import type {
  Capabilities,
  ConfigurationRelationshipKind,
  ConfigurationRelationships,
  DraftRecord,
  JsonObject,
  ValidationIssue,
} from "../../api/contracts";
import { client, requestHeaders } from "../../app/api";
import { getCapabilities } from "../../app/capabilities";
import {
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
import { safeError, short } from "../../app/format";
import { refreshDashboard } from "../../app/refresh";
import { setView } from "../../app/router";
import {
  type CodeEditorControl,
  textareaControl,
  upgradeTextarea,
} from "../../components/code-editor";
import { showStatus, statusBadge } from "../../components/status";
import { openReleasesForReview, recordPublication } from "../releases";
import { EditSession } from "./edit-session";

const DISCARD_CONFIRMATION = "Discard unsaved changes and reload the saved configuration?";
const DISCARD_SAVED_CONFIRMATION =
  "Discard all saved changes that are pending Apply and restore the active configuration?";

let draft: DraftRecord | null = null;
let documentDraft: DraftRecord | null = null;
let configurationRelationships: ConfigurationRelationships | null = null;
const edits = new EditSession();
let desiredReadOnly = false;
let control: CodeEditorControl | null = null;
let registeredWorkflowNames: () => string[] = () => [];
let workflowCreatedNavigator: (name: string) => Promise<void> = (name) =>
  editConfiguration("workflow", name);

export function setRegisteredWorkflowNamesProvider(provider: () => string[]): void {
  registeredWorkflowNames = provider;
}

export function setWorkflowCreatedNavigator(navigator: (name: string) => Promise<void>): void {
  workflowCreatedNavigator = navigator;
}

function markDirty(): void {
  edits.edit();
}

const DRAFT_TABS = ["workflows", "triggers", "document"] as const;
export type DraftTab = (typeof DRAFT_TABS)[number];

export function isDraftTab(value: string): value is DraftTab {
  return DRAFT_TABS.some((tab) => tab === value);
}

export function setDraftTab(tab: DraftTab): void {
  for (const panel of document.querySelectorAll<HTMLElement>("[data-draft-panel]")) {
    panel.hidden = panel.dataset.draftPanel !== tab;
  }
  for (const control of document.querySelectorAll<HTMLButtonElement>("[data-draft-tab]")) {
    const active = control.dataset.draftTab === tab;
    control.classList.toggle("is-active", active);
    control.setAttribute("aria-pressed", String(active));
  }
}

function editorSurface(): CodeEditorControl {
  if (control === null) {
    const host = textarea("configuration-editor");
    host.addEventListener("input", markDirty);
    control = textareaControl(host);
  }
  return control;
}

export async function initEditorSurface(): Promise<void> {
  editorSurface();
  control = await upgradeTextarea("configuration-editor", markDirty);
  control.setReadOnly(desiredReadOnly);
}

let schedulesControl: CodeEditorControl | null = null;
let schedulesVersion: number | null = null;
const scheduleEdits = new EditSession();

function markSchedulesDirty(): void {
  scheduleEdits.edit();
}

function schedulesSurface(): CodeEditorControl {
  if (schedulesControl === null) {
    const host = textarea("schedules-editor");
    host.addEventListener("input", markSchedulesDirty);
    schedulesControl = textareaControl(host);
  }
  return schedulesControl;
}

export async function initSchedulesSurface(): Promise<void> {
  schedulesSurface();
  schedulesControl = await upgradeTextarea("schedules-editor", markSchedulesDirty);
  schedulesControl.setReadOnly(desiredReadOnly);
}

/** Load the schedules-only YAML unless unsaved edits would be lost. */
export async function loadSchedulesFragment(force = false): Promise<void> {
  if (scheduleEdits.dirty && !force) return;
  const request = scheduleEdits.beginLoad();
  if (request === null) return;
  try {
    const fragment = await client.triggersFragment();
    if (!scheduleEdits.acceptLoad(request)) return;
    schedulesVersion = fragment.version;
    schedulesSurface().setValue(fragment.document);
  } catch (error) {
    if (!scheduleEdits.current(request)) return;
    showStatus(safeError(error), "is-error");
  }
}

export async function saveSchedulesFragment(): Promise<void> {
  if (schedulesVersion === null) {
    showStatus("Reload the triggers before saving.", "is-error");
    return;
  }
  const request = scheduleEdits.beginSave();
  if (request === null) return;
  try {
    draft = await client.saveTriggersFragment(schedulesSurface().value(), schedulesVersion);
    schedulesVersion = draft.version;
    scheduleEdits.acceptSave(request);
    configurationRelationships = await relationshipsForVersion(draft.version);
    updateLifecycleControls();
    updateRestartChip();
    renderEditorOverview();
    showStatus(
      scheduleEdits.dirty ? "Saved triggers. Newer edits are still unsaved." : "Saved triggers.",
      "is-success",
    );
    await refreshDashboard();
  } catch (error) {
    showStatus(safeError(error), "is-error");
  } finally {
    scheduleEdits.finishSave();
  }
}

export function isEditorDirty(): boolean {
  return edits.dirty || scheduleEdits.dirty;
}

const EDITOR_WORKFLOW_COLUMNS = 6;
const EDITOR_SCHEDULE_COLUMNS = 6;
const OVERVIEW_DESCRIPTION_MAX_LENGTH = 96;

function truncateDescription(value: string): string {
  return value.length > OVERVIEW_DESCRIPTION_MAX_LENGTH
    ? `${value.slice(0, OVERVIEW_DESCRIPTION_MAX_LENGTH)}…`
    : value;
}

function declaredStepCount(workflow: unknown): string {
  if (!isObject(workflow)) return "—";
  if (Array.isArray(workflow.flow)) return String(workflow.flow.length);
  return isObject(workflow.steps) ? String(Object.keys(workflow.steps).length) : "—";
}

function describeScheduleSpec(spec: unknown): string {
  if (!isObject(spec)) return "—";
  if (spec.kind === "cron" && Array.isArray(spec.expressions)) {
    return spec.expressions.filter((item) => typeof item === "string").join(", ");
  }
  if (spec.kind === "interval" && typeof spec.every_seconds === "number") {
    return `every ${spec.every_seconds}s`;
  }
  if (spec.kind === "calendar") return "calendar";
  return "—";
}

export function renderEditorOverview(): void {
  const workflows = element("editor-workflow-list");
  clear(workflows);
  const bundleWorkflows = draft?.bundle.workflows;
  const bundleSchedules = draft === null ? null : scheduleDeclarations(draft.bundle);
  const scheduleEntries = isObject(bundleSchedules) ? Object.entries(bundleSchedules) : [];
  const names = Array.from(
    new Set([
      ...(isObject(bundleWorkflows) ? Object.keys(bundleWorkflows) : []),
      ...removedRelationshipNames("workflow"),
    ]),
  ).sort();
  if (names.length === 0) {
    emptyTable(
      workflows,
      EDITOR_WORKFLOW_COLUMNS,
      "No workflows are declared yet. Create the first one.",
    );
  }
  for (const name of names) {
    const declaration = isObject(bundleWorkflows) ? bundleWorkflows[name] : null;
    const targetingSchedules = scheduleEntries.filter(
      (entry) => isObject(entry[1]) && entry[1].workflow === name,
    ).length;
    const row = document.createElement("tr");
    row.className = "interactive-row";
    appendCell(row, name, "cell-primary");
    appendNodeCell(row, relationshipBadge("workflow", name));
    appendCell(row, declaredStepCount(declaration));
    appendCell(row, targetingSchedules > 0 ? String(targetingSchedules) : "—");
    appendCell(
      row,
      isObject(declaration) && typeof declaration.description === "string"
        ? truncateDescription(declaration.description.trim())
        : "—",
    );
    if (isObject(declaration)) {
      const open = document.createElement("button");
      open.className = "text-button";
      open.type = "button";
      open.textContent = "Open";
      open.addEventListener("click", () => void workflowCreatedNavigator(name));
      appendNodeCell(row, open);
      row.addEventListener("click", (event) => {
        if (event.target !== open) void workflowCreatedNavigator(name);
      });
    } else {
      appendNodeCell(row, textNode("span", "—"));
    }
    workflows.appendChild(row);
  }

  const schedules = element("editor-schedule-list");
  clear(schedules);
  const scheduleNames = Array.from(
    new Set([...scheduleEntries.map((entry) => entry[0]), ...removedRelationshipNames("trigger")]),
  ).sort();
  if (scheduleNames.length === 0) {
    emptyTable(schedules, EDITOR_SCHEDULE_COLUMNS, "No triggers are declared.");
  }
  for (const name of scheduleNames) {
    const declaration = isObject(bundleSchedules) ? bundleSchedules[name] : null;
    const row = document.createElement("tr");
    appendCell(row, name, "cell-primary");
    appendNodeCell(row, relationshipBadge("trigger", name));
    appendCell(
      row,
      isObject(declaration) && typeof declaration.workflow === "string"
        ? declaration.workflow
        : "—",
    );
    appendCell(
      row,
      isObject(declaration) && isObject(declaration.component)
        ? `${String(declaration.component.name ?? "—")}@${String(declaration.component.version ?? "—")}`
        : isObject(declaration) && declaration.kind === "schedule"
          ? `schedule · ${describeScheduleSpec(declaration.spec)}`
          : isObject(declaration) && typeof declaration.kind === "string"
            ? declaration.kind
            : "—",
      "mono",
    );
    if (isObject(declaration)) {
      const paused = declaration.paused === true;
      appendNodeCell(row, scheduleStateControl(name, !paused));
      const edit = document.createElement("button");
      edit.className = "text-button";
      edit.type = "button";
      edit.textContent = "Edit";
      edit.addEventListener("click", () => void editConfiguration("schedule", name));
      appendNodeCell(row, edit);
    } else {
      appendNodeCell(row, textNode("span", "—"));
      appendNodeCell(row, textNode("span", "—"));
    }
    schedules.appendChild(row);
  }
}

function removedRelationshipNames(kind: ConfigurationRelationshipKind): string[] {
  return (
    configurationRelationships?.relationships
      .filter(
        (relationship) =>
          relationship.kind === kind && relationship.state === "removed_pending_apply",
      )
      .map((relationship) => relationship.name) ?? []
  );
}

function relationshipBadge(kind: ConfigurationRelationshipKind, name: string): Node {
  const state = configurationRelationships?.relationships.find(
    (relationship) => relationship.kind === kind && relationship.name === name,
  )?.state;
  if (state === undefined) return statusBadge("Unavailable", "unavailable");
  const labels = {
    active: "Active",
    modified: "Modified",
    new_pending_apply: "New · pending Apply",
    removed_pending_apply: "Removed · pending Apply",
  } as const;
  return statusBadge(labels[state], state === "active" ? "active" : "pending");
}

/** The badge-shaped Active/Inactive control; a plain badge without edit rights. */
export function scheduleStateControl(name: string, active: boolean): Node {
  if (getCapabilities()?.configuration_edit !== true) {
    return statusBadge(active ? "Active" : "Inactive", active ? "active" : "none");
  }
  const select = document.createElement("select");
  select.setAttribute("aria-label", `State of ${name}`);
  for (const [value, label] of [
    ["active", "Active"],
    ["inactive", "Inactive"],
  ] as const) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    select.appendChild(option);
  }
  select.value = active ? "active" : "inactive";
  const syncAppearance = () => {
    select.className = `state-select ${select.value === "active" ? "is-active" : "is-inactive"}`;
  };
  syncAppearance();
  select.addEventListener("change", () => {
    syncAppearance();
    void setScheduleActive(name, select.value === "active");
  });
  return select;
}

function updateRestartChip(): void {
  const chip = element("restart-required");
  chip.hidden = !(
    getCapabilities()?.configuration_mode === "local_source" && draft?.restartRequired === true
  );
}

function clearValidationIssues(): void {
  const container = element("validation-issues");
  clear(container);
  container.hidden = true;
}

function renderValidationIssues(issues: readonly ValidationIssue[]): void {
  renderValidationIssuesInto("validation-issues", issues);
}

export function renderValidationIssuesInto(
  containerId: string,
  issues: readonly ValidationIssue[],
): void {
  const container = element(containerId);
  clear(container);
  container.hidden = issues.length === 0;
  for (const issue of issues) {
    const row = document.createElement("div");
    row.className = `validation-issue is-${issue.severity}`;
    row.append(
      textNode("span", issue.severity, `issue-severity is-${issue.severity}`),
      textNode("span", issue.category, "issue-category"),
      textNode("code", issue.location.join(".")),
      textNode("p", issue.message),
    );
    container.appendChild(row);
  }
}

export async function editConfiguration(
  kind: "workflow" | "retire" | "schedule" | "triggers",
  name?: string,
): Promise<void> {
  setView("editor");
  if (!getCapabilities()?.configuration_view) {
    showStatus("Configuration editing is unavailable on this runtime host.", "is-error");
    return;
  }
  await loadDraft();
  if (kind === "schedule" || kind === "triggers") {
    setDraftTab("triggers");
    await loadSchedulesFragment();
    schedulesSurface().focus();
    return;
  }
  setDraftTab("document");
  const subject = name === undefined ? "schedule declarations" : `“${name}”`;
  const action =
    kind === "retire"
      ? `Remove ${subject}. Retained definitions and execution history will not be deleted.`
      : `Edit ${subject}.`;
  showStatus(authoringSaveMessage(action));
  editorSurface().focus();
}

export async function retireWorkflow(logicalWorkflow: string): Promise<void> {
  if (!getCapabilities()?.configuration_edit) {
    setView("editor");
    showStatus("Configuration editing is unavailable on this runtime host.", "is-error");
    return;
  }
  if (
    !window.confirm(
      `Retire “${logicalWorkflow}” from future starts? Existing runs and retained definitions are not deleted.`,
    )
  ) {
    return;
  }
  try {
    const current = await client.draft();
    const bundle = cloneObject(current.bundle);
    const workflows = objectChild(bundle, "workflows");
    if (!(logicalWorkflow in workflows)) {
      throw new Error("The workflow is not present in the current draft.");
    }
    const schedules = scheduleDeclarations(bundle);
    const dependentSchedules = Object.entries(schedules)
      .filter(([, declaration]) => objectChildValue(declaration, "workflow") === logicalWorkflow)
      .map(([name]) => name);
    if (dependentSchedules.length > 0) {
      throw new Error(
        `Remove dependent schedule declarations first: ${dependentSchedules.join(", ")}.`,
      );
    }
    delete workflows[logicalWorkflow];
    draft = await client.saveJsonDraft(bundle, current);
    setView("editor");
    await loadDraft();
    showStatus(authoringSaveMessage(`Retired ${logicalWorkflow} from the draft.`), "is-success");
  } catch (error) {
    window.alert(safeError(error));
  }
}

export function openWorkflowDialog(): void {
  if (!getCapabilities()?.configuration_edit) {
    setView("editor");
    showStatus("Configuration editing is unavailable on this runtime host.", "is-error");
    return;
  }
  input("workflow-name").value = "";
  textarea("workflow-description").value = "";
  dialog("workflow-dialog").showModal();
  input("workflow-name").focus();
}

export async function addWorkflow(): Promise<void> {
  const name = input("workflow-name").value.trim();
  const description = textarea("workflow-description").value.trim();
  try {
    const current = await client.draft();
    const bundle = cloneObject(current.bundle);
    const workflows = objectChild(bundle, "workflows");
    if (name in workflows) throw new Error(`Workflow “${name}” already exists.`);
    workflows[name] = {
      workflow: name,
      description,
      steps: {},
      flow: [{ name: "done", terminal: true }],
    };
    draft = await client.saveJsonDraft(bundle, current);
    dialog("workflow-dialog").close();
    await workflowCreatedNavigator(name);
  } catch (error) {
    window.alert(safeError(error));
  }
}

/** Edit the desired schedule state; the relationship projection remains the
 * authority for whether that declaration is active or pending Apply. */
export async function setScheduleActive(name: string, active: boolean): Promise<void> {
  try {
    const current = await client.draft();
    const bundle = cloneObject(current.bundle);
    const declaration = scheduleDeclarations(bundle)[name];
    if (!isObject(declaration)) {
      throw new Error(`Schedule “${name}” is missing from the configuration.`);
    }
    declaration.paused = !active;
    draft = await client.saveJsonDraft(bundle, current);
    configurationRelationships = await relationshipsForVersion(draft.version);
    updateLifecycleControls();
    updateRestartChip();
    renderEditorOverview();
    showStatus(
      authoringSaveMessage(
        `${active ? "Enabled" : "Paused"} “${name}” in the working declaration.`,
      ),
      "is-success",
    );
    await refreshDashboard();
  } catch (error) {
    window.alert(safeError(error));
  }
}

export function openScheduleDialog(workflowName?: string): void {
  if (!getCapabilities()?.configuration_edit) {
    setView("editor");
    showStatus("Configuration editing is unavailable on this runtime host.", "is-error");
    return;
  }
  if (getCapabilities()?.configuration_mode !== "local_source") {
    setView("editor");
    setDraftTab("triggers");
    showStatus(
      "Managed triggers must select an approved immutable component in the Triggers YAML.",
      "is-error",
    );
    return;
  }
  const workflowSelect = selectElement("schedule-workflow");
  clear(workflowSelect);
  for (const name of authoredWorkflowNames()) {
    const option = document.createElement("option");
    option.value = name;
    option.textContent = name;
    workflowSelect.appendChild(option);
  }
  if (workflowName !== undefined) workflowSelect.value = workflowName;
  input("schedule-name").value = "";
  dialog("schedule-dialog").showModal();
  input("schedule-name").focus();
}

export async function addSchedule(): Promise<void> {
  const name = input("schedule-name").value.trim();
  const workflow = selectElement("schedule-workflow").value;
  const kind = selectElement("schedule-kind").value;
  try {
    const parsedInput: unknown = JSON.parse(textarea("schedule-input").value);
    if (!isObject(parsedInput)) throw new TypeError("Workflow input must be a JSON object.");
    const spec =
      kind === "cron"
        ? { kind: "cron", expressions: [input("schedule-cron").value.trim()] }
        : { kind: "interval", every_seconds: Number(input("schedule-interval").value) };
    const current = await client.draft();
    const bundle = cloneObject(current.bundle);
    const schedules = scheduleDeclarations(bundle);
    if (name in schedules) throw new Error(`Schedule “${name}” already exists.`);
    schedules[name] = {
      kind: "schedule",
      workflow,
      input: parsedInput,
      spec,
      timezone: kind === "interval" ? "UTC" : input("schedule-timezone").value.trim(),
      overlap_policy: "skip",
      paused: false,
    };
    draft = await client.saveJsonDraft(bundle, current);
    dialog("schedule-dialog").close();
    setView("editor");
    await loadDraft();
    showStatus(authoringSaveMessage(`Added schedule ${name} to the draft.`), "is-success");
  } catch (error) {
    window.alert(safeError(error));
  }
}

export async function loadSchema(): Promise<void> {
  const capabilities = getCapabilities();
  if (!capabilities?.configuration_view || capabilities.configuration_mode !== "managed") {
    return;
  }
  const schema = await client.configurationSchema();
  const title = typeof schema.title === "string" ? schema.title : "tenant configuration schema";
  element("editor-help").textContent =
    `Validated against ${title}. Saving a draft does not publish or activate it.`;
}

export async function loadDraft(): Promise<void> {
  if (edits.dirty && !window.confirm(DISCARD_CONFIRMATION)) return;
  const request = edits.beginLoad();
  if (request === null) return;
  try {
    const record = await client.draft();
    const document = await client.draftYaml(record.version);
    const relationships = await relationshipsForVersion(record.version);
    if (!edits.acceptLoad(request)) return;
    draft = record;
    documentDraft = record;
    configurationRelationships = relationships;
    updateLifecycleControls();
    editorSurface().setValue(document);
    clearValidationIssues();
    updateRestartChip();
    renderEditorOverview();
    showStatus(
      getCapabilities()?.configuration_mode === "local_source"
        ? "Loaded the saved configuration. Changes take effect after restart."
        : `Loaded configuration version ${draft.version}.`,
      "is-success",
    );
  } catch (error) {
    if (!edits.current(request)) return;
    showStatus(safeError(error), "is-error");
  }
}

export async function saveDraft(): Promise<void> {
  const request = edits.beginSave();
  if (request === null) return;
  try {
    documentDraft = await client.saveYamlDraft(editorSurface().value(), documentDraft);
    draft = documentDraft;
    edits.acceptSave(request);
    configurationRelationships = await relationshipsForVersion(draft.version);
    updateLifecycleControls();
    clearValidationIssues();
    updateRestartChip();
    renderEditorOverview();
    showStatus(
      edits.dirty
        ? "Saved the submitted document. Newer edits are still unsaved."
        : getCapabilities()?.configuration_mode === "local_source"
          ? "Saved. Restart Justflow to activate the changes."
          : `Saved configuration version ${draft.version}.`,
      "is-success",
    );
  } catch (error) {
    showStatus(safeError(error), "is-error");
  } finally {
    edits.finishSave();
  }
}

export async function validateDraft(): Promise<void> {
  try {
    const report = await client.validateDraft();
    renderValidationIssues(report.issues);
    const errors = report.issues.filter((issue) => issue.severity === "error").length;
    const warnings = report.issues.length - errors;
    const warningSuffix = warnings > 0 ? ` with ${warnings} warning(s)` : "";
    showStatus(
      report.valid
        ? `Draft is valid${warningSuffix}.`
        : `Draft has ${errors} error(s)${warningSuffix}.`,
      report.valid ? "is-success" : "is-error",
    );
  } catch (error) {
    showStatus(safeError(error), "is-error");
  }
}

export async function applyDraft(): Promise<void> {
  if (draft === null) {
    showStatus("Load or save a draft before Apply.", "is-error");
    return;
  }
  if (edits.dirty || scheduleEdits.dirty) {
    showStatus("Save the edited document before Apply.", "is-error");
    return;
  }
  try {
    if (getCapabilities()?.configuration_mode === "local_source") {
      showStatus("Apply stage: validating and publishing immutable definitions…");
      const result = await client.applyLocalConfiguration(draft.version, requestHeaders());
      configurationRelationships = await relationshipsForVersion(draft.version);
      updateLifecycleControls();
      renderEditorOverview();
      updateRestartChip();
      showStatus(
        result.restartRequired
          ? "Validation and immutable definition publication completed. Restart is required; the running process did not change."
          : "Validation and immutable definition publication completed. The saved configuration already matches this process startup snapshot.",
        "is-success",
      );
      return;
    }
    showStatus("Apply stage 1/4: publishing an immutable configuration revision…");
    const publication = await client.publishDraft(draft.version, requestHeaders());
    if (publication.publishedRevisionId === null) {
      throw new Error("Publication completed without an immutable revision identity.");
    }
    recordPublication(publication.publishedRevisionId);
    showStatus("Apply stage 2/4: building the activation plan from authoritative state…");
    const plan = await client.planActivation(publication.publishedRevisionId);
    if (!getCapabilities()?.configuration_activate) {
      showStatus(
        `Published immutable revision ${short(publication.publishedRevisionId)}. Activation is not authorized, so the active revision did not change.`,
        "is-success",
      );
      await refreshDashboard();
      openReleasesForReview(publication.publishedRevisionId);
      return;
    }
    const predecessor =
      plan.expectedActiveRevisionId === null
        ? "no active predecessor"
        : `active revision ${short(plan.expectedActiveRevisionId)}`;
    if (
      !window.confirm(
        `Publication completed. Confirm activation of ${short(publication.publishedRevisionId)} against ${predecessor}?`,
      )
    ) {
      showStatus(
        `Published immutable revision ${short(publication.publishedRevisionId)}. Activation was not requested, so the active revision did not change.`,
        "is-success",
      );
      await refreshDashboard();
      openReleasesForReview(publication.publishedRevisionId);
      return;
    }
    showStatus("Apply stage 3/4: executing the confirmed activation plan…");
    const activation = await client.activate(
      publication.publishedRevisionId,
      plan.planDigest,
      requestHeaders(),
    );
    showStatus("Apply stage 4/4: reading authoritative activation readiness…");
    const readiness = await client.activationReadiness(activation.activationId);
    configurationRelationships = await relationshipsForVersion(draft.version);
    updateLifecycleControls();
    renderEditorOverview();
    await refreshDashboard();
    if (readiness.ready) {
      showStatus(
        `Publication, plan confirmation, activation, and readiness completed for ${short(readiness.targetRevisionId)}. Audit records are available in Releases.`,
        "is-success",
      );
    } else {
      showStatus(
        `Publication and activation request completed; authoritative readiness is ${readiness.state}. The console is not claiming the revision is ready.`,
        readiness.state === "failed" || readiness.state === "superseded" ? "is-error" : "",
      );
      openReleasesForReview(publication.publishedRevisionId);
    }
  } catch (error) {
    showStatus(safeError(error), "is-error");
  }
}

export async function discardDraft(): Promise<void> {
  const activeIdentity = configurationRelationships?.activeIdentity;
  if (draft === null || activeIdentity == null) {
    showStatus("Discard requires an authoritative active configuration.", "is-error");
    return;
  }
  if (!window.confirm(DISCARD_SAVED_CONFIRMATION)) return;
  try {
    const mode = getCapabilities()?.configuration_mode;
    const result = await client.discardConfiguration(
      draft.version,
      activeIdentity,
      requestHeaders(),
    );
    await loadDraft();
    showStatus(
      mode === "local_source"
        ? `Discarded saved changes and restored the process startup snapshot at version ${result.workingVersion}. The running process did not change.`
        : `Discarded saved changes and restored the draft from active revision ${short(result.activeIdentity)}. Publication and activation state did not change.`,
      "is-success",
    );
  } catch (error) {
    showStatus(safeError(error), "is-error");
  }
}

export function applyEditorCapabilities(capabilities: Capabilities): void {
  const actions: [string, keyof Capabilities][] = [
    ["load-draft", "configuration_view"],
    ["save-draft", "configuration_edit"],
    ["validate-draft", "configuration_validate"],
    ["discard-draft", "configuration_discard"],
  ];
  for (const [id, capability] of actions) button(id).disabled = !capabilities[capability];
  desiredReadOnly = !capabilities.configuration_edit;
  editorSurface().setReadOnly(desiredReadOnly);
  element("configuration-availability").hidden = capabilities.configuration_view;
  const localSource = capabilities.configuration_mode === "local_source";
  element("release-flow").hidden = localSource || !capabilities.configuration_view;
  element("configuration-subtitle").textContent = localSource
    ? "Edit workflow and trigger YAML persisted for the next local restart."
    : "Draft, validate, and publish declaration changes.";
  element("editor-help").textContent = localSource
    ? "Declarative workflows and triggers only. Saving never changes the running process or existing executions."
    : "Apply uses separate publication, plan confirmation, activation, readiness, and audit operations.";
  updateLifecycleControls();
  button("new-workflow-draft").disabled = !capabilities.configuration_edit;
  button("new-schedule").disabled = !capabilities.configuration_edit || !localSource;
  button("editor-add-schedule").disabled = !capabilities.configuration_edit || !localSource;
  button("workflow-detail-schedule").disabled = !capabilities.configuration_edit || !localSource;
  button("workflow-inline-schedule").disabled = !capabilities.configuration_edit || !localSource;
}

function updateLifecycleControls(): void {
  const capabilities = getCapabilities();
  if (capabilities === null) return;
  const localSource = capabilities.configuration_mode === "local_source";
  const apply = button("apply-draft");
  apply.textContent = localSource ? "Apply saved changes" : "Apply changes";
  apply.disabled = localSource
    ? !capabilities.configuration_apply
    : !capabilities.configuration_publish;
  button("discard-draft").disabled =
    !capabilities.configuration_discard || configurationRelationships?.activeIdentity == null;
}

async function relationshipsForVersion(version: number): Promise<ConfigurationRelationships> {
  const relationships = await client.configurationRelationships();
  if (relationships.workingVersion !== version) {
    throw new Error("Configuration changed concurrently. Reload before continuing.");
  }
  return relationships;
}

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function cloneObject(value: JsonObject): JsonObject {
  const cloned: unknown = structuredClone(value);
  if (!isObject(cloned)) throw new TypeError("Configuration draft must be an object.");
  return cloned;
}

function objectChild(parent: JsonObject, field: string): JsonObject {
  const value = parent[field];
  if (!isObject(value)) throw new TypeError(`Configuration field ${field} must be an object.`);
  return value;
}

function objectChildValue(value: unknown, field: string): string | null {
  if (!isObject(value)) return null;
  const child = value[field];
  return typeof child === "string" ? child : null;
}

function authoredWorkflowNames(): string[] {
  const names = new Set(registeredWorkflowNames());
  const authoredWorkflows = draft?.bundle.workflows;
  if (isObject(authoredWorkflows)) {
    for (const name of Object.keys(authoredWorkflows)) names.add(name);
  }
  return [...names].sort((left, right) => left.localeCompare(right));
}

function scheduleDeclarations(bundle: JsonObject): JsonObject {
  const triggers = objectChild(bundle, "triggers");
  if (getCapabilities()?.configuration_mode !== "local_source") return triggers;
  const declarations = triggers.triggers;
  if (declarations === undefined) {
    const created: JsonObject = {};
    triggers.triggers = created;
    return created;
  }
  if (!isObject(declarations)) {
    throw new TypeError("Configuration trigger declarations must be an object.");
  }
  return declarations;
}

function authoringSaveMessage(message: string): string {
  return getCapabilities()?.configuration_mode === "local_source"
    ? `${message} Restart Justflow to apply it to future starts.`
    : `${message} Apply when ready; publication, plan confirmation, activation, and readiness remain separate operations.`;
}
