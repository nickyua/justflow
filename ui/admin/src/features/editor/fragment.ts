import { client } from "../../app/api";
import { getCapabilities } from "../../app/capabilities";
import { button, element, textarea } from "../../app/dom";
import { safeError } from "../../app/format";
import { refreshDashboard } from "../../app/refresh";
import { setView } from "../../app/router";
import {
  type CodeEditorControl,
  textareaControl,
  upgradeTextarea,
} from "../../components/code-editor";
import { type GraphFrameHandle, mountGraphFrame } from "../../components/graph-frame";
import { renderGraphTable } from "../../components/graph-table";
import { type StatusKind, showStatusIn } from "../../components/status";
import { GRAPH_PROTOCOL_VERSION, type GraphViewDocument } from "../../graph-frame/protocol";
import { EditSession } from "./edit-session";
import { editConfiguration, openWorkflowDialog, renderValidationIssuesInto } from "./index";
import { refreshFragmentReference } from "./reference";

const FRAGMENT_STATUS = "fragment-status";
const FRAGMENT_ISSUES = "fragment-validation-issues";
const FRAGMENT_PREVIEW_ISSUES = "fragment-preview-issues";
const PREVIEW_DEBOUNCE_MS = 800;
const DISCARD_FRAGMENT_CONFIRMATION =
  "Discard unsaved workflow changes and reload the saved declaration?";

const NEW_WORKFLOW_TEMPLATE = `workflow: my_workflow
description: |
  Describe what this workflow does.
steps: {}
flow:
- name: done
  terminal: true
`;
const EDIT_SUBTITLE = "Edit one workflow declaration.";
const CREATE_SUBTITLE =
  "Name the workflow via the top-level `workflow:` key, then save to create it.";

let control: CodeEditorControl | null = null;
let currentWorkflow: string | null = null;
let fragmentVersion: number | null = null;
let creating = false;
const edits = new EditSession();
let previewFrame: GraphFrameHandle | null = null;
let previewTimer: ReturnType<typeof setTimeout> | null = null;
let previewAbort: AbortController | null = null;
let previewSequence = 0;

function markDirty(): void {
  edits.edit();
  schedulePreview();
}

function schedulePreview(): void {
  if (previewTimer !== null) clearTimeout(previewTimer);
  previewTimer = setTimeout(() => {
    previewTimer = null;
    void runPreview();
  }, PREVIEW_DEBOUNCE_MS);
}

/** The declared name in a fragment document — a top-level `workflow:` key. */
function declaredFragmentName(fragmentDocument: string): string | null {
  return /^workflow:\s*([A-Za-z_][A-Za-z0-9_]*)\s*$/m.exec(fragmentDocument)?.[1] ?? null;
}

export async function runPreview(): Promise<void> {
  if (control === null) return;
  const previewName = creating ? declaredFragmentName(control.value()) : currentWorkflow;
  if (previewName === null) return;
  previewAbort?.abort();
  const controller = new AbortController();
  previewAbort = controller;
  previewSequence += 1;
  const sequence = previewSequence;
  try {
    const preview = await client.previewWorkflowFragment(
      previewName,
      control.value(),
      controller.signal,
    );
    if (sequence !== previewSequence) return;
    renderValidationIssuesInto(FRAGMENT_PREVIEW_ISSUES, preview.diagnostics);
    if (preview.status === "graph_ready") {
      const graph: GraphViewDocument = {
        version: GRAPH_PROTOCOL_VERSION,
        title: `${previewName} fragment preview`,
        nodes: preview.graphNodes.map((node) => ({
          id: node.nodeId,
          label: node.label,
          kind: node.kind,
          group: node.group,
          failed: false,
          metadata: node.metadata,
        })),
        edges: preview.graphEdges.map((edge) => ({
          source: edge.source,
          target: edge.target,
          label: edge.label,
          dashed: edge.dashed,
        })),
      };
      if (previewFrame === null) {
        previewFrame = mountGraphFrame(element("fragment-preview-graph"));
      }
      previewFrame.render(graph);
      renderGraphTable(element("fragment-preview-table"), graph);
    }
  } catch (error) {
    if (controller.signal.aborted || sequence !== previewSequence) return;
    showFragmentStatus(safeError(error), "is-error");
  }
}

function surface(): CodeEditorControl {
  if (control === null) {
    // The CodeMirror upgrade may still be loading; start on the plain textarea —
    // the upgrade seeds from its value, so nothing set here is lost.
    const host = textarea("fragment-editor");
    host.addEventListener("input", markDirty);
    control = textareaControl(host);
  }
  return control;
}

export async function initFragmentSurface(): Promise<void> {
  control = await upgradeTextarea("fragment-editor", markDirty);
  control.setReadOnly(!getCapabilities()?.configuration_edit);
}

export function isFragmentDirty(): boolean {
  return edits.dirty;
}

/** Focused fragment editing exists where the fragment endpoints do: local-source authoring. */
export function fragmentEditingAvailable(): boolean {
  const capabilities = getCapabilities();
  return capabilities?.configuration_mode === "local_source" && capabilities.configuration_view;
}

/** Route an edit request to the focused editor where available, the full document otherwise. */
export async function openWorkflowEditor(name: string): Promise<void> {
  if (fragmentEditingAvailable()) {
    await openFragmentEditor(name);
    return;
  }
  await editConfiguration("workflow", name);
}

function applyCreateModeControls(): void {
  button("fragment-retire").hidden = creating;
  button("fragment-reload").hidden = creating;
  button("fragment-open-full").hidden = creating;
  element("fragment-subtitle").textContent = creating ? CREATE_SUBTITLE : EDIT_SUBTITLE;
}

/** Full-page creation: the focused editor seeded with a template, saved as a
 * new draft fragment. Falls back to the add-workflow dialog on managed hosts
 * without fragment endpoints. */
export async function openWorkflowCreator(): Promise<void> {
  if (!fragmentEditingAvailable()) {
    openWorkflowDialog();
    return;
  }
  if (edits.dirty && !window.confirm(DISCARD_FRAGMENT_CONFIRMATION)) return;
  const request = edits.beginLoad();
  if (request === null) return;
  creating = true;
  currentWorkflow = null;
  fragmentVersion = null;
  setView("workflow-editor", false);
  element("fragment-workflow-name").textContent = "New workflow";
  applyCreateModeControls();
  surface().setValue(NEW_WORKFLOW_TEMPLATE);
  edits.acceptLoad(request);
  clearFragmentIssues();
  showFragmentStatus("Not saved yet — save to create it.");
  surface().focus();
  void runPreview();
  void refreshFragmentReference();
}

export async function openFragmentEditor(name: string, updateLocation = true): Promise<void> {
  if (edits.dirty && !window.confirm(DISCARD_FRAGMENT_CONFIRMATION)) return;
  const request = edits.beginLoad();
  if (request === null) return;
  creating = false;
  currentWorkflow = name;
  fragmentVersion = null;
  setView("workflow-editor", updateLocation, name);
  element("fragment-workflow-name").textContent = name;
  applyCreateModeControls();
  showFragmentStatus("Loading the workflow fragment…");
  clearFragmentIssues();
  try {
    const fragment = await client.workflowFragment(name);
    if (!edits.acceptLoad(request)) return;
    fragmentVersion = fragment.version;
    surface().setValue(fragment.document);
    showFragmentStatus(`Editing “${name}”. Saving updates only this workflow.`, "is-success");
    surface().focus();
    void runPreview();
    void refreshFragmentReference();
  } catch (error) {
    if (!edits.current(request)) return;
    fragmentVersion = null;
    showFragmentStatus(safeError(error), "is-error");
  }
}

export async function reloadFragment(): Promise<void> {
  if (currentWorkflow !== null) await openFragmentEditor(currentWorkflow, false);
}

async function saveNewFragment(): Promise<void> {
  const fragmentDocument = surface().value();
  const name = declaredFragmentName(fragmentDocument);
  if (name === null) {
    showFragmentStatus(
      "Set the workflow name first: a top-level `workflow: <name>` key.",
      "is-error",
    );
    return;
  }
  const request = edits.beginSave();
  if (request === null) return;
  try {
    const current = await client.draft();
    const declarations = current.bundle.workflows;
    if (
      typeof declarations === "object" &&
      declarations !== null &&
      !Array.isArray(declarations) &&
      name in declarations
    ) {
      showFragmentStatus(`Workflow “${name}” already exists in the draft.`, "is-error");
      return;
    }
    const draft = await client.saveWorkflowFragment(name, fragmentDocument, current.version);
    creating = false;
    currentWorkflow = name;
    fragmentVersion = draft.version;
    edits.acceptSave(request);
    setView("workflow-editor", true, name);
    element("fragment-workflow-name").textContent = name;
    applyCreateModeControls();
    clearFragmentIssues();
    showFragmentStatus(
      edits.dirty
        ? `Created “${name}”. Newer edits are still unsaved.`
        : `Created “${name}”. Restart Justflow to activate it.`,
      "is-success",
    );
    await refreshDashboard();
    void runPreview();
  } catch (error) {
    showFragmentStatus(safeError(error), "is-error");
  } finally {
    edits.finishSave();
  }
}

export async function saveFragment(): Promise<void> {
  if (creating) {
    await saveNewFragment();
    return;
  }
  if (currentWorkflow === null || fragmentVersion === null) {
    showFragmentStatus("Load a workflow fragment before saving.", "is-error");
    return;
  }
  const request = edits.beginSave();
  if (request === null) return;
  try {
    const draft = await client.saveWorkflowFragment(
      currentWorkflow,
      surface().value(),
      fragmentVersion,
    );
    fragmentVersion = draft.version;
    edits.acceptSave(request);
    clearFragmentIssues();
    showFragmentStatus(
      edits.dirty
        ? `Saved “${currentWorkflow}”. Newer edits are still unsaved.`
        : getCapabilities()?.configuration_mode === "local_source"
          ? `Saved “${currentWorkflow}”. Restart Justflow to apply the changes to future starts.`
          : `Saved “${currentWorkflow}”.`,
      "is-success",
    );
  } catch (error) {
    showFragmentStatus(safeError(error), "is-error");
  } finally {
    edits.finishSave();
  }
}

export async function retireFragmentWorkflow(): Promise<void> {
  if (currentWorkflow === null || fragmentVersion === null) {
    showFragmentStatus("Load a workflow fragment before retiring it.", "is-error");
    return;
  }
  const name = currentWorkflow;
  if (
    !window.confirm(
      `Retire “${name}” from future starts? Existing runs and retained definitions are not deleted.`,
    )
  ) {
    return;
  }
  const request = edits.beginSave();
  if (request === null) return;
  try {
    await client.deleteWorkflowFragment(name, fragmentVersion);
    edits.acceptSave(request);
    if (edits.dirty) {
      fragmentVersion = null;
      showFragmentStatus(`Retired “${name}”. Copy your newer unsaved edits before reloading.`);
      return;
    }
    currentWorkflow = null;
    fragmentVersion = null;
    setView("workflows");
    await refreshDashboard();
  } catch (error) {
    showFragmentStatus(safeError(error), "is-error");
  } finally {
    edits.finishSave();
  }
}

export async function validateFragmentDraft(): Promise<void> {
  try {
    const report = await client.validateDraft();
    renderValidationIssuesInto(FRAGMENT_ISSUES, report.issues);
    const errors = report.issues.filter((issue) => issue.severity === "error").length;
    const warnings = report.issues.length - errors;
    const warningSuffix = warnings > 0 ? ` with ${warnings} warning(s)` : "";
    showFragmentStatus(
      report.valid
        ? `Draft is valid${warningSuffix}.`
        : `Draft has ${errors} error(s)${warningSuffix}.`,
      report.valid ? "is-success" : "is-error",
    );
  } catch (error) {
    showFragmentStatus(safeError(error), "is-error");
  }
}

export async function openFullDocumentFromFragment(): Promise<void> {
  if (currentWorkflow !== null) await editConfiguration("workflow", currentWorkflow);
}

export function applyFragmentCapabilities(): void {
  const capabilities = getCapabilities();
  const editable = capabilities?.configuration_edit === true;
  button("fragment-save").disabled = !editable;
  button("fragment-retire").disabled = !editable;
  button("fragment-validate").disabled = capabilities?.configuration_validate !== true;
  control?.setReadOnly(!editable);
}

function clearFragmentIssues(): void {
  renderValidationIssuesInto(FRAGMENT_ISSUES, []);
}

function showFragmentStatus(message: string, kind: StatusKind = ""): void {
  showStatusIn(FRAGMENT_STATUS, message, kind);
}
