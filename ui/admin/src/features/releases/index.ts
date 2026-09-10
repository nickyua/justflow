import type {
  ActivationState,
  ActivationSummary,
  Capabilities,
  ConfigurationDifferenceItem,
  RevisionSummary,
} from "../../api/contracts";
import { client, requestHeaders } from "../../app/api";
import { getCapabilities } from "../../app/capabilities";
import {
  actionButton,
  appendCell,
  appendNodeCell,
  button,
  clear,
  element,
  emptyTable,
  selectElement,
  textNode,
} from "../../app/dom";
import { formatDate, safeError, short } from "../../app/format";
import { refreshDashboard } from "../../app/refresh";
import { setView } from "../../app/router";
import { PagedCollection, STALE_LIST_NOTICE } from "../../components/pagination";
import { showStatusIn, statusBadge } from "../../components/status";

const REVISION_LIST_COLUMNS = 3;
const ACTIVATION_LIST_COLUMNS = 4;
const RELEASE_STATUS_ELEMENT = "release-status";

let activeRevisionId: string | null = null;
let publishedRevision: string | null = null;
let available = false;

const revisionPage = new PagedCollection<RevisionSummary>(async (cursor) => {
  const view = await client.configuration(cursor);
  activeRevisionId = view.activeRevisionId;
  return { items: view.revisions, nextCursor: view.revisionsNextCursor };
});
const activationPage = new PagedCollection<ActivationSummary>(async (cursor) => {
  const page = await client.activations(cursor);
  return { items: page.activations, nextCursor: page.nextCursor };
});

export function releasesAvailable(): boolean {
  return available;
}

export function recordPublication(revisionId: string): void {
  publishedRevision = revisionId;
}

/** After a publish, route to Releases with published-vs-active preselected for review. */
export function openReleasesForReview(publishedRevisionId: string): void {
  setView("releases");
  if (activeRevisionId !== null) selectElement("diff-source").value = activeRevisionId;
  selectElement("diff-target").value = publishedRevisionId;
  selectElement("activation-target").value = publishedRevisionId;
  showReleaseStatus("Review the comparison against the active revision before activating.", "");
}

export async function reloadReleases(): Promise<void> {
  await Promise.all([revisionPage.reload(), activationPage.reload()]);
  renderReleases();
}

export async function loadMoreRevisions(): Promise<void> {
  await loadMoreHistory(revisionPage, "load-more-revisions");
}

export async function loadMoreActivations(): Promise<void> {
  await loadMoreHistory(activationPage, "load-more-activations");
}

async function loadMoreHistory(
  collection: PagedCollection<unknown>,
  buttonId: string,
): Promise<void> {
  const control = button(buttonId);
  control.disabled = true;
  try {
    const outcome = await collection.loadMore();
    if (outcome === "reset") window.alert(STALE_LIST_NOTICE);
    renderReleases();
  } catch (error) {
    window.alert(safeError(error));
  } finally {
    control.disabled = false;
  }
}

function showReleaseStatus(message: string, kind: "" | "is-error" | "is-success"): void {
  showStatusIn(RELEASE_STATUS_ELEMENT, message, kind);
}

function renderReleases(): void {
  element("active-revision").textContent =
    activeRevisionId === null ? "No active revision" : `Active ${short(activeRevisionId)}`;
  renderRevisionOptions(revisionPage.items);
  renderRevisionHistory(revisionPage.items);
  renderActivationHistory(activationPage.items);
}

function renderRevisionHistory(revisions: readonly RevisionSummary[]): void {
  const target = element("revision-list");
  button("load-more-revisions").hidden = !revisionPage.hasMore;
  clear(target);
  if (revisions.length === 0) {
    emptyTable(target, REVISION_LIST_COLUMNS, "No published revisions are available.");
    return;
  }
  for (const revision of revisions) {
    const row = document.createElement("tr");
    row.className = "interactive-row";
    row.addEventListener("click", () => void openRevision(revision.revisionId));
    appendCell(row, short(revision.revisionId), "mono");
    appendCell(row, formatDate(revision.createdAt));
    appendCell(
      row,
      revision.parentRevisionId === null ? "Initial" : short(revision.parentRevisionId),
      "mono",
    );
    target.appendChild(row);
  }
}

function renderActivationHistory(activations: readonly ActivationSummary[]): void {
  const target = element("activation-list");
  button("load-more-activations").hidden = !activationPage.hasMore;
  clear(target);
  if (activations.length === 0) {
    emptyTable(target, ACTIVATION_LIST_COLUMNS, "No activation history is available.");
    return;
  }
  for (const activation of activations) {
    const row = document.createElement("tr");
    row.className = "interactive-row";
    row.addEventListener("click", () => void openActivation(activation));
    appendCell(row, short(activation.targetRevisionId), "mono");
    appendNodeCell(row, statusBadge(activation.state, activationStatusClass(activation.state)));
    appendCell(row, formatDate(activation.updatedAt));
    const actions = document.createElement("div");
    actions.className = "row-actions";
    if (getCapabilities()?.configuration_rollback && activation.state === "applied") {
      const rollback = actionButton("Rollback", "row-action");
      rollback.addEventListener("click", () => void rollbackActivation(activation.activationId));
      actions.appendChild(rollback);
    }
    appendNodeCell(row, actions);
    target.appendChild(row);
  }
}

function renderRevisionOptions(revisions: readonly RevisionSummary[]): void {
  for (const target of [
    selectElement("activation-target"),
    selectElement("diff-source"),
    selectElement("diff-target"),
  ]) {
    clear(target);
    for (const revision of revisions) {
      const option = document.createElement("option");
      option.value = revision.revisionId;
      const activeSuffix = revision.revisionId === activeRevisionId ? " · active" : "";
      option.textContent = `${short(revision.revisionId)} — ${formatDate(revision.createdAt)}${activeSuffix}`;
      target.appendChild(option);
    }
  }
  if (publishedRevision !== null) {
    selectElement("activation-target").value = publishedRevision;
  }
}

export async function activateRevision(): Promise<void> {
  const targetRevision = selectElement("activation-target").value;
  if (targetRevision.length === 0) {
    showReleaseStatus("Select or publish a revision before activation.", "is-error");
    return;
  }
  try {
    const plan = await client.planActivation(targetRevision);
    const activation = await client.activate(targetRevision, plan.planDigest, requestHeaders());
    showReleaseStatus(`Activation is ${activation.state}.`, activationStatusKind(activation.state));
    await refreshDashboard();
  } catch (error) {
    showReleaseStatus(safeError(error), "is-error");
  }
}

async function rollbackActivation(activationId: string): Promise<void> {
  try {
    const original = await client.activation(activationId);
    const predecessor = original.plan.expectedActiveRevisionId;
    if (predecessor === null) {
      throw new Error("This activation has no retained predecessor revision.");
    }
    const plan = await client.planActivation(predecessor);
    const rollback = await client.rollback(activationId, plan.planDigest, requestHeaders());
    showReleaseStatus(
      `Rollback activation is ${rollback.state}.`,
      activationStatusKind(rollback.state),
    );
    await refreshDashboard();
  } catch (error) {
    showReleaseStatus(safeError(error), "is-error");
  }
}

const MAX_DIFF_ITEMS_DISPLAYED = 1_000;

async function openRevision(revisionId: string): Promise<void> {
  try {
    const revision = await client.revision(revisionId);
    element("revision-inspector-note").textContent =
      `Revision ${short(revision.revisionId)} · created ${formatDate(revision.createdAt)} · parent ` +
      `${revision.parentRevisionId === null ? "initial" : short(revision.parentRevisionId)}.`;
    element("revision-document").textContent = JSON.stringify(revision.bundle, null, 2);
    element("revision-inspector").hidden = false;
  } catch (error) {
    showReleaseStatus(safeError(error), "is-error");
  }
}

async function openActivation(summary: ActivationSummary): Promise<void> {
  try {
    const activation = await client.activation(summary.activationId);
    const fields = element("activation-inspector-fields");
    clear(fields);
    const rows: readonly (readonly [string, string])[] = [
      ["State", activation.state],
      ["Target revision", short(summary.targetRevisionId)],
      ["Plan digest", short(activation.plan.planDigest)],
      [
        "Expected active revision",
        activation.plan.expectedActiveRevisionId === null
          ? "None (initial activation)"
          : short(activation.plan.expectedActiveRevisionId),
      ],
      ["Updated", formatDate(summary.updatedAt)],
    ];
    for (const [label, value] of rows) {
      const fieldRow = document.createElement("div");
      fieldRow.className = "metadata-field";
      fieldRow.append(textNode("span", label), textNode("strong", value));
      fields.appendChild(fieldRow);
    }
    element("activation-inspector-note").textContent =
      activation.state === "waiting_for_readiness"
        ? "Waiting for required workers and definitions to become ready before the active pointer moves."
        : "Observed state for one activation operation.";
    element("activation-inspector").hidden = false;
  } catch (error) {
    showReleaseStatus(safeError(error), "is-error");
  }
}

function renderDifferenceItems(items: readonly ConfigurationDifferenceItem[]): void {
  const target = element("diff-items");
  clear(target);
  if (items.length === 0) {
    emptyTable(target, 2, "No differences between the selected revisions.");
    return;
  }
  for (const item of items) {
    const row = document.createElement("tr");
    appendNodeCell(row, statusBadge(item.operation, item.operation));
    appendCell(row, item.path.join("."), "mono");
    target.appendChild(row);
  }
}

export async function compareRevisions(): Promise<void> {
  const source = selectElement("diff-source").value;
  const target = selectElement("diff-target").value;
  if (source.length === 0 || target.length === 0) {
    showReleaseStatus("Two revisions are required for comparison.", "is-error");
    return;
  }
  try {
    const [difference, sourceRevision, targetRevision] = await Promise.all([
      client.difference(source, target),
      client.revision(source),
      client.revision(target),
    ]);
    renderDifferenceItems(difference.items);
    element("compare-note").textContent =
      difference.items.length >= MAX_DIFF_ITEMS_DISPLAYED
        ? `Showing the first ${MAX_DIFF_ITEMS_DISPLAYED} bounded changes — the comparison was truncated.`
        : `${difference.items.length} bounded change(s) between the selected revisions.`;
    element("diff-source-label").textContent =
      `From ${short(source)} — ${formatDate(sourceRevision.createdAt)}`;
    element("diff-target-label").textContent =
      `To ${short(target)} — ${formatDate(targetRevision.createdAt)}`;
    element("diff-source-doc").textContent = JSON.stringify(sourceRevision.bundle, null, 2);
    element("diff-target-doc").textContent = JSON.stringify(targetRevision.bundle, null, 2);
    element("compare-result").hidden = false;
    element("diff-result").textContent = "";
  } catch (error) {
    showReleaseStatus(safeError(error), "is-error");
  }
}

export function applyReleasesCapabilities(capabilities: Capabilities): void {
  available = capabilities.configuration_mode === "managed" && capabilities.configuration_view;
  element("nav-releases").hidden = !available;
  button("activate-revision").disabled = !capabilities.configuration_activate;
  button("compare-revisions").disabled = !capabilities.configuration_view;
}

function activationStatusKind(value: ActivationState): "" | "is-error" | "is-success" {
  if (value === "applied" || value === "rolled_back") return "is-success";
  if (value === "failed" || value === "superseded") return "is-error";
  return "";
}

function activationStatusClass(value: ActivationState): string {
  return value === "running" ? "running-activation" : value;
}
