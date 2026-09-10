import { setConnection, showStatus } from "../components/status";
import {
  addSchedule,
  addWorkflow,
  applyDraft,
  applyEditorCapabilities,
  discardDraft,
  editConfiguration,
  initEditorSurface,
  initSchedulesSurface,
  isDraftTab,
  isEditorDirty,
  loadDraft,
  loadSchedulesFragment,
  loadSchema,
  openScheduleDialog,
  saveDraft,
  saveSchedulesFragment,
  setDraftTab,
  setRegisteredWorkflowNamesProvider,
  setWorkflowCreatedNavigator,
  validateDraft,
} from "../features/editor";
import {
  applyFragmentCapabilities,
  fragmentEditingAvailable,
  initFragmentSurface,
  isFragmentDirty,
  openFragmentEditor,
  openFullDocumentFromFragment,
  openWorkflowCreator,
  openWorkflowEditor,
  reloadFragment,
  retireFragmentWorkflow,
  runPreview,
  saveFragment,
  validateFragmentDraft,
} from "../features/editor/fragment";
import { filterComponentsView, openComponentsView } from "../features/editor/reference";
import {
  activateRevision,
  applyReleasesCapabilities,
  compareRevisions,
  loadMoreActivations,
  loadMoreRevisions,
  releasesAvailable,
} from "../features/releases";
import {
  closeRunInspector,
  isRunInspectorOpen,
  loadMoreRuns,
  onRunDetailLoaded,
  RUN_FILTERS,
  type RunFilter,
  refreshRunDetail,
  renderRuns,
  setRunFilter,
  setWatchRun,
} from "../features/runs";
import { loadMoreScheduledStarts, setWorkflowNavigator } from "../features/schedules";
import {
  copyDeclaration,
  getRegisteredWorkflowNames,
  getSelectedWorkflow,
  initDeclarationSurface,
  loadMoreWorkflowRuns,
  loadMoreWorkflows,
  loadWorkflowDetail,
  openStartRunDialog,
  renderWorkflows,
  rerenderDetailIfOpen,
  setSelectedWorkflow,
  setWorkflowTab,
  startRun,
  updateStartMode,
} from "../features/workflows";
import { client } from "./api";
import { setCapabilities } from "./capabilities";
import { loadDashboard } from "./dashboard";
import { button, dialog, element, form, input, selectElement } from "./dom";
import { safeError } from "./format";
import { setDashboardRefresher } from "./refresh";
import {
  isView,
  requestedView,
  requestedWorkflow,
  setView,
  type View,
  type WorkflowTab,
} from "./router";

function permittedView(view: View): View {
  if (view === "releases" && !releasesAvailable()) return "workflows";
  if (view === "workflow-editor" && !fragmentEditingAvailable()) return "editor";
  return view;
}

async function initialize(): Promise<void> {
  setSelectedWorkflow(requestedWorkflow());
  setView(requestedView(), false, getSelectedWorkflow());
  try {
    const capabilities = await client.capabilities();
    setCapabilities(capabilities);
    element("scope-label").textContent =
      `${capabilities.scope.tenant} / ${capabilities.scope.application} / ${capabilities.scope.environment}`;
    applyEditorCapabilities(capabilities);
    applyReleasesCapabilities(capabilities);
    applyFragmentCapabilities();
    const requested = permittedView(requestedView());
    setView(requested, false, getSelectedWorkflow());
    await Promise.all([loadSchema(), loadDashboard()]);
    if (capabilities.configuration_view) await loadDraft();
    const selected = getSelectedWorkflow();
    if (requested === "workflow-editor" && selected !== null) {
      await openFragmentEditor(selected, false);
    }
  } catch (error) {
    setConnection(false);
    showStatus(safeError(error), "is-error");
  }
}

function wire(): void {
  setDashboardRefresher(loadDashboard);
  setRegisteredWorkflowNamesProvider(getRegisteredWorkflowNames);
  setWorkflowCreatedNavigator(openWorkflowEditor);
  setWorkflowNavigator(loadWorkflowDetail);
  onRunDetailLoaded(rerenderDetailIfOpen);

  for (const item of document.querySelectorAll<HTMLButtonElement>("[data-view]")) {
    item.addEventListener("click", () => {
      const view = item.dataset.view;
      if (view !== undefined && isView(view)) {
        setSelectedWorkflow(null);
        setView(view);
      }
    });
  }
  for (const item of document.querySelectorAll<HTMLButtonElement>("[data-view-link]")) {
    item.addEventListener("click", () => {
      const view = item.dataset.viewLink;
      if (view !== undefined && isView(view)) setView(view);
    });
  }
  for (const item of document.querySelectorAll<HTMLButtonElement>("[data-edit-draft]")) {
    item.addEventListener("click", () => void editConfiguration("triggers"));
  }
  for (const item of document.querySelectorAll<HTMLButtonElement>("[data-run-filter]")) {
    item.addEventListener("click", () => {
      const filter = item.dataset.runFilter;
      if (filter === undefined || !RUN_FILTERS.some((known: RunFilter) => known === filter)) return;
      for (const tab of document.querySelectorAll<HTMLButtonElement>("[data-run-filter]")) {
        tab.classList.toggle("is-active", tab === item);
        tab.setAttribute("aria-pressed", String(tab === item));
      }
      setRunFilter(filter as RunFilter);
    });
  }
  for (const item of document.querySelectorAll<HTMLButtonElement>("[data-workflow-tab]")) {
    item.addEventListener("click", () => {
      const tab = item.dataset.workflowTab;
      if (tab === "overview" || tab === "definition" || tab === "runs" || tab === "triggers") {
        setWorkflowTab(tab as WorkflowTab);
      }
    });
  }
  button("workflow-declaration-copy").addEventListener("click", () => void copyDeclaration());
  button("load-more-workflow-runs").addEventListener("click", () => void loadMoreWorkflowRuns());
  button("load-more-runs").addEventListener("click", () => void loadMoreRuns());
  button("load-more-scheduled-starts").addEventListener(
    "click",
    () => void loadMoreScheduledStarts(),
  );
  button("load-more-workflows").addEventListener("click", () => void loadMoreWorkflows());
  button("load-more-revisions").addEventListener("click", () => void loadMoreRevisions());
  button("load-more-activations").addEventListener("click", () => void loadMoreActivations());

  input("workflow-search").addEventListener("input", renderWorkflows);
  input("run-search").addEventListener("input", renderRuns);
  button("refresh-dashboard").addEventListener("click", () => void loadDashboard());
  button("new-workflow-draft").addEventListener("click", () => void openWorkflowCreator());
  button("new-schedule").addEventListener("click", () => openScheduleDialog());
  button("editor-add-schedule").addEventListener("click", () => openScheduleDialog());
  button("workflow-detail-schedule").addEventListener("click", () => {
    const selected = getSelectedWorkflow();
    if (selected !== null) openScheduleDialog(selected);
  });
  button("workflow-inline-schedule").addEventListener("click", () => {
    const selected = getSelectedWorkflow();
    if (selected !== null) openScheduleDialog(selected);
  });
  button("workflow-detail-edit").addEventListener("click", () => {
    const selected = getSelectedWorkflow();
    if (selected !== null) {
      void openWorkflowEditor(selected);
    }
  });
  button("editor-open-components").addEventListener("click", () => void openComponentsView());
  input("component-search").addEventListener("input", filterComponentsView);
  button("back-to-editor-workflows").addEventListener("click", () => {
    setSelectedWorkflow(null);
    setView("workflows");
  });
  button("fragment-reload").addEventListener("click", () => void reloadFragment());
  button("fragment-preview-run").addEventListener("click", () => void runPreview());
  button("fragment-save").addEventListener("click", () => void saveFragment());
  button("fragment-validate").addEventListener("click", () => void validateFragmentDraft());
  button("fragment-retire").addEventListener("click", () => void retireFragmentWorkflow());
  button("fragment-open-full").addEventListener("click", () => void openFullDocumentFromFragment());
  button("back-to-workflows").addEventListener("click", () => {
    setSelectedWorkflow(null);
    setView("workflows");
  });
  button("workflow-detail-start").addEventListener("click", openStartRunDialog);
  selectElement("start-run-mode").addEventListener("change", updateStartMode);
  form("start-run-form").addEventListener("submit", (event) => {
    event.preventDefault();
    void startRun();
  });
  button("refresh-run-detail").addEventListener("click", () => void refreshRunDetail());
  input("watch-run-toggle").addEventListener("change", () => {
    setWatchRun(input("watch-run-toggle").checked);
  });
  button("close-run-inspector").addEventListener("click", closeRunInspector);
  button("inspector-scrim").addEventListener("click", closeRunInspector);
  button("load-draft").addEventListener("click", () => void loadDraft());
  button("save-draft").addEventListener("click", () => void saveDraft());
  button("schedules-reload").addEventListener("click", () => void loadSchedulesFragment(true));
  button("schedules-save").addEventListener("click", () => void saveSchedulesFragment());
  button("validate-draft").addEventListener("click", () => void validateDraft());
  button("discard-draft").addEventListener("click", () => void discardDraft());
  button("apply-draft").addEventListener("click", () => void applyDraft());
  button("activate-revision").addEventListener("click", () => void activateRevision());
  button("compare-revisions").addEventListener("click", () => void compareRevisions());
  form("workflow-form").addEventListener("submit", (event) => {
    event.preventDefault();
    void addWorkflow();
  });
  form("schedule-form").addEventListener("submit", (event) => {
    event.preventDefault();
    void addSchedule();
  });
  for (const control of document.querySelectorAll<HTMLButtonElement>("[data-close-dialog]")) {
    control.addEventListener("click", () => {
      const target = control.dataset.closeDialog;
      if (target !== undefined) dialog(target).close();
    });
  }
  selectElement("schedule-kind").addEventListener("change", () => {
    const cron = selectElement("schedule-kind").value === "cron";
    element("schedule-cron-field").hidden = !cron;
    element("schedule-interval-field").hidden = cron;
    input("schedule-cron").required = cron;
    input("schedule-interval").required = !cron;
  });
  window.addEventListener("popstate", () => {
    setSelectedWorkflow(requestedWorkflow());
    const view = permittedView(requestedView());
    setView(view, false, getSelectedWorkflow());
    const selected = getSelectedWorkflow();
    if (view === "workflow" && selected !== null) {
      void loadWorkflowDetail(selected, false);
    }
    if (view === "workflow-editor" && selected !== null) {
      void openFragmentEditor(selected, false);
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && isRunInspectorOpen()) closeRunInspector();
  });
  void initEditorSurface();
  void initFragmentSurface();
  void initDeclarationSurface();
  for (const control of document.querySelectorAll<HTMLButtonElement>("[data-draft-tab]")) {
    control.addEventListener("click", () => {
      const tab = control.dataset.draftTab ?? "";
      if (isDraftTab(tab)) {
        setDraftTab(tab);
        if (tab === "triggers") void loadSchedulesFragment();
      }
    });
  }
  void initSchedulesSurface();
  window.addEventListener("beforeunload", (event) => {
    if (isEditorDirty() || isFragmentDirty()) event.preventDefault();
  });
}

export function start(): void {
  wire();
  void initialize();
}
