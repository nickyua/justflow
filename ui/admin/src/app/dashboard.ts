import { setConnection } from "../components/status";
import { renderMetricsLinks, renderOverview } from "../features/overview";
import { releasesAvailable, reloadReleases } from "../features/releases";
import { loadFailureDetails, reloadRunView, renderRuns, setRuns } from "../features/runs";
import { reloadSchedules } from "../features/schedules";
import { renderSystem } from "../features/system";
import {
  getSelectedWorkflow,
  loadWorkflowDetail,
  reloadWorkflowCollections,
} from "../features/workflows";
import { client } from "./api";
import { button } from "./dom";

const LIVE_RUN_PAGE_LIMIT = 30;
const FAILURE_PAGE_LIMIT = 12;

export async function loadDashboard(): Promise<void> {
  button("refresh-dashboard").disabled = true;
  try {
    const [overview, running, failed] = await Promise.all([
      client.overview(),
      client.runs("running", LIVE_RUN_PAGE_LIMIT),
      client.runs("failed", FAILURE_PAGE_LIMIT),
    ]);
    setRuns(running.runs, failed.runs);
    const reloads = [reloadWorkflowCollections(), reloadRunView(), reloadSchedules()];
    if (releasesAvailable()) reloads.push(reloadReleases());
    await Promise.all(reloads);
    await loadFailureDetails();

    setConnection(true);
    renderMetricsLinks(overview.metricsLinks);
    renderOverview(overview.ready, overview.components);
    renderSystem(overview.ready, overview.components);
    renderRuns();
    const selected = getSelectedWorkflow();
    if (selected !== null) await loadWorkflowDetail(selected, false);
  } finally {
    button("refresh-dashboard").disabled = false;
  }
}
