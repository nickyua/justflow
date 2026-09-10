/** Indirection so feature modules can request a dashboard refresh without importing the orchestrator. */

type Refresher = () => Promise<void>;

let refresher: Refresher = () => Promise.resolve();

export function setDashboardRefresher(fn: Refresher): void {
  refresher = fn;
}

export function refreshDashboard(): Promise<void> {
  return refresher();
}
