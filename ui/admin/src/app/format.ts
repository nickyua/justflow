import type { WorkflowRun } from "../api/contracts";

const IDENTITY_PREVIEW_LENGTH = 12;
const DATE_FORMAT: Intl.DateTimeFormatOptions = {
  dateStyle: "medium",
  timeStyle: "short",
};
const MILLISECONDS_PER_SECOND = 1_000;
const SECONDS_PER_MINUTE = 60;
const MINUTES_PER_HOUR = 60;

export function short(value: string | null): string {
  if (value === null) return "—";
  return value.length <= IDENTITY_PREVIEW_LENGTH
    ? value
    : `${value.slice(0, IDENTITY_PREVIEW_LENGTH)}…`;
}

export function formatDate(value: string | null): string {
  if (value === null) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "Unavailable" : date.toLocaleString(undefined, DATE_FORMAT);
}

export function normalizedStatus(value: string | null): string {
  return (value ?? "unknown").toLowerCase();
}

export function runKey(run: Pick<WorkflowRun, "workflowId" | "runId">): string {
  return JSON.stringify([run.workflowId, run.runId]);
}

export function runDuration(run: WorkflowRun): string {
  const started = new Date(run.startTime).getTime();
  const closed = run.closeTime === null ? Date.now() : new Date(run.closeTime).getTime();
  if (!Number.isFinite(started) || !Number.isFinite(closed) || closed < started)
    return "Unavailable";
  const seconds = Math.floor((closed - started) / MILLISECONDS_PER_SECOND);
  if (seconds < SECONDS_PER_MINUTE) return `${seconds}s`;
  const minutes = Math.floor(seconds / SECONDS_PER_MINUTE);
  if (minutes < MINUTES_PER_HOUR) return `${minutes}m ${seconds % SECONDS_PER_MINUTE}s`;
  const hours = Math.floor(minutes / MINUTES_PER_HOUR);
  return `${hours}h ${minutes % MINUTES_PER_HOUR}m`;
}

export function truncate(value: string, limit: number): string {
  return value.length <= limit ? value : `${value.slice(0, limit - 1)}…`;
}

export function safeError(error: unknown): string {
  return error instanceof Error ? error.message : "The operation failed.";
}
