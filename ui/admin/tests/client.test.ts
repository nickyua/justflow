import { describe, expect, it, vi } from "vitest";

import { OperationsClient } from "../src/api/client";

const DIGEST = "a".repeat(64);
const PLAN_DIGEST = `sha256:${"b".repeat(64)}`;
const CAPABILITIES = {
  api_compatibility: {
    current_version: 1,
    minimum_supported_client_version: 1,
    maximum_supported_client_version: 1,
  },
  operations_view: true,
  scope: { tenant: "tenant-a", application: "orders", environment: "production" },
  configuration_mode: "managed",
  workflow_start: true,
  workflow_signal: true,
  workflow_cancel: true,
  workflow_terminate: true,
  trigger_pause: true,
  trigger_resume: true,
  trigger_run: true,
  trigger_delete: true,
  trigger_apply: true,
  scheduled_start_create: true,
  scheduled_start_view: true,
  scheduled_start_reschedule: true,
  scheduled_start_cancel: true,
  configuration_view: true,
  configuration_edit: true,
  configuration_validate: true,
  configuration_apply: false,
  configuration_discard: true,
  configuration_publish: true,
  configuration_activate: true,
  configuration_rollback: true,
};
const ACTIVATION = {
  activation_id: DIGEST,
  state: "applied",
  plan: {
    plan_digest: PLAN_DIGEST,
    expected_active_revision_id: DIGEST,
  },
};
const SCHEDULED_START_ID = "scheduled-start-opaque";
const SCHEDULED_START = {
  scheduled_start_id: SCHEDULED_START_ID,
  workflow_name: "example",
  trigger_name: "example_api",
  start_at: "2026-08-12T10:00:00Z",
  workload_class: "standard",
  state: "scheduled",
  version: 1,
  accepted_at: "2026-08-11T10:00:00Z",
  updated_at: "2026-08-11T10:00:00Z",
  dispatch_started_at: null,
  completed_at: null,
  failure_code: null,
  run: null,
};

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function responseFor(path: string, method: string): Response {
  if (path === "/v1/operations/capabilities") return json(CAPABILITIES);
  if (path === "/v1/workflows" && method === "POST") {
    return json({ workflow_id: "wf-example-1", run_id: "run-new", status: "started" }, 202);
  }
  if (path === "/v1/scheduled-starts" && method === "POST") {
    return json(
      { status: "accepted", scheduled_start: SCHEDULED_START, expected_version: null },
      202,
    );
  }
  if (path === `/v1/scheduled-starts/${SCHEDULED_START_ID}/reschedule` && method === "POST") {
    return json({
      status: "rescheduled",
      scheduled_start: { ...SCHEDULED_START, version: 2 },
      expected_version: 1,
    });
  }
  if (path === `/v1/scheduled-starts/${SCHEDULED_START_ID}/cancel` && method === "POST") {
    return json({
      status: "canceled",
      scheduled_start: { ...SCHEDULED_START, state: "canceled", version: 2 },
      expected_version: 1,
    });
  }
  if (path.startsWith("/v1/workflows/wf-example-1/events/approval?")) {
    return json({ status: "accepted" }, 202);
  }
  if (path.startsWith("/v1/workflows/run/cancel?")) return json({ status: "accepted" }, 202);
  if (path.startsWith("/v1/workflows/run/terminate?")) return json({ status: "accepted" }, 202);
  if (path === "/v1/triggers/daily/pause") return json({ status: "paused" });
  if (path === "/v1/triggers/daily/resume") return json({ status: "resumed" });
  if (path === "/v1/triggers/daily/run") return json({ status: "accepted" }, 202);
  if (path === "/v1/triggers/daily/delete") return json({ status: "deleted" });
  if (path === "/v1/operations") {
    return json({
      health: {
        ready: true,
        components: [{ component: "temporal", status: "ready", required: true }],
      },
      metrics_links: [{ label: "Metrics", url: "https://metrics.example" }],
    });
  }
  if (path === "/v1/operations/workflows/example") {
    return json({
      logical_workflow: "example",
      active_definition_digest: DIGEST,
      retained_definition_count: 2,
      required_engine_workflow_abi: "1",
      description: "Example workflow",
      graph_nodes: [
        { node_id: "done", label: "done", kind: "terminal", group: null, metadata: {} },
      ],
      graph_edges: [],
      definitions: [{ logical_workflow: "example", definition_digest: DIGEST, active: true }],
      service_dependencies: [],
      resource_dependencies: [],
      has_input_contract: false,
      has_output_contract: false,
      input_schema: null,
      referenced_globals: [],
      step_bindings: [],
      archival: null,
    });
  }
  if (path.startsWith("/v1/operations/workflows?")) {
    return json({
      workflows: [
        {
          logical_workflow: "example",
          active_definition_digest: DIGEST,
          retained_definition_count: 2,
        },
      ],
      next_cursor: path.includes("cursor=") ? null : "workflow-cursor",
    });
  }
  if (path === "/v1/operations/runs/run?run_id=run-1") {
    return json({
      workflow_id: "run",
      run_id: "run-1",
      workflow_type: "example--aaaaaaaaaaaa",
      status: "RUNNING",
      start_time: "2026-08-04T10:00:00Z",
      close_time: null,
      logical_workflow: "example",
      definition_digest: DIGEST,
      artifact_identity: {
        deployment_name: "orders",
        build_id: "build-1",
        artifact_digest: `sha256:${DIGEST}`,
      },
      execution_configuration: { configuration_revision_id: DIGEST },
      trigger_source: "api",
      pending_waits: [{ kind: "activity", state: "scheduled" }],
      pending_waits_truncated: false,
      continuation: { first_run_id: "run-1", next_run_id: null },
      failure: null,
    });
  }
  if (path.startsWith("/v1/operations/runs")) {
    return json({
      workflows: [
        {
          workflow_id: "run",
          run_id: "run-1",
          workflow_type: "example--aaaaaaaaaaaa",
          status: "RUNNING",
          start_time: "2026-08-04T10:00:00Z",
          close_time: null,
        },
      ],
      next_page_token: "run-cursor",
    });
  }
  if (path === "/v1/operations/triggers") {
    return json({
      triggers: [
        {
          name: "daily",
          kind: "schedule",
          state: "active",
          workflow_name: "example",
          schedule: {
            schedule_name: "daily",
            desired_digest: DIGEST,
            paused: false,
            workflow_name: "example",
            definition_digest: DIGEST,
            overlap_policy: "skip",
            next_run_times: ["2026-08-05T10:00:00Z"],
            recent_actions: [],
          },
        },
      ],
    });
  }
  if (path.startsWith("/v1/operations/scheduled-starts?")) {
    return json({ scheduled_starts: [SCHEDULED_START], next_cursor: "scheduled-start-cursor" });
  }
  if (path === `/v1/operations/scheduled-starts/${SCHEDULED_START_ID}`) {
    return json(SCHEDULED_START);
  }
  if (path.startsWith("/v1/operations/configuration?")) {
    return json({
      active: { revision_id: DIGEST },
      revisions: {
        revisions: [
          { revision_id: DIGEST, parent_revision_id: null, created_at: "2026-08-04T10:00:00Z" },
        ],
        next_cursor: null,
      },
    });
  }
  if (path.startsWith("/v1/operations/activations")) {
    return json({
      activations: {
        activations: [
          {
            activation_id: DIGEST,
            target_revision_id: DIGEST,
            state: "applied",
            updated_at: "2026-08-04T10:00:00Z",
          },
        ],
        next_cursor: null,
      },
    });
  }
  if (path === "/v1/operations/configuration-schema") {
    return json({ schema_document: { title: "Tenant configuration" } });
  }
  if (path === "/v1/configuration/draft/export") {
    return new Response("workflows: {}\n", { headers: { "content-type": "application/yaml" } });
  }
  if (path === "/v1/configuration/draft/workflows/example" && method === "GET") {
    return json({
      scope_digest: DIGEST,
      workflow: "example",
      version: 7,
      document: "workflow: example\n",
    });
  }
  if (path.startsWith("/v1/configuration/draft/workflows/example?expected_version=7")) {
    return json({
      version: 8,
      bundle: { workflows: {} },
      restart_required: true,
    });
  }
  if (path === "/v1/configuration/draft/workflows/example/preview") {
    return json({
      status: "graph_ready",
      draft_validated: false,
      workflow: "example",
      diagnostics: [],
      graph_nodes: [
        { node_id: "done", label: "done", kind: "terminal", group: null, metadata: {} },
      ],
      graph_edges: [],
    });
  }
  if (path === "/v1/configuration/draft/validate") {
    return json({
      valid: false,
      issues: [
        {
          severity: "error",
          category: "declaration",
          location: ["workflows", "example"],
          message: "Workflow declaration is invalid",
        },
        {
          severity: "warning",
          category: "semantic",
          location: ["workflows", "example", "steps", "unused"],
          message: "Operation definition 'unused' is not used by the workflow flow",
        },
      ],
    });
  }
  if (path === "/v1/configuration/relationships") {
    return json({
      working_version: 1,
      active_identity: DIGEST,
      relationships: [{ kind: "workflow", name: "example", state: "active" }],
      restart_required: false,
    });
  }
  if (path === "/v1/configuration/apply") {
    return json({
      mode: "local_source",
      working_version: 1,
      stages: [
        { kind: "validation", state: "completed" },
        { kind: "definition_publication", state: "completed" },
        { kind: "process_restart", state: "restart_required" },
      ],
      definitions_published: true,
      restart_required: true,
      running_process_changed: false,
    });
  }
  if (path === "/v1/configuration/draft/discard") {
    return json({
      discard_id: DIGEST,
      working_version: 2,
      active_identity: DIGEST,
      state: "applied",
      restart_required: false,
      running_process_changed: false,
    });
  }
  if (path === "/v1/configuration/draft" || path.startsWith("/v1/configuration/draft/import")) {
    return json({
      version: method === "GET" ? 1 : 2,
      bundle: { component_catalog_revision: DIGEST, workflows: {} },
    });
  }
  if (path === "/v1/configuration/publications") {
    return json({ published_revision_id: DIGEST });
  }
  if (path === "/v1/configuration/activations/plan") {
    return json(ACTIVATION.plan);
  }
  if (path === `/v1/configuration/activations/${DIGEST}/readiness`) {
    return json({
      activation_id: DIGEST,
      ready: true,
      state: "applied",
      target_revision_id: DIGEST,
      worker_readiness_registered: true,
      completed_checkpoints: ["active_pointer"],
    });
  }
  if (path === "/v1/configuration/activations" || path.includes("/activations/")) {
    return json(ACTIVATION);
  }
  if (path === `/v1/configuration/revisions/${DIGEST}`) {
    return json({
      scope_digest: DIGEST,
      revision_id: DIGEST,
      parent_revision_id: null,
      created_at: "2026-08-04T10:00:00Z",
      bundle: { workflows: {} },
    });
  }
  if (path.startsWith("/v1/configuration/revisions/compare")) {
    return json({ items: [{ operation: "replace", path: ["workflows", "example"] }] });
  }
  return json({ error: { message: "Unknown test route" } }, 404);
}

describe("OperationsClient", () => {
  it("uses only bounded Justflow API routes and validates their responses", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) =>
      responseFor(String(input), init?.method ?? "GET"),
    );
    const client = new OperationsClient(fetcher);
    const headers = { "x-idempotency-key": "request", "content-type": "application/json" };

    await expect(client.capabilities()).resolves.toEqual(CAPABILITIES);
    await expect(client.overview()).resolves.toEqual({
      ready: true,
      components: [{ component: "temporal", status: "ready", required: true }],
      metricsLinks: [{ label: "Metrics", url: "https://metrics.example" }],
    });
    const registrations = await client.workflowRegistrations();
    expect(registrations.workflows).toHaveLength(1);
    expect(registrations.nextCursor).toBe("workflow-cursor");
    const continued = await client.workflowRegistrations(registrations.nextCursor);
    expect(continued.nextCursor).toBeNull();
    await expect(client.workflowDetail("example")).resolves.toMatchObject({
      logicalWorkflow: "example",
      graphNodes: [{ nodeId: "done" }],
    });
    const running = await client.runs("running", 30);
    expect(running.runs).toHaveLength(1);
    expect(running.nextCursor).toBe("run-cursor");
    await expect(client.runs(null, 30, running.nextCursor)).resolves.toMatchObject({
      nextCursor: "run-cursor",
    });
    await expect(client.runs("failed", 12)).resolves.toMatchObject({ nextCursor: "run-cursor" });
    await expect(client.runDetail("run", "run-1")).resolves.toMatchObject({
      workflowId: "run",
      runId: "run-1",
      deploymentName: "orders",
      pendingWaits: [{ kind: "activity", state: "scheduled" }],
    });
    await expect(client.startWorkflow("example", "request-1", { mode: "full" })).resolves.toEqual({
      workflowId: "wf-example-1",
      runId: "run-new",
      duplicate: false,
    });
    const scheduled = await client.scheduleWorkflow(
      "example",
      "request-later-1",
      { mode: "full" },
      "2026-08-12T10:00:00Z",
      "standard",
      "create-key",
    );
    expect(scheduled).toMatchObject({
      status: "accepted",
      scheduledStart: { scheduledStartId: SCHEDULED_START_ID, state: "scheduled" },
    });
    await expect(client.scheduledStarts(["scheduled"])).resolves.toMatchObject({
      scheduledStarts: [{ scheduledStartId: SCHEDULED_START_ID }],
      nextCursor: "scheduled-start-cursor",
    });
    await expect(client.scheduledStart(SCHEDULED_START_ID)).resolves.toMatchObject({
      scheduledStartId: SCHEDULED_START_ID,
    });
    await expect(
      client.rescheduleScheduledStart(
        SCHEDULED_START_ID,
        "2026-08-13T10:00:00Z",
        1,
        "reschedule-key",
      ),
    ).resolves.toMatchObject({ status: "rescheduled", scheduledStart: { version: 2 } });
    await expect(
      client.cancelScheduledStart(SCHEDULED_START_ID, 1, "cancel-key"),
    ).resolves.toMatchObject({
      status: "canceled",
      scheduledStart: { state: "canceled", version: 2 },
    });
    await expect(
      client.signalWorkflow("wf-example-1", "run-new", "approval", { approved: true }),
    ).resolves.toBeUndefined();
    await expect(client.cancelRun("run", "run-1")).resolves.toBeUndefined();
    await expect(client.terminateRun("run", "run-1")).resolves.toBeUndefined();
    await expect(client.triggers()).resolves.toHaveLength(1);
    await expect(client.pauseTrigger("daily")).resolves.toBeUndefined();
    await expect(client.resumeTrigger("daily")).resolves.toBeUndefined();
    await expect(client.runTrigger("daily", "request-1")).resolves.toBeUndefined();
    await expect(client.deleteTrigger("daily", DIGEST)).resolves.toBeUndefined();
    await expect(client.configuration()).resolves.toMatchObject({
      activeRevisionId: DIGEST,
      revisionsNextCursor: null,
    });
    const activations = await client.activations();
    expect(activations.activations).toHaveLength(1);
    expect(activations.nextCursor).toBeNull();
    await expect(client.configurationSchema()).resolves.toEqual({ title: "Tenant configuration" });
    const draft = await client.draft();
    await expect(client.draftYaml()).resolves.toBe("workflows: {}\n");
    await expect(client.saveJsonDraft(draft.bundle, null)).resolves.toMatchObject({ version: 2 });
    await expect(client.saveJsonDraft(draft.bundle, draft)).resolves.toMatchObject({ version: 2 });
    await expect(client.saveYamlDraft("workflows: {}\n", draft)).resolves.toMatchObject({
      version: 2,
    });
    await expect(client.workflowFragment("example")).resolves.toEqual({
      workflow: "example",
      version: 7,
      document: "workflow: example\n",
    });
    await expect(
      client.saveWorkflowFragment("example", "workflow: example\n", 7),
    ).resolves.toMatchObject({ version: 8, restartRequired: true });
    await expect(client.deleteWorkflowFragment("example", 7)).resolves.toMatchObject({
      version: 8,
    });
    await expect(
      client.previewWorkflowFragment("example", "workflow: example\n"),
    ).resolves.toMatchObject({
      status: "graph_ready",
      draftValidated: false,
      graphNodes: [{ nodeId: "done" }],
    });
    await expect(client.validateDraft()).resolves.toEqual({
      valid: false,
      issues: [
        {
          severity: "error",
          category: "declaration",
          location: ["workflows", "example"],
          message: "Workflow declaration is invalid",
        },
        {
          severity: "warning",
          category: "semantic",
          location: ["workflows", "example", "steps", "unused"],
          message: "Operation definition 'unused' is not used by the workflow flow",
        },
      ],
    });
    await expect(client.configurationRelationships()).resolves.toMatchObject({
      workingVersion: 1,
      activeIdentity: DIGEST,
      relationships: [{ kind: "workflow", name: "example", state: "active" }],
    });
    await expect(client.applyLocalConfiguration(1, headers)).resolves.toMatchObject({
      restartRequired: true,
      runningProcessChanged: false,
    });
    await expect(client.discardConfiguration(1, DIGEST, headers)).resolves.toMatchObject({
      workingVersion: 2,
      runningProcessChanged: false,
    });
    await expect(client.publishDraft(1, headers)).resolves.toEqual({ publishedRevisionId: DIGEST });
    await expect(client.planActivation(DIGEST)).resolves.toMatchObject({ planDigest: PLAN_DIGEST });
    await expect(client.activate(DIGEST, PLAN_DIGEST, headers)).resolves.toMatchObject({
      state: "applied",
    });
    await expect(client.activation(DIGEST)).resolves.toMatchObject({ state: "applied" });
    await expect(client.activationReadiness(DIGEST)).resolves.toMatchObject({
      ready: true,
      state: "applied",
    });
    await expect(client.rollback(DIGEST, PLAN_DIGEST, headers)).resolves.toMatchObject({
      state: "applied",
    });
    await expect(client.difference(DIGEST, DIGEST)).resolves.toEqual({
      items: [{ operation: "replace", path: ["workflows", "example"] }],
    });
    await expect(client.revision(DIGEST)).resolves.toMatchObject({
      revisionId: DIGEST,
      bundle: { workflows: {} },
    });

    expect(fetcher).toHaveBeenCalled();
    for (const [, init] of fetcher.mock.calls) {
      expect(init).toMatchObject({ cache: "no-store", credentials: "same-origin" });
    }
  });

  it.each([
    ["json", json({ error: { message: "Denied" } }, 403), "Denied"],
    ["text", new Response("gateway", { status: 502 }), "Request failed (502)"],
  ])("layers %s failures without returning response bodies", async (_id, response, message) => {
    const client = new OperationsClient(vi.fn(async () => response));
    await expect(client.overview()).rejects.toThrow(message);
  });
});
