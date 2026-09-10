import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import {
  errorMessage,
  INCOMPATIBLE_API_MESSAGE,
  parseAuthoringReference,
  parseCapabilities,
  parseConfigurationDiscardResult,
  parseConfigurationRelationships,
  parseConfigurationView,
  parseLocalConfigurationApplyResult,
  parseOperationsOverview,
  parseScheduledStart,
  parseScheduledStartMutation,
  parseScheduledStartPage,
  parseTriggers,
  parseWorkflowDetail,
  parseWorkflowFragmentPreview,
  parseWorkflowRegistrationPage,
  parseWorkflowRunDetail,
  parseWorkflowRunPage,
} from "../src/api/contracts";

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
  workflow_terminate: false,
  trigger_pause: true,
  trigger_resume: true,
  trigger_run: true,
  trigger_delete: false,
  trigger_apply: false,
  scheduled_start_create: true,
  scheduled_start_view: true,
  scheduled_start_reschedule: false,
  scheduled_start_cancel: true,
  configuration_view: true,
  configuration_edit: false,
  configuration_validate: true,
  configuration_apply: false,
  configuration_discard: true,
  configuration_publish: false,
  configuration_activate: false,
  configuration_rollback: false,
};

const PUBLIC_API_FIXTURES = JSON.parse(
  readFileSync(new URL("./fixtures/api-responses.json", import.meta.url), "utf8"),
) as Record<string, unknown>;

describe("operations response contracts", () => {
  it("accepts responses validated against the published OpenAPI contract", () => {
    expect(parseCapabilities(PUBLIC_API_FIXTURES.capabilities)).toBeDefined();
    expect(parseScheduledStart(PUBLIC_API_FIXTURES.scheduled_start)).toMatchObject({
      scheduledStartId: "scheduled-start-opaque",
      state: "started",
      artifactBuildId: "build-1",
    });
  });

  it("accepts the complete capability contract", () => {
    expect(parseCapabilities(CAPABILITIES)).toEqual(CAPABILITIES);
  });

  it("projects scheduled-start lifecycle metadata without protected input", () => {
    const scheduledStart = {
      scheduled_start_id: "scheduled-start-opaque",
      workflow_name: "example",
      trigger_name: "example_api",
      start_at: "2026-08-12T10:00:00Z",
      workload_class: "standard",
      state: "started",
      version: 2,
      accepted_at: "2026-08-11T10:00:00Z",
      updated_at: "2026-08-12T10:00:01Z",
      dispatch_started_at: "2026-08-12T10:00:00Z",
      completed_at: "2026-08-12T10:00:01Z",
      failure_code: null,
      run: {
        workflow_id: "workflow-opaque",
        run_id: "run-opaque",
        definition_digest: "a".repeat(64),
        artifact_identity: {
          deployment_name: "orders",
          build_id: "build-1",
          artifact_digest: `sha256:${"b".repeat(64)}`,
        },
        environment_snapshot_digest: "c".repeat(64),
        execution_configuration: null,
      },
      input: { secret: "not-projected" },
    };

    const projected = parseScheduledStart(scheduledStart);
    expect(projected).toMatchObject({
      scheduledStartId: "scheduled-start-opaque",
      state: "started",
      definitionDigest: "a".repeat(64),
      artifactBuildId: "build-1",
    });
    expect(JSON.stringify(projected)).not.toContain("not-projected");
    expect(
      parseScheduledStartPage({ scheduled_starts: [scheduledStart], next_cursor: "next" }),
    ).toMatchObject({ nextCursor: "next", scheduledStarts: [projected] });
    expect(
      parseScheduledStartMutation({
        status: "in_progress",
        scheduled_start: scheduledStart,
        expected_version: 1,
      }),
    ).toMatchObject({
      status: "in_progress",
      scheduledStart: projected,
      expectedVersion: 1,
    });
  });

  it("parses lifecycle states without inventing runtime outcomes", () => {
    expect(
      parseConfigurationRelationships({
        working_version: 2,
        active_identity: "a".repeat(64),
        relationships: [
          { kind: "workflow", name: "orders", state: "modified" },
          { kind: "trigger", name: "daily", state: "new_pending_apply" },
        ],
        restart_required: true,
      }),
    ).toMatchObject({ workingVersion: 2, restartRequired: true });
    expect(
      parseLocalConfigurationApplyResult({
        mode: "local_source",
        working_version: 2,
        stages: [{ kind: "process_restart", state: "restart_required" }],
        definitions_published: true,
        restart_required: true,
        running_process_changed: false,
      }),
    ).toMatchObject({ restartRequired: true, runningProcessChanged: false });
    expect(
      parseConfigurationDiscardResult({
        discard_id: "b".repeat(64),
        working_version: 3,
        active_identity: "a".repeat(64),
        restart_required: false,
        running_process_changed: false,
      }),
    ).toMatchObject({ workingVersion: 3, runningProcessChanged: false });
  });

  it.each([
    ["non-object", null],
    ["missing field", { ...CAPABILITIES, configuration_view: undefined }],
    ["wrong field type", { ...CAPABILITIES, configuration_edit: "yes" }],
    ["missing scope", { ...CAPABILITIES, scope: undefined }],
    ["incomplete scope", { ...CAPABILITIES, scope: { tenant: "tenant-a" } }],
  ])("rejects invalid capabilities: %s", (_id, value) => {
    expect(() => parseCapabilities(value)).toThrow(TypeError);
  });

  it.each([
    ["missing", undefined],
    [
      "older API",
      {
        current_version: 0,
        minimum_supported_client_version: 1,
        maximum_supported_client_version: 1,
      },
    ],
    [
      "newer API",
      {
        current_version: 2,
        minimum_supported_client_version: 1,
        maximum_supported_client_version: 1,
      },
    ],
    [
      "unsupported client",
      {
        current_version: 1,
        minimum_supported_client_version: 2,
        maximum_supported_client_version: 2,
      },
    ],
  ])("fails closed for %s compatibility", (_id, apiCompatibility) => {
    expect(() =>
      parseCapabilities({ ...CAPABILITIES, api_compatibility: apiCompatibility }),
    ).toThrow(INCOMPATIBLE_API_MESSAGE);
  });

  it("projects bounded workflow fields without payloads", () => {
    expect(
      parseWorkflowRunPage({
        workflows: [
          {
            workflow_id: "workflow-1",
            run_id: "run-1",
            workflow_type: "example--aaaaaaaaaaaa",
            status: "RUNNING",
            start_time: "2026-08-04T10:00:00Z",
            close_time: null,
            ignored_payload: { secret: "not projected" },
          },
        ],
        next_page_token: "cursor-1",
      }),
    ).toEqual({
      runs: [
        {
          workflowId: "workflow-1",
          runId: "run-1",
          workflowType: "example--aaaaaaaaaaaa",
          status: "RUNNING",
          startTime: "2026-08-04T10:00:00Z",
          closeTime: null,
        },
      ],
      nextCursor: "cursor-1",
    });
  });

  it("projects catalog-backed component contracts without implementation metadata", () => {
    const reference = parseAuthoringReference({
      dimension: { availability: "available", reason: null },
      authority: "immutable_catalog",
      catalog_revision: "a".repeat(64),
      components: [
        {
          kind: "step",
          name: "notifications",
          version: "1.2.0",
          description: "Send a notification.",
          action: "send",
          capabilities: [],
          parameter_schema: { type: "object" },
          parameter_contract_identity: "sha256:parameters",
          input_schema: { type: "object" },
          input_contract_identity: "sha256:input",
          output_schema: { type: "object" },
          output_contract_identity: "sha256:output",
          resource_slots: [{ name: "cache", capability: "cache" }],
          runtime_implementation: { endpoint: "https://private.invalid" },
        },
      ],
      services: [{ name: "notifications", description: null, actions: [] }],
      resources: [{ name: "workflow_cache", description: null, capabilities: ["cache"] }],
      trigger_bindings: [{ name: "hourly", kind: "schedule" }],
      truncated: false,
    });

    expect(reference.components[0]).toMatchObject({
      name: "notifications",
      version: "1.2.0",
      resourceSlots: [{ name: "cache", capability: "cache" }],
    });
    expect(JSON.stringify(reference)).not.toContain("private.invalid");
  });

  it("projects run provenance without payload or failure details", () => {
    expect(
      parseWorkflowRunDetail({
        workflow_id: "workflow-1",
        run_id: "run-1",
        workflow_type: "example--aaaaaaaaaaaa",
        status: "FAILED",
        start_time: "2026-08-04T10:00:00Z",
        close_time: "2026-08-04T10:01:00Z",
        logical_workflow: "example",
        definition_digest: "a".repeat(64),
        artifact_identity: {
          deployment_name: "orders",
          build_id: "build-1",
          artifact_digest: `sha256:${"b".repeat(64)}`,
        },
        execution_configuration: { configuration_revision_id: "c".repeat(64) },
        trigger_source: "api",
        pending_waits: [],
        pending_waits_truncated: false,
        continuation: { first_run_id: "run-1", next_run_id: null },
        failure: {
          code: "STEP_FAILED",
          cause_code: "HTTP_UNAVAILABLE",
          category: "execution",
          phase: "operation",
          retryable: false,
          step: "fetch",
        },
        payload: { secret: "not projected" },
      }),
    ).toMatchObject({
      deploymentName: "orders",
      buildId: "build-1",
      failureCode: "STEP_FAILED",
      failureCauseCode: "HTTP_UNAVAILABLE",
      failurePhase: "operation",
      failureRetryable: false,
      failedStep: "fetch",
    });
  });

  it("projects only the bounded active workflow graph", () => {
    expect(
      parseWorkflowDetail({
        logical_workflow: "example",
        active_definition_digest: "a".repeat(64),
        retained_definition_count: 2,
        required_engine_workflow_abi: "1",
        description: "Example workflow",
        graph_nodes: [
          {
            node_id: "fetch",
            label: "fetch",
            kind: "operation",
            group: null,
            metadata: { op: "fetch" },
          },
          { node_id: "done", label: "done", kind: "terminal", group: null, metadata: {} },
        ],
        graph_edges: [
          { source: "fetch", target: "done", label: null, dashed: false, secret: "ignored" },
        ],
        definitions: [
          { logical_workflow: "example", definition_digest: "a".repeat(64), active: true },
        ],
        service_dependencies: [{ service: "billing", actions: ["charge"] }],
        resource_dependencies: ["ledger"],
        has_input_contract: true,
        has_output_contract: false,
        input_schema: { type: "object" },
        referenced_globals: ["n", "seed"],
        step_bindings: [
          {
            operation: "fetch",
            service: "billing",
            action: "charge",
            subworkflow: null,
            resources: ["ledger"],
          },
        ],
        archival: { resource: "audit_store", retention_policy: "local-demo" },
        configuration: { credentials: "not projected" },
      }),
    ).toMatchObject({
      logicalWorkflow: "example",
      description: "Example workflow",
      graphNodes: [{ nodeId: "fetch" }, { nodeId: "done" }],
      graphEdges: [{ source: "fetch", target: "done" }],
      stepBindings: [{ operation: "fetch", service: "billing", resources: ["ledger"] }],
      archival: { resource: "audit_store", retentionPolicy: "local-demo" },
    });
  });

  it("projects trigger inventory without schedule input or undeclared contracts", () => {
    expect(
      parseTriggers({
        triggers: [
          {
            name: "daily",
            kind: "schedule",
            state: "active",
            workflow_name: "example",
            schedule: {
              schedule_name: "daily",
              schedule_id: "jf-sched-daily",
              desired_digest: "a".repeat(64),
              paused: false,
              workflow_name: "example",
              definition_digest: "b".repeat(64),
              artifact_identity: {
                deployment_name: "deployment",
                build_id: "build",
                artifact_digest: `sha256:${"c".repeat(64)}`,
                package_version: "0.1.0",
              },
              environment_snapshot_digest: "d".repeat(64),
              overlap_policy: "skip",
              next_run_times: [],
              recent_actions: [
                {
                  scheduled_at: "2026-08-05T02:00:00Z",
                  started_at: "2026-08-05T02:00:01Z",
                  workflow_id: "wf-1",
                  run_id: "run-1",
                  outcome: "accepted",
                },
              ],
              input: { secret: "not projected" },
            },
          },
          {
            name: "manual",
            kind: "api",
            state: "inactive",
            workflow_name: "example",
            schedule: null,
          },
        ],
      }),
    ).toMatchObject([
      {
        name: "daily",
        kind: "schedule",
        state: "active",
        workflowName: "example",
        overlapPolicy: "skip",
        lastAction: { outcome: "accepted", runId: "run-1" },
      },
      {
        name: "manual",
        kind: "api",
        state: "inactive",
        workflowName: "example",
        desiredDigest: null,
        definitionDigest: null,
        overlapPolicy: null,
        nextRunTimes: [],
        lastAction: null,
      },
    ]);
  });

  it("parses the discriminated fragment preview and rejects illegal shapes", () => {
    const diagnostics = [
      {
        severity: "warning",
        category: "declaration",
        location: ["steps", "unused"],
        message: "Operation is not used",
      },
    ];
    expect(
      parseWorkflowFragmentPreview({
        status: "graph_ready",
        draft_validated: false,
        workflow: "example",
        diagnostics,
        graph_nodes: [
          { node_id: "done", label: "done", kind: "terminal", group: null, metadata: {} },
        ],
        graph_edges: [{ source: "done", target: "done", label: null, dashed: true }],
      }),
    ).toMatchObject({
      status: "graph_ready",
      draftValidated: false,
      graphNodes: [{ nodeId: "done" }],
      graphEdges: [{ dashed: true }],
    });
    expect(
      parseWorkflowFragmentPreview({
        status: "invalid_fragment",
        draft_validated: false,
        workflow: "example",
        diagnostics,
      }),
    ).toMatchObject({ status: "invalid_fragment", draftValidated: false });
    expect(() =>
      parseWorkflowFragmentPreview({
        status: "graph_ready",
        draft_validated: true,
        workflow: "example",
        diagnostics: [],
        graph_nodes: [],
        graph_edges: [],
      }),
    ).toThrow(TypeError);
    expect(() =>
      parseWorkflowFragmentPreview({
        status: "published",
        draft_validated: false,
        workflow: "example",
        diagnostics: [],
      }),
    ).toThrow(TypeError);
  });

  it("rejects malformed collection fields", () => {
    expect(() => parseWorkflowRegistrationPage({ workflows: "not-an-array" })).toThrow(TypeError);
  });

  it("accepts an explicitly unavailable configuration dimension", () => {
    expect(
      parseConfigurationView({
        dimension: { availability: "unavailable" },
        active: null,
        revisions: { revisions: [], next_cursor: null },
      }),
    ).toEqual({ activeRevisionId: null, revisions: [], revisionsNextCursor: null });
  });

  it.each([
    ["credential", "https://operator:secret@metrics.example"],
    ["query", "https://metrics.example/dashboard?token=secret"],
    ["script", "javascript:alert(1)"],
  ])("rejects unsafe metrics links: %s", (_id, url) => {
    expect(() =>
      parseOperationsOverview({
        health: { ready: true, components: [] },
        metrics_links: [{ label: "Metrics", url }],
      }),
    ).toThrow(TypeError);
  });

  it.each([
    ["component", { component: "database", status: "ready", required: true }],
    ["status", { component: "temporal", status: "degraded", required: true }],
  ])("rejects unknown health %s values", (_id, component) => {
    expect(() =>
      parseOperationsOverview({
        health: { ready: false, components: [component] },
        metrics_links: [],
      }),
    ).toThrow(TypeError);
  });

  it.each([
    ["server message", { error: { message: "Unavailable" } }, 503, "Unavailable"],
    ["fallback", "not-json", 502, "Request failed (502)"],
  ])("creates a safe request error: %s", (_id, value, status, expected) => {
    expect(errorMessage(value, status)).toBe(expected);
  });
});
