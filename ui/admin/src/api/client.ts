import {
  type ActivationPage,
  type ActivationPlan,
  type ActivationReadiness,
  type ActivationRecord,
  type AuthoringReference,
  type Capabilities,
  type ConfigurationDifference,
  type ConfigurationDiscardResult,
  type ConfigurationRelationships,
  type ConfigurationView,
  type DraftRecord,
  errorCode,
  errorMessage,
  type JsonObject,
  type LocalConfigurationApplyResult,
  type OperationsOverview,
  type PublicationRecord,
  parseActivation,
  parseActivationPage,
  parseActivationPlan,
  parseActivationReadiness,
  parseAuthoringReference,
  parseCapabilities,
  parseConfigurationDiscardResult,
  parseConfigurationRelationships,
  parseConfigurationSchema,
  parseConfigurationView,
  parseDifference,
  parseDraft,
  parseLocalConfigurationApplyResult,
  parseOperationsOverview,
  parsePublication,
  parseRevisionRecord,
  parseScheduledStart,
  parseScheduledStartMutation,
  parseScheduledStartPage,
  parseStartRunResult,
  parseTriggers,
  parseTriggersFragment,
  parseValidationReport,
  parseWorkflowDefinitionDocument,
  parseWorkflowDetail,
  parseWorkflowFragment,
  parseWorkflowFragmentPreview,
  parseWorkflowRegistrationPage,
  parseWorkflowRunDetail,
  parseWorkflowRunPage,
  type RevisionRecord,
  type ScheduledStart,
  type ScheduledStartMutationResult,
  type ScheduledStartPage,
  type ScheduledStartState,
  type ScheduledStartWorkloadClass,
  type StartRunResult,
  type TriggerSummary,
  type TriggersFragment,
  type ValidationReport,
  type WorkflowDefinitionDocument,
  type WorkflowDetail,
  type WorkflowFragment,
  type WorkflowFragmentPreview,
  type WorkflowRegistrationPage,
  type WorkflowRunDetail,
  type WorkflowRunPage,
} from "./contracts";

export type RunStateQuery =
  | "running"
  | "completed"
  | "failed"
  | "canceled"
  | "terminated"
  | "timed_out";

export class ApiRequestError extends Error {
  readonly status: number;
  readonly code: string | null;

  constructor(message: string, status: number, code: string | null) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

const DASHBOARD_PAGE_LIMIT = 20;
const OPERATIONS_API_PATH = "/v1/operations";
const CONFIGURATION_API_PATH = "/v1/configuration";
const CONFIGURATION_DRAFT_PATH = `${CONFIGURATION_API_PATH}/draft`;
const CONFIGURATION_ACTIVATIONS_PATH = `${CONFIGURATION_API_PATH}/activations`;

type Fetcher = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
type JsonParser<Result> = (value: unknown) => Result;

export class OperationsClient {
  readonly #fetcher: Fetcher;

  constructor(fetcher: Fetcher = globalThis.fetch.bind(globalThis)) {
    this.#fetcher = fetcher;
  }

  capabilities(): Promise<Capabilities> {
    return this.#json(`${OPERATIONS_API_PATH}/capabilities`, {}, parseCapabilities);
  }

  overview(): Promise<OperationsOverview> {
    return this.#json(OPERATIONS_API_PATH, {}, parseOperationsOverview);
  }

  workflowRegistrations(cursor: string | null = null): Promise<WorkflowRegistrationPage> {
    return this.#json(
      `${OPERATIONS_API_PATH}/workflows?${pageQuery(DASHBOARD_PAGE_LIMIT, cursor)}`,
      {},
      parseWorkflowRegistrationPage,
    );
  }

  workflowDetail(logicalWorkflow: string): Promise<WorkflowDetail> {
    return this.#json(
      `${OPERATIONS_API_PATH}/workflows/${encodeURIComponent(logicalWorkflow)}`,
      {},
      parseWorkflowDetail,
    );
  }

  runs(
    state: RunStateQuery | null,
    limit: number,
    cursor: string | null = null,
    workflow: string | null = null,
  ): Promise<WorkflowRunPage> {
    const query = new URLSearchParams({ limit: String(limit), scope: "current" });
    if (state !== null) query.set("state", state);
    if (cursor !== null) query.set("cursor", cursor);
    if (workflow !== null) query.set("workflow", workflow);
    return this.#json(`${OPERATIONS_API_PATH}/runs?${query.toString()}`, {}, parseWorkflowRunPage);
  }

  workflowDefinition(logicalWorkflow: string): Promise<WorkflowDefinitionDocument> {
    return this.#json(
      `${OPERATIONS_API_PATH}/workflows/${encodeURIComponent(logicalWorkflow)}/definition`,
      {},
      parseWorkflowDefinitionDocument,
    );
  }

  runDetail(workflowId: string, runId: string): Promise<WorkflowRunDetail> {
    const query = new URLSearchParams({ run_id: runId });
    return this.#json(
      `${OPERATIONS_API_PATH}/runs/${encodeURIComponent(workflowId)}?${query.toString()}`,
      {},
      parseWorkflowRunDetail,
    );
  }

  startWorkflow(
    workflowName: string,
    businessRequestId: string,
    input: JsonObject,
  ): Promise<StartRunResult> {
    return this.#json(
      "/v1/workflows",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          workflow_name: workflowName,
          business_request_id: businessRequestId,
          input,
          correlation_id: businessRequestId,
        }),
      },
      parseStartRunResult,
    );
  }

  scheduleWorkflow(
    workflowName: string,
    businessRequestId: string,
    input: JsonObject,
    startAt: string,
    workloadClass: ScheduledStartWorkloadClass,
    idempotencyKey: string,
  ): Promise<ScheduledStartMutationResult> {
    return this.#json(
      "/v1/scheduled-starts",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-idempotency-key": idempotencyKey,
        },
        body: JSON.stringify({
          workflow_name: workflowName,
          business_request_id: businessRequestId,
          input,
          start_at: startAt,
          workload_class: workloadClass,
        }),
      },
      parseScheduledStartMutation,
    );
  }

  scheduledStarts(
    states: ScheduledStartState[] = [],
    cursor: string | null = null,
  ): Promise<ScheduledStartPage> {
    const query = new URLSearchParams({ limit: String(DASHBOARD_PAGE_LIMIT) });
    for (const state of states) query.append("state", state);
    if (cursor !== null) query.set("cursor", cursor);
    return this.#json(
      `${OPERATIONS_API_PATH}/scheduled-starts?${query.toString()}`,
      {},
      parseScheduledStartPage,
    );
  }

  scheduledStart(scheduledStartId: string): Promise<ScheduledStart> {
    return this.#json(
      `${OPERATIONS_API_PATH}/scheduled-starts/${encodeURIComponent(scheduledStartId)}`,
      {},
      parseScheduledStart,
    );
  }

  rescheduleScheduledStart(
    scheduledStartId: string,
    startAt: string,
    expectedVersion: number,
    idempotencyKey: string,
  ): Promise<ScheduledStartMutationResult> {
    return this.#scheduledStartMutation(
      scheduledStartId,
      "reschedule",
      { start_at: startAt, expected_version: expectedVersion },
      idempotencyKey,
    );
  }

  cancelScheduledStart(
    scheduledStartId: string,
    expectedVersion: number,
    idempotencyKey: string,
  ): Promise<ScheduledStartMutationResult> {
    return this.#scheduledStartMutation(
      scheduledStartId,
      "cancel",
      { expected_version: expectedVersion },
      idempotencyKey,
    );
  }

  async signalWorkflow(
    workflowId: string,
    runId: string,
    eventName: string,
    payload: unknown,
  ): Promise<void> {
    const query = new URLSearchParams({ run_id: runId });
    await this.#request(
      `/v1/workflows/${encodeURIComponent(workflowId)}/events/${encodeURIComponent(eventName)}?${query.toString()}`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ payload }),
      },
    );
  }

  async cancelRun(workflowId: string, runId: string): Promise<void> {
    await this.#executionControl(workflowId, runId, "cancel");
  }

  async terminateRun(workflowId: string, runId: string): Promise<void> {
    await this.#executionControl(workflowId, runId, "terminate");
  }

  triggers(): Promise<TriggerSummary[]> {
    return this.#json(`${OPERATIONS_API_PATH}/triggers`, {}, parseTriggers);
  }

  async pauseTrigger(triggerName: string): Promise<void> {
    await this.#triggerControl(triggerName, "pause", {});
  }

  async resumeTrigger(triggerName: string): Promise<void> {
    await this.#triggerControl(triggerName, "resume", {});
  }

  async runTrigger(triggerName: string, idempotencyKey: string): Promise<void> {
    await this.#triggerControl(triggerName, "run", {
      headers: { "x-idempotency-key": idempotencyKey },
      body: "",
    });
  }

  async deleteTrigger(triggerName: string, confirmation: string): Promise<void> {
    await this.#triggerControl(triggerName, "delete", {
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ confirmation }),
    });
  }

  configuration(cursor: string | null = null): Promise<ConfigurationView> {
    return this.#json(
      `${OPERATIONS_API_PATH}/configuration?${pageQuery(DASHBOARD_PAGE_LIMIT, cursor)}`,
      {},
      parseConfigurationView,
    );
  }

  activations(cursor: string | null = null): Promise<ActivationPage> {
    return this.#json(
      `${OPERATIONS_API_PATH}/activations?${pageQuery(DASHBOARD_PAGE_LIMIT, cursor)}`,
      {},
      parseActivationPage,
    );
  }

  authoringReference(): Promise<AuthoringReference> {
    return this.#json(`${OPERATIONS_API_PATH}/authoring-reference`, {}, parseAuthoringReference);
  }

  configurationSchema(): Promise<JsonObject> {
    return this.#json(`${OPERATIONS_API_PATH}/configuration-schema`, {}, parseConfigurationSchema);
  }

  draft(): Promise<DraftRecord> {
    return this.#json(CONFIGURATION_DRAFT_PATH, {}, parseDraft);
  }

  async draftYaml(expectedVersion?: number): Promise<string> {
    const query = expectedVersion === undefined ? "" : `?expected_version=${expectedVersion}`;
    const response = await this.#request(`${CONFIGURATION_DRAFT_PATH}/export${query}`, {});
    return response.text();
  }

  saveJsonDraft(configuration: JsonObject, current: DraftRecord | null): Promise<DraftRecord> {
    const body = current ? { expected_version: current.version, configuration } : { configuration };
    return this.#json(
      CONFIGURATION_DRAFT_PATH,
      {
        method: current ? "PUT" : "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      },
      parseDraft,
    );
  }

  saveYamlDraft(document: string, current: DraftRecord | null): Promise<DraftRecord> {
    const query = current ? `?expected_version=${current.version}` : "";
    return this.#json(
      `${CONFIGURATION_DRAFT_PATH}/import${query}`,
      {
        method: "POST",
        headers: { "content-type": "application/yaml" },
        body: document,
      },
      parseDraft,
    );
  }

  workflowFragment(name: string): Promise<WorkflowFragment> {
    return this.#json(
      `${CONFIGURATION_DRAFT_PATH}/workflows/${encodeURIComponent(name)}`,
      {},
      parseWorkflowFragment,
    );
  }

  triggersFragment(): Promise<TriggersFragment> {
    return this.#json(`${CONFIGURATION_DRAFT_PATH}/triggers`, {}, parseTriggersFragment);
  }

  saveTriggersFragment(document: string, expectedVersion: number): Promise<DraftRecord> {
    return this.#json(
      `${CONFIGURATION_DRAFT_PATH}/triggers?expected_version=${expectedVersion}`,
      {
        method: "PUT",
        headers: { "content-type": "application/yaml" },
        body: document,
      },
      parseDraft,
    );
  }

  saveWorkflowFragment(
    name: string,
    document: string,
    expectedVersion: number,
  ): Promise<DraftRecord> {
    return this.#json(
      `${CONFIGURATION_DRAFT_PATH}/workflows/${encodeURIComponent(name)}?expected_version=${expectedVersion}`,
      {
        method: "PUT",
        headers: { "content-type": "application/yaml" },
        body: document,
      },
      parseDraft,
    );
  }

  previewWorkflowFragment(
    name: string,
    document: string,
    signal?: AbortSignal,
  ): Promise<WorkflowFragmentPreview> {
    const init: RequestInit = {
      method: "POST",
      headers: { "content-type": "application/yaml" },
      body: document,
    };
    if (signal !== undefined) init.signal = signal;
    return this.#json(
      `${CONFIGURATION_DRAFT_PATH}/workflows/${encodeURIComponent(name)}/preview`,
      init,
      parseWorkflowFragmentPreview,
    );
  }

  deleteWorkflowFragment(name: string, expectedVersion: number): Promise<DraftRecord> {
    return this.#json(
      `${CONFIGURATION_DRAFT_PATH}/workflows/${encodeURIComponent(name)}?expected_version=${expectedVersion}`,
      { method: "DELETE", body: "" },
      parseDraft,
    );
  }

  validateDraft(): Promise<ValidationReport> {
    return this.#json(
      `${CONFIGURATION_DRAFT_PATH}/validate`,
      { method: "POST", body: "" },
      parseValidationReport,
    );
  }

  configurationRelationships(): Promise<ConfigurationRelationships> {
    return this.#json(
      `${CONFIGURATION_API_PATH}/relationships`,
      {},
      parseConfigurationRelationships,
    );
  }

  applyLocalConfiguration(
    version: number,
    headers: HeadersInit,
  ): Promise<LocalConfigurationApplyResult> {
    return this.#json(
      `${CONFIGURATION_API_PATH}/apply`,
      {
        method: "POST",
        headers,
        body: JSON.stringify({ expected_draft_version: version }),
      },
      parseLocalConfigurationApplyResult,
    );
  }

  discardConfiguration(
    version: number,
    activeIdentity: string,
    headers: HeadersInit,
  ): Promise<ConfigurationDiscardResult> {
    return this.#json(
      `${CONFIGURATION_DRAFT_PATH}/discard`,
      {
        method: "POST",
        headers,
        body: JSON.stringify({
          expected_draft_version: version,
          expected_active_identity: activeIdentity,
        }),
      },
      parseConfigurationDiscardResult,
    );
  }

  publishDraft(version: number, headers: HeadersInit): Promise<PublicationRecord> {
    return this.#json(
      `${CONFIGURATION_API_PATH}/publications`,
      {
        method: "POST",
        headers,
        body: JSON.stringify({ expected_draft_version: version }),
      },
      parsePublication,
    );
  }

  planActivation(revisionId: string): Promise<ActivationPlan> {
    return this.#json(
      `${CONFIGURATION_ACTIVATIONS_PATH}/plan`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ target_revision_id: revisionId }),
      },
      parseActivationPlan,
    );
  }

  activate(
    revisionId: string,
    planDigest: string,
    headers: HeadersInit,
  ): Promise<ActivationRecord> {
    return this.#json(
      CONFIGURATION_ACTIVATIONS_PATH,
      {
        method: "POST",
        headers,
        body: JSON.stringify({ target_revision_id: revisionId, plan_digest: planDigest }),
      },
      parseActivation,
    );
  }

  activation(activationId: string): Promise<ActivationRecord> {
    return this.#json(
      `${CONFIGURATION_ACTIVATIONS_PATH}/${encodeURIComponent(activationId)}`,
      {},
      parseActivation,
    );
  }

  activationReadiness(activationId: string): Promise<ActivationReadiness> {
    return this.#json(
      `${CONFIGURATION_ACTIVATIONS_PATH}/${encodeURIComponent(activationId)}/readiness`,
      {},
      parseActivationReadiness,
    );
  }

  rollback(
    activationId: string,
    planDigest: string,
    headers: HeadersInit,
  ): Promise<ActivationRecord> {
    return this.#json(
      `${CONFIGURATION_ACTIVATIONS_PATH}/${encodeURIComponent(activationId)}/rollback`,
      {
        method: "POST",
        headers,
        body: JSON.stringify({ plan_digest: planDigest }),
      },
      parseActivation,
    );
  }

  revision(revisionId: string): Promise<RevisionRecord> {
    return this.#json(
      `${CONFIGURATION_API_PATH}/revisions/${encodeURIComponent(revisionId)}`,
      {},
      parseRevisionRecord,
    );
  }

  difference(source: string, target: string): Promise<ConfigurationDifference> {
    const query = new URLSearchParams({
      source_revision_id: source,
      target_revision_id: target,
    });
    return this.#json(
      `${CONFIGURATION_API_PATH}/revisions/compare?${query.toString()}`,
      {},
      parseDifference,
    );
  }

  async #executionControl(
    workflowId: string,
    runId: string,
    operation: "cancel" | "terminate",
  ): Promise<void> {
    const query = new URLSearchParams({ run_id: runId });
    await this.#request(
      `/v1/workflows/${encodeURIComponent(workflowId)}/${operation}?${query.toString()}`,
      { method: "POST", body: "" },
    );
  }

  async #triggerControl(
    triggerName: string,
    operation: "pause" | "resume" | "run" | "delete",
    init: RequestInit,
  ): Promise<void> {
    await this.#request(`/v1/triggers/${encodeURIComponent(triggerName)}/${operation}`, {
      method: "POST",
      ...init,
    });
  }

  #scheduledStartMutation(
    scheduledStartId: string,
    mutation: "reschedule" | "cancel",
    body: JsonObject,
    idempotencyKey: string,
  ): Promise<ScheduledStartMutationResult> {
    return this.#json(
      `/v1/scheduled-starts/${encodeURIComponent(scheduledStartId)}/${mutation}`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-idempotency-key": idempotencyKey,
        },
        body: JSON.stringify(body),
      },
      parseScheduledStartMutation,
    );
  }

  /** Reconcile declared schedule triggers into managed Temporal schedules. */
  async applyTriggers(): Promise<void> {
    await this.#request("/v1/triggers/apply", { method: "POST", body: "" });
  }

  async #json<Result>(
    path: string,
    init: RequestInit,
    parser: JsonParser<Result>,
  ): Promise<Result> {
    const response = await this.#request(path, init);
    return parser(await response.json());
  }

  async #request(path: string, init: RequestInit): Promise<Response> {
    const response = await this.#fetcher(path, {
      credentials: "same-origin",
      cache: "no-store",
      ...init,
    });
    if (response.ok) return response;
    const contentType = response.headers.get("content-type") ?? "";
    const body: unknown = contentType.includes("json")
      ? await response.json()
      : await response.text();
    throw new ApiRequestError(
      errorMessage(body, response.status),
      response.status,
      errorCode(body),
    );
  }
}

function pageQuery(limit: number, cursor: string | null): string {
  const query = new URLSearchParams({ limit: String(limit) });
  if (cursor !== null) query.set("cursor", cursor);
  return query.toString();
}
