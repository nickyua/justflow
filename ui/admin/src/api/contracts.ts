export type JsonObject = Record<string, unknown>;

const HTTP_PROTOCOLS = new Set(["http:", "https:"]);
const HEALTH_COMPONENT_NAMES = [
  "catalog",
  "temporal",
  "worker",
  "trigger_consumer",
  "response_consumer",
  "providers",
] as const;
const HEALTH_STATUSES = ["ready", "unavailable"] as const;
const ACTIVATION_STATES = [
  "pending",
  "running",
  "waiting_for_readiness",
  "applied",
  "failed",
  "superseded",
  "rolled_back",
] as const;
const CONFIGURATION_MODES = ["managed", "local_source", "unavailable"] as const;
const CONFIGURATION_RELATIONSHIP_KINDS = ["workflow", "trigger"] as const;
const CONFIGURATION_RELATIONSHIP_STATES = [
  "active",
  "modified",
  "new_pending_apply",
  "removed_pending_apply",
] as const;
const CONFIGURATION_APPLY_STAGE_KINDS = [
  "validation",
  "definition_publication",
  "publication",
  "plan_confirmation",
  "activation",
  "readiness",
  "process_restart",
] as const;
const CONFIGURATION_APPLY_STAGE_STATES = [
  "completed",
  "pending",
  "restart_required",
  "not_applicable",
] as const;
const VALIDATION_SEVERITIES = ["error", "warning"] as const;
const AUTHORING_REFERENCE_AUTHORITIES = ["immutable_catalog", "observed_host"] as const;
const AUTHORING_REFERENCE_AVAILABILITIES = ["available", "unavailable"] as const;
const AUTHORING_REFERENCE_COMPONENT_KINDS = ["step", "trigger"] as const;
const TRIGGER_KINDS = ["api", "schedule", "webhook", "event", "broker", "host"] as const;
const TRIGGER_OPERATIONAL_STATES = ["active", "inactive"] as const;
const SCHEDULED_START_STATES = [
  "scheduled",
  "dispatching",
  "started",
  "canceled",
  "failed",
] as const;
const SCHEDULED_START_WORKLOAD_CLASSES = ["interactive", "standard", "batch"] as const;
const SCHEDULED_START_MUTATION_STATUSES = [
  "accepted",
  "duplicate",
  "rescheduled",
  "canceled",
  "in_progress",
] as const;
const ADMIN_CLIENT_COMPATIBILITY_VERSION = 1;
const MINIMUM_SUPPORTED_API_VERSION = 1;
const MAXIMUM_SUPPORTED_API_VERSION = 1;
export const INCOMPATIBLE_API_MESSAGE =
  "This beta console is not compatible with the installed Justflow API. Upgrade justflow and justflow-admin together.";

export interface ScopeLabels {
  tenant: string;
  application: string;
  environment: string;
}

export interface Capabilities {
  api_compatibility: ApiCompatibility;
  operations_view: boolean;
  scope: ScopeLabels;
  configuration_mode: ConfigurationMode;
  workflow_start: boolean;
  workflow_signal: boolean;
  workflow_cancel: boolean;
  workflow_terminate: boolean;
  trigger_pause: boolean;
  trigger_resume: boolean;
  trigger_run: boolean;
  trigger_delete: boolean;
  trigger_apply: boolean;
  scheduled_start_create: boolean;
  scheduled_start_view: boolean;
  scheduled_start_reschedule: boolean;
  scheduled_start_cancel: boolean;
  configuration_view: boolean;
  configuration_edit: boolean;
  configuration_validate: boolean;
  configuration_apply: boolean;
  configuration_discard: boolean;
  configuration_publish: boolean;
  configuration_activate: boolean;
  configuration_rollback: boolean;
}

export interface ApiCompatibility {
  current_version: number;
  minimum_supported_client_version: number;
  maximum_supported_client_version: number;
}

export interface MetricsLink {
  label: string;
  url: string;
}

export type HealthComponentName = (typeof HEALTH_COMPONENT_NAMES)[number];

export type HealthStatus = (typeof HEALTH_STATUSES)[number];

export interface ComponentHealth {
  component: HealthComponentName;
  status: HealthStatus;
  required: boolean;
}

export interface OperationsOverview {
  ready: boolean;
  components: ComponentHealth[];
  metricsLinks: MetricsLink[];
}

export interface WorkflowRun {
  workflowId: string;
  runId: string;
  workflowType: string;
  status: string | null;
  startTime: string;
  closeTime: string | null;
}

export interface WorkflowRunDetail extends WorkflowRun {
  logicalWorkflow: string | null;
  definitionDigest: string | null;
  deploymentName: string | null;
  buildId: string | null;
  artifactDigest: string | null;
  configurationRevisionId: string | null;
  triggerSource: string | null;
  pendingWaits: { kind: string; state: string }[];
  pendingWaitsTruncated: boolean;
  firstRunId: string | null;
  nextRunId: string | null;
  failureCode: string | null;
  failureCauseCode: string | null;
  failureCategory: string | null;
  failurePhase: string | null;
  failureRetryable: boolean | null;
  failedStep: string | null;
}

export interface WorkflowRegistration {
  logicalWorkflow: string;
  activeDefinitionDigest: string;
  retainedDefinitionCount: number;
}

export interface WorkflowRegistrationPage {
  workflows: WorkflowRegistration[];
  nextCursor: string | null;
}

export interface WorkflowRunPage {
  runs: WorkflowRun[];
  nextCursor: string | null;
}

export interface WorkflowGraphNode {
  nodeId: string;
  label: string;
  kind: string;
  group: string | null;
  metadata: Record<string, string>;
}

export interface WorkflowGraphEdge {
  source: string;
  target: string;
  label: string | null;
  dashed: boolean;
}

export interface WorkflowServiceDependency {
  service: string;
  actions: string[];
}

export interface WorkflowStepBinding {
  operation: string;
  service: string | null;
  action: string | null;
  subworkflow: string | null;
  resources: string[];
}

export interface WorkflowArchivalBinding {
  resource: string;
  retentionPolicy: string;
}

export interface WorkflowDetail extends WorkflowRegistration {
  description: string;
  requiredEngineWorkflowAbi: string;
  graphNodes: WorkflowGraphNode[];
  graphEdges: WorkflowGraphEdge[];
  definitions: DefinitionSummary[];
  serviceDependencies: WorkflowServiceDependency[];
  resourceDependencies: string[];
  hasInputContract: boolean;
  hasOutputContract: boolean;
  inputSchema: JsonObject | null;
  referencedGlobals: string[];
  stepBindings: WorkflowStepBinding[];
  archival: WorkflowArchivalBinding | null;
}

export interface WorkflowDefinitionDocument {
  logicalWorkflow: string;
  definitionDigest: string;
  document: string;
}

export interface DefinitionSummary {
  logicalWorkflow: string;
  definitionDigest: string;
  active: boolean;
}

export interface ScheduleRecentAction {
  startedAt: string;
  outcome: string;
  workflowId: string;
  runId: string;
}

export type ScheduledStartState = (typeof SCHEDULED_START_STATES)[number];
export type ScheduledStartWorkloadClass = (typeof SCHEDULED_START_WORKLOAD_CLASSES)[number];
export type ScheduledStartMutationStatus = (typeof SCHEDULED_START_MUTATION_STATUSES)[number];

export interface ScheduledStart {
  scheduledStartId: string;
  workflowName: string;
  triggerName: string;
  startAt: string;
  workloadClass: ScheduledStartWorkloadClass;
  state: ScheduledStartState;
  version: number;
  acceptedAt: string;
  updatedAt: string;
  dispatchStartedAt: string | null;
  completedAt: string | null;
  failureCode: string | null;
  workflowId: string | null;
  runId: string | null;
  definitionDigest: string | null;
  artifactBuildId: string | null;
}

export interface ScheduledStartPage {
  scheduledStarts: ScheduledStart[];
  nextCursor: string | null;
}

export interface ScheduledStartMutationResult {
  status: ScheduledStartMutationStatus;
  scheduledStart: ScheduledStart;
  expectedVersion: number | null;
}

export type TriggerKind = (typeof TRIGGER_KINDS)[number];
export type TriggerOperationalState = (typeof TRIGGER_OPERATIONAL_STATES)[number];

export interface TriggerSummary {
  name: string;
  kind: TriggerKind;
  state: TriggerOperationalState;
  workflowName: string;
  desiredDigest: string | null;
  definitionDigest: string | null;
  overlapPolicy: string | null;
  nextRunTimes: string[];
  lastAction: ScheduleRecentAction | null;
}

export interface RevisionSummary {
  revisionId: string;
  parentRevisionId: string | null;
  createdAt: string;
}

export interface ConfigurationView {
  activeRevisionId: string | null;
  revisions: RevisionSummary[];
  revisionsNextCursor: string | null;
}

export interface ActivationSummary {
  activationId: string;
  targetRevisionId: string;
  state: ActivationState;
  updatedAt: string;
}

export interface ActivationPage {
  activations: ActivationSummary[];
  nextCursor: string | null;
}

export interface DraftRecord {
  version: number;
  bundle: JsonObject;
  restartRequired: boolean;
}

export type ConfigurationRelationshipKind = (typeof CONFIGURATION_RELATIONSHIP_KINDS)[number];
export type ConfigurationRelationshipState = (typeof CONFIGURATION_RELATIONSHIP_STATES)[number];

export interface ConfigurationRelationship {
  kind: ConfigurationRelationshipKind;
  name: string;
  state: ConfigurationRelationshipState;
}

export interface ConfigurationRelationships {
  workingVersion: number;
  activeIdentity: string | null;
  relationships: ConfigurationRelationship[];
  restartRequired: boolean;
}

export interface LocalConfigurationApplyResult {
  mode: "local_source";
  workingVersion: number;
  stages: {
    kind: (typeof CONFIGURATION_APPLY_STAGE_KINDS)[number];
    state: (typeof CONFIGURATION_APPLY_STAGE_STATES)[number];
  }[];
  definitionsPublished: boolean;
  restartRequired: boolean;
  runningProcessChanged: false;
}

export interface ConfigurationDiscardResult {
  discardId: string;
  workingVersion: number;
  activeIdentity: string;
  restartRequired: boolean;
  runningProcessChanged: false;
}

export interface AuthoringReferenceAction {
  name: string;
  workflows: string[];
  inputSchema: JsonObject | null;
  inputContractIdentity: string | null;
  outputSchema: JsonObject | null;
  outputContractIdentity: string | null;
  contractConflict: boolean;
}

export interface AuthoringReferenceService {
  name: string;
  description: string | null;
  actions: AuthoringReferenceAction[];
}

export interface AuthoringReferenceResource {
  name: string;
  description: string | null;
  capabilities: string[];
}

export interface AuthoringReferenceComponent {
  kind: (typeof AUTHORING_REFERENCE_COMPONENT_KINDS)[number];
  name: string;
  version: string;
  description: string | null;
  action: string | null;
  capabilities: string[];
  parameterSchema: JsonObject | null;
  parameterContractIdentity: string | null;
  inputSchema: JsonObject | null;
  inputContractIdentity: string | null;
  outputSchema: JsonObject | null;
  outputContractIdentity: string | null;
  resourceSlots: { name: string; capability: string }[];
}

export interface AuthoringReferenceTriggerBinding {
  name: string;
  kind: string;
}

export interface AuthoringReference {
  availability: (typeof AUTHORING_REFERENCE_AVAILABILITIES)[number];
  unavailableReason: string | null;
  authority: (typeof AUTHORING_REFERENCE_AUTHORITIES)[number];
  catalogRevision: string | null;
  components: AuthoringReferenceComponent[];
  services: AuthoringReferenceService[];
  resources: AuthoringReferenceResource[];
  triggerBindings: AuthoringReferenceTriggerBinding[];
  truncated: boolean;
}

export type WorkflowFragmentPreview =
  | {
      status: "graph_ready";
      draftValidated: false;
      diagnostics: ValidationIssue[];
      graphNodes: WorkflowGraphNode[];
      graphEdges: WorkflowGraphEdge[];
    }
  | {
      status: "invalid_fragment";
      draftValidated: false;
      diagnostics: ValidationIssue[];
    };

export interface WorkflowFragment {
  workflow: string;
  version: number;
  document: string;
}

export type ValidationSeverity = (typeof VALIDATION_SEVERITIES)[number];

export interface ValidationIssue {
  severity: ValidationSeverity;
  category: string;
  location: string[];
  message: string;
}

export interface ValidationReport {
  valid: boolean;
  issues: ValidationIssue[];
}

export interface PublicationRecord {
  publishedRevisionId: string | null;
}

export interface ActivationPlan {
  planDigest: string;
  expectedActiveRevisionId: string | null;
}

export interface ActivationRecord {
  activationId: string;
  state: ActivationState;
  plan: ActivationPlan;
}

export interface ActivationReadiness {
  activationId: string;
  ready: boolean;
  state: ActivationState;
  targetRevisionId: string;
  workerReadinessRegistered: boolean;
  completedCheckpoints: string[];
}

export type ActivationState = (typeof ACTIVATION_STATES)[number];
export type ConfigurationMode = (typeof CONFIGURATION_MODES)[number];

export interface ConfigurationDifferenceItem {
  operation: string;
  path: string[];
}

export interface ConfigurationDifference {
  items: ConfigurationDifferenceItem[];
}

export interface RevisionRecord {
  revisionId: string;
  parentRevisionId: string | null;
  createdAt: string;
  bundle: JsonObject;
}

export interface StartRunResult {
  workflowId: string;
  runId: string | null;
  duplicate: boolean;
}

export function parseStartRunResult(value: unknown): StartRunResult {
  const record = objectValue(value, "start result");
  const runId = record.run_id;
  return {
    workflowId: stringField(record, "workflow_id"),
    runId: runId === null || runId === undefined ? null : stringValue(runId, "run_id"),
    duplicate: stringField(record, "status") === "duplicate",
  };
}

export function parseCapabilities(value: unknown): Capabilities {
  const record = objectValue(value, "capabilities");
  const scope = objectField(record, "scope");
  return {
    api_compatibility: compatibilityField(record),
    operations_view: booleanField(record, "operations_view"),
    scope: {
      tenant: stringField(scope, "tenant"),
      application: stringField(scope, "application"),
      environment: stringField(scope, "environment"),
    },
    configuration_mode: enumField(record, "configuration_mode", CONFIGURATION_MODES),
    workflow_start: booleanField(record, "workflow_start"),
    workflow_signal: booleanField(record, "workflow_signal"),
    workflow_cancel: booleanField(record, "workflow_cancel"),
    workflow_terminate: booleanField(record, "workflow_terminate"),
    trigger_pause: booleanField(record, "trigger_pause"),
    trigger_resume: booleanField(record, "trigger_resume"),
    trigger_run: booleanField(record, "trigger_run"),
    trigger_delete: booleanField(record, "trigger_delete"),
    trigger_apply: booleanField(record, "trigger_apply"),
    scheduled_start_create: booleanField(record, "scheduled_start_create"),
    scheduled_start_view: booleanField(record, "scheduled_start_view"),
    scheduled_start_reschedule: booleanField(record, "scheduled_start_reschedule"),
    scheduled_start_cancel: booleanField(record, "scheduled_start_cancel"),
    configuration_view: booleanField(record, "configuration_view"),
    configuration_edit: booleanField(record, "configuration_edit"),
    configuration_validate: booleanField(record, "configuration_validate"),
    configuration_apply: booleanField(record, "configuration_apply"),
    configuration_discard: booleanField(record, "configuration_discard"),
    configuration_publish: booleanField(record, "configuration_publish"),
    configuration_activate: booleanField(record, "configuration_activate"),
    configuration_rollback: booleanField(record, "configuration_rollback"),
  };
}

export function parseScheduledStartPage(value: unknown): ScheduledStartPage {
  const record = objectValue(value, "scheduled starts");
  return {
    scheduledStarts: arrayField(record, "scheduled_starts").map(parseScheduledStart),
    nextCursor: nullableStringField(record, "next_cursor"),
  };
}

export function parseScheduledStart(value: unknown): ScheduledStart {
  const record = objectValue(value, "scheduled start");
  const run = nullableObjectField(record, "run");
  const artifact = run === null ? null : nullableObjectField(run, "artifact_identity");
  return {
    scheduledStartId: stringField(record, "scheduled_start_id"),
    workflowName: stringField(record, "workflow_name"),
    triggerName: stringField(record, "trigger_name"),
    startAt: stringField(record, "start_at"),
    workloadClass: enumField(record, "workload_class", SCHEDULED_START_WORKLOAD_CLASSES),
    state: enumField(record, "state", SCHEDULED_START_STATES),
    version: numberField(record, "version"),
    acceptedAt: stringField(record, "accepted_at"),
    updatedAt: stringField(record, "updated_at"),
    dispatchStartedAt: nullableStringField(record, "dispatch_started_at"),
    completedAt: nullableStringField(record, "completed_at"),
    failureCode: nullableStringField(record, "failure_code"),
    workflowId: nullableObjectStringField(run, "workflow_id"),
    runId: nullableObjectStringField(run, "run_id"),
    definitionDigest: nullableObjectStringField(run, "definition_digest"),
    artifactBuildId: nullableObjectStringField(artifact, "build_id"),
  };
}

export function parseScheduledStartMutation(value: unknown): ScheduledStartMutationResult {
  const record = objectValue(value, "scheduled-start mutation");
  return {
    status: enumField(record, "status", SCHEDULED_START_MUTATION_STATUSES),
    scheduledStart: parseScheduledStart(objectField(record, "scheduled_start")),
    expectedVersion: nullableNumberField(record, "expected_version"),
  };
}

function compatibilityField(record: JsonObject): ApiCompatibility {
  try {
    const compatibility = objectField(record, "api_compatibility");
    const currentVersion = numberField(compatibility, "current_version");
    const minimumClientVersion = numberField(compatibility, "minimum_supported_client_version");
    const maximumClientVersion = numberField(compatibility, "maximum_supported_client_version");
    const apiSupported =
      currentVersion >= MINIMUM_SUPPORTED_API_VERSION &&
      currentVersion <= MAXIMUM_SUPPORTED_API_VERSION;
    const clientSupported =
      ADMIN_CLIENT_COMPATIBILITY_VERSION >= minimumClientVersion &&
      ADMIN_CLIENT_COMPATIBILITY_VERSION <= maximumClientVersion;
    if (!apiSupported || !clientSupported || minimumClientVersion > maximumClientVersion) {
      throw new Error(INCOMPATIBLE_API_MESSAGE);
    }
    return {
      current_version: currentVersion,
      minimum_supported_client_version: minimumClientVersion,
      maximum_supported_client_version: maximumClientVersion,
    };
  } catch {
    throw new Error(INCOMPATIBLE_API_MESSAGE);
  }
}

export function parseOperationsOverview(value: unknown): OperationsOverview {
  const record = objectValue(value, "operations overview");
  const health = objectField(record, "health");
  return {
    ready: booleanField(health, "ready"),
    components: arrayField(health, "components").map((item) => {
      const component = objectValue(item, "health component");
      return {
        component: healthComponentField(component, "component"),
        status: healthStatusField(component, "status"),
        required: booleanField(component, "required"),
      };
    }),
    metricsLinks: arrayField(record, "metrics_links").map((item) => {
      const link = objectValue(item, "metrics link");
      return { label: stringField(link, "label"), url: httpUrlField(link, "url") };
    }),
  };
}

export function parseWorkflowRunPage(value: unknown): WorkflowRunPage {
  const record = objectValue(value, "workflow runs");
  return {
    runs: arrayField(record, "workflows").map((item) => {
      const run = objectValue(item, "workflow run");
      return {
        workflowId: stringField(run, "workflow_id"),
        runId: stringField(run, "run_id"),
        workflowType: stringField(run, "workflow_type"),
        status: nullableStringField(run, "status"),
        startTime: stringField(run, "start_time"),
        closeTime: nullableStringField(run, "close_time"),
      };
    }),
    nextCursor: nullableStringField(record, "next_page_token"),
  };
}

export function parseWorkflowRunDetail(value: unknown): WorkflowRunDetail {
  const run = objectValue(value, "workflow run detail");
  const artifact = nullableObjectField(run, "artifact_identity");
  const configuration = nullableObjectField(run, "execution_configuration");
  const continuation = nullableObjectField(run, "continuation");
  const failure = nullableObjectField(run, "failure");
  return {
    workflowId: stringField(run, "workflow_id"),
    runId: stringField(run, "run_id"),
    workflowType: stringField(run, "workflow_type"),
    status: nullableStringField(run, "status"),
    startTime: stringField(run, "start_time"),
    closeTime: nullableStringField(run, "close_time"),
    logicalWorkflow: nullableStringField(run, "logical_workflow"),
    definitionDigest: nullableStringField(run, "definition_digest"),
    deploymentName: nullableObjectStringField(artifact, "deployment_name"),
    buildId: nullableObjectStringField(artifact, "build_id"),
    artifactDigest: nullableObjectStringField(artifact, "artifact_digest"),
    configurationRevisionId: nullableObjectStringField(configuration, "configuration_revision_id"),
    triggerSource: nullableStringField(run, "trigger_source"),
    pendingWaits: arrayField(run, "pending_waits").map((item) => {
      const wait = objectValue(item, "pending wait");
      return { kind: stringField(wait, "kind"), state: stringField(wait, "state") };
    }),
    pendingWaitsTruncated: booleanField(run, "pending_waits_truncated"),
    firstRunId: nullableObjectStringField(continuation, "first_run_id"),
    nextRunId: nullableObjectNullableStringField(continuation, "next_run_id"),
    failureCode: nullableObjectStringField(failure, "code"),
    failureCauseCode: nullableObjectNullableStringField(failure, "cause_code"),
    failureCategory: nullableObjectNullableStringField(failure, "category"),
    failurePhase: nullableObjectNullableStringField(failure, "phase"),
    failureRetryable: nullableObjectBooleanField(failure, "retryable"),
    failedStep: nullableObjectNullableStringField(failure, "step"),
  };
}

export function parseWorkflowRegistrationPage(value: unknown): WorkflowRegistrationPage {
  const record = objectValue(value, "workflow registrations");
  return {
    workflows: arrayField(record, "workflows").map((item) => {
      const workflow = objectValue(item, "workflow registration");
      return {
        logicalWorkflow: stringField(workflow, "logical_workflow"),
        activeDefinitionDigest: stringField(workflow, "active_definition_digest"),
        retainedDefinitionCount: numberField(workflow, "retained_definition_count"),
      };
    }),
    nextCursor: nullableStringField(record, "next_cursor"),
  };
}

export function parseWorkflowDetail(value: unknown): WorkflowDetail {
  const workflow = objectValue(value, "workflow detail");
  return {
    logicalWorkflow: stringField(workflow, "logical_workflow"),
    activeDefinitionDigest: stringField(workflow, "active_definition_digest"),
    retainedDefinitionCount: numberField(workflow, "retained_definition_count"),
    requiredEngineWorkflowAbi: stringField(workflow, "required_engine_workflow_abi"),
    description: stringField(workflow, "description"),
    graphNodes: arrayField(workflow, "graph_nodes").map((item) => {
      const node = objectValue(item, "workflow graph node");
      return {
        nodeId: stringField(node, "node_id"),
        label: stringField(node, "label"),
        kind: stringField(node, "kind"),
        group: nullableStringField(node, "group"),
        metadata: stringMapField(node, "metadata"),
      };
    }),
    graphEdges: arrayField(workflow, "graph_edges").map((item) => {
      const edge = objectValue(item, "workflow graph edge");
      return {
        source: stringField(edge, "source"),
        target: stringField(edge, "target"),
        label: nullableStringField(edge, "label"),
        dashed: booleanField(edge, "dashed"),
      };
    }),
    definitions: arrayField(workflow, "definitions").map((item) => {
      const definition = objectValue(item, "workflow definition");
      return {
        logicalWorkflow: stringField(definition, "logical_workflow"),
        definitionDigest: stringField(definition, "definition_digest"),
        active: booleanField(definition, "active"),
      };
    }),
    serviceDependencies: arrayField(workflow, "service_dependencies").map((item) => {
      const dependency = objectValue(item, "service dependency");
      return {
        service: stringField(dependency, "service"),
        actions: arrayField(dependency, "actions").map((action) =>
          stringValue(action, "service dependency action"),
        ),
      };
    }),
    resourceDependencies: arrayField(workflow, "resource_dependencies").map((resource) =>
      stringValue(resource, "resource dependency"),
    ),
    hasInputContract: booleanField(workflow, "has_input_contract"),
    hasOutputContract: booleanField(workflow, "has_output_contract"),
    inputSchema: workflow.input_schema === null ? null : objectField(workflow, "input_schema"),
    referencedGlobals: arrayField(workflow, "referenced_globals").map((name) =>
      stringValue(name, "referenced global"),
    ),
    stepBindings: arrayField(workflow, "step_bindings").map((item) => {
      const binding = objectValue(item, "workflow step binding");
      return {
        operation: stringField(binding, "operation"),
        service: nullableStringField(binding, "service"),
        action: nullableStringField(binding, "action"),
        subworkflow: nullableStringField(binding, "subworkflow"),
        resources: arrayField(binding, "resources").map((resource) =>
          stringValue(resource, "step binding resource"),
        ),
      };
    }),
    archival: parseArchival(workflow),
  };
}

function parseArchival(workflow: JsonObject): WorkflowArchivalBinding | null {
  if (workflow.archival === null || workflow.archival === undefined) return null;
  const archival = objectField(workflow, "archival");
  return {
    resource: stringField(archival, "resource"),
    retentionPolicy: stringField(archival, "retention_policy"),
  };
}

export function parseWorkflowDefinitionDocument(value: unknown): WorkflowDefinitionDocument {
  const record = objectValue(value, "workflow definition document");
  return {
    logicalWorkflow: stringField(record, "logical_workflow"),
    definitionDigest: stringField(record, "definition_digest"),
    document: stringField(record, "document"),
  };
}

export function parseTriggers(value: unknown): TriggerSummary[] {
  const record = objectValue(value, "triggers");
  return arrayField(record, "triggers").map((item) => {
    const trigger = objectValue(item, "trigger");
    const schedule = trigger.schedule === null ? null : objectField(trigger, "schedule");
    return {
      name: stringField(trigger, "name"),
      kind: enumField(trigger, "kind", TRIGGER_KINDS),
      state: enumField(trigger, "state", TRIGGER_OPERATIONAL_STATES),
      workflowName: stringField(trigger, "workflow_name"),
      desiredDigest: schedule === null ? null : stringField(schedule, "desired_digest"),
      definitionDigest: schedule === null ? null : stringField(schedule, "definition_digest"),
      overlapPolicy: schedule === null ? null : stringField(schedule, "overlap_policy"),
      nextRunTimes:
        schedule === null
          ? []
          : arrayField(schedule, "next_run_times").map((time) =>
              stringValue(time, "schedule-trigger next-run time"),
            ),
      lastAction: schedule === null ? null : parseLastAction(schedule),
    };
  });
}

function parseLastAction(schedule: JsonObject): ScheduleRecentAction | null {
  const actions = arrayField(schedule, "recent_actions");
  const last = actions[actions.length - 1];
  if (last === undefined) return null;
  const action = objectValue(last, "schedule recent action");
  return {
    startedAt: stringField(action, "started_at"),
    outcome: stringField(action, "outcome"),
    workflowId: stringField(action, "workflow_id"),
    runId: stringField(action, "run_id"),
  };
}

export function parseConfigurationView(value: unknown): ConfigurationView {
  const record = objectValue(value, "configuration view");
  const active = record.active === null ? null : objectValue(record.active, "active revision");
  const revisionsPage = objectField(record, "revisions");
  return {
    activeRevisionId: active === null ? null : stringField(active, "revision_id"),
    revisionsNextCursor: nullableStringField(revisionsPage, "next_cursor"),
    revisions: arrayField(revisionsPage, "revisions").map((item) => {
      const revision = objectValue(item, "revision");
      return {
        revisionId: stringField(revision, "revision_id"),
        parentRevisionId: nullableStringField(revision, "parent_revision_id"),
        createdAt: stringField(revision, "created_at"),
      };
    }),
  };
}

export function parseActivationPage(value: unknown): ActivationPage {
  const record = objectValue(value, "activations view");
  const page = objectField(record, "activations");
  return {
    activations: arrayField(page, "activations").map((item) => {
      const activation = objectValue(item, "activation");
      return {
        activationId: stringField(activation, "activation_id"),
        targetRevisionId: stringField(activation, "target_revision_id"),
        state: activationStateField(activation, "state"),
        updatedAt: stringField(activation, "updated_at"),
      };
    }),
    nextCursor: nullableStringField(page, "next_cursor"),
  };
}

export function parseConfigurationSchema(value: unknown): JsonObject {
  return objectField(objectValue(value, "configuration schema response"), "schema_document");
}

export function parseAuthoringReference(value: unknown): AuthoringReference {
  const record = objectValue(value, "authoring reference");
  const dimension = objectField(record, "dimension");
  return {
    availability: enumField(dimension, "availability", AUTHORING_REFERENCE_AVAILABILITIES),
    unavailableReason: nullableStringField(dimension, "reason"),
    authority: enumField(record, "authority", AUTHORING_REFERENCE_AUTHORITIES),
    catalogRevision: nullableStringField(record, "catalog_revision"),
    components: arrayField(record, "components").map(parseAuthoringReferenceComponent),
    services: arrayField(record, "services").map((item) => {
      const service = objectValue(item, "authoring reference service");
      return {
        name: stringField(service, "name"),
        description: nullableStringField(service, "description"),
        actions: arrayField(service, "actions").map((entry) => {
          const action = objectValue(entry, "authoring reference action");
          return {
            name: stringField(action, "name"),
            workflows: arrayField(action, "workflows").map((workflow) =>
              stringValue(workflow, "authoring reference action workflow"),
            ),
            inputSchema: action.input_schema === null ? null : objectField(action, "input_schema"),
            inputContractIdentity: nullableStringField(action, "input_contract_identity"),
            outputSchema:
              action.output_schema === null ? null : objectField(action, "output_schema"),
            outputContractIdentity: nullableStringField(action, "output_contract_identity"),
            contractConflict: booleanField(action, "contract_conflict"),
          };
        }),
      };
    }),
    resources: arrayField(record, "resources").map((item) => {
      const resource = objectValue(item, "authoring reference resource");
      return {
        name: stringField(resource, "name"),
        description: nullableStringField(resource, "description"),
        capabilities: arrayField(resource, "capabilities").map((capability) =>
          stringValue(capability, "resource capability"),
        ),
      };
    }),
    triggerBindings: arrayField(record, "trigger_bindings").map((item) => {
      const binding = objectValue(item, "authoring reference trigger binding");
      return { name: stringField(binding, "name"), kind: stringField(binding, "kind") };
    }),
    truncated: booleanField(record, "truncated"),
  };
}

function parseAuthoringReferenceComponent(value: unknown): AuthoringReferenceComponent {
  const component = objectValue(value, "authoring reference component");
  return {
    kind: enumField(component, "kind", AUTHORING_REFERENCE_COMPONENT_KINDS),
    name: stringField(component, "name"),
    version: stringField(component, "version"),
    description: nullableStringField(component, "description"),
    action: nullableStringField(component, "action"),
    capabilities: arrayField(component, "capabilities").map((capability) =>
      stringValue(capability, "component capability"),
    ),
    parameterSchema:
      component.parameter_schema === null ? null : objectField(component, "parameter_schema"),
    parameterContractIdentity: nullableStringField(component, "parameter_contract_identity"),
    inputSchema: component.input_schema === null ? null : objectField(component, "input_schema"),
    inputContractIdentity: nullableStringField(component, "input_contract_identity"),
    outputSchema: component.output_schema === null ? null : objectField(component, "output_schema"),
    outputContractIdentity: nullableStringField(component, "output_contract_identity"),
    resourceSlots: arrayField(component, "resource_slots").map((item) => {
      const slot = objectValue(item, "component resource slot");
      return { name: stringField(slot, "name"), capability: stringField(slot, "capability") };
    }),
  };
}

export function parseWorkflowFragmentPreview(value: unknown): WorkflowFragmentPreview {
  const record = objectValue(value, "workflow fragment preview");
  if (record.draft_validated !== false) {
    throw new TypeError("Fragment previews must state that the draft is not validated");
  }
  const diagnostics = arrayField(record, "diagnostics").map((item) => {
    const issue = objectValue(item, "preview diagnostic");
    return {
      severity: enumField(issue, "severity", VALIDATION_SEVERITIES),
      category: stringField(issue, "category"),
      location: arrayField(issue, "location").map((part) =>
        stringValue(part, "preview diagnostic location"),
      ),
      message: stringField(issue, "message"),
    };
  });
  const status = stringField(record, "status");
  if (status === "invalid_fragment") {
    return { status, draftValidated: false, diagnostics };
  }
  if (status !== "graph_ready") {
    throw new TypeError("Fragment preview status is unsupported");
  }
  return {
    status,
    draftValidated: false,
    diagnostics,
    graphNodes: arrayField(record, "graph_nodes").map((item) => {
      const node = objectValue(item, "preview graph node");
      return {
        nodeId: stringField(node, "node_id"),
        label: stringField(node, "label"),
        kind: stringField(node, "kind"),
        group: nullableStringField(node, "group"),
        metadata: stringMapField(node, "metadata"),
      };
    }),
    graphEdges: arrayField(record, "graph_edges").map((item) => {
      const edge = objectValue(item, "preview graph edge");
      return {
        source: stringField(edge, "source"),
        target: stringField(edge, "target"),
        label: nullableStringField(edge, "label"),
        dashed: booleanField(edge, "dashed"),
      };
    }),
  };
}

export function parseWorkflowFragment(value: unknown): WorkflowFragment {
  const record = objectValue(value, "workflow fragment");
  return {
    workflow: stringField(record, "workflow"),
    version: numberField(record, "version"),
    document: stringField(record, "document"),
  };
}

export interface TriggersFragment {
  version: number;
  document: string;
}

export function parseTriggersFragment(value: unknown): TriggersFragment {
  const record = objectValue(value, "triggers fragment");
  return {
    version: numberField(record, "version"),
    document: stringField(record, "document"),
  };
}

export function parseDraft(value: unknown): DraftRecord {
  const record = objectValue(value, "configuration draft");
  return {
    version: numberField(record, "version"),
    bundle: objectField(record, "bundle"),
    restartRequired: typeof record.restart_required === "boolean" ? record.restart_required : false,
  };
}

export function parseConfigurationRelationships(value: unknown): ConfigurationRelationships {
  const record = objectValue(value, "configuration relationships");
  return {
    workingVersion: numberField(record, "working_version"),
    activeIdentity: nullableStringField(record, "active_identity"),
    relationships: arrayField(record, "relationships").map((item) => {
      const relationship = objectValue(item, "configuration relationship");
      return {
        kind: enumField(relationship, "kind", CONFIGURATION_RELATIONSHIP_KINDS),
        name: stringField(relationship, "name"),
        state: enumField(relationship, "state", CONFIGURATION_RELATIONSHIP_STATES),
      };
    }),
    restartRequired: booleanField(record, "restart_required"),
  };
}

export function parseLocalConfigurationApplyResult(value: unknown): LocalConfigurationApplyResult {
  const record = objectValue(value, "local configuration apply result");
  if (record.mode !== "local_source" || record.running_process_changed !== false) {
    throw new TypeError("Local Apply result has an unsupported runtime outcome");
  }
  return {
    mode: "local_source",
    workingVersion: numberField(record, "working_version"),
    stages: arrayField(record, "stages").map((item) => {
      const stage = objectValue(item, "configuration Apply stage");
      return {
        kind: enumField(stage, "kind", CONFIGURATION_APPLY_STAGE_KINDS),
        state: enumField(stage, "state", CONFIGURATION_APPLY_STAGE_STATES),
      };
    }),
    definitionsPublished: booleanField(record, "definitions_published"),
    restartRequired: booleanField(record, "restart_required"),
    runningProcessChanged: false,
  };
}

export function parseConfigurationDiscardResult(value: unknown): ConfigurationDiscardResult {
  const record = objectValue(value, "configuration discard result");
  if (record.running_process_changed !== false) {
    throw new TypeError("Discard result has an unsupported runtime outcome");
  }
  return {
    discardId: stringField(record, "discard_id"),
    workingVersion: numberField(record, "working_version"),
    activeIdentity: stringField(record, "active_identity"),
    restartRequired: booleanField(record, "restart_required"),
    runningProcessChanged: false,
  };
}

export function parseValidationReport(value: unknown): ValidationReport {
  const record = objectValue(value, "validation report");
  return {
    valid: booleanField(record, "valid"),
    issues: arrayField(record, "issues").map((item) => {
      const issue = objectValue(item, "validation issue");
      return {
        severity: enumField(issue, "severity", VALIDATION_SEVERITIES),
        category: stringField(issue, "category"),
        location: arrayField(issue, "location").map((part) =>
          stringValue(part, "validation issue location"),
        ),
        message: stringField(issue, "message"),
      };
    }),
  };
}

export function parsePublication(value: unknown): PublicationRecord {
  return {
    publishedRevisionId: nullableStringField(
      objectValue(value, "publication"),
      "published_revision_id",
    ),
  };
}

export function parseActivationPlan(value: unknown): ActivationPlan {
  const record = objectValue(value, "activation plan");
  return {
    planDigest: stringField(record, "plan_digest"),
    expectedActiveRevisionId: nullableStringField(record, "expected_active_revision_id"),
  };
}

export function parseActivation(value: unknown): ActivationRecord {
  const record = objectValue(value, "activation");
  return {
    activationId: stringField(record, "activation_id"),
    state: activationStateField(record, "state"),
    plan: parseActivationPlan(objectField(record, "plan")),
  };
}

export function parseActivationReadiness(value: unknown): ActivationReadiness {
  const record = objectValue(value, "activation readiness");
  return {
    activationId: stringField(record, "activation_id"),
    ready: booleanField(record, "ready"),
    state: activationStateField(record, "state"),
    targetRevisionId: stringField(record, "target_revision_id"),
    workerReadinessRegistered: booleanField(record, "worker_readiness_registered"),
    completedCheckpoints: arrayField(record, "completed_checkpoints").map((item) =>
      stringValue(item, "activation readiness checkpoint"),
    ),
  };
}

export function parseDifference(value: unknown): ConfigurationDifference {
  const record = objectValue(value, "configuration difference");
  return {
    items: arrayField(record, "items").map((item) => {
      const difference = objectValue(item, "difference item");
      return {
        operation: stringField(difference, "operation"),
        path: arrayField(difference, "path").map((part) => stringValue(part, "difference path")),
      };
    }),
  };
}

export function parseRevisionRecord(value: unknown): RevisionRecord {
  const record = objectValue(value, "revision record");
  return {
    revisionId: stringField(record, "revision_id"),
    parentRevisionId: nullableStringField(record, "parent_revision_id"),
    createdAt: stringField(record, "created_at"),
    bundle: objectField(record, "bundle"),
  };
}

export function errorMessage(value: unknown, status: number): string {
  if (typeof value === "object" && value !== null) {
    const error = Reflect.get(value, "error");
    if (typeof error === "object" && error !== null) {
      const message = Reflect.get(error, "message");
      if (typeof message === "string" && message.length > 0) return message;
    }
  }
  return `Request failed (${status})`;
}

export function errorCode(value: unknown): string | null {
  if (typeof value === "object" && value !== null) {
    const error = Reflect.get(value, "error");
    if (typeof error === "object" && error !== null) {
      const code = Reflect.get(error, "code");
      if (typeof code === "string" && code.length > 0) return code;
    }
  }
  return null;
}

function objectValue(value: unknown, label: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`);
  }
  return value as JsonObject;
}

function objectField(record: JsonObject, field: string): JsonObject {
  return objectValue(record[field], field);
}

function nullableObjectField(record: JsonObject, field: string): JsonObject | null {
  const value = record[field];
  return value === null ? null : objectValue(value, field);
}

function arrayField(record: JsonObject, field: string): unknown[] {
  const value = record[field];
  if (!Array.isArray(value)) throw new TypeError(`${field} must be an array`);
  return value;
}

function stringMapField(record: JsonObject, field: string): Record<string, string> {
  const value = objectField(record, field);
  const projected: Record<string, string> = {};
  for (const [key, entry] of Object.entries(value)) {
    projected[key] = stringValue(entry, `${field}.${key}`);
  }
  return projected;
}

function stringValue(value: unknown, label: string): string {
  if (typeof value !== "string") throw new TypeError(`${label} must be a string`);
  return value;
}

function stringField(record: JsonObject, field: string): string {
  return stringValue(record[field], field);
}

function nullableStringField(record: JsonObject, field: string): string | null {
  const value = record[field];
  return value === null ? null : stringValue(value, field);
}

function nullableObjectStringField(record: JsonObject | null, field: string): string | null {
  return record === null ? null : stringField(record, field);
}

function nullableObjectNullableStringField(
  record: JsonObject | null,
  field: string,
): string | null {
  return record === null ? null : nullableStringField(record, field);
}

function booleanField(record: JsonObject, field: string): boolean {
  const value = record[field];
  if (typeof value !== "boolean") throw new TypeError(`${field} must be a boolean`);
  return value;
}

function nullableObjectBooleanField(record: JsonObject | null, field: string): boolean | null {
  if (record === null) return null;
  const value = record[field];
  return value === null ? null : booleanField(record, field);
}

function numberField(record: JsonObject, field: string): number {
  const value = record[field];
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new TypeError(`${field} must be a safe integer`);
  }
  return value;
}

function nullableNumberField(record: JsonObject, field: string): number | null {
  return record[field] === null ? null : numberField(record, field);
}

function enumField<const Values extends readonly string[]>(
  record: JsonObject,
  field: string,
  values: Values,
): Values[number] {
  const value = stringField(record, field);
  if (!values.some((candidate) => candidate === value)) {
    throw new TypeError(`${field} has an unsupported value`);
  }
  return value;
}

function httpUrlField(record: JsonObject, field: string): string {
  const value = stringField(record, field);
  const url = new URL(value);
  if (
    !HTTP_PROTOCOLS.has(url.protocol) ||
    url.username.length > 0 ||
    url.password.length > 0 ||
    url.search.length > 0 ||
    url.hash.length > 0
  ) {
    throw new TypeError(`${field} must be a credential-free HTTP(S) URL`);
  }
  return value;
}

function healthComponentField(record: JsonObject, field: string): HealthComponentName {
  const value = stringField(record, field);
  if (!isIncluded(HEALTH_COMPONENT_NAMES, value)) {
    throw new TypeError(`${field} must be a known runtime component`);
  }
  return value;
}

function healthStatusField(record: JsonObject, field: string): HealthStatus {
  const value = stringField(record, field);
  if (!isIncluded(HEALTH_STATUSES, value)) {
    throw new TypeError(`${field} must be a known health status`);
  }
  return value;
}

function activationStateField(record: JsonObject, field: string): ActivationState {
  const value = stringField(record, field);
  if (!isIncluded(ACTIVATION_STATES, value)) {
    throw new TypeError(`${field} must be a known activation state`);
  }
  return value;
}

function isIncluded<const Value extends string>(
  values: readonly Value[],
  candidate: string,
): candidate is Value {
  return values.some((value) => value === candidate);
}
