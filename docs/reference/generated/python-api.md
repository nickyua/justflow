# Public Python facades

Only the symbols exported by these facade modules are part of the documented Python 
surface. Import through the facade rather than an implementation module.

## `justflow.sdk`

| Symbol | Kind | Summary |
| --- | --- | --- |
| `GRPC_SCOPE_DIGEST_METADATA_KEY` | constant | str(object='') -> str |
| `HTTP_SCOPE_DIGEST_HEADER` | constant | str(object='') -> str |
| `LAMBDA_SERVICE_CALL_CONTEXT_FIELD` | constant | str(object='') -> str |
| `PROTOCOL_VERSION` | constant | str(object='') -> str |
| `SERVICE_CALL_CONTEXT_VERSION` | constant | str(object='') -> str |
| `BaseAction` | class | Base class for all service action handlers. |
| `EventEnvelope` | class | External event targeted at one exact Temporal workflow execution. |
| `MessageKind` | class | str(object='') -> str |
| `ResourceLoader` | class | Builds typed resources and owns their reverse-order lifecycle. |
| `Router` | class | Generic router that discovers BaseAction subclasses and dispatches by action name. |
| `ServiceCallContext` | class | Opaque scope identity asserted over an authenticated service boundary. |
| `StepErrorBody` | class | !!! abstract "Usage Documentation" |
| `StepRequestEnvelope` | class | Request for a service to execute one exact workflow step invocation. |
| `StepRequestPayload` | class | !!! abstract "Usage Documentation" |
| `StepResponseEnvelope` | class | Response for one exact step request and Temporal execution. |
| `StepSuccessBody` | class | !!! abstract "Usage Documentation" |
| `TriggerEnvelope` | class | External request to start one immutable workflow definition. |
| `parse_async_envelope` | function | Public symbol. |

## `justflow.runtime`

| Symbol | Kind | Summary |
| --- | --- | --- |
| `LOCAL_RUNTIME_SCOPE` | constant | One validated tenant/application/environment authority boundary. |
| `ActivationControllerError` | exception | A configuration publication or activation operation failed. |
| `ActivationMetricState` | class | str(object='') -> str |
| `ActivationOperationsView` | class | !!! abstract "Usage Documentation" |
| `ActivationPreparationSource` | class | Base class for protocol classes. |
| `AdminPanel` | class | Small serving facade implemented by the optional admin distribution. |
| `AdminPanelAsset` | class | AdminPanelAsset(*, body: 'bytes', content_type: 'bytes', headers: 'tuple[tuple[bytes, bytes], ...]' = ((b'cache-control', b'no-store'), (b'x-content-type-options', b'nosniff'), (b'referrer-policy', b'no-referrer'))) |
| `ApplicationIdentity` | class | Host-defined application identity within a tenant. |
| `AuthenticatedPrincipal` | class | Verified host identity with granted scopes and one selected effective scope. |
| `AuthenticationError` | exception | Credentials are absent, invalid or expired; the API returns a generic 401. |
| `AuthenticationProvider` | class | Host-owned identity boundary shared by custom clients and the administration UI. |
| `AuthenticationRequest` | class | Validated request metadata; credentials are sensitive and excluded from repr. |
| `AuthorizationAction` | class | Closed set of host permissions checked by the public control and authoring APIs. |
| `AuthorizationRequest` | class | Action and trusted scope to authorize, optionally with a scoped resource digest. |
| `BoundScheduleControlService` | class | Base class for protocol classes. |
| `BrokerSourceIdentity` | class | !!! abstract "Usage Documentation" |
| `CloudEventErrorCode` | class | str(object='') -> str |
| `CloudEventIngress` | class | Public symbol. |
| `CloudEventMapper` | class | Base class for protocol classes. |
| `CloudEventMappingError` | exception | A cloud event could not be mapped through its trusted host binding. |
| `CloudEventMappingRegistry` | class | Public symbol. |
| `CloudEventMetadata` | class | !!! abstract "Usage Documentation" |
| `CloudEventPayloadError` | exception | A source adapter permanently rejected an external event payload. |
| `CloudEventSourceIdentity` | class | !!! abstract "Usage Documentation" |
| `ComponentHealth` | class | !!! abstract "Usage Documentation" |
| `ConfigurationActivationController` | class | Public symbol. |
| `ConfigurationApi` | class | Public symbol. |
| `ConfigurationOperationsView` | class | !!! abstract "Usage Documentation" |
| `ConfigurationSchema` | class | !!! abstract "Usage Documentation" |
| `ContinuationChain` | class | !!! abstract "Usage Documentation" |
| `ControlApi` | class | ASGI application exposing bounded workflow runtime controls. |
| `ControlApiSourceIdentity` | class | !!! abstract "Usage Documentation" |
| `ControlErrorCode` | class | str(object='') -> str |
| `ControlOperationError` | exception | Common base class for all non-exit exceptions. |
| `DefinitionCatalogSource` | class | Base class for protocol classes. |
| `DefinitionPage` | class | !!! abstract "Usage Documentation" |
| `DefinitionSummary` | class | !!! abstract "Usage Documentation" |
| `DesiredSchedule` | class | DesiredSchedule(*, schedule_name: 'str', schedule_id: 'str', desired_digest: 'str', target: 'ScheduleTargetIdentity', schedule: 'Schedule', memo: 'Mapping[str, str]') |
| `EnvironmentIdentity` | class | Host-defined deployment environment identity within an application. |
| `EventBridgeEnvelope` | class | !!! abstract "Usage Documentation" |
| `EventBridgeEventMapper` | class | Map one exact EventBridge source and optional detail type to a workflow. |
| `EventBridgeWorkflowEvent` | class | !!! abstract "Usage Documentation" |
| `ExternalOperationConfirmation` | class | !!! abstract "Usage Documentation" |
| `HealthComponent` | class | str(object='') -> str |
| `HealthReason` | class | str(object='') -> str |
| `HealthRegistry` | class | Public symbol. |
| `HealthReport` | class | !!! abstract "Usage Documentation" |
| `HealthStatus` | class | str(object='') -> str |
| `HostSourceIdentity` | class | !!! abstract "Usage Documentation" |
| `ManagedScheduleDescription` | class | !!! abstract "Usage Documentation" |
| `MetricsLink` | class | !!! abstract "Usage Documentation" |
| `MetricsRegistry` | class | Public symbol. |
| `ObservedSchedule` | class | ObservedSchedule(*, schedule_id: 'str', owner: 'str \| None', schedule_name: 'str \| None', desired_digest: 'str \| None', scope_digest: 'str \| None' = None, action_valid: 'bool' = True, corrupt_metadata_keys: 'tuple[str, ...]' = ()) |
| `OperationsApi` | class | Public symbol. |
| `OperationsApiRequestError` | exception | Common base class for all non-exit exceptions. |
| `OperationsApiResponse` | class | OperationsApiResponse(*, status: 'HTTPStatus', payload: 'object') |
| `OperationsAvailability` | class | str(object='') -> str |
| `OperationsDimension` | class | !!! abstract "Usage Documentation" |
| `OperationsOverview` | class | !!! abstract "Usage Documentation" |
| `OperationsQueryError` | exception | Common base class for all non-exit exceptions. |
| `OperationsQueryErrorCode` | class | str(object='') -> str |
| `OperationsQueryService` | class | Public symbol. |
| `OperationsRoute` | class | OperationsRoute(*, kind: 'OperationsRouteKind', action: 'AuthorizationAction', resource: 'str \| None' = None) |
| `OperationsRouteKind` | class | str(object='') -> str |
| `PendingWait` | class | !!! abstract "Usage Documentation" |
| `PendingWaitKind` | class | str(object='') -> str |
| `PreparedActivation` | class | PreparedActivation(*, scope: 'RuntimeScope', revision: 'RevisionRecord', policy_digest: 'str', artifact: 'WorkerArtifactIdentity', task_queue_identity_digest: 'str', manifests: 'Mapping[str, DefinitionManifest]', environment_snapshots: 'Mapping[str, ExecutionEnvironmentSnapshot]', start_targets: 'Mapping[str, WorkflowStartTarget]', desired_schedules: 'Mapping[str, DesiredSchedule]') |
| `PreparedRuntimeIndex` | class | Base class for protocol classes. |
| `PreparedWorkerDeployment` | class | PreparedWorkerDeployment(*, target: 'PreparedActivation', assignments: 'tuple[PreparedActivation, ...]') |
| `ReconcilerScheduleActivationBinding` | class | Public symbol. |
| `RuntimeApplication` | class | Validated host composition shared by worker and gateway processes. |
| `RuntimeAsgiApplication` | class | ASGI lifecycle owner for a gateway and optional co-located worker. |
| `RuntimeGateway` | class | Ingress and control plane that can scale independently of workers. |
| `RuntimeIndexSnapshot` | class | RuntimeIndexSnapshot(*, revision_id: 'RevisionIdentity', policy_digest: 'str', artifact: 'WorkerArtifactIdentity', targets: 'Mapping[str, WorkflowStartTarget]', triggers: 'TriggersConfig') |
| `RuntimeScope` | class | One validated tenant/application/environment authority boundary. |
| `S3ObjectCreatedEventMapper` | class | Map one bounded S3 object-created source filter to a workflow. |
| `S3ObjectCreatedWorkflowEvent` | class | !!! abstract "Usage Documentation" |
| `S3ObjectKeyFilter` | class | !!! abstract "Usage Documentation" |
| `ScheduleActivationBinding` | class | Base class for protocol classes. |
| `ScheduleApplyErrorCode` | class | str(object='') -> str |
| `ScheduleApplyItem` | class | ScheduleApplyItem(*, schedule_id: 'str', schedule_name: 'str \| None', change: 'ScheduleChangeKind', status: 'ScheduleApplyStatus', error_code: 'ScheduleApplyErrorCode \| None' = None) |
| `ScheduleApplyResult` | class | ScheduleApplyResult(*, plan_digest: 'str', items: 'tuple[ScheduleApplyItem, ...]') |
| `ScheduleApplyStatus` | class | str(object='') -> str |
| `ScheduleChange` | class | ScheduleChange(*, kind: 'ScheduleChangeKind', schedule_id: 'str', schedule_name: 'str \| None', reason: 'str') |
| `ScheduleChangeKind` | class | str(object='') -> str |
| `ScheduleControlService` | class | Base class for protocol classes. |
| `ScheduleDispatchPlan` | class | !!! abstract "Usage Documentation" |
| `ScheduleMetricOperation` | class | str(object='') -> str |
| `ScheduleMetricOutcome` | class | str(object='') -> str |
| `ScheduleOperationError` | exception | Common base class for all non-exit exceptions. |
| `ScheduleOperationErrorCode` | class | str(object='') -> str |
| `ScheduleOperator` | class | Public symbol. |
| `SchedulePlan` | class | SchedulePlan(*, desired: 'Mapping[str, DesiredSchedule]', observed: 'Mapping[str, ObservedSchedule]', changes: 'tuple[ScheduleChange, ...]', plan_digest: 'str', unscoped_decision: 'UnscopedScheduleDecision') |
| `ScheduleRecentAction` | class | !!! abstract "Usage Documentation" |
| `ScheduleReconciler` | class | Public symbol. |
| `ScheduleReconciliationError` | exception | Common base class for all non-exit exceptions. |
| `ScheduleReconciliationErrorCode` | class | str(object='') -> str |
| `ScheduleRuntime` | class | Public symbol. |
| `ScheduleSourceIdentity` | class | !!! abstract "Usage Documentation" |
| `ScheduleTargetIdentity` | class | !!! abstract "Usage Documentation" |
| `ScheduledStartCancelRequest` | class | !!! abstract "Usage Documentation" |
| `ScheduledStartCreateRequest` | class | !!! abstract "Usage Documentation" |
| `ScheduledStartDecisionRecorder` | class | Base class for protocol classes. |
| `ScheduledStartDescription` | class | !!! abstract "Usage Documentation" |
| `ScheduledStartError` | exception | Common base class for all non-exit exceptions. |
| `ScheduledStartErrorCode` | class | str(object='') -> str |
| `ScheduledStartFailureCode` | class | str(object='') -> str |
| `ScheduledStartMetricOutcome` | class | str(object='') -> str |
| `ScheduledStartMutationResult` | class | !!! abstract "Usage Documentation" |
| `ScheduledStartMutationStatus` | class | str(object='') -> str |
| `ScheduledStartPage` | class | !!! abstract "Usage Documentation" |
| `ScheduledStartQuotaController` | class | Base class for protocol classes. |
| `ScheduledStartRescheduleRequest` | class | !!! abstract "Usage Documentation" |
| `ScheduledStartService` | class | Public symbol. |
| `ScheduledStartState` | class | str(object='') -> str |
| `ScheduledStartWorkloadClass` | class | str(object='') -> str |
| `ScheduledStartWorkloadPolicy` | class | Base class for protocol classes. |
| `ScopeBindingKind` | class | str(object='') -> str |
| `ScopeResolutionError` | exception | Trusted policy cannot resolve the requested runtime scope. |
| `ScopedDeclaredTriggerSource` | class | Public symbol. |
| `ScopedDefinitionCatalogSource` | class | Public symbol. |
| `ScopedScheduleControlService` | class | Public symbol. |
| `ScopedScheduleOperationsSource` | class | Public symbol. |
| `ScopedWorkflowTargetResolver` | class | Immutable workflow-target mappings partitioned by runtime scope. |
| `SignedWebhookRequest` | class | SignedWebhookRequest(*, headers: 'tuple[tuple[bytes, bytes], ...]', body: 'bytes') |
| `StartErrorCode` | class | str(object='') -> str |
| `StartStatus` | class | str(object='') -> str |
| `StartWorkflowRequest` | class | !!! abstract "Usage Documentation" |
| `StartWorkflowResult` | class | !!! abstract "Usage Documentation" |
| `TemporalConnectionBinding` | class | TemporalConnectionBinding(*, root_ca: 'bytes \| None' = None, client_certificate: 'bytes \| None' = None, client_private_key: 'bytes \| None' = None, api_key: 'SecretStr \| None' = None) |
| `TemporalConnectionConfigurationError` | exception | Inappropriate argument value (of correct type). |
| `TemporalConnectionError` | exception | Unspecified run-time error. |
| `TemporalConnectionPolicy` | class | TemporalConnectionPolicy(*, address: 'str', namespace: 'str', mode: 'str', _tls: 'TLSConfig \| None', _api_key: 'str \| None') |
| `TenantIdentity` | class | Host-defined tenant identity retained only at trusted boundaries. |
| `TriggerOperationalState` | class | str(object='') -> str |
| `TriggerOperationsView` | class | !!! abstract "Usage Documentation" |
| `TriggerRunNowStatus` | class | str(object='') -> str |
| `TriggerSource` | class | str(object='') -> str |
| `TriggerSummary` | class | !!! abstract "Usage Documentation" |
| `TrustedScopeBinding` | class | A scope assignment supplied by host policy rather than event data. |
| `UnscopedScheduleDecision` | class | str(object='') -> str |
| `WebhookError` | exception | Common base class for all non-exit exceptions. |
| `WebhookErrorCode` | class | str(object='') -> str |
| `WebhookEventIdentity` | class | !!! abstract "Usage Documentation" |
| `WebhookIngress` | class | Public symbol. |
| `WebhookPayloadError` | exception | Common base class for all non-exit exceptions. |
| `WebhookSourceAdapter` | class | Base class for protocol classes. |
| `WebhookSourceIdentity` | class | !!! abstract "Usage Documentation" |
| `WebhookSourceRegistry` | class | Public symbol. |
| `WebhookVerificationError` | exception | Common base class for all non-exit exceptions. |
| `WorkerAsgiApplication` | class | ASGI lifecycle owner exposing only worker liveness and readiness. |
| `WorkerDeploymentBinding` | class | Base class for protocol classes. |
| `WorkerDeploymentObservation` | class | !!! abstract "Usage Documentation" |
| `WorkerDeploymentRetirement` | class | !!! abstract "Usage Documentation" |
| `WorkflowControlService` | class | Public symbol. |
| `WorkflowDescription` | class | !!! abstract "Usage Documentation" |
| `WorkflowExecutionState` | class | str(object='') -> str |
| `WorkflowFailureClassification` | class | !!! abstract "Usage Documentation" |
| `WorkflowListQuery` | class | !!! abstract "Usage Documentation" |
| `WorkflowListResult` | class | !!! abstract "Usage Documentation" |
| `WorkflowRegistration` | class | !!! abstract "Usage Documentation" |
| `WorkflowRegistrationPage` | class | !!! abstract "Usage Documentation" |
| `WorkflowStartError` | exception | Common base class for all non-exit exceptions. |
| `WorkflowStarter` | class | Resolve, validate, and start workflows with one idempotency policy. |
| `WorkflowSummary` | class | !!! abstract "Usage Documentation" |
| `WorkflowTargetResolver` | class | Base class for protocol classes. |
| `authorization_resource_digest` | function | Public symbol. |
| `make_cloud_event_business_request_id` | function | Public symbol. |
| `make_schedule_run_now_workflow_id` | function | Public symbol. |
| `make_webhook_business_request_id` | function | Public symbol. |
| `resolve_temporal_connection` | function | Public symbol. |

## `justflow.resources`

| Symbol | Kind | Summary |
| --- | --- | --- |
| `ArchiveStore` | class | Base class for protocol classes. |
| `CacheStore` | class | Base class for protocol classes. |
| `ConfigReader` | class | Base class for protocol classes. |
| `ConfiguredResource` | class | Contract required by application-local resource classes. |
| `Database` | class | Base class for protocol classes. |
| `DuplicateResourceProviderError` | exception | Common base class for all non-exit exceptions. |
| `KeyValueStore` | class | Base class for protocol classes. |
| `ManagedResource` | class | Base class for protocol classes. |
| `ObjectStore` | class | Base class for protocol classes. |
| `ResourceCapability` | class | str(object='') -> str |
| `ResourceClassImportError` | exception | Common base class for all non-exit exceptions. |
| `ResourceCollection` | class | A Mapping is a generic container for associating key/value |
| `ResourceConfigError` | exception | Common base class for all non-exit exceptions. |
| `ResourceDependency` | class | ResourceDependency(*, resource_name: 'str', capability: 'ResourceCapability', secret_alias: 'str \| None' = None) |
| `ResourceDependencyError` | exception | Common base class for all non-exit exceptions. |
| `ResourceError` | exception | Base error exposed by the resource boundary. |
| `ResourceFactoryContext` | class | ResourceFactoryContext(*, postgres_dsns: 'Mapping[str, SecretStr]' = <factory>, redis_urls: 'Mapping[str, SecretStr]' = <factory>, aws_client_factory: 'AwsClientFactory \| None' = None, resource_resolver: 'ResourceResolver \| None' = None) |
| `ResourceGrant` | class | A Mapping is a generic container for associating key/value |
| `ResourceProvider` | class | ResourceProvider(*, name: 'str', contract_version: 'str', config_model: 'type[ConfigT]', capabilities: 'frozenset[ResourceCapability]', factory: 'ResourceFactory[ConfigT]', dependency_resolver: 'ResourceDependencyResolver[ConfigT] \| None' = None, secret_alias_resolver: 'SecretAliasResolver[ConfigT] \| None' = None) |
| `ResourceRegistry` | class | Public symbol. |
| `ResourceRegistryError` | exception | Common base class for all non-exit exceptions. |
| `SecretReader` | class | Base class for protocol classes. |
| `StrictResourceConfig` | class | Provider-owned resource configuration parsed before construction. |
| `UnknownResourceProviderError` | exception | Common base class for all non-exit exceptions. |
| `builtin_resource_registry` | function | Public symbol. |

## `justflow.transports`

| Symbol | Kind | Summary |
| --- | --- | --- |
| `AwaitingResponse` | class | AwaitingResponse(*, key: 'str', timeout_policy: 'TimeoutPolicy') |
| `Completed` | class | Completed(*, data: 'Any' = None) |
| `ConfiguredService` | class | ConfiguredService(*, definition: 'ResolvedService', transport: 'ConfiguredTransport', supports_async_response: 'bool') |
| `ConfiguredTransport` | class | Base class for protocol classes. |
| `ConnectionTimeoutError` | exception | Common base class for all non-exit exceptions. |
| `DeadlineMode` | class | str(object='') -> str |
| `DeadlineRequirements` | class | DeadlineRequirements(*, connect: 'DeadlineMode' = <DeadlineMode.OPTIONAL: 'optional'>, response: 'DeadlineMode' = <DeadlineMode.FORBIDDEN: 'forbidden'>) |
| `DispatchTimeoutError` | exception | Common base class for all non-exit exceptions. |
| `DuplicateTransportProviderError` | exception | Common base class for all non-exit exceptions. |
| `ResolvedService` | class | ResolvedService(*, name: 'str', provider_name: 'str', provider_contract_version: 'str', transport_config: 'StrictTransportConfig', connect_timeout_sec: 'int \| None', dispatch_timeout_sec: 'int', response_timeout_sec: 'int \| None', retries: 'int', params: 'Mapping[str, object]') |
| `ServiceCallContext` | class | Opaque scope identity asserted over an authenticated service boundary. |
| `StrictTransportConfig` | class | !!! abstract "Usage Documentation" |
| `TimeoutPolicy` | class | TimeoutPolicy(*, response_timeout_sec: 'int') |
| `TransportConfigError` | exception | Common base class for all non-exit exceptions. |
| `TransportConfigurationError` | exception | Common base class for all non-exit exceptions. |
| `TransportConnectionError` | exception | Common base class for all non-exit exceptions. |
| `TransportDispatch` | constant | Represent a union type |
| `TransportError` | exception | Common base class for all non-exit exceptions. |
| `TransportFactoryContext` | class | TransportFactoryContext(*, resources: 'Mapping[str, Any]', connect_timeout_sec: 'int \| None', dispatch_timeout_sec: 'int', response_timeout_sec: 'int \| None', security: 'TransportSecuritySettings' = <factory>, message_publishers: 'Mapping[str, MessagePublisher]' = <factory>, reply_destination: 'str \| None' = None) |
| `TransportFactoryError` | exception | Common base class for all non-exit exceptions. |
| `TransportProvider` | class | TransportProvider(*, name: 'str', contract_version: 'str', config_model: 'type[ConfigT]', factory: 'TransportFactory[ConfigT]', supports_async_response: 'bool' = False, action_validator: 'TransportActionValidator \| None' = None, startup_validator: 'TransportStartupValidator[ConfigT] \| None' = None, deadline_requirements: 'DeadlineRequirements' = <factory>) |
| `TransportProviderDefinitionError` | exception | Common base class for all non-exit exceptions. |
| `TransportProviderValidationError` | exception | Common base class for all non-exit exceptions. |
| `TransportRegistry` | class | Public symbol. |
| `TransportRegistryError` | exception | Common base class for all non-exit exceptions. |
| `TransportRequest` | class | TransportRequest(*, service_name: 'str', action: 'str', input: 'Any', globals: 'dict[str, Any]', request_id: 'str', correlation_id: 'str', trace_id: 'str \| None', workflow_id: 'str', workflow_run_id: 'str', flow_name: 'str', definition_digest: 'str \| None', step_name: 'str', scope_digest: 'str \| None' = None, required_resources: 'tuple[str, ...]' = ()) |
| `UnknownTransportProviderError` | exception | Common base class for all non-exit exceptions. |
| `UnsupportedAsyncTransportError` | exception | Common base class for all non-exit exceptions. |
| `builtin_transport_registry` | function | Public symbol. |

## `justflow.brokers`

| Symbol | Kind | Summary |
| --- | --- | --- |
| `Ack` | class | Ack(*, kind: "Literal['ack']" = 'ack') |
| `BrokerConfigError` | exception | Common base class for all non-exit exceptions. |
| `BrokerConfigurationError` | exception | Common base class for all non-exit exceptions. |
| `BrokerConnectionError` | exception | Common base class for all non-exit exceptions. |
| `BrokerError` | exception | Common base class for all non-exit exceptions. |
| `BrokerFactoryError` | exception | Common base class for all non-exit exceptions. |
| `BrokerProvider` | class | BrokerProvider(*, name: 'str', contract_version: 'str', config_model: 'type[ConfigT]', factory: 'BrokerFactory[ConfigT]') |
| `BrokerProviderDefinitionError` | exception | Common base class for all non-exit exceptions. |
| `BrokerRegistry` | class | Public symbol. |
| `BrokerRegistryError` | exception | Common base class for all non-exit exceptions. |
| `ConfiguredBroker` | class | Base class for protocol classes. |
| `DeadLetter` | class | DeadLetter(*, reason: 'str', kind: "Literal['dead_letter']" = 'dead_letter') |
| `DeliveryPolicy` | class | DeliveryPolicy(*, max_delivery_attempts: 'int', max_redelivery_window_seconds: 'float') |
| `DuplicateBrokerProviderError` | exception | Common base class for all non-exit exceptions. |
| `MessageConsumer` | class | Base class for protocol classes. |
| `MessagePublisher` | class | Base class for protocol classes. |
| `ProcessingOutcome` | constant | Represent a union type |
| `PublishedMessage` | class | PublishedMessage(*, body: 'str', message_id: 'str') |
| `ReceivedMessage` | class | ReceivedMessage(*, body: 'str', broker_message_id: 'str', delivery_attempt: 'int', settlement_token: 'object') |
| `Retry` | class | Retry(*, reason: 'str', kind: "Literal['retry']" = 'retry') |
| `StrictBrokerConfig` | class | !!! abstract "Usage Documentation" |
| `UnknownBrokerProviderError` | exception | Common base class for all non-exit exceptions. |

## `justflow.configuration`

| Symbol | Kind | Summary |
| --- | --- | --- |
| `ActivationChange` | class | !!! abstract "Usage Documentation" |
| `ActivationChangeKind` | class | str(object='') -> str |
| `ActivationChangeOperation` | class | str(object='') -> str |
| `ActivationCheckpoint` | class | !!! abstract "Usage Documentation" |
| `ActivationCheckpointKind` | class | str(object='') -> str |
| `ActivationCompatibilityRisk` | class | str(object='') -> str |
| `ActivationConflictError` | exception | Activation control-plane state changed concurrently. |
| `ActivationError` | exception | A configuration publication or activation operation failed. |
| `ActivationExternalOutcome` | class | str(object='') -> str |
| `ActivationIntegrityError` | exception | Persisted activation state is corrupt or internally inconsistent. |
| `ActivationLimitError` | exception | An activation operation exceeds a configured bound. |
| `ActivationNotFoundError` | exception | Requested activation control-plane state does not exist. |
| `ActivationObservations` | class | !!! abstract "Usage Documentation" |
| `ActivationOutcomeCode` | class | str(object='') -> str |
| `ActivationPage` | class | !!! abstract "Usage Documentation" |
| `ActivationPlan` | class | !!! abstract "Usage Documentation" |
| `ActivationRecord` | class | !!! abstract "Usage Documentation" |
| `ActivationState` | class | str(object='') -> str |
| `ActivationStore` | class | Base class for protocol classes. |
| `ActivationSubjectKind` | class | str(object='') -> str |
| `ActivationTransition` | class | !!! abstract "Usage Documentation" |
| `ActivationUnavailableError` | exception | The activation backend or required external system is unavailable. |
| `ActivePointer` | class | !!! abstract "Usage Documentation" |
| `AwsConfigurationStore` | class | Immutable S3 bodies with DynamoDB conditional metadata and pointers. |
| `ComponentCatalogRevision` | constant | Runtime representation of an annotated type. |
| `ComponentReference` | class | !!! abstract "Usage Documentation" |
| `ComponentReplayContract` | class | Determinism and retirement metadata retained with the exact component. |
| `ComponentRuntimeIdentity` | class | Host-owned runtime implementation selected by a component revision. |
| `ConfigurationApplyStage` | class | !!! abstract "Usage Documentation" |
| `ConfigurationApplyStageKind` | class | str(object='') -> str |
| `ConfigurationApplyStageState` | class | str(object='') -> str |
| `ConfigurationBundle` | class | !!! abstract "Usage Documentation" |
| `ConfigurationConflictError` | exception | Configuration state changed concurrently. |
| `ConfigurationDeclarationKind` | class | str(object='') -> str |
| `ConfigurationDiff` | class | !!! abstract "Usage Documentation" |
| `ConfigurationDiffItem` | class | !!! abstract "Usage Documentation" |
| `ConfigurationDiffOperation` | class | str(object='') -> str |
| `ConfigurationDiscardErrorCode` | class | str(object='') -> str |
| `ConfigurationDiscardRecord` | class | !!! abstract "Usage Documentation" |
| `ConfigurationDiscardResult` | class | !!! abstract "Usage Documentation" |
| `ConfigurationDiscardState` | class | str(object='') -> str |
| `ConfigurationDocument` | constant | Represent a union type |
| `ConfigurationEnvelope` | class | !!! abstract "Usage Documentation" |
| `ConfigurationError` | exception | Configuration content or an operation is invalid. |
| `ConfigurationIntegrityError` | exception | Stored configuration state is corrupt or internally inconsistent. |
| `ConfigurationLimitError` | exception | A configuration operation exceeds a documented bound. |
| `ConfigurationNotFoundError` | exception | Requested configuration state does not exist in the scope. |
| `ConfigurationPublicationService` | class | Expose writable configuration primitives through one validated scope. |
| `ConfigurationRelationship` | class | !!! abstract "Usage Documentation" |
| `ConfigurationRelationshipState` | class | str(object='') -> str |
| `ConfigurationRelationships` | class | !!! abstract "Usage Documentation" |
| `ConfigurationScopeError` | exception | A configuration lookup attempted to cross its trusted scope. |
| `ConfigurationSnapshot` | class | !!! abstract "Usage Documentation" |
| `ConfigurationSource` | class | Base class for protocol classes. |
| `ConfigurationStore` | class | Base class for protocol classes. |
| `ConfigurationUnavailableError` | exception | The configured backend could not complete the operation. |
| `ConfigurationValidationCategory` | class | str(object='') -> str |
| `ConfigurationValidationIssue` | class | !!! abstract "Usage Documentation" |
| `ConfigurationValidationReport` | class | !!! abstract "Usage Documentation" |
| `ConfigurationValidationSeverity` | class | str(object='') -> str |
| `DefinitionActivationAction` | class | str(object='') -> str |
| `DraftRecord` | class | !!! abstract "Usage Documentation" |
| `DynamoDbActivationStore` | class | Conditional activation records in one scope-partitioned DynamoDB table. |
| `FileConfigurationSource` | class | Immutable local/Git-managed YAML bound to exactly one runtime scope. |
| `FilePlatformComponentCatalogSource` | class | Read canonical immutable component catalogs packaged by exact revision. |
| `LocalConfigurationApplyResult` | class | !!! abstract "Usage Documentation" |
| `PlatformComponentCatalog` | class | !!! abstract "Usage Documentation" |
| `PlatformComponentCatalogSource` | class | Base class for protocol classes. |
| `PlatformStepComponent` | class | !!! abstract "Usage Documentation" |
| `PlatformTriggerComponent` | class | !!! abstract "Usage Documentation" |
| `PublicationErrorCode` | class | str(object='') -> str |
| `PublicationOperationError` | exception | Configuration content or an operation is invalid. |
| `PublicationRecord` | class | !!! abstract "Usage Documentation" |
| `PublicationState` | class | str(object='') -> str |
| `ResolvedTenantTrigger` | class | ResolvedTenantTrigger(*, component: 'ComponentReference', kind: 'TriggerKind', workflow: 'str', binding_alias: 'str', resource_bindings: 'Mapping[str, str]', parameters: 'Mapping[str, object]', output_schema: 'Mapping[str, object]') |
| `ResourceAuthoringGrant` | class | !!! abstract "Usage Documentation" |
| `RetentionResult` | class | !!! abstract "Usage Documentation" |
| `RevisionIdentity` | class | !!! abstract "Usage Documentation" |
| `RevisionPage` | class | !!! abstract "Usage Documentation" |
| `RevisionRecord` | class | !!! abstract "Usage Documentation" |
| `RevisionSummary` | class | !!! abstract "Usage Documentation" |
| `RoutingActivationAction` | class | str(object='') -> str |
| `ScheduleActivationAction` | class | str(object='') -> str |
| `ServiceAuthoringGrant` | class | !!! abstract "Usage Documentation" |
| `SqliteActivationStore` | class | Conditional activation records sharing no mutable runtime state. |
| `SqliteConfigurationStore` | class | Deterministic writable store for a single local host. |
| `StaticTenantAuthoringPolicySource` | class | Public symbol. |
| `StoredConfigurationSource` | class | Public symbol. |
| `TemporalIsolationMode` | class | str(object='') -> str |
| `TemporalIsolationPolicy` | class | !!! abstract "Usage Documentation" |
| `TenantAuthoringPolicy` | class | !!! abstract "Usage Documentation" |
| `TenantAuthoringPolicySource` | class | Base class for protocol classes. |
| `TenantComponentOperation` | class | !!! abstract "Usage Documentation" |
| `TenantConfiguration` | class | !!! abstract "Usage Documentation" |
| `TenantConfigurationResolution` | class | !!! abstract "Usage Documentation" |
| `TenantTriggerDeclaration` | class | !!! abstract "Usage Documentation" |
| `TenantValidatedConfiguration` | class | TenantValidatedConfiguration(*, bundle: 'ConfigurationBundle', component_catalog_revision: 'ComponentCatalogRevision', component_references: 'Mapping[str, Mapping[str, ComponentReference]]', triggers: 'Mapping[str, ResolvedTenantTrigger]', resources: 'Mapping[str, ResolvedResource]', services: 'Mapping[str, ResolvedService]', diagnostics: 'tuple[ValidationDiagnostic, ...]') |
| `TenantWorkflowConfig` | class | !!! abstract "Usage Documentation" |
| `TenantWorkflowOperation` | class | !!! abstract "Usage Documentation" |
| `TriggerBindingGrant` | class | !!! abstract "Usage Documentation" |
| `TriggerKind` | class | str(object='') -> str |
| `WorkerActivationAction` | class | str(object='') -> str |
| `WorkerReadinessRegistration` | class | !!! abstract "Usage Documentation" |
| `configured_activation_store` | function | Public symbol. |
| `configured_configuration_source` | function | Public symbol. |
| `configured_configuration_store` | function | Public symbol. |
| `parse_tenant_configuration_yaml` | function | Public symbol. |
| `plan_configuration_activation` | function | Public symbol. |
| `render_configuration_yaml` | function | Public symbol. |
| `render_tenant_configuration_yaml` | function | Public symbol. |
| `validate_tenant_authoring` | function | Public symbol. |

## `justflow.definitions`

| Symbol | Kind | Summary |
| --- | --- | --- |
| `ENGINE_WORKFLOW_ABI` | constant | str(object='') -> str |
| `MANIFEST_FORMAT_VERSION` | constant | int([x]) -> integer |
| `CatalogBackend` | class | Base class for protocol classes. |
| `CatalogBundle` | class | !!! abstract "Usage Documentation" |
| `CatalogConflictError` | exception | Catalog state changed concurrently or immutable content disagrees. |
| `CatalogDriftError` | exception | Authored definitions and the published catalog disagree. |
| `CatalogError` | exception | Definition catalog content or operation is invalid. |
| `CatalogImportConflict` | class | !!! abstract "Usage Documentation" |
| `CatalogImportPlan` | class | !!! abstract "Usage Documentation" |
| `CatalogMigrationError` | exception | A catalog bundle or migration operation is invalid. |
| `CatalogState` | class | CatalogState(*, catalog: 'DefinitionCatalog', alias_version: 'str \| None') |
| `CatalogStorageError` | exception | Catalog storage could not complete an operation. |
| `CatalogStore` | class | Public symbol. |
| `DefinitionCatalog` | class | Public symbol. |
| `DefinitionCatalogStore` | class | Public symbol. |
| `DefinitionManifest` | class | !!! abstract "Usage Documentation" |
| `DefinitionManifestError` | exception | A workflow cannot be represented by a stable definition identity. |
| `DefinitionStartTarget` | class | DefinitionStartTarget(*, manifest: 'DefinitionManifest', deployment: 'WorkerDeployment', environment_snapshot_digest: 'str', scope_digest: 'str \| None' = None, execution_configuration: 'ExecutionConfigurationIdentity \| None' = None, workflow_class: 'type') |
| `DeploymentRoutingError` | exception | No unambiguous compatible worker deployment can run a definition. |
| `ExecutionEnvironmentSnapshot` | class | Canonical, content-addressed, secret-free execution environment identity. |
| `LocalCatalogBackend` | class | Public symbol. |
| `RuntimeProfile` | class | str(object='') -> str |
| `S3CatalogBackend` | class | Public symbol. |
| `WorkerArtifactIdentity` | class | Immutable executable artifact selected for a workflow execution. |
| `WorkerDeployment` | class | WorkerDeployment(*, artifact_identity: 'WorkerArtifactIdentity', compatible_engine_workflow_abis: 'frozenset[str]') |
| `WorkerDeploymentRouter` | class | Public symbol. |
| `WorkflowStartTarget` | class | WorkflowStartTarget(*, manifest: 'DefinitionManifest', deployment: 'WorkerDeployment', environment_snapshot_digest: 'str', scope_digest: 'str \| None' = None, execution_configuration: 'ExecutionConfigurationIdentity \| None' = None) |
| `build_definition_manifests` | function | Public symbol. |
| `build_execution_environment_snapshot` | function | Public symbol. |
| `build_execution_environment_snapshots` | function | Public symbol. |
| `export_catalog` | function | Public symbol. |
| `import_catalog` | function | Public symbol. |
| `plan_catalog_import` | function | Public symbol. |
| `sanitized_runtime_configuration` | function | Public symbol. |
| `workflow_type_name` | function | Public symbol. |

## `justflow.schemas`

| Symbol | Kind | Summary |
| --- | --- | --- |
| `AUTHORING_SCHEMA_VERSION` | constant | str(object='') -> str |
| `SCHEMA_FILE_NAMES` | constant | Built-in immutable sequence. |
| `SchemaExportConflictError` | exception | A schema export would overwrite different repository content. |
| `build_authoring_schemas` | function | Public symbol. |
| `export_authoring_schemas` | function | Public symbol. |
| `load_bundled_schemas` | function | Public symbol. |

## `justflow.openapi`

| Symbol | Kind | Summary |
| --- | --- | --- |
| `OPENAPI_FILE_NAME` | constant | str(object='') -> str |
| `OPENAPI_VERSION` | constant | str(object='') -> str |
| `OpenApiExportConflictError` | exception | An OpenAPI export would overwrite different repository content. |
| `build_openapi_document` | function | Public symbol. |
| `export_openapi_document` | function | Public symbol. |
| `load_bundled_openapi_document` | function | Public symbol. |
| `public_api_routes` | function | Public symbol. |
| `render_openapi_document` | function | Public symbol. |

## `justflow.visualization`

| Symbol | Kind | Summary |
| --- | --- | --- |
| `WorkflowGraph` | class | WorkflowGraph(name: 'str', description: 'str', nodes: 'list[GraphNode]' = <factory>, edges: 'list[GraphEdge]' = <factory>) |
| `build_graph` | function | Build a WorkflowGraph from a WorkflowConfig. |
| `render_html` | function | Render a self-contained HTML page with tabs, diagram, and hover overlays. |
| `render_mermaid` | function | Render a WorkflowGraph as a Mermaid flowchart definition. |
