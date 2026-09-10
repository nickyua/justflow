# Settings and environment

Justflow loads one typed settings tree. Environment names start with `JUSTFLOW_` 
and use `__` between nested fields. Invalid values fail startup.
Secret values are never rendered here; structured dictionaries and lists use JSON.

## `catalog`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_CATALOG__BACKEND` | `Literal[local]` | `local` | LocalCatalogSettings |
| `JUSTFLOW_CATALOG__BACKEND` | `Literal[s3]` | `s3` | S3CatalogSettings |
| `JUSTFLOW_CATALOG__BUCKET` | `str` | `required` | S3CatalogSettings |
| `JUSTFLOW_CATALOG__ENDPOINT_URL` | `str \| NoneType` | `null` | S3CatalogSettings |
| `JUSTFLOW_CATALOG__EXPECTED_BUCKET_OWNER` | `str \| NoneType` | `null` | S3CatalogSettings |
| `JUSTFLOW_CATALOG__KMS_KEY_ID` | `str \| NoneType` | `null` | S3CatalogSettings |
| `JUSTFLOW_CATALOG__PREFIX` | `str` | `justflow/definitions/` | S3CatalogSettings |
| `JUSTFLOW_CATALOG__REGION` | `str \| NoneType` | `null` | S3CatalogSettings |
| `JUSTFLOW_CATALOG__SERVER_SIDE_ENCRYPTION` | `Literal[AES256, aws:kms]` | `AES256` | S3CatalogSettings |

## `configuration`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_CONFIGURATION__ACTIVATION_INDEX_NAME` | `str` | `scope-activations` | AwsConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__BACKEND` | `Literal[aws]` | `aws` | AwsConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__BACKEND` | `Literal[files]` | `files` | FileConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__BACKEND` | `Literal[sqlite]` | `sqlite` | SqliteConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__BUCKET` | `str` | `required` | AwsConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__ENDPOINT_URL` | `str \| NoneType` | `null` | AwsConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__EXPECTED_BUCKET_OWNER` | `str \| NoneType` | `null` | AwsConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__KMS_KEY_ID` | `str \| NoneType` | `null` | AwsConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__PATH` | `str` | `.justflow/configuration.sqlite3` | SqliteConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__PREFIX` | `str` | `justflow/configuration/` | AwsConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__REGION` | `str \| NoneType` | `null` | AwsConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__REVISION_INDEX_NAME` | `str` | `required` | AwsConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__SERVER_SIDE_ENCRYPTION` | `Literal[AES256, aws:kms]` | `AES256` | AwsConfigurationSettings |
| `JUSTFLOW_CONFIGURATION__TABLE_NAME` | `str` | `required` | AwsConfigurationSettings |

## `temporal`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_TEMPORAL__ADDRESS` | `str` | `localhost:7233` | all modes |
| `JUSTFLOW_TEMPORAL__CONNECTION__API_KEY` | `SecretStr \| NoneType` | `null` | TlsTemporalConnectionSettings |
| `JUSTFLOW_TEMPORAL__CONNECTION__CLIENT_CERTIFICATE_PATH` | `str \| NoneType` | `null` | TlsTemporalConnectionSettings |
| `JUSTFLOW_TEMPORAL__CONNECTION__CLIENT_PRIVATE_KEY_PATH` | `str \| NoneType` | `null` | TlsTemporalConnectionSettings |
| `JUSTFLOW_TEMPORAL__CONNECTION__HOST` | `str \| NoneType` | `null` | LocalTemporalConnectionSettings |
| `JUSTFLOW_TEMPORAL__CONNECTION__MODE` | `Literal[local_plaintext]` | `local_plaintext` | LocalTemporalConnectionSettings |
| `JUSTFLOW_TEMPORAL__CONNECTION__MODE` | `Literal[tls]` | `tls` | TlsTemporalConnectionSettings |
| `JUSTFLOW_TEMPORAL__CONNECTION__ROOT_CA_PATH` | `str \| NoneType` | `null` | TlsTemporalConnectionSettings |
| `JUSTFLOW_TEMPORAL__CONNECTION__SERVER_NAME` | `str` | `localhost` | TlsTemporalConnectionSettings |
| `JUSTFLOW_TEMPORAL__DEPLOYMENT_REGISTRATION__ATTEMPTS` | `int` | `50` | DeploymentRegistrationSettings |
| `JUSTFLOW_TEMPORAL__DEPLOYMENT_REGISTRATION__INTERVAL_SECONDS` | `float` | `0.1` | DeploymentRegistrationSettings |
| `JUSTFLOW_TEMPORAL__NAMESPACE` | `str` | `default` | all modes |
| `JUSTFLOW_TEMPORAL__PAYLOAD_PROTECTION__ACTIVE_KEY_ID` | `str` | `required` | CodecPayloadProtection |
| `JUSTFLOW_TEMPORAL__PAYLOAD_PROTECTION__MODE` | `Literal[codec]` | `codec` | CodecPayloadProtection |
| `JUSTFLOW_TEMPORAL__PAYLOAD_PROTECTION__MODE` | `Literal[plaintext]` | `plaintext` | PlaintextPayloadProtection |
| `JUSTFLOW_TEMPORAL__PAYLOAD_PROTECTION__READABLE_KEY_IDS` | `frozenset[str]` | `required` | CodecPayloadProtection |
| `JUSTFLOW_TEMPORAL__TASK_QUEUE` | `str` | `gateway-workflows` | all modes |

## `brokers`

This is a structured or provider-defined mapping. Supply it as JSON at 
`JUSTFLOW_BROKERS` or use nested fields where supported.

## `messaging`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_MESSAGING__CONSUMER_READY_TIMEOUT_SECONDS` | `float` | `30.0` | all modes |
| `JUSTFLOW_MESSAGING__DEDUPLICATION_CAPACITY` | `int` | `10000` | all modes |
| `JUSTFLOW_MESSAGING__DEDUPLICATION_RETENTION_SECONDS` | `float` | `86400.0` | all modes |
| `JUSTFLOW_MESSAGING__MESSAGE_CONCURRENCY` | `int` | `10` | all modes |
| `JUSTFLOW_MESSAGING__RECONNECT__INITIAL_DELAY_SECONDS` | `float` | `0.1` | ConsumerReconnectSettings |
| `JUSTFLOW_MESSAGING__RECONNECT__JITTER_FRACTION` | `float` | `0.2` | ConsumerReconnectSettings |
| `JUSTFLOW_MESSAGING__RECONNECT__MAX_DELAY_SECONDS` | `float` | `30.0` | ConsumerReconnectSettings |
| `JUSTFLOW_MESSAGING__RECONNECT__MULTIPLIER` | `float` | `2.0` | ConsumerReconnectSettings |
| `JUSTFLOW_MESSAGING__RESPONSE__BROKER` | `str` | `required` | ConsumerEndpoint |
| `JUSTFLOW_MESSAGING__RESPONSE__DEAD_LETTER_DESTINATION` | `str` | `required` | ConsumerEndpoint |
| `JUSTFLOW_MESSAGING__RESPONSE__DESTINATION` | `str` | `required` | ConsumerEndpoint |
| `JUSTFLOW_MESSAGING__TRIGGER__BROKER` | `str` | `required` | ConsumerEndpoint |
| `JUSTFLOW_MESSAGING__TRIGGER__DEAD_LETTER_DESTINATION` | `str` | `required` | ConsumerEndpoint |
| `JUSTFLOW_MESSAGING__TRIGGER__DESTINATION` | `str` | `required` | ConsumerEndpoint |

## `transport_security`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_TRANSPORT_SECURITY__GRPC_TLS_PROFILES` | `dict[str, GrpcTlsProfile]` | `{}` | all modes |
| `JUSTFLOW_TRANSPORT_SECURITY__HTTP__ALLOWED_ORIGINS` | `frozenset[str]` | `[]` | HttpEndpointPolicy |

## `resource_connections`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_RESOURCE_CONNECTIONS__POSTGRES_DSNS` | `dict[str, SecretStr]` | `{}` | all modes |
| `JUSTFLOW_RESOURCE_CONNECTIONS__REDIS_URLS` | `dict[str, SecretStr]` | `{}` | all modes |

## `paths`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_PATHS__CATALOG_DIR` | `str \| NoneType` | `null` | all modes |
| `JUSTFLOW_PATHS__CONFIG_DIR` | `str` | `configs` | all modes |

## `logging`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_LOGGING__LEVEL` | `str` | `INFO` | all modes |

## `runtime`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_RUNTIME__PINNED_START_RETRY__ATTEMPTS` | `int` | `50` | PinnedStartRetrySettings |
| `JUSTFLOW_RUNTIME__PINNED_START_RETRY__INTERVAL_SECONDS` | `float` | `0.1` | PinnedStartRetrySettings |
| `JUSTFLOW_RUNTIME__PROFILE` | `RuntimeProfile` | `production` | all modes |
| `JUSTFLOW_RUNTIME__SCOPE__APPLICATION__ROOT` | `str` | `justflow` | RuntimeScope → ApplicationIdentity |
| `JUSTFLOW_RUNTIME__SCOPE__ENVIRONMENT__ROOT` | `str` | `development` | RuntimeScope → EnvironmentIdentity |
| `JUSTFLOW_RUNTIME__SCOPE__TENANT__ROOT` | `str` | `local` | RuntimeScope → TenantIdentity |
| `JUSTFLOW_RUNTIME__WORKER_READY_TIMEOUT_SECONDS` | `float` | `60.0` | all modes |

## `control`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_CONTROL__DEFAULT_LIST_LIMIT` | `int` | `50` | all modes |
| `JUSTFLOW_CONTROL__HOST` | `str` | `127.0.0.1` | all modes |
| `JUSTFLOW_CONTROL__MAX_HEADER_BYTES` | `int` | `32768` | all modes |
| `JUSTFLOW_CONTROL__MAX_HEADER_COUNT` | `int` | `100` | all modes |
| `JUSTFLOW_CONTROL__MAX_QUERY_BYTES` | `int` | `4096` | all modes |
| `JUSTFLOW_CONTROL__MAX_REQUEST_BODY_BYTES` | `int` | `65536` | all modes |
| `JUSTFLOW_CONTROL__MAX_RESPONSE_BODY_BYTES` | `int` | `262144` | all modes |
| `JUSTFLOW_CONTROL__PORT` | `int` | `8080` | all modes |
| `JUSTFLOW_CONTROL__SHUTDOWN_GRACE_SECONDS` | `int` | `30` | all modes |
| `JUSTFLOW_CONTROL__TEMPORAL_RPC_TIMEOUT_SECONDS` | `float` | `10.0` | all modes |

## `operations`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_OPERATIONS__ADMIN_PANEL_ENABLED` | `bool` | `false` | all modes |
| `JUSTFLOW_OPERATIONS__INDEXED_SEARCH_ATTRIBUTES_ENABLED` | `bool` | `false` | all modes |
| `JUSTFLOW_OPERATIONS__LOCAL_SOURCE_AUTHORING_ENABLED` | `bool` | `false` | all modes |
| `JUSTFLOW_OPERATIONS__METRICS_LINKS` | `tuple[OperationsMetricsLinkSettings, Ellipsis]` | `[]` | all modes |

## `schedules`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_SCHEDULES__DESCRIBE_CONCURRENCY` | `int` | `10` | all modes |
| `JUSTFLOW_SCHEDULES__DISPATCH_ATTEMPTS` | `int` | `5` | all modes |
| `JUSTFLOW_SCHEDULES__DISPATCH_TIMEOUT_SECONDS` | `int` | `30` | all modes |
| `JUSTFLOW_SCHEDULES__MAX_SCHEDULES` | `int` | `1000` | all modes |
| `JUSTFLOW_SCHEDULES__PAGE_SIZE` | `int` | `100` | all modes |
| `JUSTFLOW_SCHEDULES__RECENT_ACTION_LIMIT` | `int` | `20` | all modes |
| `JUSTFLOW_SCHEDULES__TASK_QUEUE` | `str` | `justflow-schedules` | all modes |
| `JUSTFLOW_SCHEDULES__TEMPORAL_RPC_TIMEOUT_SECONDS` | `float` | `10.0` | all modes |

## `scheduled_starts`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_SCHEDULED_STARTS__CLEANUP_INTERVAL_SECONDS` | `int` | `3600` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__CLEANUP_PAGE_SIZE` | `int` | `100` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__CLOCK_SKEW_SECONDS` | `int` | `5` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__DEFAULT_LIST_LIMIT` | `int` | `50` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__DESCRIBE_CONCURRENCY` | `int` | `10` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__DISPATCH_ATTEMPTS` | `int` | `5` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__DISPATCH_TIMEOUT_SECONDS` | `int` | `30` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__MAX_HORIZON_SECONDS` | `int` | `31536000` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__MAX_INPUT_BYTES` | `int` | `262144` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__MAX_LIST_LIMIT` | `int` | `100` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__MAX_PENDING_PER_SCOPE` | `int` | `1000` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__MAX_SCHEDULES` | `int` | `10000` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__MUTATION_WAIT_SECONDS` | `int` | `10` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__RECOVERY_MAX_INTERVAL_SECONDS` | `int` | `60` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__TERMINAL_RETENTION_SECONDS` | `int` | `604800` | all modes |
| `JUSTFLOW_SCHEDULED_STARTS__WORKLOAD_POLICY__ALLOWED_CLASSES` | `frozenset[ScheduledStartWorkloadClass]` | `["batch","interactive","standard"]` | ScheduledStartWorkloadPolicySettings |
| `JUSTFLOW_SCHEDULED_STARTS__WORKLOAD_POLICY__BATCH_PRIORITY_KEY` | `int` | `10` | ScheduledStartWorkloadPolicySettings |
| `JUSTFLOW_SCHEDULED_STARTS__WORKLOAD_POLICY__FAIRNESS_WEIGHT` | `float` | `1.0` | ScheduledStartWorkloadPolicySettings |
| `JUSTFLOW_SCHEDULED_STARTS__WORKLOAD_POLICY__INTERACTIVE_PRIORITY_KEY` | `int` | `1` | ScheduledStartWorkloadPolicySettings |
| `JUSTFLOW_SCHEDULED_STARTS__WORKLOAD_POLICY__STANDARD_PRIORITY_KEY` | `int` | `5` | ScheduledStartWorkloadPolicySettings |

## `activation`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_ACTIVATION__LEASE_SECONDS` | `int` | `30` | all modes |
| `JUSTFLOW_ACTIVATION__MAX_SCOPES` | `int` | `1000` | all modes |
| `JUSTFLOW_ACTIVATION__MAX_TARGETS_PER_SCOPE` | `int` | `1000` | all modes |

## `deployment`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_DEPLOYMENT__ARTIFACT_DIGEST` | `str \| NoneType` | `null` | all modes |
| `JUSTFLOW_DEPLOYMENT__BUILD_ID` | `str \| NoneType` | `null` | all modes |
| `JUSTFLOW_DEPLOYMENT__COMPATIBLE_ENGINE_WORKFLOW_ABIS` | `frozenset[str]` | `["justflow.workflow.v1"]` | all modes |
| `JUSTFLOW_DEPLOYMENT__NAME` | `str` | `justflow` | all modes |
| `JUSTFLOW_DEPLOYMENT__PACKAGE_VERSION` | `str \| NoneType` | `installed version or null` | all modes |
| `JUSTFLOW_DEPLOYMENT__SOURCE_REVISION` | `str \| NoneType` | `null` | all modes |

## `limits`

| Environment variable | Type | Default | Applies to |
| --- | --- | --- | --- |
| `JUSTFLOW_LIMITS__ACTIVITY_INPUT_BYTES` | `int` | `524288` | all modes |
| `JUSTFLOW_LIMITS__ACTIVITY_OUTPUT_BYTES` | `int` | `524288` | all modes |
| `JUSTFLOW_LIMITS__AUDIT_RECORD_BYTES` | `int` | `1048576` | all modes |
| `JUSTFLOW_LIMITS__CACHE_ENTRY_BYTES` | `int` | `524288` | all modes |
| `JUSTFLOW_LIMITS__COLLECTION_ITEMS` | `int` | `10000` | all modes |
| `JUSTFLOW_LIMITS__CONTINUATION_INPUT_BYTES` | `int` | `1572864` | all modes |
| `JUSTFLOW_LIMITS__FAILURE_RECORD_BYTES` | `int` | `32768` | all modes |
| `JUSTFLOW_LIMITS__FANOUT_CHUNK_ITEMS` | `int` | `20` | all modes |
| `JUSTFLOW_LIMITS__FANOUT_ITEMS` | `int` | `1000` | all modes |
| `JUSTFLOW_LIMITS__HISTORY_BYTES` | `int` | `10485760` | all modes |
| `JUSTFLOW_LIMITS__HISTORY_EVENTS` | `int` | `10000` | all modes |
| `JUSTFLOW_LIMITS__LOOP_ATTEMPTS` | `int` | `1000` | all modes |
| `JUSTFLOW_LIMITS__PARALLELISM` | `int` | `32` | all modes |
| `JUSTFLOW_LIMITS__QUEUED_MESSAGES` | `int` | `1000` | all modes |
| `JUSTFLOW_LIMITS__QUEUED_MESSAGE_BYTES` | `int` | `262144` | all modes |
| `JUSTFLOW_LIMITS__SIGNAL_PAYLOAD_BYTES` | `int` | `262144` | all modes |
| `JUSTFLOW_LIMITS__TOTAL_INVOCATIONS` | `int` | `10000` | all modes |
| `JUSTFLOW_LIMITS__TRIGGER_PAYLOAD_BYTES` | `int` | `262144` | all modes |
| `JUSTFLOW_LIMITS__WORKFLOW_OUTPUT_BYTES` | `int` | `1048576` | all modes |
| `JUSTFLOW_LIMITS__WORKFLOW_STATE_BYTES` | `int` | `786432` | all modes |
