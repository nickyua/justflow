"""Engine settings - single typed configuration object, loaded once at startup.

Values come from environment variables (prefix ``JUSTFLOW_``, nested groups joined
with ``__``, e.g. ``JUSTFLOW_TEMPORAL__ADDRESS``) with sane local-dev defaults.
CLI flags in ``__main__`` override individual fields after loading.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Literal, Self
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from justflow.catalog_validation import (
    normalize_s3_catalog_prefix,
    validate_s3_bucket,
)
from justflow.config.grammar import Identifier
from justflow.config.runtime_limits import (
    DEFAULT_ACTIVITY_INPUT_BYTES,
    DEFAULT_ACTIVITY_OUTPUT_BYTES,
    DEFAULT_AUDIT_RECORD_BYTES,
    DEFAULT_CACHE_ENTRY_BYTES,
    DEFAULT_COLLECTION_ITEMS,
    DEFAULT_CONTINUATION_INPUT_BYTES,
    DEFAULT_FAILURE_RECORD_BYTES,
    DEFAULT_FANOUT_CHUNK_ITEMS,
    DEFAULT_FANOUT_ITEMS,
    DEFAULT_HISTORY_BYTES,
    DEFAULT_HISTORY_EVENTS,
    DEFAULT_LOOP_ATTEMPTS,
    DEFAULT_PARALLELISM,
    DEFAULT_QUEUED_MESSAGE_BYTES,
    DEFAULT_QUEUED_MESSAGES,
    DEFAULT_SIGNAL_PAYLOAD_BYTES,
    DEFAULT_TOTAL_INVOCATIONS,
    DEFAULT_TRIGGER_PAYLOAD_BYTES,
    DEFAULT_WORKFLOW_OUTPUT_BYTES,
    DEFAULT_WORKFLOW_STATE_BYTES,
    RuntimeLimits,
)
from justflow.definitions.manifest import ENGINE_WORKFLOW_ABI
from justflow.provenance import RuntimeProfile, installed_engine_version
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope
from justflow.transports.security import MAX_SERVER_NAME_LENGTH, TransportSecuritySettings

ENV_PREFIX = "JUSTFLOW_"
ENV_NESTED_DELIMITER = "__"
MAX_IDENTIFIER_LENGTH = 128
MAX_NETWORK_LOCATION_LENGTH = 2048
MAX_PATH_LENGTH = 4096
MAX_MESSAGE_CONCURRENCY = 10
MAX_DEDUPLICATION_CAPACITY = 1_000_000
MAX_DEDUPLICATION_RETENTION_SECONDS = 2_592_000.0
MAX_PAYLOAD_KEY_IDS = 100
DEFAULT_MESSAGE_CONCURRENCY = 10
DEFAULT_DEDUPLICATION_CAPACITY = 10_000
DEFAULT_DEDUPLICATION_RETENTION_SECONDS = 86_400.0
DEFAULT_CONSUMER_READY_TIMEOUT_SECONDS = 30.0
MAX_CONSUMER_READY_TIMEOUT_SECONDS = 300.0
DEFAULT_CONSUMER_RECONNECT_INITIAL_SECONDS = 0.1
DEFAULT_CONSUMER_RECONNECT_MAX_SECONDS = 30.0
DEFAULT_CONSUMER_RECONNECT_MULTIPLIER = 2.0
DEFAULT_CONSUMER_RECONNECT_JITTER_FRACTION = 0.2
MAX_CONSUMER_RECONNECT_SECONDS = 300.0
MAX_CONSUMER_RECONNECT_MULTIPLIER = 10.0
DEFAULT_DEPLOYMENT_REGISTRATION_ATTEMPTS = 50
MAX_DEPLOYMENT_REGISTRATION_ATTEMPTS = 1_000
DEFAULT_DEPLOYMENT_REGISTRATION_INTERVAL_SECONDS = 0.1
MAX_DEPLOYMENT_REGISTRATION_INTERVAL_SECONDS = 60.0
DEFAULT_PINNED_START_RETRY_ATTEMPTS = 50
MAX_PINNED_START_RETRY_ATTEMPTS = 1_000
DEFAULT_PINNED_START_RETRY_INTERVAL_SECONDS = 0.1
MAX_PINNED_START_RETRY_INTERVAL_SECONDS = 60.0
DEFAULT_WORKER_READY_TIMEOUT_SECONDS = 60.0
MAX_WORKER_READY_TIMEOUT_SECONDS = 600.0
DEFAULT_S3_CATALOG_PREFIX = "justflow/definitions/"
DEFAULT_S3_CONFIGURATION_PREFIX = "justflow/configuration/"
DEFAULT_SQLITE_CONFIGURATION_PATH = ".justflow/configuration.sqlite3"
MAX_S3_PREFIX_LENGTH = 1024
AWS_ACCOUNT_ID_LENGTH = 12
DEFAULT_CONTROL_PORT = 8080
DEFAULT_CONTROL_REQUEST_BYTES = 65_536
DEFAULT_CONTROL_RESPONSE_BYTES = 262_144
DEFAULT_CONTROL_LIST_LIMIT = 50
MAX_CONTROL_LIST_LIMIT = 100
DEFAULT_CONTROL_RPC_TIMEOUT_SECONDS = 10.0
MAX_CONTROL_RPC_TIMEOUT_SECONDS = 60.0
DEFAULT_CONTROL_SHUTDOWN_GRACE_SECONDS = 30
MAX_CONTROL_SHUTDOWN_GRACE_SECONDS = 300
DEFAULT_CONTROL_HEADER_BYTES = 32_768
DEFAULT_CONTROL_HEADER_COUNT = 100
DEFAULT_CONTROL_QUERY_BYTES = 4_096
MIN_CONTROL_RESPONSE_BYTES = 256
MAX_CONTROL_REQUEST_BYTES = 1_048_576
MAX_CONTROL_RESPONSE_BYTES = 4_194_304
MAX_CONTROL_HEADER_BYTES = 65_536
MAX_CONTROL_HEADER_COUNT = 200
MAX_CONTROL_QUERY_BYTES = 16_384
MAX_TCP_PORT = 65_535
DEFAULT_SCHEDULE_LIMIT = 1_000
MAX_SCHEDULE_LIMIT = 10_000
DEFAULT_SCHEDULE_PAGE_SIZE = 100
MAX_SCHEDULE_PAGE_SIZE = 1_000
DEFAULT_SCHEDULE_DESCRIBE_CONCURRENCY = 10
MAX_SCHEDULE_DESCRIBE_CONCURRENCY = 100
DEFAULT_SCHEDULE_RPC_TIMEOUT_SECONDS = 10.0
MAX_SCHEDULE_RPC_TIMEOUT_SECONDS = 60.0
DEFAULT_SCHEDULE_RECENT_ACTION_LIMIT = 20
MAX_SCHEDULE_RECENT_ACTION_LIMIT = 100
MIN_SCHEDULE_DISPATCH_TIMEOUT_SECONDS = 1
MAX_SCHEDULE_DISPATCH_TIMEOUT_SECONDS = 300
DEFAULT_SCHEDULE_DISPATCH_TIMEOUT_SECONDS = 30
MIN_SCHEDULE_DISPATCH_ATTEMPTS = 1
MAX_SCHEDULE_DISPATCH_ATTEMPTS = 100
DEFAULT_SCHEDULE_DISPATCH_ATTEMPTS = 5
DEFAULT_SCHEDULE_TASK_QUEUE = "justflow-schedules"
DEFAULT_SCHEDULED_START_HORIZON_SECONDS = 31_536_000
MAX_SCHEDULED_START_HORIZON_SECONDS = 315_360_000
DEFAULT_SCHEDULED_START_INPUT_BYTES = DEFAULT_TRIGGER_PAYLOAD_BYTES
MAX_SCHEDULED_START_INPUT_BYTES = 1_048_576
DEFAULT_PENDING_SCHEDULED_STARTS_PER_SCOPE = 1_000
MAX_PENDING_SCHEDULED_STARTS_PER_SCOPE = 10_000
DEFAULT_SCHEDULED_START_LIST_LIMIT = 50
MAX_SCHEDULED_START_LIST_LIMIT = 100
DEFAULT_SCHEDULED_START_COLLECTION_LIMIT = 10_000
MAX_SCHEDULED_START_COLLECTION_LIMIT = 100_000
DEFAULT_SCHEDULED_START_TERMINAL_RETENTION_SECONDS = 604_800
MAX_SCHEDULED_START_TERMINAL_RETENTION_SECONDS = 31_536_000
DEFAULT_SCHEDULED_START_CLEANUP_PAGE_SIZE = 100
MAX_SCHEDULED_START_CLEANUP_PAGE_SIZE = 1_000
DEFAULT_SCHEDULED_START_CLEANUP_INTERVAL_SECONDS = 3_600
MIN_SCHEDULED_START_CLEANUP_INTERVAL_SECONDS = 60
MAX_SCHEDULED_START_CLEANUP_INTERVAL_SECONDS = 86_400
DEFAULT_SCHEDULED_START_CLOCK_SKEW_SECONDS = 5
MAX_SCHEDULED_START_CLOCK_SKEW_SECONDS = 300
DEFAULT_SCHEDULED_START_MUTATION_WAIT_SECONDS = 10
MAX_SCHEDULED_START_MUTATION_WAIT_SECONDS = 60
DEFAULT_SCHEDULED_START_RECOVERY_MAX_INTERVAL_SECONDS = 60
MAX_SCHEDULED_START_RECOVERY_MAX_INTERVAL_SECONDS = 3_600
DEFAULT_INTERACTIVE_PRIORITY_KEY = 1
DEFAULT_STANDARD_PRIORITY_KEY = 5
DEFAULT_BATCH_PRIORITY_KEY = 10
MIN_TEMPORAL_PRIORITY_KEY = 1
MAX_TEMPORAL_PRIORITY_KEY = 255
DEFAULT_TEMPORAL_FAIRNESS_WEIGHT = 1.0
MAX_TEMPORAL_FAIRNESS_WEIGHT = 100.0
DEFAULT_ACTIVATION_INDEX_NAME = "scope-activations"
DEFAULT_ACTIVATION_LEASE_SECONDS = 30
MAX_ACTIVATION_LEASE_SECONDS = 3_600
DEFAULT_ACTIVATION_SCOPE_LIMIT = 1_000
MAX_ACTIVATION_SCOPE_LIMIT = 10_000
DEFAULT_ACTIVATION_TARGET_LIMIT = 1_000
MAX_ACTIVATION_TARGET_LIMIT = 10_000
MAX_OPERATIONS_METRICS_LINKS = 20
MAX_OPERATIONS_LINK_LABEL_LENGTH = 64
MAX_OPERATIONS_LINK_URL_LENGTH = 2_048


class SettingsGroup(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)


class PlaintextPayloadProtection(SettingsGroup):
    mode: Literal["plaintext"] = "plaintext"


class CodecPayloadProtection(SettingsGroup):
    mode: Literal["codec"] = "codec"
    active_key_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    readable_key_ids: frozenset[str] = Field(min_length=1, max_length=MAX_PAYLOAD_KEY_IDS)

    @model_validator(mode="after")
    def validate_key_rotation(self) -> CodecPayloadProtection:
        if self.active_key_id not in self.readable_key_ids:
            raise ValueError("The active payload key must also be readable")
        if any(
            not key_id or len(key_id) > MAX_IDENTIFIER_LENGTH for key_id in self.readable_key_ids
        ):
            raise ValueError(
                f"Payload key identifiers must contain 1-{MAX_IDENTIFIER_LENGTH} characters"
            )
        return self


PayloadProtectionSettings = Annotated[
    PlaintextPayloadProtection | CodecPayloadProtection,
    Field(discriminator="mode"),
]


class LocalCatalogSettings(SettingsGroup):
    backend: Literal["local"] = "local"


class S3CatalogSettings(SettingsGroup):
    backend: Literal["s3"] = "s3"
    bucket: str = Field(min_length=3, max_length=63)
    prefix: str = Field(
        default=DEFAULT_S3_CATALOG_PREFIX,
        min_length=1,
        max_length=MAX_S3_PREFIX_LENGTH,
    )
    region: str | None = Field(default=None, min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    endpoint_url: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_NETWORK_LOCATION_LENGTH,
    )
    expected_bucket_owner: str | None = Field(
        default=None,
        min_length=AWS_ACCOUNT_ID_LENGTH,
        max_length=AWS_ACCOUNT_ID_LENGTH,
        pattern=rf"^[0-9]{{{AWS_ACCOUNT_ID_LENGTH}}}$",
    )
    server_side_encryption: Literal["AES256", "aws:kms"] = "AES256"
    kms_key_id: str | None = Field(default=None, min_length=1, max_length=MAX_PATH_LENGTH)

    @model_validator(mode="after")
    def validate_s3_configuration(self) -> Self:
        validate_s3_bucket(self.bucket)
        self.prefix = normalize_s3_catalog_prefix(self.prefix)
        if self.server_side_encryption == "aws:kms" and self.kms_key_id is None:
            raise ValueError("S3 catalog aws:kms encryption requires kms_key_id")
        if self.server_side_encryption != "aws:kms" and self.kms_key_id is not None:
            raise ValueError("S3 catalog kms_key_id requires aws:kms encryption")
        if self.endpoint_url is not None:
            endpoint = urlsplit(self.endpoint_url)
            if (
                endpoint.scheme not in {"http", "https"}
                or not endpoint.hostname
                or endpoint.username is not None
                or endpoint.password is not None
                or endpoint.query
                or endpoint.fragment
            ):
                raise ValueError(
                    "S3 catalog endpoint_url must be an HTTP(S) origin without credentials, "
                    "query, or fragment"
                )
        return self


CatalogSettings = Annotated[
    LocalCatalogSettings | S3CatalogSettings,
    Field(discriminator="backend"),
]


class FileConfigurationSettings(SettingsGroup):
    backend: Literal["files"] = "files"


class SqliteConfigurationSettings(SettingsGroup):
    backend: Literal["sqlite"] = "sqlite"
    path: str = Field(
        default=DEFAULT_SQLITE_CONFIGURATION_PATH,
        min_length=1,
        max_length=MAX_PATH_LENGTH,
    )


class AwsConfigurationSettings(SettingsGroup):
    backend: Literal["aws"] = "aws"
    bucket: str = Field(min_length=3, max_length=63)
    table_name: str = Field(min_length=3, max_length=255)
    revision_index_name: str = Field(min_length=3, max_length=255)
    activation_index_name: str = Field(
        default=DEFAULT_ACTIVATION_INDEX_NAME,
        min_length=3,
        max_length=255,
    )
    prefix: str = Field(
        default=DEFAULT_S3_CONFIGURATION_PREFIX,
        min_length=1,
        max_length=MAX_S3_PREFIX_LENGTH,
    )
    region: str | None = Field(default=None, min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    endpoint_url: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_NETWORK_LOCATION_LENGTH,
    )
    expected_bucket_owner: str | None = Field(
        default=None,
        min_length=AWS_ACCOUNT_ID_LENGTH,
        max_length=AWS_ACCOUNT_ID_LENGTH,
        pattern=rf"^[0-9]{{{AWS_ACCOUNT_ID_LENGTH}}}$",
    )
    server_side_encryption: Literal["AES256", "aws:kms"] = "AES256"
    kms_key_id: str | None = Field(default=None, min_length=1, max_length=MAX_PATH_LENGTH)

    @model_validator(mode="after")
    def validate_aws_configuration(self) -> Self:
        validate_s3_bucket(self.bucket)
        self.prefix = normalize_s3_catalog_prefix(self.prefix)
        if (self.server_side_encryption == "aws:kms") != (self.kms_key_id is not None):
            raise ValueError("S3 configuration KMS key requires aws:kms encryption")
        if self.endpoint_url is not None:
            endpoint = urlsplit(self.endpoint_url)
            if (
                endpoint.scheme not in {"http", "https"}
                or not endpoint.hostname
                or endpoint.username is not None
                or endpoint.password is not None
                or endpoint.query
                or endpoint.fragment
            ):
                raise ValueError(
                    "AWS configuration endpoint_url must be an HTTP(S) origin without "
                    "credentials, query, or fragment"
                )
        return self


ConfigurationSettings = Annotated[
    FileConfigurationSettings | SqliteConfigurationSettings | AwsConfigurationSettings,
    Field(discriminator="backend"),
]


class LocalTemporalConnectionSettings(SettingsGroup):
    mode: Literal["local_plaintext"] = "local_plaintext"
    host: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_NETWORK_LOCATION_LENGTH,
    )


class TlsTemporalConnectionSettings(SettingsGroup):
    mode: Literal["tls"] = "tls"
    server_name: str = Field(min_length=1, max_length=MAX_SERVER_NAME_LENGTH)
    root_ca_path: str | None = Field(default=None, min_length=1, max_length=MAX_PATH_LENGTH)
    client_certificate_path: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_PATH_LENGTH,
    )
    client_private_key_path: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_PATH_LENGTH,
    )
    api_key: SecretStr | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_PATH_LENGTH,
        repr=False,
    )

    @model_validator(mode="after")
    def validate_client_identity(self) -> Self:
        if (self.client_certificate_path is None) != (self.client_private_key_path is None):
            raise ValueError(
                "Temporal client certificate and private key paths must be configured together"
            )
        return self


TemporalConnectionSettings = Annotated[
    LocalTemporalConnectionSettings | TlsTemporalConnectionSettings,
    Field(discriminator="mode"),
]


class DeploymentRegistrationSettings(SettingsGroup):
    attempts: int = Field(
        default=DEFAULT_DEPLOYMENT_REGISTRATION_ATTEMPTS,
        ge=1,
        le=MAX_DEPLOYMENT_REGISTRATION_ATTEMPTS,
    )
    interval_seconds: float = Field(
        default=DEFAULT_DEPLOYMENT_REGISTRATION_INTERVAL_SECONDS,
        gt=0,
        le=MAX_DEPLOYMENT_REGISTRATION_INTERVAL_SECONDS,
    )


class TemporalSettings(SettingsGroup):
    address: str = Field(
        default="localhost:7233", min_length=1, max_length=MAX_NETWORK_LOCATION_LENGTH
    )
    task_queue: str = Field(
        default="gateway-workflows", min_length=1, max_length=MAX_IDENTIFIER_LENGTH
    )
    namespace: str = Field(default="default", min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    connection: TemporalConnectionSettings = Field(
        default_factory=lambda: TlsTemporalConnectionSettings(server_name="localhost")
    )
    payload_protection: PayloadProtectionSettings = Field(
        default_factory=PlaintextPayloadProtection
    )
    deployment_registration: DeploymentRegistrationSettings = Field(
        default_factory=DeploymentRegistrationSettings
    )


class BrokerDeclaration(SettingsGroup):
    provider: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    config: dict[str, object] = Field(default_factory=dict)


class ConsumerEndpoint(SettingsGroup):
    broker: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    destination: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    dead_letter_destination: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH)


class ConsumerReconnectSettings(SettingsGroup):
    initial_delay_seconds: float = Field(
        default=DEFAULT_CONSUMER_RECONNECT_INITIAL_SECONDS,
        gt=0,
        le=MAX_CONSUMER_RECONNECT_SECONDS,
    )
    max_delay_seconds: float = Field(
        default=DEFAULT_CONSUMER_RECONNECT_MAX_SECONDS,
        gt=0,
        le=MAX_CONSUMER_RECONNECT_SECONDS,
    )
    multiplier: float = Field(
        default=DEFAULT_CONSUMER_RECONNECT_MULTIPLIER,
        gt=1,
        le=MAX_CONSUMER_RECONNECT_MULTIPLIER,
    )
    jitter_fraction: float = Field(
        default=DEFAULT_CONSUMER_RECONNECT_JITTER_FRACTION,
        ge=0,
        le=1,
    )

    @model_validator(mode="after")
    def validate_delay_bounds(self) -> Self:
        if self.initial_delay_seconds > self.max_delay_seconds:
            raise ValueError("Consumer reconnect initial delay cannot exceed its maximum delay")
        return self


class MessagingSettings(SettingsGroup):
    trigger: ConsumerEndpoint | None = None
    response: ConsumerEndpoint | None = None
    message_concurrency: int = Field(
        default=DEFAULT_MESSAGE_CONCURRENCY,
        ge=1,
        le=MAX_MESSAGE_CONCURRENCY,
    )
    deduplication_capacity: int = Field(
        default=DEFAULT_DEDUPLICATION_CAPACITY,
        ge=1,
        le=MAX_DEDUPLICATION_CAPACITY,
    )
    deduplication_retention_seconds: float = Field(
        default=DEFAULT_DEDUPLICATION_RETENTION_SECONDS,
        gt=0,
        le=MAX_DEDUPLICATION_RETENTION_SECONDS,
    )
    consumer_ready_timeout_seconds: float = Field(
        default=DEFAULT_CONSUMER_READY_TIMEOUT_SECONDS,
        gt=0,
        le=MAX_CONSUMER_READY_TIMEOUT_SECONDS,
    )
    reconnect: ConsumerReconnectSettings = Field(default_factory=ConsumerReconnectSettings)


class PathSettings(SettingsGroup):
    config_dir: str = Field(default="configs", min_length=1, max_length=MAX_PATH_LENGTH)
    catalog_dir: str | None = Field(default=None, min_length=1, max_length=MAX_PATH_LENGTH)

    @property
    def definition_catalog_dir(self) -> str:
        return self.catalog_dir or self.config_dir


class LoggingSettings(SettingsGroup):
    level: str = Field(default="INFO", min_length=1, max_length=MAX_IDENTIFIER_LENGTH)


class PinnedStartRetrySettings(SettingsGroup):
    attempts: int = Field(
        default=DEFAULT_PINNED_START_RETRY_ATTEMPTS,
        ge=1,
        le=MAX_PINNED_START_RETRY_ATTEMPTS,
    )
    interval_seconds: float = Field(
        default=DEFAULT_PINNED_START_RETRY_INTERVAL_SECONDS,
        gt=0,
        le=MAX_PINNED_START_RETRY_INTERVAL_SECONDS,
    )


class RuntimeSettings(SettingsGroup):
    profile: RuntimeProfile = RuntimeProfile.PRODUCTION
    scope: RuntimeScope = LOCAL_RUNTIME_SCOPE
    pinned_start_retry: PinnedStartRetrySettings = Field(default_factory=PinnedStartRetrySettings)
    worker_ready_timeout_seconds: float = Field(
        default=DEFAULT_WORKER_READY_TIMEOUT_SECONDS,
        gt=0,
        le=MAX_WORKER_READY_TIMEOUT_SECONDS,
    )


class ControlSettings(SettingsGroup):
    host: str = Field(default="127.0.0.1", min_length=1, max_length=MAX_NETWORK_LOCATION_LENGTH)
    port: int = Field(default=DEFAULT_CONTROL_PORT, ge=1, le=MAX_TCP_PORT)
    max_request_body_bytes: int = Field(
        default=DEFAULT_CONTROL_REQUEST_BYTES,
        ge=1,
        le=MAX_CONTROL_REQUEST_BYTES,
    )
    max_response_body_bytes: int = Field(
        default=DEFAULT_CONTROL_RESPONSE_BYTES,
        ge=MIN_CONTROL_RESPONSE_BYTES,
        le=MAX_CONTROL_RESPONSE_BYTES,
    )
    default_list_limit: int = Field(
        default=DEFAULT_CONTROL_LIST_LIMIT,
        ge=1,
        le=MAX_CONTROL_LIST_LIMIT,
    )
    temporal_rpc_timeout_seconds: float = Field(
        default=DEFAULT_CONTROL_RPC_TIMEOUT_SECONDS,
        gt=0,
        le=MAX_CONTROL_RPC_TIMEOUT_SECONDS,
    )
    shutdown_grace_seconds: int = Field(
        default=DEFAULT_CONTROL_SHUTDOWN_GRACE_SECONDS,
        ge=1,
        le=MAX_CONTROL_SHUTDOWN_GRACE_SECONDS,
    )
    max_header_bytes: int = Field(
        default=DEFAULT_CONTROL_HEADER_BYTES,
        ge=1,
        le=MAX_CONTROL_HEADER_BYTES,
    )
    max_header_count: int = Field(
        default=DEFAULT_CONTROL_HEADER_COUNT,
        ge=1,
        le=MAX_CONTROL_HEADER_COUNT,
    )
    max_query_bytes: int = Field(
        default=DEFAULT_CONTROL_QUERY_BYTES,
        ge=1,
        le=MAX_CONTROL_QUERY_BYTES,
    )


class OperationsMetricsLinkSettings(SettingsGroup):
    label: str = Field(min_length=1, max_length=MAX_OPERATIONS_LINK_LABEL_LENGTH)
    url: str = Field(min_length=1, max_length=MAX_OPERATIONS_LINK_URL_LENGTH)

    @model_validator(mode="after")
    def validate_url(self) -> Self:
        parsed = urlsplit(self.url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Operations metrics links require an absolute HTTP(S) URL")
        if (
            parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "Operations metrics links cannot contain credentials, query, or fragment"
            )
        return self


class OperationsSettings(SettingsGroup):
    admin_panel_enabled: bool = False
    indexed_search_attributes_enabled: bool = False
    local_source_authoring_enabled: bool = False
    metrics_links: tuple[OperationsMetricsLinkSettings, ...] = Field(
        default_factory=tuple,
        max_length=MAX_OPERATIONS_METRICS_LINKS,
    )


class ScheduleSettings(SettingsGroup):
    task_queue: str = Field(
        default=DEFAULT_SCHEDULE_TASK_QUEUE,
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
    )
    max_schedules: int = Field(
        default=DEFAULT_SCHEDULE_LIMIT,
        ge=1,
        le=MAX_SCHEDULE_LIMIT,
    )
    page_size: int = Field(
        default=DEFAULT_SCHEDULE_PAGE_SIZE,
        ge=1,
        le=MAX_SCHEDULE_PAGE_SIZE,
    )
    describe_concurrency: int = Field(
        default=DEFAULT_SCHEDULE_DESCRIBE_CONCURRENCY,
        ge=1,
        le=MAX_SCHEDULE_DESCRIBE_CONCURRENCY,
    )
    temporal_rpc_timeout_seconds: float = Field(
        default=DEFAULT_SCHEDULE_RPC_TIMEOUT_SECONDS,
        gt=0,
        le=MAX_SCHEDULE_RPC_TIMEOUT_SECONDS,
    )
    recent_action_limit: int = Field(
        default=DEFAULT_SCHEDULE_RECENT_ACTION_LIMIT,
        ge=1,
        le=MAX_SCHEDULE_RECENT_ACTION_LIMIT,
    )
    dispatch_timeout_seconds: int = Field(
        default=DEFAULT_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
        ge=MIN_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
        le=MAX_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
    )
    dispatch_attempts: int = Field(
        default=DEFAULT_SCHEDULE_DISPATCH_ATTEMPTS,
        ge=MIN_SCHEDULE_DISPATCH_ATTEMPTS,
        le=MAX_SCHEDULE_DISPATCH_ATTEMPTS,
    )


class ScheduledStartWorkloadClass(str, Enum):
    INTERACTIVE = "interactive"
    STANDARD = "standard"
    BATCH = "batch"


class ScheduledStartWorkloadPolicySettings(SettingsGroup):
    allowed_classes: frozenset[ScheduledStartWorkloadClass] = frozenset(ScheduledStartWorkloadClass)
    interactive_priority_key: int = Field(
        default=DEFAULT_INTERACTIVE_PRIORITY_KEY,
        ge=MIN_TEMPORAL_PRIORITY_KEY,
        le=MAX_TEMPORAL_PRIORITY_KEY,
    )
    standard_priority_key: int = Field(
        default=DEFAULT_STANDARD_PRIORITY_KEY,
        ge=MIN_TEMPORAL_PRIORITY_KEY,
        le=MAX_TEMPORAL_PRIORITY_KEY,
    )
    batch_priority_key: int = Field(
        default=DEFAULT_BATCH_PRIORITY_KEY,
        ge=MIN_TEMPORAL_PRIORITY_KEY,
        le=MAX_TEMPORAL_PRIORITY_KEY,
    )
    fairness_weight: float = Field(
        default=DEFAULT_TEMPORAL_FAIRNESS_WEIGHT,
        gt=0,
        le=MAX_TEMPORAL_FAIRNESS_WEIGHT,
    )

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        if not self.allowed_classes:
            raise ValueError("Scheduled-start workload policy must grant at least one class")
        if not (
            self.interactive_priority_key <= self.standard_priority_key <= self.batch_priority_key
        ):
            raise ValueError(
                "Scheduled-start priority keys must order interactive, standard, then batch"
            )
        return self

    def priority_key(self, workload_class: ScheduledStartWorkloadClass) -> int:
        return {
            ScheduledStartWorkloadClass.INTERACTIVE: self.interactive_priority_key,
            ScheduledStartWorkloadClass.STANDARD: self.standard_priority_key,
            ScheduledStartWorkloadClass.BATCH: self.batch_priority_key,
        }[workload_class]


class ScheduledStartSettings(SettingsGroup):
    max_horizon_seconds: int = Field(
        default=DEFAULT_SCHEDULED_START_HORIZON_SECONDS,
        ge=1,
        le=MAX_SCHEDULED_START_HORIZON_SECONDS,
    )
    max_input_bytes: int = Field(
        default=DEFAULT_SCHEDULED_START_INPUT_BYTES,
        ge=1,
        le=MAX_SCHEDULED_START_INPUT_BYTES,
    )
    max_pending_per_scope: int = Field(
        default=DEFAULT_PENDING_SCHEDULED_STARTS_PER_SCOPE,
        ge=1,
        le=MAX_PENDING_SCHEDULED_STARTS_PER_SCOPE,
    )
    default_list_limit: int = Field(
        default=DEFAULT_SCHEDULED_START_LIST_LIMIT,
        ge=1,
        le=MAX_SCHEDULED_START_LIST_LIMIT,
    )
    max_list_limit: int = Field(
        default=MAX_SCHEDULED_START_LIST_LIMIT,
        ge=1,
        le=MAX_SCHEDULED_START_LIST_LIMIT,
    )
    max_schedules: int = Field(
        default=DEFAULT_SCHEDULED_START_COLLECTION_LIMIT,
        ge=1,
        le=MAX_SCHEDULED_START_COLLECTION_LIMIT,
    )
    describe_concurrency: int = Field(
        default=DEFAULT_SCHEDULE_DESCRIBE_CONCURRENCY,
        ge=1,
        le=MAX_SCHEDULE_DESCRIBE_CONCURRENCY,
    )
    terminal_retention_seconds: int = Field(
        default=DEFAULT_SCHEDULED_START_TERMINAL_RETENTION_SECONDS,
        ge=1,
        le=MAX_SCHEDULED_START_TERMINAL_RETENTION_SECONDS,
    )
    cleanup_page_size: int = Field(
        default=DEFAULT_SCHEDULED_START_CLEANUP_PAGE_SIZE,
        ge=1,
        le=MAX_SCHEDULED_START_CLEANUP_PAGE_SIZE,
    )
    cleanup_interval_seconds: int = Field(
        default=DEFAULT_SCHEDULED_START_CLEANUP_INTERVAL_SECONDS,
        ge=MIN_SCHEDULED_START_CLEANUP_INTERVAL_SECONDS,
        le=MAX_SCHEDULED_START_CLEANUP_INTERVAL_SECONDS,
    )
    clock_skew_seconds: int = Field(
        default=DEFAULT_SCHEDULED_START_CLOCK_SKEW_SECONDS,
        ge=0,
        le=MAX_SCHEDULED_START_CLOCK_SKEW_SECONDS,
    )
    dispatch_timeout_seconds: int = Field(
        default=DEFAULT_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
        ge=MIN_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
        le=MAX_SCHEDULE_DISPATCH_TIMEOUT_SECONDS,
    )
    dispatch_attempts: int = Field(
        default=DEFAULT_SCHEDULE_DISPATCH_ATTEMPTS,
        ge=MIN_SCHEDULE_DISPATCH_ATTEMPTS,
        le=MAX_SCHEDULE_DISPATCH_ATTEMPTS,
    )
    mutation_wait_seconds: int = Field(
        default=DEFAULT_SCHEDULED_START_MUTATION_WAIT_SECONDS,
        ge=1,
        le=MAX_SCHEDULED_START_MUTATION_WAIT_SECONDS,
    )
    recovery_max_interval_seconds: int = Field(
        default=DEFAULT_SCHEDULED_START_RECOVERY_MAX_INTERVAL_SECONDS,
        ge=1,
        le=MAX_SCHEDULED_START_RECOVERY_MAX_INTERVAL_SECONDS,
    )
    workload_policy: ScheduledStartWorkloadPolicySettings = Field(
        default_factory=ScheduledStartWorkloadPolicySettings
    )

    @model_validator(mode="after")
    def validate_list_limits(self) -> Self:
        if self.default_list_limit > self.max_list_limit:
            raise ValueError("Scheduled-start default list limit cannot exceed its maximum")
        if self.max_pending_per_scope > self.max_schedules:
            raise ValueError(
                "Scheduled-start pending quota cannot exceed its collection observation bound"
            )
        return self


class ActivationSettings(SettingsGroup):
    lease_seconds: int = Field(
        default=DEFAULT_ACTIVATION_LEASE_SECONDS,
        ge=1,
        le=MAX_ACTIVATION_LEASE_SECONDS,
    )
    max_scopes: int = Field(
        default=DEFAULT_ACTIVATION_SCOPE_LIMIT,
        ge=1,
        le=MAX_ACTIVATION_SCOPE_LIMIT,
    )
    max_targets_per_scope: int = Field(
        default=DEFAULT_ACTIVATION_TARGET_LIMIT,
        ge=1,
        le=MAX_ACTIVATION_TARGET_LIMIT,
    )


class ResourceConnectionSettings(SettingsGroup):
    postgres_dsns: dict[Identifier, SecretStr] = Field(default_factory=dict)
    redis_urls: dict[Identifier, SecretStr] = Field(default_factory=dict)


class DeploymentSettings(SettingsGroup):
    name: str = Field(default="justflow", min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    build_id: str | None = Field(default=None, min_length=1, max_length=MAX_IDENTIFIER_LENGTH)
    artifact_digest: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
    )
    package_version: str | None = Field(
        default_factory=installed_engine_version,
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
    )
    source_revision: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
    )
    compatible_engine_workflow_abis: frozenset[str] = Field(
        default_factory=lambda: frozenset({ENGINE_WORKFLOW_ABI})
    )


class RuntimeLimitSettings(SettingsGroup):
    trigger_payload_bytes: int = Field(default=DEFAULT_TRIGGER_PAYLOAD_BYTES, ge=1)
    workflow_output_bytes: int = Field(default=DEFAULT_WORKFLOW_OUTPUT_BYTES, ge=1)
    activity_input_bytes: int = Field(default=DEFAULT_ACTIVITY_INPUT_BYTES, ge=1)
    activity_output_bytes: int = Field(default=DEFAULT_ACTIVITY_OUTPUT_BYTES, ge=1)
    failure_record_bytes: int = Field(default=DEFAULT_FAILURE_RECORD_BYTES, ge=1)
    audit_record_bytes: int = Field(default=DEFAULT_AUDIT_RECORD_BYTES, ge=1)
    cache_entry_bytes: int = Field(default=DEFAULT_CACHE_ENTRY_BYTES, ge=1)
    fanout_items: int = Field(default=DEFAULT_FANOUT_ITEMS, ge=1)
    fanout_chunk_items: int = Field(default=DEFAULT_FANOUT_CHUNK_ITEMS, ge=1)
    parallelism: int = Field(default=DEFAULT_PARALLELISM, ge=1)
    loop_attempts: int = Field(default=DEFAULT_LOOP_ATTEMPTS, ge=1)
    total_invocations: int = Field(default=DEFAULT_TOTAL_INVOCATIONS, ge=1)
    queued_messages: int = Field(default=DEFAULT_QUEUED_MESSAGES, ge=1)
    signal_payload_bytes: int = Field(default=DEFAULT_SIGNAL_PAYLOAD_BYTES, ge=1)
    queued_message_bytes: int = Field(default=DEFAULT_QUEUED_MESSAGE_BYTES, ge=1)
    collection_items: int = Field(default=DEFAULT_COLLECTION_ITEMS, ge=1)
    workflow_state_bytes: int = Field(default=DEFAULT_WORKFLOW_STATE_BYTES, ge=1)
    continuation_input_bytes: int = Field(default=DEFAULT_CONTINUATION_INPUT_BYTES, ge=1)
    history_events: int = Field(default=DEFAULT_HISTORY_EVENTS, ge=1)
    history_bytes: int = Field(default=DEFAULT_HISTORY_BYTES, ge=1)

    @model_validator(mode="after")
    def validate_fanout_chunk(self) -> RuntimeLimitSettings:
        if self.fanout_chunk_items > self.fanout_items:
            raise ValueError("fanout_chunk_items cannot exceed fanout_items")
        return self

    def snapshot(self) -> RuntimeLimits:
        return RuntimeLimits(**self.model_dump())


class Settings(BaseSettings):
    catalog: CatalogSettings = Field(default_factory=LocalCatalogSettings)
    configuration: ConfigurationSettings = Field(default_factory=FileConfigurationSettings)
    temporal: TemporalSettings = TemporalSettings()
    brokers: dict[str, BrokerDeclaration] = Field(default_factory=dict)
    messaging: MessagingSettings = MessagingSettings()
    transport_security: TransportSecuritySettings = TransportSecuritySettings()
    resource_connections: ResourceConnectionSettings = ResourceConnectionSettings()
    paths: PathSettings = PathSettings()
    logging: LoggingSettings = LoggingSettings()
    runtime: RuntimeSettings = RuntimeSettings()
    control: ControlSettings = ControlSettings()
    operations: OperationsSettings = OperationsSettings()
    schedules: ScheduleSettings = ScheduleSettings()
    scheduled_starts: ScheduledStartSettings = ScheduledStartSettings()
    activation: ActivationSettings = ActivationSettings()
    deployment: DeploymentSettings = DeploymentSettings()
    limits: RuntimeLimitSettings = RuntimeLimitSettings()

    model_config = SettingsConfigDict(
        env_prefix=ENV_PREFIX,
        env_nested_delimiter=ENV_NESTED_DELIMITER,
        extra="forbid",
    )

    @model_validator(mode="after")
    def validate_temporal_runtime_profile(self) -> Self:
        if (
            isinstance(self.temporal.connection, LocalTemporalConnectionSettings)
            and self.runtime.profile is not RuntimeProfile.LOCAL
        ):
            raise ValueError("Temporal local plaintext requires the explicit local runtime profile")
        if self.temporal.task_queue == self.schedules.task_queue:
            raise ValueError(
                "Temporal business and schedule dispatch task queues must be different"
            )
        if self.scheduled_starts.max_input_bytes > self.limits.trigger_payload_bytes:
            raise ValueError(
                "Scheduled-start input bound cannot exceed the workflow trigger payload bound"
            )
        if self.operations.local_source_authoring_enabled:
            if self.runtime.profile is not RuntimeProfile.LOCAL:
                raise ValueError("Local source authoring requires the local runtime profile")
            if not isinstance(self.configuration, FileConfigurationSettings):
                raise ValueError("Local source authoring requires file-backed configuration")
        if (
            self.runtime.profile is RuntimeProfile.PRODUCTION
            and self.runtime.scope == LOCAL_RUNTIME_SCOPE
        ):
            raise ValueError("Production requires an explicit non-local runtime scope")
        return self


def load_settings(*, runtime_profile: RuntimeProfile | None = None) -> Settings:
    """Load settings from the environment; fails fast on invalid values."""
    overrides: dict[str, Any] = {}
    if runtime_profile is not None:
        overrides["runtime"] = {"profile": runtime_profile}
    return Settings(**overrides)
