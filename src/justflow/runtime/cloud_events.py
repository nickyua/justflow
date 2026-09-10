"""Trusted host mappings for bounded cloud-event workflow ingress."""

from __future__ import annotations

import hashlib
import ipaddress
import json
from collections.abc import Mapping, Sequence
from enum import Enum
from types import MappingProxyType
from typing import Annotated, Literal, Protocol, cast

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    TypeAdapter,
    ValidationError,
    field_validator,
)

from justflow.config.grammar import ProviderName, WorkflowName
from justflow.config.provider_names import is_valid_provider_name
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.engine.data import DataNormalizationError, normalize_json_object
from justflow.engine.limits import (
    LimitExceededError,
    PayloadSerializationError,
    enforce_payload_bytes,
)
from justflow.runtime.starter import (
    CloudEventSourceIdentity,
    StartWorkflowRequest,
    StartWorkflowResult,
    WorkflowStarter,
)
from justflow.scope import (
    LOCAL_RUNTIME_SCOPE,
    RuntimeScope,
    ScopeBindingKind,
    TrustedScopeBinding,
)
from justflow.sdk.message_contract import MAX_IDENTIFIER_LENGTH

EVENTBRIDGE_VERSION: Literal["0"] = "0"
EVENTBRIDGE_S3_SOURCE = "aws.s3"
EVENTBRIDGE_S3_OBJECT_CREATED = "Object Created"
MAX_CLOUD_EVENT_SOURCE_LENGTH = 256
MAX_CLOUD_EVENT_SUBJECT_LENGTH = 2_048
MAX_EVENTBRIDGE_DETAIL_TYPE_LENGTH = 256
MAX_EVENTBRIDGE_ACCOUNT_LENGTH = 64
MAX_EVENTBRIDGE_REGION_LENGTH = 64
MAX_EVENTBRIDGE_RESOURCES = 32
MAX_EVENTBRIDGE_RESOURCE_LENGTH = 2_048
MAX_CLOUD_EVENT_MAPPINGS = 1_000
MAX_CLOUD_EVENT_ERROR_LENGTH = 256
MAX_S3_BUCKET_LENGTH = 63
MIN_S3_BUCKET_LENGTH = 3
MAX_S3_KEY_BYTES = 1_024
MAX_S3_ETAG_LENGTH = 256
MAX_S3_VERSION_ID_BYTES = 1_024
MAX_S3_SEQUENCER_LENGTH = 256
MAX_S3_REQUEST_ID_LENGTH = 256
MAX_S3_REQUESTER_LENGTH = 256
MAX_S3_SOURCE_IP_LENGTH = 64
MAX_S3_REASON_LENGTH = 128
MAX_S3_WRITE_DESTINATIONS = 128
S3_BUCKET_PATTERN = r"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$"
S3_RESERVED_PREFIXES = ("xn--", "sthree-", "amzn_s3_demo_")
S3_RESERVED_SUFFIXES = ("-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3")

_WORKFLOW_NAME_ADAPTER = TypeAdapter(WorkflowName)

EventBridgeResource = Annotated[
    StrictStr,
    Field(min_length=1, max_length=MAX_EVENTBRIDGE_RESOURCE_LENGTH),
]


class StrictCloudEventModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class CloudEventErrorCode(str, Enum):
    UNKNOWN_MAPPING = "unknown_mapping"
    INVALID_PAYLOAD = "invalid_payload"
    INVALID_IDENTITY = "invalid_identity"
    SOURCE_REJECTED = "source_rejected"
    SELF_TRIGGER_LOOP = "self_trigger_loop"
    MAPPING_UNAVAILABLE = "mapping_unavailable"


class CloudEventMappingError(Exception):
    """A cloud event could not be mapped through its trusted host binding."""

    def __init__(
        self,
        message: str,
        *,
        code: CloudEventErrorCode = CloudEventErrorCode.INVALID_PAYLOAD,
        retryable: bool = False,
        dead_letter_reason: str = "invalid cloud-event delivery",
    ) -> None:
        if (
            not message
            or len(message) > MAX_CLOUD_EVENT_ERROR_LENGTH
            or not dead_letter_reason
            or len(dead_letter_reason) > MAX_CLOUD_EVENT_ERROR_LENGTH
        ):
            raise ValueError("Cloud-event error detail is invalid")
        self.code = code
        self.retryable = retryable
        self.dead_letter_reason = dead_letter_reason
        super().__init__(message)


class CloudEventPayloadError(Exception):
    """A source adapter permanently rejected an external event payload."""


class CloudEventMetadata(StrictCloudEventModel):
    source: str = Field(min_length=1, max_length=MAX_CLOUD_EVENT_SOURCE_LENGTH)
    event_id: str = Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH, repr=False)
    subject: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_CLOUD_EVENT_SUBJECT_LENGTH,
    )
    timestamp: AwareDatetime
    correlation_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
        repr=False,
    )
    trace_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
        repr=False,
    )


class EventBridgeEnvelope(StrictCloudEventModel):
    version: Literal["0"]
    event_id: str = Field(
        alias="id",
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
        repr=False,
    )
    detail_type: str = Field(
        alias="detail-type",
        min_length=1,
        max_length=MAX_EVENTBRIDGE_DETAIL_TYPE_LENGTH,
    )
    source: str = Field(min_length=1, max_length=MAX_CLOUD_EVENT_SOURCE_LENGTH)
    account: str = Field(min_length=1, max_length=MAX_EVENTBRIDGE_ACCOUNT_LENGTH, repr=False)
    timestamp: AwareDatetime = Field(alias="time")
    region: str = Field(min_length=1, max_length=MAX_EVENTBRIDGE_REGION_LENGTH)
    resources: tuple[EventBridgeResource, ...] = Field(max_length=MAX_EVENTBRIDGE_RESOURCES)
    detail: dict[str, object] = Field(repr=False)


class EventBridgeWorkflowEvent(StrictCloudEventModel):
    metadata: CloudEventMetadata
    envelope_version: Literal["0"] = EVENTBRIDGE_VERSION
    detail_type: str = Field(min_length=1, max_length=MAX_EVENTBRIDGE_DETAIL_TYPE_LENGTH)
    account: str = Field(min_length=1, max_length=MAX_EVENTBRIDGE_ACCOUNT_LENGTH, repr=False)
    region: str = Field(min_length=1, max_length=MAX_EVENTBRIDGE_REGION_LENGTH)
    resources: tuple[EventBridgeResource, ...] = Field(max_length=MAX_EVENTBRIDGE_RESOURCES)
    detail: dict[str, object] = Field(repr=False)


class S3EventBucket(StrictCloudEventModel):
    name: str = Field(
        min_length=MIN_S3_BUCKET_LENGTH,
        max_length=MAX_S3_BUCKET_LENGTH,
        pattern=S3_BUCKET_PATTERN,
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        _validate_s3_bucket(value)
        return value


class S3EventObject(StrictCloudEventModel):
    key: str = Field(min_length=1)
    size: StrictInt | None = Field(default=None, ge=0)
    etag: str | None = Field(default=None, min_length=1, max_length=MAX_S3_ETAG_LENGTH)
    version_id: str | None = Field(
        default=None,
        alias="version-id",
        min_length=1,
        max_length=MAX_S3_VERSION_ID_BYTES,
        repr=False,
    )
    sequencer: str = Field(min_length=1, max_length=MAX_S3_SEQUENCER_LENGTH, repr=False)

    @field_validator("version_id")
    @classmethod
    def validate_version_id(cls, value: str | None) -> str | None:
        if value is not None and len(value.encode("utf-8")) > MAX_S3_VERSION_ID_BYTES:
            raise ValueError("S3 object version identity is too large")
        return value


class S3ObjectCreatedDetail(StrictCloudEventModel):
    version: Literal["0"]
    bucket: S3EventBucket
    object: S3EventObject
    request_id: str | None = Field(
        default=None,
        alias="request-id",
        min_length=1,
        max_length=MAX_S3_REQUEST_ID_LENGTH,
        repr=False,
    )
    requester: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_S3_REQUESTER_LENGTH,
        repr=False,
    )
    source_ip_address: str | None = Field(
        default=None,
        alias="source-ip-address",
        min_length=1,
        max_length=MAX_S3_SOURCE_IP_LENGTH,
        repr=False,
    )
    reason: str | None = Field(default=None, min_length=1, max_length=MAX_S3_REASON_LENGTH)
    correlation_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
        repr=False,
    )
    trace_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
        repr=False,
    )


class S3ObjectCreatedWorkflowEvent(StrictCloudEventModel):
    metadata: CloudEventMetadata
    bucket: str = Field(min_length=MIN_S3_BUCKET_LENGTH, max_length=MAX_S3_BUCKET_LENGTH)
    key: str = Field(min_length=1)
    size: StrictInt | None = Field(default=None, ge=0)
    version_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_S3_VERSION_ID_BYTES,
        repr=False,
    )
    etag: str | None = Field(default=None, min_length=1, max_length=MAX_S3_ETAG_LENGTH)
    sequencer: str = Field(min_length=1, max_length=MAX_S3_SEQUENCER_LENGTH, repr=False)


class S3ObjectKeyFilter(StrictCloudEventModel):
    bucket: str = Field(
        min_length=MIN_S3_BUCKET_LENGTH,
        max_length=MAX_S3_BUCKET_LENGTH,
        pattern=S3_BUCKET_PATTERN,
    )
    prefix: str = ""
    suffix: str = ""

    @field_validator("bucket")
    @classmethod
    def validate_bucket(cls, value: str) -> str:
        _validate_s3_bucket(value)
        return value

    def matches(self, bucket: str, key: str) -> bool:
        return self.bucket == bucket and key.startswith(self.prefix) and key.endswith(self.suffix)


class CloudEventMapper(Protocol):
    async def event_identity(self, event: object) -> str: ...

    async def to_start_request(
        self,
        event: object,
        event_identity: str,
    ) -> StartWorkflowRequest: ...


class EventBridgeEventMapper:
    """Map one exact EventBridge source and optional detail type to a workflow."""

    def __init__(
        self,
        *,
        mapping_name: str,
        workflow_name: str,
        source: str,
        detail_type: str | None = None,
    ) -> None:
        self._mapping_name = _mapping_name(mapping_name)
        self._workflow_name = _WORKFLOW_NAME_ADAPTER.validate_python(workflow_name)
        self._source = _bounded_text(
            source,
            name="EventBridge source",
            maximum=MAX_CLOUD_EVENT_SOURCE_LENGTH,
        )
        self._detail_type = (
            _bounded_text(
                detail_type,
                name="EventBridge detail type",
                maximum=MAX_EVENTBRIDGE_DETAIL_TYPE_LENGTH,
            )
            if detail_type is not None
            else None
        )

    async def event_identity(self, event: object) -> str:
        return self._parse(event).event_id

    async def to_start_request(
        self,
        event: object,
        event_identity: str,
    ) -> StartWorkflowRequest:
        envelope = self._parse(event)
        if event_identity != envelope.event_id:
            raise CloudEventPayloadError("EventBridge identity changed during mapping")
        detail = normalize_json_object(envelope.detail, path="$.detail")
        correlation_id = _metadata_identifier(detail, "correlation_id") or envelope.event_id
        trace_id = _metadata_identifier(detail, "trace_id")
        subject = _metadata_subject(detail) or (
            envelope.resources[0] if envelope.resources else None
        )
        workflow_event = EventBridgeWorkflowEvent(
            metadata=CloudEventMetadata(
                source=envelope.source,
                event_id=envelope.event_id,
                subject=subject,
                timestamp=envelope.timestamp,
                correlation_id=correlation_id,
                trace_id=trace_id,
            ),
            detail_type=envelope.detail_type,
            account=envelope.account,
            region=envelope.region,
            resources=envelope.resources,
            detail=cast(dict[str, object], detail),
        )
        return _start_request(
            workflow_name=self._workflow_name,
            mapping=self._mapping_name,
            event_identity=event_identity,
            workflow_event=workflow_event,
            correlation_id=correlation_id,
            trace_id=trace_id,
        )

    def _parse(self, event: object) -> EventBridgeEnvelope:
        envelope = EventBridgeEnvelope.model_validate(event)
        if envelope.source != self._source or (
            self._detail_type is not None and envelope.detail_type != self._detail_type
        ):
            raise CloudEventMappingError(
                "EventBridge event does not match its registered source mapping",
                code=CloudEventErrorCode.SOURCE_REJECTED,
                dead_letter_reason="cloud event does not match source mapping",
            )
        return envelope


class S3ObjectCreatedEventMapper:
    """Map one bounded S3 object-created source filter to a workflow."""

    def __init__(
        self,
        *,
        mapping_name: str,
        workflow_name: str,
        source: S3ObjectKeyFilter,
        write_destinations: Sequence[S3ObjectKeyFilter] = (),
    ) -> None:
        self._mapping_name = _mapping_name(mapping_name)
        self._workflow_name = _WORKFLOW_NAME_ADAPTER.validate_python(workflow_name)
        _validate_s3_filter(source)
        destinations = tuple(write_destinations)
        if len(destinations) > MAX_S3_WRITE_DESTINATIONS:
            raise ValueError("S3 mapping has too many configured write destinations")
        for destination in destinations:
            _validate_s3_filter(destination)
            if _s3_filters_overlap(source, destination):
                raise ValueError("S3 event source overlaps a configured workflow write destination")
        self._source = source

    async def event_identity(self, event: object) -> str:
        envelope, detail = self._parse(event)
        del envelope
        return _s3_object_event_identity(detail)

    async def to_start_request(
        self,
        event: object,
        event_identity: str,
    ) -> StartWorkflowRequest:
        envelope, detail = self._parse(event)
        if event_identity != _s3_object_event_identity(detail):
            raise CloudEventPayloadError("S3 object identity changed during mapping")
        object_detail = detail.object
        subject = f"s3://{detail.bucket.name}/{object_detail.key}"
        workflow_event = S3ObjectCreatedWorkflowEvent(
            metadata=CloudEventMetadata(
                source=envelope.source,
                event_id=envelope.event_id,
                subject=subject,
                timestamp=envelope.timestamp,
                correlation_id=detail.correlation_id or envelope.event_id,
                trace_id=detail.trace_id,
            ),
            bucket=detail.bucket.name,
            key=object_detail.key,
            size=object_detail.size,
            version_id=object_detail.version_id,
            etag=object_detail.etag,
            sequencer=object_detail.sequencer,
        )
        return _start_request(
            workflow_name=self._workflow_name,
            mapping=self._mapping_name,
            event_identity=event_identity,
            workflow_event=workflow_event,
            correlation_id=detail.correlation_id or envelope.event_id,
            trace_id=detail.trace_id,
        )

    def _parse(self, event: object) -> tuple[EventBridgeEnvelope, S3ObjectCreatedDetail]:
        envelope = EventBridgeEnvelope.model_validate(event)
        if (
            envelope.source != EVENTBRIDGE_S3_SOURCE
            or envelope.detail_type != EVENTBRIDGE_S3_OBJECT_CREATED
        ):
            raise CloudEventMappingError(
                "EventBridge event is not an S3 object-created event",
                code=CloudEventErrorCode.SOURCE_REJECTED,
                dead_letter_reason="cloud event does not match S3 object-created mapping",
            )
        detail = S3ObjectCreatedDetail.model_validate(envelope.detail)
        _validate_s3_key(detail.object.key)
        if not self._source.matches(detail.bucket.name, detail.object.key):
            raise CloudEventMappingError(
                "S3 object does not match the registered key filter",
                code=CloudEventErrorCode.SOURCE_REJECTED,
                dead_letter_reason="S3 object does not match source mapping",
            )
        return envelope, detail


class CloudEventMappingRegistry:
    def __init__(self, *, scope: RuntimeScope = LOCAL_RUNTIME_SCOPE) -> None:
        self._default_scope = scope
        self._mappers: dict[str, CloudEventMapper] = {}
        self._scopes: dict[str, RuntimeScope] = {}

    @property
    def mappings(self) -> Mapping[str, CloudEventMapper]:
        return MappingProxyType(self._mappers)

    def register(
        self,
        name: str,
        mapper: CloudEventMapper,
        *,
        scope: RuntimeScope | None = None,
    ) -> None:
        if not is_valid_provider_name(name):
            raise ValueError("Cloud-event mapping name is invalid")
        if name in self._mappers:
            raise ValueError("Cloud-event mapping is already registered")
        if len(self._mappers) >= MAX_CLOUD_EVENT_MAPPINGS:
            raise ValueError("Cloud-event mapping registry is full")
        self._mappers[name] = mapper
        self._scopes[name] = scope or self._default_scope

    def resolve(self, name: str) -> tuple[CloudEventMapper, RuntimeScope]:
        try:
            return self._mappers[name], self._scopes[name]
        except KeyError as exc:
            raise CloudEventMappingError(
                "Cloud-event mapping is not registered",
                code=CloudEventErrorCode.UNKNOWN_MAPPING,
                dead_letter_reason="cloud-event mapping is not registered",
            ) from exc

    def scope_for(self, name: str) -> RuntimeScope:
        return self.resolve(name)[1]


class CloudEventIngress:
    def __init__(
        self,
        registry: CloudEventMappingRegistry,
        starter: WorkflowStarter,
        *,
        limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
    ) -> None:
        self._registry = registry
        self._starter = starter
        self._limits = limits

    def scope_for(self, mapping_name: str) -> RuntimeScope:
        return self._registry.scope_for(mapping_name)

    async def receive(self, mapping_name: str, event: object) -> StartWorkflowResult:
        mapper, bound_scope = self._registry.resolve(mapping_name)
        try:
            enforce_payload_bytes(
                event,
                boundary="cloud_event.payload",
                limit=self._limits.trigger_payload_bytes,
            )
            event_identity = await mapper.event_identity(event)
            request = await mapper.to_start_request(event, event_identity)
        except CloudEventMappingError:
            raise
        except (
            CloudEventPayloadError,
            DataNormalizationError,
            LimitExceededError,
            PayloadSerializationError,
            ValidationError,
        ) as exc:
            raise CloudEventMappingError(
                "Cloud-event payload is invalid",
                code=CloudEventErrorCode.INVALID_PAYLOAD,
                dead_letter_reason="invalid cloud-event payload",
            ) from exc
        except Exception as exc:
            raise CloudEventMappingError(
                "Cloud-event mapping is temporarily unavailable",
                code=CloudEventErrorCode.MAPPING_UNAVAILABLE,
                retryable=True,
                dead_letter_reason="cloud-event mapping unavailable",
            ) from exc
        source = request.source
        if (
            not isinstance(source, CloudEventSourceIdentity)
            or source.mapping != mapping_name
            or source.event_id != event_identity
            or request.business_request_id
            != make_cloud_event_business_request_id(mapping_name, event_identity)
        ):
            raise CloudEventMappingError(
                "Cloud-event mapping did not preserve its trusted source identity",
                code=CloudEventErrorCode.INVALID_IDENTITY,
                dead_letter_reason="invalid cloud-event source identity",
            )
        return await self._starter.start(
            request,
            scope_binding=TrustedScopeBinding.create(
                kind=ScopeBindingKind.CLOUD_EVENT,
                scope=bound_scope,
                binding_id=mapping_name,
            ),
        )


def make_cloud_event_business_request_id(mapping: str, event_identity: str) -> str:
    _validate_event_identity(mapping, event_identity)
    canonical = json.dumps(
        ("cloud_event", mapping, event_identity),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _start_request(
    *,
    workflow_name: str,
    mapping: ProviderName,
    event_identity: str,
    workflow_event: StrictCloudEventModel,
    correlation_id: str,
    trace_id: str | None,
) -> StartWorkflowRequest:
    return StartWorkflowRequest(
        workflow_name=workflow_name,
        business_request_id=make_cloud_event_business_request_id(mapping, event_identity),
        input=workflow_event.model_dump(mode="json", exclude_none=True),
        source=CloudEventSourceIdentity(mapping=mapping, event_id=event_identity),
        correlation_id=correlation_id,
        trace_id=trace_id,
    )


def _validate_event_identity(mapping: str, event_identity: str) -> None:
    if not is_valid_provider_name(mapping):
        raise ValueError("Cloud-event mapping name is invalid")
    if not event_identity or len(event_identity) > MAX_IDENTIFIER_LENGTH:
        raise ValueError("Cloud-event identity is invalid")


def _bounded_text(value: str, *, name: str, maximum: int) -> str:
    if not value or len(value) > maximum:
        raise ValueError(f"{name} is invalid")
    return value


def _metadata_identifier(detail: Mapping[str, object], name: str) -> str | None:
    value = detail.get(name)
    if value is None:
        return None
    if type(value) is not str or not value or len(value) > MAX_IDENTIFIER_LENGTH:
        raise CloudEventPayloadError(f"EventBridge {name} metadata is invalid")
    return value


def _metadata_subject(detail: Mapping[str, object]) -> str | None:
    value = detail.get("subject")
    if value is None:
        return None
    if type(value) is not str or not value or len(value) > MAX_CLOUD_EVENT_SUBJECT_LENGTH:
        raise CloudEventPayloadError("EventBridge subject metadata is invalid")
    return value


def _s3_object_event_identity(detail: S3ObjectCreatedDetail) -> str:
    object_detail = detail.object
    canonical = json.dumps(
        (
            "s3_object_created",
            detail.bucket.name,
            object_detail.key,
            object_detail.version_id,
            object_detail.etag,
            object_detail.sequencer,
        ),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _validate_s3_filter(key_filter: S3ObjectKeyFilter) -> None:
    if key_filter.prefix:
        _validate_s3_key(key_filter.prefix)
    if key_filter.suffix:
        _validate_s3_key(key_filter.suffix)


def _validate_s3_key(key: str) -> None:
    try:
        size = len(key.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError("S3 object key is invalid") from exc
    if not key or size > MAX_S3_KEY_BYTES:
        raise ValueError("S3 object key is invalid")


def _validate_s3_bucket(bucket: str) -> None:
    if ".." in bucket or ".-" in bucket or "-." in bucket:
        raise ValueError("S3 bucket contains invalid adjacent delimiters")
    if bucket.startswith(S3_RESERVED_PREFIXES) or bucket.endswith(S3_RESERVED_SUFFIXES):
        raise ValueError("S3 bucket uses an AWS-reserved prefix or suffix")
    try:
        ipaddress.ip_address(bucket)
    except ValueError:
        return
    raise ValueError("S3 bucket must not be formatted as an IP address")


def _s3_filters_overlap(left: S3ObjectKeyFilter, right: S3ObjectKeyFilter) -> bool:
    if left.bucket != right.bucket:
        return False
    prefixes_overlap = left.prefix.startswith(right.prefix) or right.prefix.startswith(left.prefix)
    suffixes_overlap = left.suffix.endswith(right.suffix) or right.suffix.endswith(left.suffix)
    return prefixes_overlap and suffixes_overlap


def _mapping_name(value: str) -> ProviderName:
    if not is_valid_provider_name(value):
        raise ValueError("Cloud-event mapping name is invalid")
    return value
