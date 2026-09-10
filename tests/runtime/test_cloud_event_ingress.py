from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import cast

import pytest

from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.provenance import WorkerArtifactIdentity
from justflow.runtime.cloud_events import (
    CloudEventErrorCode,
    CloudEventIngress,
    CloudEventMappingError,
    CloudEventMappingRegistry,
    CloudEventPayloadError,
    EventBridgeEventMapper,
    S3ObjectCreatedEventMapper,
    S3ObjectKeyFilter,
    make_cloud_event_business_request_id,
)
from justflow.runtime.starter import (
    CloudEventSourceIdentity,
    StartStatus,
    StartWorkflowRequest,
    StartWorkflowResult,
    WorkflowStarter,
)
from justflow.scope import RuntimeScope, TrustedScopeBinding
from justflow.sdk.message_contract import MAX_IDENTIFIER_LENGTH

ENVIRONMENT_SNAPSHOT_DIGEST = "e" * 64
SCOPE = RuntimeScope.create(
    tenant="tenant-a",
    application="orders",
    environment="production",
)
OTHER_SCOPE = RuntimeScope.create(
    tenant="tenant-b",
    application="orders",
    environment="production",
)
EVENTBRIDGE_MAPPING = "order-events"
S3_MAPPING = "object-created"
EVENTBRIDGE_EVENT: dict[str, object] = {
    "version": "0",
    "id": "event-1",
    "detail-type": "Order Created",
    "source": "com.example.orders",
    "account": "123456789012",
    "time": "2026-08-06T08:30:00Z",
    "region": "eu-central-1",
    "resources": ["arn:aws:events:eu-central-1:123456789012:event-bus/orders"],
    "detail": {
        "order_id": "order-1",
        "subject": "orders/order-1",
        "correlation_id": "correlation-1",
        "trace_id": "trace-1",
    },
}
S3_EVENT: dict[str, object] = {
    "version": "0",
    "id": "s3-event-1",
    "detail-type": "Object Created",
    "source": "aws.s3",
    "account": "123456789012",
    "time": "2026-08-06T08:30:00Z",
    "region": "eu-central-1",
    "resources": ["arn:aws:s3:::incoming-bucket"],
    "detail": {
        "version": "0",
        "bucket": {"name": "incoming-bucket"},
        "object": {
            "key": "incoming/order-1.json",
            "size": 128,
            "etag": "etag-1",
            "version-id": "version-1",
            "sequencer": "00655AED6DCD90281E",
        },
        "request-id": "request-1",
        "requester": "123456789012",
        "source-ip-address": "192.0.2.1",
        "reason": "PutObject",
    },
}


@dataclass(frozen=True, kw_only=True)
class MappingRejectionCase:
    id: str
    event: object
    expected_code: CloudEventErrorCode


MAPPING_REJECTION_CASES = [
    MappingRejectionCase(
        id="unsupported-envelope-field",
        event={**EVENTBRIDGE_EVENT, "unsupported": "value"},
        expected_code=CloudEventErrorCode.INVALID_PAYLOAD,
    ),
    MappingRejectionCase(
        id="wrong-source",
        event={**EVENTBRIDGE_EVENT, "source": "com.example.other"},
        expected_code=CloudEventErrorCode.SOURCE_REJECTED,
    ),
    MappingRejectionCase(
        id="wrong-event-id-type",
        event={**EVENTBRIDGE_EVENT, "id": 1},
        expected_code=CloudEventErrorCode.INVALID_PAYLOAD,
    ),
    MappingRejectionCase(
        id="unbounded-correlation",
        event={
            **EVENTBRIDGE_EVENT,
            "detail": {"correlation_id": "x" * (MAX_IDENTIFIER_LENGTH + 1)},
        },
        expected_code=CloudEventErrorCode.INVALID_PAYLOAD,
    ),
]


class FakeMapper:
    def __init__(self, *, source_mapping: str = EVENTBRIDGE_MAPPING) -> None:
        self._source_mapping = source_mapping

    async def event_identity(self, event: object) -> str:
        return "event-1"

    async def to_start_request(
        self,
        event: object,
        event_identity: str,
    ) -> StartWorkflowRequest:
        return StartWorkflowRequest(
            workflow_name="example",
            business_request_id=make_cloud_event_business_request_id(
                EVENTBRIDGE_MAPPING,
                event_identity,
            ),
            source=CloudEventSourceIdentity(
                mapping=self._source_mapping,
                event_id=event_identity,
            ),
        )


class PayloadRejectingMapper(FakeMapper):
    async def event_identity(self, event: object) -> str:
        raise CloudEventPayloadError("private payload detail")


class UnavailableMapper(FakeMapper):
    async def event_identity(self, event: object) -> str:
        raise RuntimeError("private provider detail")


class FakeStarter:
    def __init__(self) -> None:
        self.requests: list[StartWorkflowRequest] = []
        self.scope_bindings: list[TrustedScopeBinding] = []

    async def start(
        self,
        request: StartWorkflowRequest,
        *,
        scope_binding: TrustedScopeBinding,
    ) -> StartWorkflowResult:
        status = (
            StartStatus.DUPLICATE
            if any(
                existing.business_request_id == request.business_request_id
                for existing in self.requests
            )
            else StartStatus.STARTED
        )
        self.requests.append(request)
        self.scope_bindings.append(scope_binding)
        return StartWorkflowResult(
            workflow_id="workflow-id",
            run_id="run-id",
            workflow_name="example",
            definition_digest="d" * 64,
            environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
            trigger_name="example_event",
            artifact_identity=WorkerArtifactIdentity(
                deployment_name="deployment",
                build_id="build",
                artifact_digest=f"sha256:{'a' * 64}",
                package_version="1.0.0",
            ),
            source_identity_digest="b" * 64,
            scope_digest=scope_binding.scope.digest,
            status=status,
        )


def _eventbridge_registry(*, scope: RuntimeScope = SCOPE) -> CloudEventMappingRegistry:
    registry = CloudEventMappingRegistry(scope=OTHER_SCOPE)
    registry.register(
        EVENTBRIDGE_MAPPING,
        EventBridgeEventMapper(
            mapping_name=EVENTBRIDGE_MAPPING,
            workflow_name="example",
            source="com.example.orders",
            detail_type="Order Created",
        ),
        scope=scope,
    )
    return registry


def _s3_registry() -> CloudEventMappingRegistry:
    registry = CloudEventMappingRegistry(scope=SCOPE)
    registry.register(
        S3_MAPPING,
        S3ObjectCreatedEventMapper(
            mapping_name=S3_MAPPING,
            workflow_name="example",
            source=S3ObjectKeyFilter(
                bucket="incoming-bucket",
                prefix="incoming/",
                suffix=".json",
            ),
            write_destinations=(S3ObjectKeyFilter(bucket="incoming-bucket", prefix="archive/"),),
        ),
    )
    return registry


def _ingress(
    registry: CloudEventMappingRegistry,
    starter: FakeStarter,
    *,
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
) -> CloudEventIngress:
    return CloudEventIngress(
        registry,
        cast(WorkflowStarter, starter),
        limits=limits,
    )


async def test_eventbridge_mapping_preserves_metadata_and_registered_scope() -> None:
    starter = FakeStarter()
    ingress = _ingress(_eventbridge_registry(), starter)

    result = await ingress.receive(EVENTBRIDGE_MAPPING, EVENTBRIDGE_EVENT)

    request = starter.requests[0]
    assert result.status is StartStatus.STARTED
    assert starter.scope_bindings[0].scope == SCOPE
    assert request.source == CloudEventSourceIdentity(
        mapping=EVENTBRIDGE_MAPPING,
        event_id="event-1",
    )
    assert request.correlation_id == "correlation-1"
    assert request.trace_id == "trace-1"
    assert request.input == {
        "metadata": {
            "source": "com.example.orders",
            "event_id": "event-1",
            "subject": "orders/order-1",
            "timestamp": "2026-08-06T08:30:00Z",
            "correlation_id": "correlation-1",
            "trace_id": "trace-1",
        },
        "envelope_version": "0",
        "detail_type": "Order Created",
        "account": "123456789012",
        "region": "eu-central-1",
        "resources": ["arn:aws:events:eu-central-1:123456789012:event-bus/orders"],
        "detail": {
            "order_id": "order-1",
            "subject": "orders/order-1",
            "correlation_id": "correlation-1",
            "trace_id": "trace-1",
        },
    }


async def test_cloud_event_payload_bound_applies_before_mapping() -> None:
    starter = FakeStarter()
    ingress = _ingress(
        _eventbridge_registry(),
        starter,
        limits=RuntimeLimits(trigger_payload_bytes=1),
    )

    with pytest.raises(CloudEventMappingError) as raised:
        await ingress.receive(EVENTBRIDGE_MAPPING, EVENTBRIDGE_EVENT)

    assert raised.value.code is CloudEventErrorCode.INVALID_PAYLOAD
    assert starter.requests == []


@pytest.mark.parametrize("case", MAPPING_REJECTION_CASES, ids=lambda case: case.id)
async def test_eventbridge_mapping_rejects_invalid_or_misrouted_events(
    case: MappingRejectionCase,
) -> None:
    ingress = _ingress(_eventbridge_registry(), FakeStarter())

    with pytest.raises(CloudEventMappingError) as raised:
        await ingress.receive(EVENTBRIDGE_MAPPING, case.event)

    assert raised.value.code is case.expected_code
    assert raised.value.retryable is False


async def test_s3_object_mapping_preserves_object_fields_and_stable_identity() -> None:
    starter = FakeStarter()
    ingress = _ingress(_s3_registry(), starter)
    redelivery = deepcopy(S3_EVENT)
    redelivery["id"] = "repackaged-event-id"

    first = await ingress.receive(S3_MAPPING, S3_EVENT)
    duplicate = await ingress.receive(S3_MAPPING, redelivery)

    first_request, duplicate_request = starter.requests
    assert first.status is StartStatus.STARTED
    assert duplicate.status is StartStatus.DUPLICATE
    assert first_request.business_request_id == duplicate_request.business_request_id
    assert first_request.source == duplicate_request.source
    assert first_request.input == {
        "metadata": {
            "source": "aws.s3",
            "event_id": "s3-event-1",
            "subject": "s3://incoming-bucket/incoming/order-1.json",
            "timestamp": "2026-08-06T08:30:00Z",
            "correlation_id": "s3-event-1",
        },
        "bucket": "incoming-bucket",
        "key": "incoming/order-1.json",
        "size": 128,
        "version_id": "version-1",
        "etag": "etag-1",
        "sequencer": "00655AED6DCD90281E",
    }


@pytest.mark.parametrize(
    ("key", "expected_code"),
    [
        pytest.param("other/order-1.json", CloudEventErrorCode.SOURCE_REJECTED, id="prefix"),
        pytest.param("incoming/order-1.csv", CloudEventErrorCode.SOURCE_REJECTED, id="suffix"),
    ],
)
async def test_s3_object_mapping_enforces_key_filters(
    key: str,
    expected_code: CloudEventErrorCode,
) -> None:
    event = deepcopy(S3_EVENT)
    detail = cast(dict[str, object], event["detail"])
    object_detail = cast(dict[str, object], detail["object"])
    object_detail["key"] = key

    with pytest.raises(CloudEventMappingError) as raised:
        await _ingress(_s3_registry(), FakeStarter()).receive(S3_MAPPING, event)

    assert raised.value.code is expected_code


def test_s3_mapping_rejects_overlapping_write_destination() -> None:
    with pytest.raises(ValueError, match="write destination"):
        S3ObjectCreatedEventMapper(
            mapping_name=S3_MAPPING,
            workflow_name="example",
            source=S3ObjectKeyFilter(bucket="incoming-bucket", prefix="incoming/"),
            write_destinations=(
                S3ObjectKeyFilter(
                    bucket="incoming-bucket",
                    prefix="incoming/archive/",
                ),
            ),
        )


@pytest.mark.parametrize(
    "bucket",
    [
        pytest.param("bucket..name", id="adjacent-dots"),
        pytest.param("192.168.0.1", id="ip-address"),
        pytest.param("bucket-s3alias", id="reserved-suffix"),
    ],
)
def test_s3_mapping_rejects_invalid_bucket_names(bucket: str) -> None:
    with pytest.raises(ValueError):
        S3ObjectKeyFilter(bucket=bucket)


@pytest.mark.parametrize(
    ("mapper", "expected_code", "retryable"),
    [
        pytest.param(
            PayloadRejectingMapper(),
            CloudEventErrorCode.INVALID_PAYLOAD,
            False,
            id="payload",
        ),
        pytest.param(
            UnavailableMapper(),
            CloudEventErrorCode.MAPPING_UNAVAILABLE,
            True,
            id="provider",
        ),
    ],
)
async def test_host_mapping_failures_have_safe_delivery_dispositions(
    mapper: FakeMapper,
    expected_code: CloudEventErrorCode,
    retryable: bool,
) -> None:
    registry = CloudEventMappingRegistry(scope=SCOPE)
    registry.register(EVENTBRIDGE_MAPPING, mapper)

    with pytest.raises(CloudEventMappingError) as raised:
        await _ingress(registry, FakeStarter()).receive(EVENTBRIDGE_MAPPING, {})

    assert raised.value.code is expected_code
    assert raised.value.retryable is retryable
    assert "private" not in str(raised.value)


async def test_cloud_event_rejects_identity_rebinding() -> None:
    registry = CloudEventMappingRegistry(scope=SCOPE)
    registry.register(EVENTBRIDGE_MAPPING, FakeMapper(source_mapping="other"))

    with pytest.raises(CloudEventMappingError, match="preserve") as raised:
        await _ingress(registry, FakeStarter()).receive(EVENTBRIDGE_MAPPING, {})

    assert raised.value.code is CloudEventErrorCode.INVALID_IDENTITY
