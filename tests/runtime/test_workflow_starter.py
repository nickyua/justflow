"""Tests for the shared workflow-start policy facade."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TypeAlias
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.config.schedules import IntervalScheduleSpec
from justflow.config.triggers import (
    ApiTriggerDeclaration,
    BrokerTriggerDeclaration,
    EventTriggerDeclaration,
    HostTriggerDeclaration,
    ScheduleTriggerDeclaration,
    TriggersConfig,
    WebhookTriggerDeclaration,
)
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    SHA256_HEX_LENGTH,
    build_definition_manifests,
)
from justflow.definitions.routing import (
    MEMO_CONFIGURATION_RESOLUTION_DIGEST,
    MEMO_CONFIGURATION_REVISION,
    MEMO_SCOPE_DIGEST,
    DefinitionStartTarget,
    WorkerDeployment,
    WorkflowStartTarget,
)
from justflow.provenance import ExecutionConfigurationIdentity, WorkerArtifactIdentity
from justflow.runtime import (
    BrokerSourceIdentity,
    CloudEventSourceIdentity,
    ControlApiSourceIdentity,
    HostSourceIdentity,
    ScheduleSourceIdentity,
    ScopedWorkflowTargetResolver,
    StartErrorCode,
    StartStatus,
    StartWorkflowRequest,
    WebhookSourceIdentity,
    WorkflowStarter,
    WorkflowStartError,
)
from justflow.runtime.starter import (
    MEMO_CORRELATION_IDENTITY_DIGEST,
    MEMO_SOURCE_IDENTITY_DIGEST,
    MEMO_SOURCE_PROVIDER,
    MEMO_TRIGGER_NAME,
    MEMO_TRIGGER_SOURCE,
    SourceIdentity,
    source_identity_digest,
)
from justflow.runtime.visibility import (
    DEFINITION_DIGEST_SEARCH_ATTRIBUTE,
    LOGICAL_WORKFLOW_SEARCH_ATTRIBUTE,
    SCOPE_DIGEST_SEARCH_ATTRIBUTE,
    TRIGGER_SOURCE_SEARCH_ATTRIBUTE,
    WORKER_ARTIFACT_SEARCH_ATTRIBUTE,
)
from justflow.scope import (
    LOCAL_RUNTIME_SCOPE,
    RuntimeScope,
    ScopeBindingKind,
    TrustedScopeBinding,
)
from justflow.sdk.message_contract import make_workflow_id

TASK_QUEUE = "test-queue"
WORKFLOW_NAME = "record_flow"
BUSINESS_REQUEST_ID = "business-1"
CORRELATION_ID = "correlation-1"
TRACE_ID = "trace-1"
EVENT_ID = "event-1"
TRIGGER_NAME = "test_host_trigger"
TEST_ARTIFACT_DIGEST = f"sha256:{'a' * 64}"
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * SHA256_HEX_LENGTH
EXECUTION_CONFIGURATION = ExecutionConfigurationIdentity(
    configuration_revision_id="c" * SHA256_HEX_LENGTH,
    resolution_digest=f"sha256:{'d' * SHA256_HEX_LENGTH}",
)
TINY_PAYLOAD_LIMIT = 1
SENSITIVE_SENTINEL = "synthetic-temporal-secret"
PINNED_VERSION_NOT_PRESENT = (
    "Pinned version 'justflow:test-build' is not present in task queue 'test-queue' "
    "of type 'Workflow'"
)


class FakeWorkflowClass:
    @staticmethod
    def run() -> None:
        return None


WORKFLOW_CONFIG = WorkflowConfig(
    workflow=WORKFLOW_NAME,
    input_schema={
        "type": "object",
        "properties": {"count": {"type": "integer"}},
        "required": ["count"],
        "additionalProperties": False,
    },
    steps={},
    flow=[FlowStep.model_validate({"name": "done", "terminal": True})],
)
MANIFEST = build_definition_manifests(
    {WORKFLOW_NAME: WORKFLOW_CONFIG},
    {},
    DEFAULT_RUNTIME_LIMITS,
)[WORKFLOW_NAME]
DEPLOYMENT = WorkerDeployment(
    artifact_identity=WorkerArtifactIdentity(
        deployment_name="justflow",
        build_id="test-build",
        artifact_digest=TEST_ARTIFACT_DIGEST,
        package_version="0.1.0",
    ),
    compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
)
TARGET = DefinitionStartTarget(
    manifest=MANIFEST,
    workflow_class=FakeWorkflowClass,
    deployment=DEPLOYMENT,
    environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
    execution_configuration=EXECUTION_CONFIGURATION,
)
SOURCE = HostSourceIdentity(adapter="test_host", event_id=EVENT_ID)
HOST_TRIGGERS = TriggersConfig(
    triggers={
        TRIGGER_NAME: HostTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            adapter="test_host",
        )
    }
)
HOST_SCOPE_BINDING = TrustedScopeBinding.create(
    kind=ScopeBindingKind.HOST,
    scope=LOCAL_RUNTIME_SCOPE,
    binding_id="test_host",
)
SCOPE_A = RuntimeScope.create(
    tenant="tenant-a",
    application="shared-application",
    environment="production",
)
SCOPE_B = RuntimeScope.create(
    tenant="tenant-b",
    application="shared-application",
    environment="production",
)


def request(
    *,
    workflow_name: str = WORKFLOW_NAME,
    definition_digest: str | None = None,
    input: dict[str, object] | None = None,
    trace_id: str | None = TRACE_ID,
) -> StartWorkflowRequest:
    return StartWorkflowRequest.model_validate(
        {
            "workflow_name": workflow_name,
            "business_request_id": BUSINESS_REQUEST_ID,
            "definition_digest": definition_digest,
            "input": input if input is not None else {"count": 2},
            "source": SOURCE,
            "correlation_id": CORRELATION_ID,
            "trace_id": trace_id,
        }
    )


def temporal_client() -> MagicMock:
    client = MagicMock()
    client.start_workflow = AsyncMock()
    return client


def existing_workflow_description(*, run_id: str) -> MagicMock:
    description = MagicMock()
    description.memo = AsyncMock(
        return_value={
            **TARGET.memo,
            MEMO_TRIGGER_NAME: TRIGGER_NAME,
            MEMO_SOURCE_IDENTITY_DIGEST: source_identity_digest(SOURCE),
            MEMO_SCOPE_DIGEST: LOCAL_RUNTIME_SCOPE.digest,
        }
    )
    description.raw_description.workflow_execution_info.execution.run_id = run_id
    return description


def starter(
    client: MagicMock,
    *,
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
) -> WorkflowStarter:
    return WorkflowStarter(
        client,
        TASK_QUEUE,
        {WORKFLOW_NAME: TARGET},
        triggers=HOST_TRIGGERS,
        limits=limits,
    )


class TestWorkflowStarter:
    async def test_start_applies_identity_routing_provenance_and_temporal_options(self):
        client = temporal_client()
        client.start_workflow.return_value = MagicMock(run_id="run-1")

        result = await starter(client).start(
            request(),
            scope_binding=HOST_SCOPE_BINDING,
        )

        args, kwargs = client.start_workflow.await_args
        assert args[0] == TARGET.workflow_type
        assert args[1] == {
            "request_id": BUSINESS_REQUEST_ID,
            "globals": {"count": 2},
            "correlation_id": CORRELATION_ID,
            "trace_id": TRACE_ID,
            "definition_digest": MANIFEST.definition_digest,
            "worker_deployment": DEPLOYMENT.name,
            "worker_build_id": DEPLOYMENT.build_id,
            "worker_artifact": DEPLOYMENT.artifact_identity.model_dump(
                mode="json",
                exclude_none=True,
            ),
            "environment_snapshot_digest": ENVIRONMENT_SNAPSHOT_DIGEST,
            "trigger_source": "host",
            "trigger_name": TRIGGER_NAME,
            "source_identity_digest": source_identity_digest(SOURCE),
            "correlation_identity_digest": kwargs["memo"][MEMO_CORRELATION_IDENTITY_DIGEST],
            "scope_digest": LOCAL_RUNTIME_SCOPE.digest,
            "execution_configuration": EXECUTION_CONFIGURATION.model_dump(
                mode="json",
                exclude_none=True,
            ),
        }
        assert kwargs["id"] == make_workflow_id(
            WORKFLOW_NAME,
            BUSINESS_REQUEST_ID,
            scope=LOCAL_RUNTIME_SCOPE,
        )
        assert kwargs["task_queue"] == TASK_QUEUE
        assert kwargs["id_reuse_policy"] == WorkflowIDReusePolicy.REJECT_DUPLICATE
        assert kwargs["id_conflict_policy"] == WorkflowIDConflictPolicy.FAIL
        assert kwargs["versioning_override"] == TARGET.versioning_override
        assert kwargs["memo"][MEMO_TRIGGER_SOURCE] == "host"
        assert kwargs["memo"][MEMO_TRIGGER_NAME] == TRIGGER_NAME
        assert kwargs["memo"][MEMO_SOURCE_IDENTITY_DIGEST] == source_identity_digest(SOURCE)
        assert kwargs["memo"][MEMO_SOURCE_PROVIDER] == "test_host"
        assert kwargs["memo"][MEMO_CONFIGURATION_REVISION] == "c" * SHA256_HEX_LENGTH
        assert kwargs["memo"][MEMO_CONFIGURATION_RESOLUTION_DIGEST] == (
            f"sha256:{'d' * SHA256_HEX_LENGTH}"
        )
        assert EVENT_ID not in str(kwargs["memo"])
        assert CORRELATION_ID not in str(kwargs["memo"])
        assert result.status is StartStatus.STARTED
        assert result.run_id == "run-1"
        assert result.artifact_identity == DEPLOYMENT.artifact_identity
        assert result.execution_configuration == EXECUTION_CONFIGURATION

    async def test_start_retries_transient_pinned_worker_registration(self) -> None:
        client = temporal_client()
        client.start_workflow.side_effect = (
            RPCError(PINNED_VERSION_NOT_PRESENT, RPCStatusCode.FAILED_PRECONDITION, b""),
            MagicMock(run_id="run-1"),
        )

        result = await starter(client).start(
            request(),
            scope_binding=HOST_SCOPE_BINDING,
        )

        assert result.status is StartStatus.STARTED
        assert client.start_workflow.await_count == 2
        first_call, second_call = client.start_workflow.await_args_list
        assert first_call == second_call

    async def test_start_preserves_missing_trace_context(self) -> None:
        client = temporal_client()
        client.start_workflow.return_value = MagicMock(run_id="run-1")

        await starter(client).start(
            request(trace_id=None),
            scope_binding=HOST_SCOPE_BINDING,
        )

        trigger_payload = client.start_workflow.await_args.args[1]
        assert trigger_payload["correlation_id"] == CORRELATION_ID
        assert "trace_id" not in trigger_payload

    async def test_start_rejects_a_target_without_a_compatible_worker(self) -> None:
        client = temporal_client()
        incompatible_target = replace(
            TARGET,
            deployment=replace(
                DEPLOYMENT,
                compatible_engine_workflow_abis=frozenset({"another-engine-abi"}),
            ),
        )
        workflow_starter = WorkflowStarter(
            client,
            TASK_QUEUE,
            {WORKFLOW_NAME: incompatible_target},
            triggers=HOST_TRIGGERS,
        )

        with pytest.raises(WorkflowStartError, match="no compatible worker") as raised:
            await workflow_starter.start(
                request(),
                scope_binding=HOST_SCOPE_BINDING,
            )

        assert raised.value.code is StartErrorCode.INCOMPATIBLE_WORKER
        client.start_workflow.assert_not_awaited()

    @pytest.mark.parametrize(
        "enabled",
        [
            pytest.param(False, id="disabled"),
            pytest.param(True, id="enabled"),
        ],
    )
    async def test_safe_visibility_indexes_are_explicitly_host_enabled(
        self,
        enabled: bool,
    ) -> None:
        client = temporal_client()
        client.start_workflow.return_value = MagicMock(run_id="run-1")
        workflow_starter = WorkflowStarter(
            client,
            TASK_QUEUE,
            {WORKFLOW_NAME: TARGET},
            triggers=HOST_TRIGGERS,
            indexed_search_attributes_enabled=enabled,
        )

        await workflow_starter.start(request(), scope_binding=HOST_SCOPE_BINDING)

        attributes = client.start_workflow.await_args.kwargs["search_attributes"]
        if not enabled:
            assert attributes is None
            return
        values = {pair.key.name: pair.value for pair in attributes.search_attributes}
        assert values == {
            SCOPE_DIGEST_SEARCH_ATTRIBUTE: LOCAL_RUNTIME_SCOPE.digest,
            LOGICAL_WORKFLOW_SEARCH_ATTRIBUTE: WORKFLOW_NAME,
            DEFINITION_DIGEST_SEARCH_ATTRIBUTE: MANIFEST.definition_digest,
            TRIGGER_SOURCE_SEARCH_ATTRIBUTE: "host",
            WORKER_ARTIFACT_SEARCH_ATTRIBUTE: TEST_ARTIFACT_DIGEST,
        }
        assert "tenant" not in str(values)

    async def test_duplicate_returns_the_existing_run(self):
        client = temporal_client()
        client.start_workflow.side_effect = WorkflowAlreadyStartedError(
            workflow_id=make_workflow_id(WORKFLOW_NAME, BUSINESS_REQUEST_ID),
            workflow_type=TARGET.workflow_type,
            run_id="existing-run",
        )
        handle = MagicMock()
        handle.describe = AsyncMock(
            return_value=existing_workflow_description(run_id="existing-run")
        )
        client.get_workflow_handle.return_value = handle

        result = await starter(client).start(
            request(),
            scope_binding=HOST_SCOPE_BINDING,
        )

        assert result.status is StartStatus.DUPLICATE
        assert result.run_id == "existing-run"
        client.get_workflow_handle.assert_called_once_with(
            make_workflow_id(
                WORKFLOW_NAME,
                BUSINESS_REQUEST_ID,
                scope=LOCAL_RUNTIME_SCOPE,
            ),
            run_id="existing-run",
        )

    async def test_duplicate_without_run_identity_resolves_it_from_temporal(self):
        client = temporal_client()
        client.start_workflow.side_effect = WorkflowAlreadyStartedError(
            workflow_id=make_workflow_id(WORKFLOW_NAME, BUSINESS_REQUEST_ID),
            workflow_type=TARGET.workflow_type,
        )
        handle = MagicMock()
        handle.describe = AsyncMock(
            return_value=existing_workflow_description(run_id="described-run")
        )
        client.get_workflow_handle.return_value = handle

        result = await starter(client).start(
            request(),
            scope_binding=HOST_SCOPE_BINDING,
        )

        assert result.status is StartStatus.DUPLICATE
        assert result.run_id == "described-run"

    async def test_existing_run_lookup_returns_none_only_for_authoritative_absence(self):
        client = temporal_client()
        handle = MagicMock()
        handle.describe = AsyncMock(side_effect=RPCError("not found", RPCStatusCode.NOT_FOUND, b""))
        client.get_workflow_handle.return_value = handle

        result = await starter(client).find_existing(
            WORKFLOW_NAME,
            BUSINESS_REQUEST_ID,
            scope=LOCAL_RUNTIME_SCOPE,
        )

        assert result is None

    async def test_existing_run_lookup_keeps_transport_uncertainty_typed(self):
        client = temporal_client()
        handle = MagicMock()
        handle.describe = AsyncMock(
            side_effect=RPCError("unavailable", RPCStatusCode.UNAVAILABLE, b"")
        )
        client.get_workflow_handle.return_value = handle

        with pytest.raises(WorkflowStartError) as raised:
            await starter(client).find_existing(
                WORKFLOW_NAME,
                BUSINESS_REQUEST_ID,
                scope=LOCAL_RUNTIME_SCOPE,
            )

        assert raised.value.code is StartErrorCode.TEMPORAL_UNAVAILABLE

    async def test_resolved_start_uses_a_pinned_target_outside_active_aliases(self):
        client = temporal_client()
        client.start_workflow.return_value = MagicMock(run_id="run-1")
        pinned_target = WorkflowStartTarget(
            manifest=MANIFEST,
            deployment=DEPLOYMENT,
            environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
        )
        workflow_starter = WorkflowStarter(
            client,
            TASK_QUEUE,
            {},
            triggers=TriggersConfig(triggers={}),
        )
        schedule_name = "pinned_schedule"
        schedule_request = request(definition_digest=MANIFEST.definition_digest).model_copy(
            update={
                "source": ScheduleSourceIdentity(
                    schedule=schedule_name,
                    occurrence_id=EVENT_ID,
                )
            }
        )

        result = await workflow_starter.start_resolved(
            schedule_request,
            pinned_target,
            trigger_name=schedule_name,
            scope_binding=TrustedScopeBinding.create(
                kind=ScopeBindingKind.SCHEDULE,
                scope=LOCAL_RUNTIME_SCOPE,
                binding_id=schedule_name,
            ),
        )

        assert result.definition_digest == MANIFEST.definition_digest
        assert client.start_workflow.await_args.args[0] == pinned_target.workflow_type

    async def test_resolved_start_requires_an_explicit_definition_identity(self):
        client = temporal_client()
        pinned_target = WorkflowStartTarget(
            manifest=MANIFEST,
            deployment=DEPLOYMENT,
            environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
        )

        with pytest.raises(WorkflowStartError, match="immutable definition identity") as raised:
            await WorkflowStarter(
                client,
                TASK_QUEUE,
                {},
                triggers=TriggersConfig(triggers={}),
            ).start_resolved(
                request().model_copy(
                    update={
                        "source": ScheduleSourceIdentity(
                            schedule="pinned_schedule",
                            occurrence_id=EVENT_ID,
                        )
                    }
                ),
                pinned_target,
                trigger_name="pinned_schedule",
                scope_binding=TrustedScopeBinding.create(
                    kind=ScopeBindingKind.SCHEDULE,
                    scope=LOCAL_RUNTIME_SCOPE,
                    binding_id="pinned_schedule",
                ),
            )

        assert raised.value.code is StartErrorCode.DEFINITION_UNAVAILABLE
        client.start_workflow.assert_not_awaited()

    async def test_one_starter_resolves_two_scopes_without_identity_collisions(self) -> None:
        client = temporal_client()
        client.start_workflow.side_effect = (
            MagicMock(run_id="run-a"),
            MagicMock(run_id="run-b"),
        )
        target_a = replace(TARGET, scope_digest=SCOPE_A.digest)
        target_b = replace(TARGET, scope_digest=SCOPE_B.digest)
        workflow_starter = WorkflowStarter(
            client,
            TASK_QUEUE,
            target_resolver=ScopedWorkflowTargetResolver(
                {
                    SCOPE_A: {WORKFLOW_NAME: target_a},
                    SCOPE_B: {WORKFLOW_NAME: target_b},
                },
                {SCOPE_A: HOST_TRIGGERS, SCOPE_B: HOST_TRIGGERS},
            ),
        )

        result_a = await workflow_starter.start(
            request(),
            scope_binding=TrustedScopeBinding.create(
                kind=ScopeBindingKind.HOST,
                scope=SCOPE_A,
                binding_id="test_host",
            ),
        )
        result_b = await workflow_starter.start(
            request(),
            scope_binding=TrustedScopeBinding.create(
                kind=ScopeBindingKind.HOST,
                scope=SCOPE_B,
                binding_id="test_host",
            ),
        )

        assert result_a.workflow_id != result_b.workflow_id
        assert result_a.scope_digest == SCOPE_A.digest
        assert result_b.scope_digest == SCOPE_B.digest
        assert target_a.workflow_type != target_b.workflow_type
        assert [call.args[0] for call in client.start_workflow.await_args_list] == [
            target_a.workflow_type,
            target_b.workflow_type,
        ]

    async def test_start_rejects_a_binding_for_another_ingress_kind(self) -> None:
        client = temporal_client()

        with pytest.raises(WorkflowStartError, match="trusted scope binding") as raised:
            await starter(client).start(
                request(),
                scope_binding=TrustedScopeBinding.create(
                    kind=ScopeBindingKind.API,
                    scope=LOCAL_RUNTIME_SCOPE,
                    binding_id="principal",
                ),
            )

        assert raised.value.code is StartErrorCode.INVALID_REQUEST
        client.start_workflow.assert_not_awaited()


@dataclass(frozen=True, kw_only=True)
class TriggerPolicyReturns:
    trigger_name: str


@dataclass(frozen=True, kw_only=True)
class TriggerPolicyRaises:
    code: StartErrorCode
    match: str


TriggerPolicyOutcome: TypeAlias = TriggerPolicyReturns | TriggerPolicyRaises


@dataclass(frozen=True, kw_only=True)
class TriggerPolicyCase:
    id: str
    triggers: TriggersConfig
    source: SourceIdentity
    binding: TrustedScopeBinding
    outcome: TriggerPolicyOutcome


def _binding(kind: ScopeBindingKind, binding_id: str) -> TrustedScopeBinding:
    return TrustedScopeBinding.create(
        kind=kind,
        scope=LOCAL_RUNTIME_SCOPE,
        binding_id=binding_id,
    )


TRIGGER_POLICY_CASES = [
    TriggerPolicyCase(
        id="api",
        triggers=TriggersConfig(triggers={"api": ApiTriggerDeclaration(workflow=WORKFLOW_NAME)}),
        source=ControlApiSourceIdentity(request_id=EVENT_ID),
        binding=_binding(ScopeBindingKind.API, "principal"),
        outcome=TriggerPolicyReturns(trigger_name="api"),
    ),
    TriggerPolicyCase(
        id="webhook",
        triggers=TriggersConfig(
            triggers={
                "hook": WebhookTriggerDeclaration(
                    workflow=WORKFLOW_NAME,
                    source="stripe",
                )
            }
        ),
        source=WebhookSourceIdentity(provider="stripe", event_id=EVENT_ID),
        binding=_binding(ScopeBindingKind.WEBHOOK, "stripe"),
        outcome=TriggerPolicyReturns(trigger_name="hook"),
    ),
    TriggerPolicyCase(
        id="event",
        triggers=TriggersConfig(
            triggers={
                "event": EventTriggerDeclaration(
                    workflow=WORKFLOW_NAME,
                    mapping="orders",
                )
            }
        ),
        source=CloudEventSourceIdentity(mapping="orders", event_id=EVENT_ID),
        binding=_binding(ScopeBindingKind.CLOUD_EVENT, "orders"),
        outcome=TriggerPolicyReturns(trigger_name="event"),
    ),
    TriggerPolicyCase(
        id="broker",
        triggers=TriggersConfig(
            triggers={
                "queue": BrokerTriggerDeclaration(
                    workflow=WORKFLOW_NAME,
                    broker="orders",
                )
            }
        ),
        source=BrokerSourceIdentity(broker="orders", message_id=EVENT_ID),
        binding=_binding(ScopeBindingKind.BROKER, "orders"),
        outcome=TriggerPolicyReturns(trigger_name="queue"),
    ),
    TriggerPolicyCase(
        id="host",
        triggers=HOST_TRIGGERS,
        source=SOURCE,
        binding=HOST_SCOPE_BINDING,
        outcome=TriggerPolicyReturns(trigger_name=TRIGGER_NAME),
    ),
    TriggerPolicyCase(
        id="schedule",
        triggers=TriggersConfig(
            triggers={
                "hourly": ScheduleTriggerDeclaration(
                    workflow=WORKFLOW_NAME,
                    spec=IntervalScheduleSpec(every_seconds=3_600),
                )
            }
        ),
        source=ScheduleSourceIdentity(schedule="hourly", occurrence_id=EVENT_ID),
        binding=_binding(ScopeBindingKind.SCHEDULE, "hourly"),
        outcome=TriggerPolicyReturns(trigger_name="hourly"),
    ),
    TriggerPolicyCase(
        id="undeclared",
        triggers=TriggersConfig(triggers={}),
        source=SOURCE,
        binding=HOST_SCOPE_BINDING,
        outcome=TriggerPolicyRaises(
            code=StartErrorCode.TRIGGER_UNAVAILABLE,
            match="No active workflow trigger",
        ),
    ),
    TriggerPolicyCase(
        id="source-mismatch",
        triggers=HOST_TRIGGERS,
        source=HostSourceIdentity(adapter="other_host", event_id=EVENT_ID),
        binding=_binding(ScopeBindingKind.HOST, "other_host"),
        outcome=TriggerPolicyRaises(
            code=StartErrorCode.TRIGGER_UNAVAILABLE,
            match="No active workflow trigger",
        ),
    ),
    TriggerPolicyCase(
        id="paused",
        triggers=TriggersConfig(
            triggers={
                "paused": HostTriggerDeclaration(
                    workflow=WORKFLOW_NAME,
                    adapter="test_host",
                    paused=True,
                )
            }
        ),
        source=SOURCE,
        binding=HOST_SCOPE_BINDING,
        outcome=TriggerPolicyRaises(
            code=StartErrorCode.TRIGGER_PAUSED,
            match="trigger is paused",
        ),
    ),
    TriggerPolicyCase(
        id="binding-mismatch",
        triggers=HOST_TRIGGERS,
        source=SOURCE,
        binding=_binding(ScopeBindingKind.HOST, "other_host"),
        outcome=TriggerPolicyRaises(
            code=StartErrorCode.INVALID_REQUEST,
            match="trusted binding identity",
        ),
    ),
]


@pytest.mark.parametrize("case", TRIGGER_POLICY_CASES, ids=lambda case: case.id)
async def test_declared_trigger_policy(case: TriggerPolicyCase) -> None:
    client = temporal_client()
    client.start_workflow.return_value = MagicMock(run_id="run-1")
    workflow_starter = WorkflowStarter(
        client,
        TASK_QUEUE,
        {WORKFLOW_NAME: TARGET},
        triggers=case.triggers,
    )
    start_request = request().model_copy(update={"source": case.source})

    if isinstance(case.outcome, TriggerPolicyReturns):
        result = await workflow_starter.start(
            start_request,
            scope_binding=case.binding,
        )
        assert result.trigger_name == case.outcome.trigger_name
        return

    with pytest.raises(WorkflowStartError, match=case.outcome.match) as raised:
        await workflow_starter.start(start_request, scope_binding=case.binding)
    assert raised.value.code is case.outcome.code
    client.start_workflow.assert_not_awaited()


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: StartStatus


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[WorkflowStartError]
    match: str
    code: StartErrorCode
    retryable: bool


Outcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class StartCase:
    id: str
    workflow_name: str = WORKFLOW_NAME
    definition_digest: str | None = None
    input: dict[str, object] | None = None
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS
    client_error: Exception | None = None
    outcome: Outcome


START_CASES = [
    StartCase(id="valid-active-alias", outcome=Returns(value=StartStatus.STARTED)),
    StartCase(
        id="unknown-workflow",
        workflow_name="missing_flow",
        outcome=Raises(
            exc=WorkflowStartError,
            match="not registered",
            code=StartErrorCode.UNKNOWN_WORKFLOW,
            retryable=False,
        ),
    ),
    StartCase(
        id="unavailable-definition",
        definition_digest="f" * SHA256_HEX_LENGTH,
        outcome=Raises(
            exc=WorkflowStartError,
            match="definition is not available",
            code=StartErrorCode.DEFINITION_UNAVAILABLE,
            retryable=False,
        ),
    ),
    StartCase(
        id="contract-rejected-input",
        input={"count": "two"},
        outcome=Raises(
            exc=WorkflowStartError,
            match="does not satisfy",
            code=StartErrorCode.INPUT_REJECTED,
            retryable=False,
        ),
    ),
    StartCase(
        id="oversized-input",
        limits=RuntimeLimits(trigger_payload_bytes=TINY_PAYLOAD_LIMIT),
        outcome=Raises(
            exc=WorkflowStartError,
            match="bounded JSON",
            code=StartErrorCode.INVALID_REQUEST,
            retryable=False,
        ),
    ),
    StartCase(
        id="non-json-input",
        input={"count": object()},
        outcome=Raises(
            exc=WorkflowStartError,
            match="bounded JSON",
            code=StartErrorCode.INVALID_REQUEST,
            retryable=False,
        ),
    ),
    StartCase(
        id="temporal-failure",
        client_error=RuntimeError(SENSITIVE_SENTINEL),
        outcome=Raises(
            exc=WorkflowStartError,
            match="did not accept",
            code=StartErrorCode.TEMPORAL_UNAVAILABLE,
            retryable=True,
        ),
    ),
]


@pytest.mark.parametrize("case", START_CASES, ids=lambda case: case.id)
async def test_start_behavior(case: StartCase):
    client = temporal_client()
    if case.client_error is None:
        client.start_workflow.return_value = MagicMock(run_id="run-1")
    else:
        client.start_workflow.side_effect = case.client_error
    workflow_starter = starter(client, limits=case.limits)
    start_request = request(
        workflow_name=case.workflow_name,
        definition_digest=case.definition_digest,
        input=case.input,
    )

    if isinstance(case.outcome, Returns):
        result = await workflow_starter.start(
            start_request,
            scope_binding=HOST_SCOPE_BINDING,
        )
        assert result.status is case.outcome.value
        return

    with pytest.raises(case.outcome.exc, match=case.outcome.match) as raised:
        await workflow_starter.start(
            start_request,
            scope_binding=HOST_SCOPE_BINDING,
        )
    assert raised.value.code is case.outcome.code
    assert raised.value.retryable is case.outcome.retryable
    assert SENSITIVE_SENTINEL not in str(raised.value)
