from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from typing import cast

import pytest

from justflow.config.settings import ControlSettings
from justflow.config.triggers import TriggersConfig
from justflow.configuration.activation import (
    ActivationObservations,
    ActivationOutcomeCode,
    ActivationPlan,
    ActivationRecord,
    PublicationErrorCode,
    WorkerReadinessRegistration,
    activation_identity,
    activation_request_digest,
    control_identity_digest,
    plan_configuration_activation,
)
from justflow.configuration.activation_errors import (
    ActivationConflictError,
    ActivationIntegrityError,
    ActivationLimitError,
    ActivationNotFoundError,
    ActivationUnavailableError,
)
from justflow.configuration.errors import (
    ConfigurationConflictError,
    ConfigurationError,
    ConfigurationIntegrityError,
    ConfigurationLimitError,
    ConfigurationNotFoundError,
    ConfigurationUnavailableError,
)
from justflow.configuration.models import (
    ConfigurationBundle,
    DraftRecord,
    RevisionIdentity,
    TenantConfiguration,
)
from justflow.configuration.publication import (
    ConfigurationPublicationService,
    PublicationOperationError,
)
from justflow.provenance import WorkerArtifactIdentity, provenance_digest
from justflow.runtime.configuration_activation import (
    ActivationControllerError,
    ConfigurationActivationController,
)
from justflow.runtime.configuration_api import (
    ConfigurationApi,
    ConfigurationApiRequestError,
)
from justflow.scope import RuntimeScope, ScopeBindingKind, TrustedScopeBinding

SCOPE = RuntimeScope.create(
    tenant="tenant-a",
    application="orders",
    environment="production",
)
NOW = datetime(2026, 8, 4, 12, tzinfo=UTC)
REVISION = RevisionIdentity("a" * 64)
OTHER_REVISION = RevisionIdentity("b" * 64)
POLICY_DIGEST = provenance_digest({"policy": "current"})
TASK_QUEUE_DIGEST = provenance_digest({"task_queue": "orders"})
WORKER_REGISTRATION_DIGEST = provenance_digest({"worker_registration": "orders"})
WORKER_OBSERVATION_DIGEST = provenance_digest({"worker": "current"})
SCHEDULE_OBSERVATION_DIGEST = provenance_digest({"schedules": "current"})
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="justflow",
    build_id="release-1",
    artifact_digest=f"sha256:{'c' * 64}",
    package_version="0.1.0",
)
IDEMPOTENCY_HEADERS = (
    (b"x-idempotency-key", b"operation-1"),
    (b"x-correlation-id", b"request-1"),
)
ACTIVATION_ID = activation_identity(SCOPE.digest, "operation-1")


class Dumpable:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def model_dump(self, *, mode: str) -> dict[str, object]:
        assert mode == "json"
        return dict(self.payload)


def activation_plan() -> ActivationPlan:
    return plan_configuration_activation(
        scope_digest=SCOPE.digest,
        target_revision_id=REVISION,
        target=ConfigurationBundle(workflows={}, triggers=TriggersConfig(triggers={})),
        target_artifact=ARTIFACT,
        target_task_queue_identity_digest=TASK_QUEUE_DIGEST,
        target_worker_registration_digest=WORKER_REGISTRATION_DIGEST,
        target_definition_digests={},
        observations=ActivationObservations(
            policy_digest=POLICY_DIGEST,
            worker_observation_digest=WORKER_OBSERVATION_DIGEST,
            schedule_observation_digest=SCHEDULE_OBSERVATION_DIGEST,
        ),
    )


def activation_record() -> ActivationRecord:
    plan = activation_plan()
    return ActivationRecord(
        activation_id=ACTIVATION_ID,
        scope_digest=SCOPE.digest,
        idempotency_key_digest=control_identity_digest("activation-key", "operation-1"),
        request_digest=activation_request_digest(plan),
        actor_digest=control_identity_digest("actor", "principal"),
        correlation_digest=control_identity_digest("correlation", "request-1"),
        plan=plan,
        created_at=NOW,
        updated_at=NOW,
    )


class FakePublication:
    def __init__(self) -> None:
        self.operations: list[str] = []
        self.scopes: list[RuntimeScope] = []
        self.draft = DraftRecord(
            scope_digest=SCOPE.digest,
            version=1,
            bundle=TenantConfiguration(
                component_catalog_revision="d" * 64,
                workflows={},
                triggers={},
            ),
        )

    def _record(self, operation: str, scope: RuntimeScope) -> None:
        self.operations.append(operation)
        self.scopes.append(scope)

    def create_draft(
        self,
        scope: RuntimeScope,
        configuration: TenantConfiguration,
    ) -> DraftRecord:
        self._record("create_draft", scope)
        self.draft = DraftRecord(scope_digest=scope.digest, version=1, bundle=configuration)
        return self.draft

    def read_draft(self, scope: RuntimeScope) -> DraftRecord:
        self._record("read_draft", scope)
        return self.draft

    def update_draft(
        self,
        scope: RuntimeScope,
        configuration: TenantConfiguration,
        *,
        expected_version: int,
    ) -> DraftRecord:
        self._record("update_draft", scope)
        self.draft = DraftRecord(
            scope_digest=scope.digest,
            version=expected_version + 1,
            bundle=configuration,
        )
        return self.draft

    def import_draft_yaml(
        self,
        scope: RuntimeScope,
        payload: bytes,
        *,
        expected_version: int | None,
    ) -> DraftRecord:
        assert payload
        self._record("import_draft", scope)
        return DraftRecord(
            scope_digest=scope.digest,
            version=1 if expected_version is None else expected_version + 1,
            bundle=self.draft.bundle,
        )

    def export_draft_yaml(
        self, scope: RuntimeScope, *, expected_version: int | None = None
    ) -> bytes:
        if expected_version is not None and expected_version != self.draft.version:
            raise ConfigurationConflictError(scope.digest)
        self._record("export_draft", scope)
        return b"component_catalog_revision: synthetic\nworkflows: {}\n"

    def validate_draft(self, scope: RuntimeScope) -> Dumpable:
        self._record("validate_draft", scope)
        return Dumpable({"valid": True, "issues": []})

    def relationships(self, scope: RuntimeScope) -> Dumpable:
        self._record("relationships", scope)
        return Dumpable(
            {
                "working_version": 1,
                "active_identity": str(REVISION),
                "relationships": [],
                "restart_required": False,
            }
        )

    def discard(
        self,
        scope: RuntimeScope,
        *,
        expected_draft_version: int,
        expected_active_identity: RevisionIdentity,
        idempotency_key: str,
        actor_identity: str,
        correlation_identity: str,
    ) -> Dumpable:
        assert expected_draft_version == 1
        assert expected_active_identity == REVISION
        assert (idempotency_key, actor_identity, correlation_identity) == (
            "operation-1",
            "principal",
            "request-1",
        )
        self._record("discard", scope)
        return Dumpable(
            {
                "discard_id": "discard-1",
                "working_version": 2,
                "active_identity": str(REVISION),
                "state": "applied",
                "running_process_changed": False,
            }
        )

    def read_discard(self, scope: RuntimeScope, discard_id: str) -> Dumpable:
        self._record("read_discard", scope)
        return Dumpable({"discard_id": discard_id, "state": "applied"})

    def list_history(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None,
    ) -> Dumpable:
        self._record("list_revisions", scope)
        return Dumpable({"revisions": [], "limit": limit, "next_cursor": cursor})

    def read_revision(self, scope: RuntimeScope, revision_id: RevisionIdentity) -> Dumpable:
        self._record("read_revision", scope)
        return Dumpable({"revision_id": str(revision_id)})

    def compare_revisions(
        self,
        scope: RuntimeScope,
        source_revision_id: RevisionIdentity,
        target_revision_id: RevisionIdentity,
    ) -> Dumpable:
        self._record("compare_revisions", scope)
        return Dumpable(
            {
                "source_revision_id": str(source_revision_id),
                "target_revision_id": str(target_revision_id),
                "items": [],
            }
        )

    def publish(
        self,
        scope: RuntimeScope,
        *,
        expected_draft_version: int,
        idempotency_key: str,
        actor_identity: str,
        correlation_identity: str,
    ) -> Dumpable:
        assert expected_draft_version == 1
        assert (idempotency_key, actor_identity, correlation_identity) == (
            "operation-1",
            "principal",
            "request-1",
        )
        self._record("publish", scope)
        return Dumpable({"publication_id": "publication-1", "state": "applied"})

    def read_publication(self, scope: RuntimeScope, publication_id: str) -> Dumpable:
        self._record("read_publication", scope)
        return Dumpable({"publication_id": publication_id, "state": "applied"})


class FakeActivation:
    def __init__(self) -> None:
        self.operations: list[str] = []
        self.scopes: list[RuntimeScope] = []
        self.record = activation_record()

    def _record(self, operation: str, scope: RuntimeScope) -> None:
        self.operations.append(operation)
        self.scopes.append(scope)

    async def plan(self, scope: RuntimeScope, revision_id: RevisionIdentity) -> ActivationPlan:
        assert revision_id == REVISION
        self._record("plan_activation", scope)
        return self.record.plan

    async def activate(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
        *,
        plan_digest: str,
        idempotency_key: str,
        actor_identity: str,
        correlation_identity: str,
    ) -> ActivationRecord:
        assert revision_id == REVISION
        assert plan_digest == self.record.plan.plan_digest
        assert (idempotency_key, actor_identity, correlation_identity) == (
            "operation-1",
            "principal",
            "request-1",
        )
        self._record("activate", scope)
        return self.record

    def list(self, scope: RuntimeScope, *, limit: int, cursor: str | None) -> Dumpable:
        self._record("list_activations", scope)
        return Dumpable({"activations": [], "limit": limit, "next_cursor": cursor})

    def read(self, scope: RuntimeScope, activation_id: str) -> ActivationRecord:
        assert activation_id == ACTIVATION_ID
        self._record("read_activation", scope)
        return self.record

    async def register_readiness(
        self,
        scope: RuntimeScope,
        activation_id: str,
        registration: WorkerReadinessRegistration,
    ) -> ActivationRecord:
        assert activation_id == ACTIVATION_ID
        assert registration.deployment_registration_digest == WORKER_REGISTRATION_DIGEST
        self._record("register_readiness", scope)
        return self.record

    async def rollback(
        self,
        scope: RuntimeScope,
        activation_id: str,
        *,
        plan_digest: str,
        idempotency_key: str,
        actor_identity: str,
        correlation_identity: str,
    ) -> ActivationRecord:
        assert activation_id == ACTIVATION_ID
        assert plan_digest == self.record.plan.plan_digest
        assert (idempotency_key, actor_identity, correlation_identity) == (
            "operation-1",
            "principal",
            "request-1",
        )
        self._record("rollback", scope)
        return self.record


@dataclass(frozen=True, kw_only=True)
class RouteCase:
    id: str
    method: str
    segments: tuple[str, ...]
    query: dict[str, list[str]]
    body: bytes
    headers: tuple[tuple[bytes, bytes], ...]
    expected_status: HTTPStatus
    expected_operation: str
    expected_raw_body: bytes | None = None


CONFIGURATION = TenantConfiguration(
    component_catalog_revision="d" * 64,
    workflows={},
    triggers={},
).model_dump(mode="json")
READINESS = WorkerReadinessRegistration(
    configuration_revision_id=REVISION,
    artifact=ARTIFACT,
    definition_digests=(),
    task_queue_identity_digest=TASK_QUEUE_DIGEST,
    deployment_registration_digest=WORKER_REGISTRATION_DIGEST,
    registered_at=NOW,
).model_dump(mode="json")


def json_body(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


ROUTE_CASES = [
    RouteCase(
        id="create-draft",
        method="POST",
        segments=("v1", "configuration", "draft"),
        query={},
        body=json_body({"configuration": CONFIGURATION}),
        headers=(),
        expected_status=HTTPStatus.CREATED,
        expected_operation="create_draft",
    ),
    RouteCase(
        id="read-draft",
        method="GET",
        segments=("v1", "configuration", "draft"),
        query={},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="read_draft",
    ),
    RouteCase(
        id="update-draft",
        method="PUT",
        segments=("v1", "configuration", "draft"),
        query={},
        body=json_body({"expected_version": 1, "configuration": CONFIGURATION}),
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="update_draft",
    ),
    RouteCase(
        id="import-draft",
        method="POST",
        segments=("v1", "configuration", "draft", "import"),
        query={"expected_version": ["1"]},
        body=b"component_catalog_revision: synthetic\nworkflows: {}\n",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="import_draft",
    ),
    RouteCase(
        id="export-draft",
        method="GET",
        segments=("v1", "configuration", "draft", "export"),
        query={},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="export_draft",
        expected_raw_body=b"component_catalog_revision: synthetic\nworkflows: {}\n",
    ),
    RouteCase(
        id="validate-draft",
        method="POST",
        segments=("v1", "configuration", "draft", "validate"),
        query={},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="validate_draft",
    ),
    RouteCase(
        id="relationships",
        method="GET",
        segments=("v1", "configuration", "relationships"),
        query={},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="relationships",
    ),
    RouteCase(
        id="discard-draft",
        method="POST",
        segments=("v1", "configuration", "draft", "discard"),
        query={},
        body=json_body(
            {
                "expected_draft_version": 1,
                "expected_active_identity": str(REVISION),
            }
        ),
        headers=IDEMPOTENCY_HEADERS,
        expected_status=HTTPStatus.OK,
        expected_operation="discard",
    ),
    RouteCase(
        id="read-discard",
        method="GET",
        segments=("v1", "configuration", "discards", "discard-1"),
        query={},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="read_discard",
    ),
    RouteCase(
        id="list-revisions",
        method="GET",
        segments=("v1", "configuration", "revisions"),
        query={"limit": ["10"], "cursor": ["next"]},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="list_revisions",
    ),
    RouteCase(
        id="read-revision",
        method="GET",
        segments=("v1", "configuration", "revisions", str(REVISION)),
        query={},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="read_revision",
    ),
    RouteCase(
        id="compare-revisions",
        method="GET",
        segments=("v1", "configuration", "revisions", "compare"),
        query={
            "source_revision_id": [str(REVISION)],
            "target_revision_id": [str(OTHER_REVISION)],
        },
        body=b"",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="compare_revisions",
    ),
    RouteCase(
        id="publish",
        method="POST",
        segments=("v1", "configuration", "publications"),
        query={},
        body=json_body({"expected_draft_version": 1}),
        headers=IDEMPOTENCY_HEADERS,
        expected_status=HTTPStatus.ACCEPTED,
        expected_operation="publish",
    ),
    RouteCase(
        id="read-publication",
        method="GET",
        segments=("v1", "configuration", "publications", "publication-1"),
        query={},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="read_publication",
    ),
    RouteCase(
        id="plan-activation",
        method="POST",
        segments=("v1", "configuration", "activations", "plan"),
        query={},
        body=json_body({"target_revision_id": str(REVISION)}),
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="plan_activation",
    ),
    RouteCase(
        id="activate",
        method="POST",
        segments=("v1", "configuration", "activations"),
        query={},
        body=json_body(
            {
                "target_revision_id": str(REVISION),
                "plan_digest": activation_plan().plan_digest,
            }
        ),
        headers=IDEMPOTENCY_HEADERS,
        expected_status=HTTPStatus.ACCEPTED,
        expected_operation="activate",
    ),
    RouteCase(
        id="list-activations",
        method="GET",
        segments=("v1", "configuration", "activations"),
        query={"limit": ["10"], "cursor": ["next"]},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="list_activations",
    ),
    RouteCase(
        id="read-activation",
        method="GET",
        segments=("v1", "configuration", "activations", ACTIVATION_ID),
        query={},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="read_activation",
    ),
    RouteCase(
        id="read-readiness",
        method="GET",
        segments=("v1", "configuration", "activations", ACTIVATION_ID, "readiness"),
        query={},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.OK,
        expected_operation="read_activation",
    ),
    RouteCase(
        id="register-readiness",
        method="POST",
        segments=("v1", "configuration", "activations", ACTIVATION_ID, "readiness"),
        query={},
        body=json_body(READINESS),
        headers=(),
        expected_status=HTTPStatus.ACCEPTED,
        expected_operation="register_readiness",
    ),
    RouteCase(
        id="rollback",
        method="POST",
        segments=("v1", "configuration", "activations", ACTIVATION_ID, "rollback"),
        query={},
        body=json_body({"plan_digest": activation_plan().plan_digest}),
        headers=IDEMPOTENCY_HEADERS,
        expected_status=HTTPStatus.ACCEPTED,
        expected_operation="rollback",
    ),
]


def configuration_api(
    publication: FakePublication | None = None,
    activation: FakeActivation | None = None,
) -> tuple[ConfigurationApi, FakePublication, FakeActivation]:
    publication_service = publication or FakePublication()
    activation_controller = activation or FakeActivation()
    return (
        ConfigurationApi(
            settings=ControlSettings(),
            publication=cast(ConfigurationPublicationService, publication_service),
            activation=cast(ConfigurationActivationController, activation_controller),
        ),
        publication_service,
        activation_controller,
    )


def scope_binding() -> TrustedScopeBinding:
    return TrustedScopeBinding.create(
        kind=ScopeBindingKind.API,
        scope=SCOPE,
        binding_id="principal",
    )


@pytest.mark.parametrize("case", ROUTE_CASES, ids=lambda case: case.id)
async def test_configuration_api_dispatches_bounded_scoped_operations(case: RouteCase) -> None:
    api, publication, activation = configuration_api()
    route = api.resolve_route(case.method, case.segments)

    response = await api.dispatch(
        route,
        query=case.query,
        headers=case.headers,
        read_body=lambda: _body(case.body),
        scope_binding=scope_binding(),
    )

    operations = [*publication.operations, *activation.operations]
    scopes = [*publication.scopes, *activation.scopes]
    assert response.status is case.expected_status
    assert response.raw_body == case.expected_raw_body
    assert operations == [case.expected_operation]
    assert scopes == [SCOPE]


async def _body(value: bytes) -> bytes:
    return value


class RaisingPublication(FakePublication):
    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error

    def create_draft(
        self,
        scope: RuntimeScope,
        configuration: TenantConfiguration,
    ) -> DraftRecord:
        del scope, configuration
        raise self.error


class RaisingActivation(FakeActivation):
    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error

    async def plan(self, scope: RuntimeScope, revision_id: RevisionIdentity) -> ActivationPlan:
        del scope, revision_id
        raise self.error


@dataclass(frozen=True, kw_only=True)
class ErrorCase:
    id: str
    boundary: str
    error: Exception
    expected_status: HTTPStatus
    expected_code: str


ERROR_CASES = [
    ErrorCase(
        id="publication-conflict",
        boundary="publication",
        error=PublicationOperationError(
            PublicationErrorCode.CONFLICT,
            "Configuration publication conflicted",
            retryable=False,
        ),
        expected_status=HTTPStatus.CONFLICT,
        expected_code="conflict",
    ),
    ErrorCase(
        id="configuration-not-found",
        boundary="publication",
        error=ConfigurationNotFoundError("missing"),
        expected_status=HTTPStatus.NOT_FOUND,
        expected_code="configuration_not_found",
    ),
    ErrorCase(
        id="configuration-conflict",
        boundary="publication",
        error=ConfigurationConflictError("changed"),
        expected_status=HTTPStatus.CONFLICT,
        expected_code="configuration_conflict",
    ),
    ErrorCase(
        id="configuration-limit",
        boundary="publication",
        error=ConfigurationLimitError("bounded"),
        expected_status=HTTPStatus.BAD_REQUEST,
        expected_code="configuration_limit",
    ),
    ErrorCase(
        id="configuration-unavailable",
        boundary="publication",
        error=ConfigurationUnavailableError("unavailable"),
        expected_status=HTTPStatus.SERVICE_UNAVAILABLE,
        expected_code="configuration_unavailable",
    ),
    ErrorCase(
        id="configuration-integrity",
        boundary="publication",
        error=ConfigurationIntegrityError("inconsistent"),
        expected_status=HTTPStatus.INTERNAL_SERVER_ERROR,
        expected_code="configuration_integrity",
    ),
    ErrorCase(
        id="configuration-invalid",
        boundary="publication",
        error=ConfigurationError("invalid"),
        expected_status=HTTPStatus.UNPROCESSABLE_ENTITY,
        expected_code="invalid_configuration",
    ),
    ErrorCase(
        id="activation-controller-stale",
        boundary="activation",
        error=ActivationControllerError(
            code=ActivationOutcomeCode.STALE_PLAN,
            message="Activation plan is stale",
            retryable=False,
        ),
        expected_status=HTTPStatus.CONFLICT,
        expected_code="stale_plan",
    ),
    ErrorCase(
        id="activation-not-found",
        boundary="activation",
        error=ActivationNotFoundError("missing"),
        expected_status=HTTPStatus.NOT_FOUND,
        expected_code="activation_not_found",
    ),
    ErrorCase(
        id="activation-conflict",
        boundary="activation",
        error=ActivationConflictError("changed"),
        expected_status=HTTPStatus.CONFLICT,
        expected_code="activation_conflict",
    ),
    ErrorCase(
        id="activation-limit",
        boundary="activation",
        error=ActivationLimitError("bounded"),
        expected_status=HTTPStatus.BAD_REQUEST,
        expected_code="activation_limit",
    ),
    ErrorCase(
        id="activation-unavailable",
        boundary="activation",
        error=ActivationUnavailableError("unavailable"),
        expected_status=HTTPStatus.SERVICE_UNAVAILABLE,
        expected_code="activation_unavailable",
    ),
    ErrorCase(
        id="activation-integrity",
        boundary="activation",
        error=ActivationIntegrityError("inconsistent"),
        expected_status=HTTPStatus.INTERNAL_SERVER_ERROR,
        expected_code="activation_integrity",
    ),
]


@pytest.mark.parametrize("case", ERROR_CASES, ids=lambda case: case.id)
async def test_configuration_api_translates_layered_errors(case: ErrorCase) -> None:
    if case.boundary == "publication":
        api, _, _ = configuration_api(publication=RaisingPublication(case.error))
        route = api.resolve_route("POST", ("v1", "configuration", "draft"))
        body = json_body({"configuration": CONFIGURATION})
    else:
        api, _, _ = configuration_api(activation=RaisingActivation(case.error))
        route = api.resolve_route("POST", ("v1", "configuration", "activations", "plan"))
        body = json_body({"target_revision_id": str(REVISION)})

    with pytest.raises(ConfigurationApiRequestError) as raised:
        await api.dispatch(
            route,
            query={},
            headers=(),
            read_body=lambda: _body(body),
            scope_binding=scope_binding(),
        )

    assert raised.value.status is case.expected_status
    assert raised.value.code == case.expected_code


@pytest.mark.parametrize(
    ("method", "segments", "expected_status", "expected_code"),
    [
        pytest.param(
            "GET",
            ("v1", "configuration", "unknown"),
            HTTPStatus.NOT_FOUND,
            "not_found",
            id="unknown-route",
        ),
        pytest.param(
            "GET",
            ("v1", "configuration", "a", "b", "c", "d", "e"),
            HTTPStatus.REQUEST_URI_TOO_LONG,
            "path_too_large",
            id="route-segment-bound",
        ),
    ],
)
def test_configuration_api_rejects_unknown_or_oversized_routes(
    method: str,
    segments: tuple[str, ...],
    expected_status: HTTPStatus,
    expected_code: str,
) -> None:
    api, _, _ = configuration_api()

    with pytest.raises(ConfigurationApiRequestError) as raised:
        api.resolve_route(method, segments)

    assert raised.value.status is expected_status
    assert raised.value.code == expected_code


@dataclass(frozen=True, kw_only=True)
class InvalidDispatchCase:
    id: str
    method: str
    segments: tuple[str, ...]
    query: dict[str, list[str]]
    body: bytes
    headers: tuple[tuple[bytes, bytes], ...]
    expected_status: HTTPStatus
    expected_code: str


INVALID_DISPATCH_CASES = [
    InvalidDispatchCase(
        id="malformed-json",
        method="POST",
        segments=("v1", "configuration", "draft"),
        query={},
        body=b"{",
        headers=(),
        expected_status=HTTPStatus.BAD_REQUEST,
        expected_code="invalid_json",
    ),
    InvalidDispatchCase(
        id="duplicate-json-key",
        method="POST",
        segments=("v1", "configuration", "draft"),
        query={},
        body=b'{"configuration":{},"configuration":{}}',
        headers=(),
        expected_status=HTTPStatus.BAD_REQUEST,
        expected_code="invalid_json",
    ),
    InvalidDispatchCase(
        id="non-finite-json-number",
        method="POST",
        segments=("v1", "configuration", "draft"),
        query={},
        body=b'{"configuration":NaN}',
        headers=(),
        expected_status=HTTPStatus.BAD_REQUEST,
        expected_code="invalid_json",
    ),
    InvalidDispatchCase(
        id="missing-idempotency-header",
        method="POST",
        segments=("v1", "configuration", "publications"),
        query={},
        body=json_body({"expected_draft_version": 1}),
        headers=(),
        expected_status=HTTPStatus.UNPROCESSABLE_ENTITY,
        expected_code="invalid_request",
    ),
    InvalidDispatchCase(
        id="duplicate-idempotency-header",
        method="POST",
        segments=("v1", "configuration", "publications"),
        query={},
        body=json_body({"expected_draft_version": 1}),
        headers=(
            (b"x-idempotency-key", b"first"),
            (b"x-idempotency-key", b"second"),
        ),
        expected_status=HTTPStatus.UNPROCESSABLE_ENTITY,
        expected_code="invalid_request",
    ),
    InvalidDispatchCase(
        id="invalid-header-encoding",
        method="POST",
        segments=("v1", "configuration", "publications"),
        query={},
        body=json_body({"expected_draft_version": 1}),
        headers=((b"x-idempotency-key", b"\xff"),),
        expected_status=HTTPStatus.UNPROCESSABLE_ENTITY,
        expected_code="invalid_request",
    ),
    InvalidDispatchCase(
        id="nonempty-validation-body",
        method="POST",
        segments=("v1", "configuration", "draft", "validate"),
        query={},
        body=b"{}",
        headers=(),
        expected_status=HTTPStatus.UNPROCESSABLE_ENTITY,
        expected_code="invalid_request",
    ),
    InvalidDispatchCase(
        id="unexpected-query-field",
        method="GET",
        segments=("v1", "configuration", "draft"),
        query={"unexpected": ["value"]},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.UNPROCESSABLE_ENTITY,
        expected_code="invalid_request",
    ),
    InvalidDispatchCase(
        id="repeated-query-value",
        method="GET",
        segments=("v1", "configuration", "revisions"),
        query={"limit": ["1", "2"]},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.UNPROCESSABLE_ENTITY,
        expected_code="invalid_request",
    ),
    InvalidDispatchCase(
        id="list-limit-bound",
        method="GET",
        segments=("v1", "configuration", "revisions"),
        query={"limit": ["0"]},
        body=b"",
        headers=(),
        expected_status=HTTPStatus.UNPROCESSABLE_ENTITY,
        expected_code="invalid_request",
    ),
]


@pytest.mark.parametrize(
    "case",
    INVALID_DISPATCH_CASES,
    ids=lambda case: case.id,
)
async def test_configuration_api_rejects_invalid_bounded_inputs(
    case: InvalidDispatchCase,
) -> None:
    api, _, _ = configuration_api()
    route = api.resolve_route(case.method, case.segments)

    with pytest.raises(ConfigurationApiRequestError) as raised:
        await api.dispatch(
            route,
            query=case.query,
            headers=case.headers,
            read_body=lambda: _body(case.body),
            scope_binding=scope_binding(),
        )

    assert raised.value.status is case.expected_status
    assert raised.value.code == case.expected_code
