from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, cast

import pytest

from justflow.config.triggers import TriggersConfig
from justflow.configuration.activation import (
    ActivationObservations,
    ActivationRecord,
    ActivationState,
    PublicationRecord,
    activation_identity,
    activation_request_digest,
    control_identity_digest,
    plan_configuration_activation,
    publication_identity,
)
from justflow.configuration.activation_aws import DynamoDbActivationStore
from justflow.configuration.activation_errors import (
    ActivationConflictError,
    ActivationIntegrityError,
    ActivationLimitError,
    ActivationNotFoundError,
)
from justflow.configuration.activation_store import complete_publication, transition_activation
from justflow.configuration.aws import DynamoConfigurationClient
from justflow.configuration.lifecycle import (
    ConfigurationDiscardRecord,
    complete_discard,
    discard_identity,
)
from justflow.configuration.models import ConfigurationBundle, RevisionIdentity
from justflow.provenance import WorkerArtifactIdentity, provenance_digest
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope

NOW = datetime(2026, 8, 4, 12, tzinfo=UTC)
REVISION = RevisionIdentity("a" * 64)
POLICY_DIGEST = provenance_digest({"policy": "current"})
TASK_QUEUE_IDENTITY_DIGEST = provenance_digest({"task_queue": "current"})
WORKER_REGISTRATION_DIGEST = provenance_digest({"worker_registration": "current"})
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="justflow",
    build_id="release-1",
    artifact_digest=f"sha256:{'b' * 64}",
    package_version="0.1.0",
)
OTHER_SCOPE = RuntimeScope.create(
    tenant="other",
    application="justflow",
    environment="development",
)


class ConditionalError(Exception):
    response: ClassVar[dict[str, dict[str, str]]] = {
        "Error": {"Code": "ConditionalCheckFailedException"}
    }


class FakeDynamoClient:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}

    def put_item(self, **kwargs: object) -> Mapping[str, Any]:
        item = _item(kwargs["Item"])
        key = _key(item)
        current = self.items.get(key)
        condition = kwargs["ConditionExpression"]
        values = kwargs.get("ExpressionAttributeValues")
        if condition == "attribute_not_exists(metadata_key)":
            matches = current is None
        elif condition == "version = :expected_version":
            assert isinstance(values, dict)
            matches = current is not None and current["version"] == values[":expected_version"]
        else:
            raise AssertionError(f"Unsupported condition: {condition}")
        if not matches:
            raise ConditionalError
        self.items[key] = item
        return {}

    def get_item(self, **kwargs: object) -> Mapping[str, Any]:
        current = self.items.get(_key(_item(kwargs["Key"])))
        return {} if current is None else {"Item": current}

    def query(self, **kwargs: object) -> Mapping[str, Any]:
        values = kwargs["ExpressionAttributeValues"]
        assert isinstance(values, dict)
        scope_digest = values[":scope_digest"]
        limit = kwargs["Limit"]
        assert isinstance(limit, int)
        items = [
            item
            for item in self.items.values()
            if item.get("record_type") == {"S": "activation"}
            and item.get("scope_digest") == scope_digest
        ]
        items.sort(key=lambda item: item["activation_order"]["S"], reverse=True)
        start = 0
        exclusive = kwargs.get("ExclusiveStartKey")
        if isinstance(exclusive, dict):
            exclusive_key = _key(_item(exclusive))
            start = next(
                index + 1 for index, item in enumerate(items) if _key(item) == exclusive_key
            )
        selected = items[start : start + limit]
        response: dict[str, object] = {"Items": selected}
        if start + limit < len(items):
            last = selected[-1]
            response["LastEvaluatedKey"] = {
                "scope_key": last["scope_key"],
                "metadata_key": last["metadata_key"],
            }
        return response


def publication(*, request: str = "one") -> PublicationRecord:
    key = "publish-orders"
    return PublicationRecord(
        publication_id=publication_identity(LOCAL_RUNTIME_SCOPE.digest, key),
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        idempotency_key_digest=control_identity_digest("publication-key", key),
        request_digest=provenance_digest({"request": request}),
        actor_digest=control_identity_digest("actor", "operator"),
        correlation_digest=control_identity_digest("correlation", "request"),
        policy_digest=POLICY_DIGEST,
        created_at=NOW,
        updated_at=NOW,
    )


def activation(
    *,
    key: str = "activate-orders",
    created_at: datetime = NOW,
) -> ActivationRecord:
    plan = plan_configuration_activation(
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        target_revision_id=REVISION,
        target=ConfigurationBundle(workflows={}, triggers=TriggersConfig(triggers={})),
        target_artifact=ARTIFACT,
        target_task_queue_identity_digest=TASK_QUEUE_IDENTITY_DIGEST,
        target_worker_registration_digest=WORKER_REGISTRATION_DIGEST,
        target_definition_digests={},
        observations=ActivationObservations(
            policy_digest=POLICY_DIGEST,
            worker_observation_digest=provenance_digest({"worker": "empty"}),
            schedule_observation_digest=provenance_digest({"schedules": "empty"}),
        ),
    )
    return ActivationRecord(
        activation_id=activation_identity(LOCAL_RUNTIME_SCOPE.digest, key),
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        idempotency_key_digest=control_identity_digest("activation-key", key),
        request_digest=activation_request_digest(plan),
        actor_digest=control_identity_digest("actor", "operator"),
        correlation_digest=control_identity_digest("correlation", "request"),
        plan=plan,
        created_at=created_at,
        updated_at=created_at,
    )


def discard() -> ConfigurationDiscardRecord:
    key = "discard-orders"
    return ConfigurationDiscardRecord(
        discard_id=discard_identity(LOCAL_RUNTIME_SCOPE.digest, key),
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        idempotency_key_digest=control_identity_digest("discard-key", key),
        request_digest=provenance_digest({"request": "discard"}),
        actor_digest=control_identity_digest("discard-actor", "operator"),
        correlation_digest=control_identity_digest("discard-correlation", "request"),
        expected_draft_version=1,
        expected_active_identity=REVISION,
        active_document_digest=provenance_digest({"configuration": "active"}),
        created_at=NOW,
        updated_at=NOW,
    )


def test_dynamodb_activation_store_is_idempotent_conditional_and_bounded() -> None:
    store = DynamoDbActivationStore(
        table_name="justflow-configuration",
        activation_index_name="scope-activations",
        client=cast(DynamoConfigurationClient, FakeDynamoClient()),
    )
    publication_record = publication()
    pending = activation()

    assert store.create_publication(LOCAL_RUNTIME_SCOPE, publication_record) == publication_record
    assert store.create_publication(LOCAL_RUNTIME_SCOPE, publication_record) == publication_record
    with pytest.raises(ActivationConflictError):
        store.create_publication(LOCAL_RUNTIME_SCOPE, publication(request="changed"))

    assert store.create_activation(LOCAL_RUNTIME_SCOPE, pending) == pending
    running = transition_activation(pending, ActivationState.RUNNING, occurred_at=NOW)
    assert (
        store.update_activation(
            LOCAL_RUNTIME_SCOPE,
            running,
            expected_version=pending.version,
        )
        == running
    )
    with pytest.raises(ActivationConflictError):
        store.update_activation(
            LOCAL_RUNTIME_SCOPE,
            running,
            expected_version=pending.version,
        )
    page = store.list_activations(LOCAL_RUNTIME_SCOPE, limit=1)
    assert len(page.activations) == 1
    assert page.activations[0].activation_id == pending.activation_id


def test_dynamodb_activation_store_reads_updates_and_pages_with_scope_bound_cursor() -> None:
    client = FakeDynamoClient()
    store = DynamoDbActivationStore(
        table_name="justflow-configuration",
        activation_index_name="scope-activations",
        client=cast(DynamoConfigurationClient, client),
    )
    pending_publication = publication()
    pending_discard = discard()
    applied_discard = complete_discard(
        pending_discard,
        result_draft_version=2,
        occurred_at=NOW + timedelta(seconds=1),
    )
    applied_publication = complete_publication(
        pending_publication,
        source_revision_id=REVISION,
        published_revision_id=REVISION,
        occurred_at=NOW + timedelta(seconds=1),
    )
    first = activation(key="first", created_at=NOW)
    second = activation(key="second", created_at=NOW + timedelta(seconds=1))

    store.create_publication(LOCAL_RUNTIME_SCOPE, pending_publication)
    store.create_discard(LOCAL_RUNTIME_SCOPE, pending_discard)
    assert store.create_discard(LOCAL_RUNTIME_SCOPE, pending_discard) == pending_discard
    with pytest.raises(ActivationConflictError):
        store.create_discard(
            LOCAL_RUNTIME_SCOPE,
            pending_discard.model_copy(
                update={"request_digest": provenance_digest({"request": "changed"})}
            ),
        )
    with pytest.raises(ActivationIntegrityError, match="version transition"):
        store.update_discard(
            LOCAL_RUNTIME_SCOPE,
            pending_discard,
            expected_version=pending_discard.version,
        )
    assert (
        store.update_discard(
            LOCAL_RUNTIME_SCOPE,
            applied_discard,
            expected_version=pending_discard.version,
        )
        == applied_discard
    )
    assert store.read_discard(LOCAL_RUNTIME_SCOPE, pending_discard.discard_id) == applied_discard
    with pytest.raises(ActivationConflictError):
        store.update_discard(
            LOCAL_RUNTIME_SCOPE,
            applied_discard,
            expected_version=pending_discard.version,
        )
    assert (
        store.read_publication(LOCAL_RUNTIME_SCOPE, pending_publication.publication_id)
        == pending_publication
    )
    assert (
        store.update_publication(
            LOCAL_RUNTIME_SCOPE,
            applied_publication,
            expected_version=pending_publication.version,
        )
        == applied_publication
    )
    assert (
        store.read_publication(LOCAL_RUNTIME_SCOPE, pending_publication.publication_id)
        == applied_publication
    )
    store.create_activation(LOCAL_RUNTIME_SCOPE, first)
    store.create_activation(LOCAL_RUNTIME_SCOPE, second)
    assert store.read_activation(LOCAL_RUNTIME_SCOPE, first.activation_id) == first

    page = store.list_activations(LOCAL_RUNTIME_SCOPE, limit=1)
    assert [item.activation_id for item in page.activations] == [second.activation_id]
    assert page.next_cursor is not None
    next_page = store.list_activations(
        LOCAL_RUNTIME_SCOPE,
        limit=1,
        cursor=page.next_cursor,
    )
    assert [item.activation_id for item in next_page.activations] == [first.activation_id]
    with pytest.raises(ActivationIntegrityError, match="cursor"):
        store.list_activations(OTHER_SCOPE, limit=1, cursor=page.next_cursor)


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param("publication", id="missing-publication"),
        pytest.param("activation", id="missing-activation"),
        pytest.param("discard", id="missing-discard"),
    ],
)
def test_dynamodb_activation_store_reports_missing_records(operation: str) -> None:
    store = DynamoDbActivationStore(
        table_name="justflow-configuration",
        activation_index_name="scope-activations",
        client=cast(DynamoConfigurationClient, FakeDynamoClient()),
    )

    with pytest.raises(ActivationNotFoundError):
        if operation == "publication":
            store.read_publication(LOCAL_RUNTIME_SCOPE, "missing")
        elif operation == "discard":
            store.read_discard(LOCAL_RUNTIME_SCOPE, "missing")
        else:
            store.read_activation(LOCAL_RUNTIME_SCOPE, "missing")


def test_dynamodb_activation_store_enforces_page_and_scope_bounds() -> None:
    store = DynamoDbActivationStore(
        table_name="justflow-configuration",
        activation_index_name="scope-activations",
        client=cast(DynamoConfigurationClient, FakeDynamoClient()),
    )

    with pytest.raises(ActivationLimitError):
        store.list_activations(LOCAL_RUNTIME_SCOPE, limit=0)
    with pytest.raises(ActivationIntegrityError, match="another runtime scope"):
        store.create_activation(OTHER_SCOPE, activation())


def _item(value: object) -> dict[str, Any]:
    assert isinstance(value, dict)
    return value


def _key(item: dict[str, Any]) -> tuple[str, str]:
    return item["scope_key"]["S"], item["metadata_key"]["S"]
