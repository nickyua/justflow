from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator, Mapping
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from itertools import count
from pathlib import Path
from typing import Any, ClassVar

import pytest
from pydantic import ValidationError

from justflow.config.models import FlowStep, ResourceConfig, ResourcesConfig, WorkflowConfig
from justflow.config.triggers import TriggersConfig
from justflow.configuration import (
    AwsConfigurationStore,
    ConfigurationBundle,
    ConfigurationConflictError,
    ConfigurationIntegrityError,
    ConfigurationLimitError,
    ConfigurationNotFoundError,
    ConfigurationScopeError,
    ConfigurationStore,
    ConfigurationUnavailableError,
    RevisionIdentity,
    SqliteConfigurationStore,
)
from justflow.scope import RuntimeScope

SCOPE_A = RuntimeScope.create(
    tenant="tenant-a",
    application="orders",
    environment="production",
)
SCOPE_B = RuntimeScope.create(
    tenant="tenant-b",
    application="orders",
    environment="production",
)
FIXED_TIME = datetime(2026, 8, 4, 10, 0, tzinfo=UTC)


class AwsConditionalError(Exception):
    response: ClassVar[dict[str, dict[str, str]]] = {
        "Error": {"Code": "ConditionalCheckFailedException"}
    }


class AwsNotFoundError(Exception):
    response: ClassVar[dict[str, dict[str, str]]] = {"Error": {"Code": "NoSuchKey"}}


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def get_object(self, **kwargs: object) -> Mapping[str, Any]:
        key = (_required_text(kwargs, "Bucket"), _required_text(kwargs, "Key"))
        if key not in self.objects:
            raise AwsNotFoundError
        return {"Body": self.objects[key]}

    def put_object(self, **kwargs: object) -> Mapping[str, Any]:
        key = (_required_text(kwargs, "Bucket"), _required_text(kwargs, "Key"))
        body = kwargs["Body"]
        assert isinstance(body, bytes)
        if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
            raise AwsConditionalError
        self.objects[key] = body
        return {}

    def delete_object(self, **kwargs: object) -> Mapping[str, Any]:
        key = (_required_text(kwargs, "Bucket"), _required_text(kwargs, "Key"))
        self.objects.pop(key, None)
        return {}


class FakeDynamoClient:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}

    def get_item(self, **kwargs: object) -> Mapping[str, Any]:
        item = self.items.get(_dynamo_key(kwargs["Key"]))
        return {} if item is None else {"Item": item}

    def put_item(self, **kwargs: object) -> Mapping[str, Any]:
        item = kwargs["Item"]
        assert isinstance(item, dict)
        key = _dynamo_key(item)
        current = self.items.get(key)
        condition = kwargs["ConditionExpression"]
        values = kwargs.get("ExpressionAttributeValues")
        if not _condition_matches(current, condition, values):
            raise AwsConditionalError
        self.items[key] = dict(item)
        return {}

    def query(self, **kwargs: object) -> Mapping[str, Any]:
        values = kwargs["ExpressionAttributeValues"]
        assert isinstance(values, dict)
        scope_digest = values[":scope_digest"]["S"]
        limit = kwargs["Limit"]
        assert isinstance(limit, int)
        items = [
            item
            for item in self.items.values()
            if item.get("record_type") == {"S": "revision"}
            and item.get("scope_digest") == {"S": scope_digest}
        ]
        items.sort(key=lambda item: item["revision_order"]["S"], reverse=True)
        start = 0
        exclusive = kwargs.get("ExclusiveStartKey")
        if isinstance(exclusive, dict):
            exclusive_key = _dynamo_key(exclusive)
            start = next(
                index + 1 for index, item in enumerate(items) if _dynamo_key(item) == exclusive_key
            )
        page = items[start : start + limit]
        response: dict[str, Any] = {"Items": page}
        if start + limit < len(items):
            response["LastEvaluatedKey"] = {
                "scope_key": page[-1]["scope_key"],
                "metadata_key": page[-1]["metadata_key"],
            }
        return response

    def delete_item(self, **kwargs: object) -> Mapping[str, Any]:
        key = _dynamo_key(kwargs["Key"])
        current = self.items.get(key)
        condition = kwargs.get("ConditionExpression")
        values = kwargs.get("ExpressionAttributeValues")
        if current is None or (
            isinstance(condition, str) and not _condition_matches(current, condition, values)
        ):
            raise AwsConditionalError
        del self.items[key]
        return {}

    def transact_write_items(self, **kwargs: object) -> Mapping[str, Any]:
        operations = kwargs["TransactItems"]
        assert isinstance(operations, list)
        original = deepcopy(self.items)
        try:
            for operation in operations:
                assert isinstance(operation, dict)
                if "ConditionCheck" in operation:
                    request = operation["ConditionCheck"]
                    assert isinstance(request, dict)
                    item = self.items.get(_dynamo_key(request["Key"]))
                    if not _condition_matches(
                        item,
                        request["ConditionExpression"],
                        request.get("ExpressionAttributeValues"),
                    ):
                        raise AwsConditionalError
                elif "Put" in operation:
                    request = operation["Put"]
                    assert isinstance(request, dict)
                    self.put_item(**request)
                elif "Delete" in operation:
                    request = operation["Delete"]
                    assert isinstance(request, dict)
                    self.delete_item(**request)
                elif "Update" in operation:
                    request = operation["Update"]
                    assert isinstance(request, dict)
                    self._update_item(request)
                else:
                    raise AssertionError("Unknown DynamoDB transaction operation")
        except Exception:
            self.items = original
            raise
        return {}

    def _update_item(self, request: Mapping[str, object]) -> None:
        key = _dynamo_key(request["Key"])
        current = self.items.get(key)
        condition = request["ConditionExpression"]
        values = request["ExpressionAttributeValues"]
        assert isinstance(condition, str)
        assert isinstance(values, dict)
        if not _condition_matches(current, condition, values):
            raise AwsConditionalError
        assert current is not None
        expression = request["UpdateExpression"]
        assert isinstance(expression, str)
        operation, field, value_name = expression.split()
        assert operation == "ADD"
        current_value = int(current[field]["N"])
        delta = int(values[value_name]["N"])
        updated = dict(current)
        updated[field] = {"N": str(current_value + delta)}
        self.items[key] = updated


class InterleavingDynamoClient(FakeDynamoClient):
    def __init__(self) -> None:
        super().__init__()
        self.before_revision_delete: Callable[[], None] | None = None

    def transact_write_items(self, **kwargs: object) -> Mapping[str, Any]:
        operations = kwargs["TransactItems"]
        assert isinstance(operations, list)
        if self.before_revision_delete is not None and any(
            isinstance(operation, dict) and "Delete" in operation for operation in operations
        ):
            callback = self.before_revision_delete
            self.before_revision_delete = None
            callback()
        return super().transact_write_items(**kwargs)


class UnavailableDynamoClient(FakeDynamoClient):
    def get_item(self, **kwargs: object) -> Mapping[str, Any]:
        del kwargs
        raise RuntimeError("credential-value-must-not-escape")


@pytest.fixture(params=["sqlite", "aws"])
def configuration_store(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> Iterator[ConfigurationStore]:
    ticks = count()

    def clock() -> datetime:
        return FIXED_TIME + timedelta(seconds=next(ticks))

    if request.param == "sqlite":
        store = SqliteConfigurationStore(tmp_path / "configuration.sqlite3", clock=clock)
        try:
            yield store
        finally:
            store.close()
        return
    yield AwsConfigurationStore(
        bucket="example-configuration",
        table_name="justflow-configuration",
        revision_index_name="scope-revisions",
        s3_client=FakeS3Client(),
        dynamodb_client=FakeDynamoClient(),
        clock=clock,
    )


def configuration_bundle(name: str = "example") -> ConfigurationBundle:
    workflow = WorkflowConfig(
        workflow=name,
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    return ConfigurationBundle(
        workflows={name: workflow},
        triggers=TriggersConfig(triggers={}),
    )


def test_store_isolates_scopes_and_immutable_revision_identities(
    configuration_store: ConfigurationStore,
) -> None:
    bundle = configuration_bundle()
    revision_a = configuration_store.create_revision(SCOPE_A, bundle, parent_revision_id=None)
    revision_b = configuration_store.create_revision(SCOPE_B, bundle, parent_revision_id=None)

    assert revision_a.revision_id != revision_b.revision_id
    assert (
        configuration_store.create_revision(SCOPE_A, bundle, parent_revision_id=None) == revision_a
    )
    with pytest.raises(ConfigurationNotFoundError):
        configuration_store.read_revision(SCOPE_B, revision_a.revision_id)


def test_store_uses_compare_and_swap_for_drafts_and_active_pointers(
    configuration_store: ConfigurationStore,
) -> None:
    bundle = configuration_bundle()
    draft = configuration_store.compare_and_swap_draft(
        SCOPE_A,
        bundle,
        expected_version=None,
    )
    revision = configuration_store.create_revision(
        SCOPE_A,
        bundle,
        parent_revision_id=None,
    )
    active = configuration_store.compare_and_swap_active(
        SCOPE_A,
        revision.revision_id,
        expected_revision_id=None,
    )

    assert draft.version == 1
    assert configuration_store.read_draft(SCOPE_A) == draft
    assert active.revision_id == revision.revision_id
    with pytest.raises(ConfigurationConflictError):
        configuration_store.compare_and_swap_draft(
            SCOPE_A,
            bundle,
            expected_version=None,
        )
    with pytest.raises(ConfigurationConflictError):
        configuration_store.compare_and_swap_active(
            SCOPE_A,
            revision.revision_id,
            expected_revision_id=None,
        )

    updated = configuration_store.compare_and_swap_draft(
        SCOPE_A,
        configuration_bundle("updated"),
        expected_version=draft.version,
    )
    assert updated.version == 2


def test_active_pointer_compare_and_swap_rejects_an_aba_version(
    configuration_store: ConfigurationStore,
) -> None:
    first = configuration_store.create_revision(
        SCOPE_A,
        configuration_bundle("first"),
        parent_revision_id=None,
    )
    second = configuration_store.create_revision(
        SCOPE_A,
        configuration_bundle("second"),
        parent_revision_id=None,
    )
    initial = configuration_store.compare_and_swap_active(
        SCOPE_A,
        first.revision_id,
        expected_revision_id=None,
    )
    changed = configuration_store.compare_and_swap_active(
        SCOPE_A,
        second.revision_id,
        expected_revision_id=first.revision_id,
        expected_version=initial.version,
    )
    restored = configuration_store.compare_and_swap_active(
        SCOPE_A,
        first.revision_id,
        expected_revision_id=second.revision_id,
        expected_version=changed.version,
    )

    assert restored.version == 3
    with pytest.raises(ConfigurationConflictError):
        configuration_store.compare_and_swap_active(
            SCOPE_A,
            second.revision_id,
            expected_revision_id=first.revision_id,
            expected_version=initial.version,
        )


def test_store_pagination_cursor_is_bounded_to_its_scope(
    configuration_store: ConfigurationStore,
) -> None:
    for name in ("first", "second", "third"):
        configuration_store.create_revision(
            SCOPE_A,
            configuration_bundle(name),
            parent_revision_id=None,
        )

    first_page = configuration_store.list_revisions(SCOPE_A, limit=2)

    assert len(first_page.revisions) == 2
    assert first_page.next_cursor is not None
    assert (
        len(
            configuration_store.list_revisions(
                SCOPE_A,
                limit=2,
                cursor=first_page.next_cursor,
            ).revisions
        )
        == 1
    )
    with pytest.raises(ConfigurationScopeError):
        configuration_store.list_revisions(
            SCOPE_B,
            limit=2,
            cursor=first_page.next_cursor,
        )


def test_store_retention_preserves_active_and_referenced_revisions(
    configuration_store: ConfigurationStore,
) -> None:
    active_revision = configuration_store.create_revision(
        SCOPE_A,
        configuration_bundle("active"),
        parent_revision_id=None,
    )
    deletable_revision = configuration_store.create_revision(
        SCOPE_A,
        configuration_bundle("deletable"),
        parent_revision_id=None,
    )
    configuration_store.create_revision(
        SCOPE_A,
        configuration_bundle("latest"),
        parent_revision_id=None,
    )
    configuration_store.compare_and_swap_active(
        SCOPE_A,
        active_revision.revision_id,
        expected_revision_id=None,
    )

    result = configuration_store.retain_revisions(
        SCOPE_A,
        keep_latest=1,
        delete_limit=10,
    )

    assert result.deleted_revision_ids == (deletable_revision.revision_id,)
    assert configuration_store.read_active(SCOPE_A) is not None


def test_store_preserves_parent_relationships_and_operation_bounds(
    configuration_store: ConfigurationStore,
) -> None:
    parent = configuration_store.create_revision(
        SCOPE_A,
        configuration_bundle("parent"),
        parent_revision_id=None,
    )
    child = configuration_store.create_revision(
        SCOPE_A,
        configuration_bundle("child"),
        parent_revision_id=parent.revision_id,
    )

    assert configuration_store.read_revision(SCOPE_A, child.revision_id) == child
    assert (
        configuration_store.create_revision(
            SCOPE_A,
            configuration_bundle("child"),
            parent_revision_id=parent.revision_id,
        )
        == child
    )
    assert (
        configuration_store.retain_revisions(
            SCOPE_A,
            keep_latest=1,
            delete_limit=1,
        ).deleted_revision_ids
        == ()
    )
    with pytest.raises(ConfigurationNotFoundError):
        configuration_store.create_revision(
            SCOPE_A,
            configuration_bundle("orphan"),
            parent_revision_id=RevisionIdentity("f" * 64),
        )
    with pytest.raises(ConfigurationLimitError):
        configuration_store.list_revisions(SCOPE_A, limit=0)
    with pytest.raises(ConfigurationLimitError):
        configuration_store.retain_revisions(
            SCOPE_A,
            keep_latest=0,
            delete_limit=1,
        )


def test_aws_retention_rejects_concurrent_activation() -> None:
    ticks = count()
    dynamodb = InterleavingDynamoClient()
    store = AwsConfigurationStore(
        bucket="example-configuration",
        table_name="justflow-configuration",
        revision_index_name="scope-revisions",
        s3_client=FakeS3Client(),
        dynamodb_client=dynamodb,
        clock=lambda: FIXED_TIME + timedelta(seconds=next(ticks)),
    )
    store.create_revision(
        SCOPE_A,
        configuration_bundle("oldest"),
        parent_revision_id=None,
    )
    candidate = store.create_revision(
        SCOPE_A,
        configuration_bundle("candidate"),
        parent_revision_id=None,
    )
    latest = store.create_revision(
        SCOPE_A,
        configuration_bundle("latest"),
        parent_revision_id=None,
    )
    store.compare_and_swap_active(
        SCOPE_A,
        latest.revision_id,
        expected_revision_id=None,
    )
    dynamodb.before_revision_delete = lambda: store.compare_and_swap_active(
        SCOPE_A,
        candidate.revision_id,
        expected_revision_id=latest.revision_id,
    )

    with pytest.raises(ConfigurationConflictError):
        store.retain_revisions(SCOPE_A, keep_latest=1, delete_limit=1)

    active = store.read_active(SCOPE_A)
    assert active is not None
    assert active.revision_id == candidate.revision_id
    assert store.read_revision(SCOPE_A, candidate.revision_id) == candidate


def test_aws_retention_rejects_concurrent_child_creation() -> None:
    ticks = count()
    dynamodb = InterleavingDynamoClient()
    store = AwsConfigurationStore(
        bucket="example-configuration",
        table_name="justflow-configuration",
        revision_index_name="scope-revisions",
        s3_client=FakeS3Client(),
        dynamodb_client=dynamodb,
        clock=lambda: FIXED_TIME + timedelta(seconds=next(ticks)),
    )
    store.create_revision(
        SCOPE_A,
        configuration_bundle("oldest"),
        parent_revision_id=None,
    )
    candidate = store.create_revision(
        SCOPE_A,
        configuration_bundle("candidate"),
        parent_revision_id=None,
    )
    store.create_revision(
        SCOPE_A,
        configuration_bundle("latest"),
        parent_revision_id=None,
    )
    child_records = []
    dynamodb.before_revision_delete = lambda: child_records.append(
        store.create_revision(
            SCOPE_A,
            configuration_bundle("child"),
            parent_revision_id=candidate.revision_id,
        )
    )

    with pytest.raises(ConfigurationConflictError):
        store.retain_revisions(SCOPE_A, keep_latest=1, delete_limit=1)

    assert len(child_records) == 1
    assert store.read_revision(SCOPE_A, candidate.revision_id) == candidate
    assert store.read_revision(SCOPE_A, child_records[0].revision_id) == child_records[0]


def test_sqlite_backend_errors_remain_layered(tmp_path: Path) -> None:
    store = SqliteConfigurationStore(tmp_path / "configuration.sqlite3")
    store.close()

    with pytest.raises(ConfigurationUnavailableError):
        store.read_draft(SCOPE_A)


def test_sqlite_rejects_an_unknown_schema_version(tmp_path: Path) -> None:
    path = tmp_path / "configuration.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version = 99")
    connection.close()

    with pytest.raises(
        ConfigurationIntegrityError,
        match="schema version is unsupported",
    ) as raised:
        SqliteConfigurationStore(path)

    assert isinstance(raised.value, ConfigurationIntegrityError)


def test_aws_backend_outage_uses_layered_error_without_leaking_cause() -> None:
    store = AwsConfigurationStore(
        bucket="example-configuration",
        table_name="justflow-configuration",
        revision_index_name="scope-revisions",
        s3_client=FakeS3Client(),
        dynamodb_client=UnavailableDynamoClient(),
    )

    with pytest.raises(ConfigurationUnavailableError) as raised:
        store.read_draft(SCOPE_A)

    assert "credential-value" not in str(raised.value)


def test_configuration_bundle_rejects_secret_values() -> None:
    with pytest.raises(ValidationError, match="secret aliases only"):
        ConfigurationBundle(
            resources=ResourcesConfig(
                resources={
                    "unsafe": ResourceConfig(
                        provider="memory",
                        config={"password": "not-allowed"},
                    )
                }
            ),
            workflows=configuration_bundle().workflows,
            triggers=TriggersConfig(triggers={}),
        )


def _required_text(values: Mapping[str, object], key: str) -> str:
    value = values[key]
    assert isinstance(value, str)
    return value


def _dynamo_key(value: object) -> tuple[str, str]:
    assert isinstance(value, dict)
    return value["scope_key"]["S"], value["metadata_key"]["S"]


def _condition_matches(
    item: Mapping[str, Any] | None,
    condition: object,
    values: object,
) -> bool:
    assert isinstance(condition, str)
    expression_values = values if isinstance(values, dict) else {}
    if condition == "attribute_not_exists(metadata_key)":
        return item is None
    if condition == "version = :expected_version":
        return item is not None and item.get("version") == expression_values[":expected_version"]
    if condition == "revision_id = :expected_revision_id":
        return (
            item is not None
            and item.get("revision_id") == expression_values[":expected_revision_id"]
        )
    if condition == "revision_id = :expected_revision_id AND version = :expected_version":
        return (
            item is not None
            and item.get("revision_id") == expression_values[":expected_revision_id"]
            and item.get("version") == expression_values[":expected_version"]
        )
    if condition == "record_type = :cleanup_record":
        return item is not None and item.get("record_type") == expression_values[":cleanup_record"]
    if condition.startswith("attribute_not_exists(metadata_key) OR revision_id <>"):
        return item is None or item.get("revision_id") != expression_values[":revision_id"]
    if not condition.startswith("attribute_exists(metadata_key)"):
        raise AssertionError(f"Unsupported DynamoDB condition: {condition}")
    if item is None:
        return False
    if "record_type = :revision_record" in condition and (
        item.get("record_type") != expression_values[":revision_record"]
    ):
        return False
    if "child_reference_count = :zero" in condition and (
        item.get("child_reference_count") != expression_values[":zero"]
    ):
        return False
    if "child_reference_count > :zero" in condition:
        count_value = item.get("child_reference_count")
        return isinstance(count_value, dict) and int(count_value["N"]) > int(
            expression_values[":zero"]["N"]
        )
    return True
