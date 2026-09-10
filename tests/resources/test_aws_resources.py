"""Deterministic tests for scoped AWS resource providers."""

from __future__ import annotations

from io import BytesIO
from typing import Any

import pytest

from justflow.resources.aws import (
    MAX_DYNAMODB_TTL_SECONDS,
    DynamoDbConfig,
    DynamoDbResource,
    ParameterSelection,
    ParameterStoreConfig,
    ParameterStoreResource,
    SecretsManagerConfig,
    SecretsManagerResource,
    SecretsManagerSelection,
)
from justflow.resources.base import (
    ResourceOperationError,
    SecretNotFoundError,
)
from justflow.resources.s3 import S3ObjectConfig, S3ObjectOperation, S3ObjectResource


class FakeAwsError(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}
        super().__init__("synthetic AWS error")


class FakeClientFactory:
    def __init__(self, clients: dict[str, object]) -> None:
        self.clients = clients
        self.calls: list[tuple[str, str | None, str | None]] = []

    def client(
        self,
        service_name: str,
        *,
        region_name: str | None,
        endpoint_url: str | None,
    ) -> Any:
        self.calls.append((service_name, region_name, endpoint_url))
        return self.clients[service_name]


class FakeSecretsManagerClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get_secret_value(self, **request: object) -> dict[str, str]:
        self.calls.append(request)
        return {"SecretString": "synthetic-value"}


class FakeParameterStoreClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get_parameter(self, **request: object) -> dict[str, dict[str, str]]:
        self.calls.append(request)
        return {"Parameter": {"Value": "synthetic-value"}}


class FakeDynamoDbClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.item: dict[str, dict[str, str]] | None = None
        self.conditional_conflict = False

    def get_item(self, **request: object) -> dict[str, object]:
        self.calls.append(("get", request))
        return {} if self.item is None else {"Item": self.item}

    def put_item(self, **request: object) -> None:
        self.calls.append(("put", request))
        if self.conditional_conflict:
            raise FakeAwsError("ConditionalCheckFailedException")
        item = request["Item"]
        assert isinstance(item, dict)
        self.item = item

    def delete_item(self, **request: object) -> None:
        self.calls.append(("delete", request))
        self.item = None


class FakeS3Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.body = b"synthetic-object"
        self.closed = False

    def get_object(self, **request: object) -> dict[str, object]:
        self.calls.append(("get", request))
        return {"Body": BytesIO(self.body)}

    def put_object(self, **request: object) -> None:
        self.calls.append(("put", request))

    def delete_object(self, **request: object) -> None:
        self.calls.append(("delete", request))

    def close(self) -> None:
        self.closed = True


async def test_secrets_manager_is_read_only_scoped_and_versioned() -> None:
    client = FakeSecretsManagerClient()
    factory = FakeClientFactory({"secretsmanager": client})
    resource = SecretsManagerResource(
        SecretsManagerConfig(
            region="eu-west-1",
            secrets={
                "database": SecretsManagerSelection(
                    secret_id="configured/database",
                    version_stage="AWSCURRENT",
                )
            },
        ),
        factory,
    )

    await resource.initialize()
    value = await resource.read_secret("database")

    assert value.get_secret_value() == "synthetic-value"
    assert client.calls == [{"SecretId": "configured/database", "VersionStage": "AWSCURRENT"}]
    with pytest.raises(SecretNotFoundError, match="not-configured"):
        await resource.read_secret("not-configured")


async def test_parameter_store_uses_decryption_and_selected_version() -> None:
    client = FakeParameterStoreClient()
    factory = FakeClientFactory({"ssm": client})
    resource = ParameterStoreResource(
        ParameterStoreConfig(
            parameters={"database": ParameterSelection(name="/configured/database", version=3)}
        ),
        factory,
    )

    await resource.initialize()
    value = await resource.read_secret("database")

    assert value.get_secret_value() == "synthetic-value"
    assert client.calls == [{"Name": "/configured/database:3", "WithDecryption": True}]


async def test_dynamodb_enforces_table_prefix_ttl_and_conditional_create() -> None:
    client = FakeDynamoDbClient()
    factory = FakeClientFactory({"dynamodb": client})
    resource = DynamoDbResource(
        DynamoDbConfig(
            table_name="workflow-state",
            partition_key="id",
            ttl_attribute="expires_at",
            key_prefix="tenant-a:",
        ),
        factory,
        clock=lambda: 100.0,
    )
    await resource.initialize()

    assert await resource.put_value(
        "request-1",
        {"status": "accepted"},
        ttl_seconds=60,
        if_absent=True,
    )
    put_request = client.calls[-1][1]
    assert put_request["TableName"] == "workflow-state"
    assert put_request["ConditionExpression"] == "attribute_not_exists(#key)"
    assert client.item is not None
    assert client.item["id"] == {"S": "tenant-a:request-1"}
    assert client.item["expires_at"] == {"N": "160"}
    assert await resource.get_value("request-1") == {"status": "accepted"}

    client.conditional_conflict = True
    assert not await resource.put_value("request-1", {"status": "duplicate"}, if_absent=True)

    with pytest.raises(ResourceOperationError) as exc_info:
        await resource.put_value(
            "request-2",
            {"status": "invalid"},
            ttl_seconds=MAX_DYNAMODB_TTL_SECONDS + 1,
        )
    assert exc_info.value.retryable is False


async def test_s3_object_provider_enforces_prefix_and_allowed_operations() -> None:
    client = FakeS3Client()
    factory = FakeClientFactory({"s3": client})
    resource = S3ObjectResource(
        S3ObjectConfig(
            bucket="workflow-objects",
            prefix="tenant-a",
            allowed_operations=frozenset({S3ObjectOperation.READ, S3ObjectOperation.WRITE}),
        ),
        factory,
    )
    await resource.initialize()

    await resource.write_object("request-1.json", b"synthetic-object")
    assert await resource.read_object("request-1.json") == b"synthetic-object"
    assert client.calls[0] == (
        "put",
        {
            "Bucket": "workflow-objects",
            "Key": "tenant-a/request-1.json",
            "Body": b"synthetic-object",
            "ServerSideEncryption": "AES256",
        },
    )
    with pytest.raises(ResourceOperationError) as exc_info:
        await resource.delete_object("request-1.json")
    assert exc_info.value.retryable is False

    await resource.close()
    assert client.closed is True
