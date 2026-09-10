"""Scoped AWS resource providers using the normal SDK credential chain."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any, Self
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, model_validator

from justflow.config.grammar import Identifier
from justflow.engine.serialization import (
    StrictJsonError,
    StrictJsonLayout,
    dumps_strict_json,
    loads_strict_json,
)
from justflow.optional_dependencies import load_optional_dependency
from justflow.resources.base import (
    AwsClientFactory,
    ResourceCapability,
    ResourceFactoryContext,
    ResourceOperationError,
    SecretNotFoundError,
    StrictResourceConfig,
)
from justflow.resources.registry import ResourceProvider

MAX_AWS_REGION_LENGTH = 64
MAX_AWS_ENDPOINT_LENGTH = 2048
MAX_AWS_RESOURCE_ID_LENGTH = 2048
MAX_SECRET_BINDINGS = 256
DEFAULT_SECRET_VALUE_BYTES = 64 * 1024
MAX_SECRET_VALUE_BYTES = 1024 * 1024
DEFAULT_DYNAMODB_VALUE_BYTES = 400 * 1024
MAX_DYNAMODB_KEY_BYTES = 2048
MAX_DYNAMODB_NAME_LENGTH = 255
MAX_DYNAMODB_TTL_SECONDS = 10 * 365 * 24 * 60 * 60
AWS_TRANSIENT_ERROR_CODES = frozenset(
    {
        "InternalFailure",
        "InternalServerError",
        "ProvisionedThroughputExceededException",
        "RequestLimitExceeded",
        "ServiceUnavailable",
        "Throttling",
        "ThrottlingException",
    }
)


class Boto3ClientFactory:
    def __init__(self) -> None:
        self._session: Any | None = None

    def client(
        self,
        service_name: str,
        *,
        region_name: str | None,
        endpoint_url: str | None,
    ) -> Any:
        if self._session is None:
            boto3 = load_optional_dependency("boto3", extra="aws", feature="AWS resources")
            self._session = boto3.session.Session()
        return self._session.client(
            service_name,
            region_name=region_name,
            endpoint_url=endpoint_url,
        )


DEFAULT_AWS_CLIENT_FACTORY = Boto3ClientFactory()


class AwsResourceConfig(StrictResourceConfig):
    region: str | None = Field(default=None, min_length=1, max_length=MAX_AWS_REGION_LENGTH)
    endpoint_url: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_AWS_ENDPOINT_LENGTH,
    )

    @model_validator(mode="after")
    def validate_endpoint(self) -> Self:
        if self.endpoint_url is None:
            return self
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
                "endpoint_url must be an HTTP(S) origin without credentials, query, or fragment"
            )
        return self


class SecretsManagerSelection(StrictResourceConfig):
    secret_id: str = Field(min_length=1, max_length=MAX_AWS_RESOURCE_ID_LENGTH)
    version_id: str | None = Field(
        default=None, min_length=1, max_length=MAX_AWS_RESOURCE_ID_LENGTH
    )
    version_stage: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_AWS_RESOURCE_ID_LENGTH,
    )

    @model_validator(mode="after")
    def validate_version_selection(self) -> Self:
        if self.version_id is not None and self.version_stage is not None:
            raise ValueError("select at most one of version_id or version_stage")
        return self


class SecretsManagerConfig(AwsResourceConfig):
    secrets: dict[Identifier, SecretsManagerSelection] = Field(
        min_length=1,
        max_length=MAX_SECRET_BINDINGS,
    )
    max_value_bytes: int = Field(
        default=DEFAULT_SECRET_VALUE_BYTES,
        ge=1,
        le=MAX_SECRET_VALUE_BYTES,
    )


class ParameterSelection(StrictResourceConfig):
    name: str = Field(min_length=1, max_length=MAX_AWS_RESOURCE_ID_LENGTH)
    version: int | None = Field(default=None, ge=1)


class ParameterStoreConfig(AwsResourceConfig):
    parameters: dict[Identifier, ParameterSelection] = Field(
        min_length=1,
        max_length=MAX_SECRET_BINDINGS,
    )
    max_value_bytes: int = Field(
        default=DEFAULT_SECRET_VALUE_BYTES,
        ge=1,
        le=MAX_SECRET_VALUE_BYTES,
    )


class DynamoDbConfig(AwsResourceConfig):
    table_name: str = Field(min_length=1, max_length=MAX_DYNAMODB_NAME_LENGTH)
    partition_key: str = Field(min_length=1, max_length=MAX_DYNAMODB_NAME_LENGTH)
    value_attribute: str = Field(default="value", min_length=1, max_length=MAX_DYNAMODB_NAME_LENGTH)
    ttl_attribute: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_DYNAMODB_NAME_LENGTH,
    )
    key_prefix: str = Field(default="", max_length=MAX_DYNAMODB_KEY_BYTES)
    max_value_bytes: int = Field(
        default=DEFAULT_DYNAMODB_VALUE_BYTES,
        ge=1,
        le=DEFAULT_DYNAMODB_VALUE_BYTES,
    )

    @model_validator(mode="after")
    def validate_attribute_names(self) -> Self:
        names = [self.partition_key, self.value_attribute]
        if self.ttl_attribute is not None:
            names.append(self.ttl_attribute)
        if len(names) != len(set(names)):
            raise ValueError("DynamoDB key, value, and TTL attributes must be distinct")
        return self


class SecretsManagerResource:
    def __init__(
        self,
        config: SecretsManagerConfig,
        client_factory: AwsClientFactory,
    ) -> None:
        self._config = config
        self._client_factory = client_factory
        self._client: Any | None = None

    async def initialize(self) -> None:
        self._client = self._client_factory.client(
            "secretsmanager",
            region_name=self._config.region,
            endpoint_url=self._config.endpoint_url,
        )

    async def close(self) -> None:
        client = self._client
        self._client = None
        await _close_aws_client(client)

    async def read_secret(self, name: str) -> SecretStr:
        try:
            selection = self._config.secrets[name]
        except KeyError as exc:
            raise SecretNotFoundError(name) from exc
        request: dict[str, object] = {"SecretId": selection.secret_id}
        if selection.version_id is not None:
            request["VersionId"] = selection.version_id
        if selection.version_stage is not None:
            request["VersionStage"] = selection.version_stage
        try:
            response = await asyncio.to_thread(self._require_client().get_secret_value, **request)
        except Exception as exc:
            if _aws_error_code(exc) == "ResourceNotFoundException":
                raise SecretNotFoundError(name) from exc
            raise ResourceOperationError(
                f"Secrets Manager read failed for configured secret '{name}'",
                retryable=_is_retryable_aws_error(exc),
            ) from exc
        raw_value = response.get("SecretString")
        if raw_value is None:
            raw_value = response.get("SecretBinary")
        value = _secret_text(raw_value, name=name)
        _enforce_value_bytes(value, self._config.max_value_bytes, kind="secret")
        return SecretStr(value)

    def _require_client(self) -> Any:
        if self._client is None:
            raise ResourceOperationError(
                "Secrets Manager resource is not initialized", retryable=False
            )
        return self._client


class ParameterStoreResource:
    def __init__(
        self,
        config: ParameterStoreConfig,
        client_factory: AwsClientFactory,
    ) -> None:
        self._config = config
        self._client_factory = client_factory
        self._client: Any | None = None

    async def initialize(self) -> None:
        self._client = self._client_factory.client(
            "ssm",
            region_name=self._config.region,
            endpoint_url=self._config.endpoint_url,
        )

    async def close(self) -> None:
        client = self._client
        self._client = None
        await _close_aws_client(client)

    async def read_secret(self, name: str) -> SecretStr:
        try:
            selection = self._config.parameters[name]
        except KeyError as exc:
            raise SecretNotFoundError(name) from exc
        parameter_name = (
            f"{selection.name}:{selection.version}"
            if selection.version is not None
            else selection.name
        )
        try:
            response = await asyncio.to_thread(
                self._require_client().get_parameter,
                Name=parameter_name,
                WithDecryption=True,
            )
        except Exception as exc:
            if _aws_error_code(exc) == "ParameterNotFound":
                raise SecretNotFoundError(name) from exc
            raise ResourceOperationError(
                f"Parameter Store read failed for configured parameter '{name}'",
                retryable=_is_retryable_aws_error(exc),
            ) from exc
        parameter = response.get("Parameter")
        raw_value = parameter.get("Value") if isinstance(parameter, dict) else None
        value = _secret_text(raw_value, name=name)
        _enforce_value_bytes(value, self._config.max_value_bytes, kind="parameter")
        return SecretStr(value)

    def _require_client(self) -> Any:
        if self._client is None:
            raise ResourceOperationError(
                "Parameter Store resource is not initialized", retryable=False
            )
        return self._client


class DynamoDbResource:
    def __init__(
        self,
        config: DynamoDbConfig,
        client_factory: AwsClientFactory,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._config = config
        self._client_factory = client_factory
        self._clock = clock
        self._client: Any | None = None

    async def initialize(self) -> None:
        self._client = self._client_factory.client(
            "dynamodb",
            region_name=self._config.region,
            endpoint_url=self._config.endpoint_url,
        )

    async def close(self) -> None:
        client = self._client
        self._client = None
        await _close_aws_client(client)

    async def get_value(self, key: str) -> Any | None:
        scoped_key = self._key(key)
        try:
            response = await asyncio.to_thread(
                self._require_client().get_item,
                TableName=self._config.table_name,
                Key={self._config.partition_key: {"S": scoped_key}},
                ConsistentRead=True,
            )
        except Exception as exc:
            raise self._operation_error("read", exc) from exc
        item = response.get("Item")
        if not isinstance(item, dict):
            return None
        encoded = item.get(self._config.value_attribute)
        value = encoded.get("S") if isinstance(encoded, dict) else None
        if not isinstance(value, str):
            raise ResourceOperationError(
                "DynamoDB value has an invalid storage representation",
                retryable=False,
            )
        _enforce_value_bytes(value, self._config.max_value_bytes, kind="DynamoDB value")
        try:
            return loads_strict_json(value)
        except StrictJsonError as exc:
            raise ResourceOperationError(
                "DynamoDB value failed strict JSON validation",
                retryable=False,
            ) from exc

    async def put_value(
        self,
        key: str,
        value: Any,
        *,
        ttl_seconds: int | None = None,
        if_absent: bool = False,
    ) -> bool:
        scoped_key = self._key(key)
        try:
            encoded = dumps_strict_json(value, layout=StrictJsonLayout.CANONICAL)
        except StrictJsonError as exc:
            raise ResourceOperationError(
                "DynamoDB value must be strict JSON",
                retryable=False,
            ) from exc
        _enforce_value_bytes(encoded, self._config.max_value_bytes, kind="DynamoDB value")
        item: dict[str, dict[str, str]] = {
            self._config.partition_key: {"S": scoped_key},
            self._config.value_attribute: {"S": encoded},
        }
        if ttl_seconds is not None:
            if (
                not 1 <= ttl_seconds <= MAX_DYNAMODB_TTL_SECONDS
                or self._config.ttl_attribute is None
            ):
                raise ResourceOperationError(
                    "DynamoDB TTL requires a bounded positive duration and configured "
                    "TTL attribute",
                    retryable=False,
                )
            item[self._config.ttl_attribute] = {"N": str(int(self._clock()) + ttl_seconds)}
        request: dict[str, object] = {
            "TableName": self._config.table_name,
            "Item": item,
        }
        if if_absent:
            request.update(
                {
                    "ConditionExpression": "attribute_not_exists(#key)",
                    "ExpressionAttributeNames": {"#key": self._config.partition_key},
                }
            )
        try:
            await asyncio.to_thread(self._require_client().put_item, **request)
        except Exception as exc:
            if if_absent and _aws_error_code(exc) == "ConditionalCheckFailedException":
                return False
            raise self._operation_error("write", exc) from exc
        return True

    async def delete_value(self, key: str) -> None:
        try:
            await asyncio.to_thread(
                self._require_client().delete_item,
                TableName=self._config.table_name,
                Key={self._config.partition_key: {"S": self._key(key)}},
            )
        except Exception as exc:
            raise self._operation_error("delete", exc) from exc

    def _key(self, key: str) -> str:
        if not key:
            raise ResourceOperationError("DynamoDB key must not be empty", retryable=False)
        scoped = f"{self._config.key_prefix}{key}"
        if len(scoped.encode("utf-8")) > MAX_DYNAMODB_KEY_BYTES:
            raise ResourceOperationError(
                f"DynamoDB key exceeds {MAX_DYNAMODB_KEY_BYTES} UTF-8 bytes",
                retryable=False,
            )
        return scoped

    def _require_client(self) -> Any:
        if self._client is None:
            raise ResourceOperationError("DynamoDB resource is not initialized", retryable=False)
        return self._client

    @staticmethod
    def _operation_error(operation: str, error: Exception) -> ResourceOperationError:
        return ResourceOperationError(
            f"DynamoDB {operation} operation failed",
            retryable=_is_retryable_aws_error(error),
        )


def aws_resource_providers() -> tuple[ResourceProvider[Any], ...]:
    return (
        ResourceProvider(
            name="aws_secrets_manager",
            contract_version="1",
            config_model=SecretsManagerConfig,
            capabilities=frozenset({ResourceCapability.SECRET_READER}),
            factory=lambda config, context: SecretsManagerResource(
                config,
                _aws_factory(context),
            ),
            secret_alias_resolver=lambda config: frozenset(config.secrets),
        ),
        ResourceProvider(
            name="aws_ssm",
            contract_version="1",
            config_model=ParameterStoreConfig,
            capabilities=frozenset({ResourceCapability.SECRET_READER}),
            factory=lambda config, context: ParameterStoreResource(
                config,
                _aws_factory(context),
            ),
            secret_alias_resolver=lambda config: frozenset(config.parameters),
        ),
        ResourceProvider(
            name="dynamodb",
            contract_version="1",
            config_model=DynamoDbConfig,
            capabilities=frozenset({ResourceCapability.KEY_VALUE}),
            factory=lambda config, context: DynamoDbResource(
                config,
                _aws_factory(context),
            ),
        ),
    )


def _aws_factory(context: ResourceFactoryContext) -> AwsClientFactory:
    return context.aws_client_factory or DEFAULT_AWS_CLIENT_FACTORY


def _aws_error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return None
    error_data = response.get("Error")
    if not isinstance(error_data, dict):
        return None
    code = error_data.get("Code")
    return code if isinstance(code, str) else None


def _is_retryable_aws_error(error: Exception) -> bool:
    code = _aws_error_code(error)
    return code in AWS_TRANSIENT_ERROR_CODES if code is not None else True


def _secret_text(value: object, *, name: str) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ResourceOperationError(
                f"Configured secret '{name}' is not UTF-8 text",
                retryable=False,
            ) from exc
    raise ResourceOperationError(
        f"Configured secret '{name}' has no readable value",
        retryable=False,
    )


def _enforce_value_bytes(value: str, limit: int, *, kind: str) -> None:
    if len(value.encode("utf-8")) > limit:
        raise ResourceOperationError(
            f"{kind} exceeds the configured {limit}-byte limit",
            retryable=False,
        )


async def _close_aws_client(client: object) -> None:
    close = getattr(client, "close", None)
    if callable(close):
        await asyncio.to_thread(close)
