"""S3 resource - archival/storage backend bound to one bucket."""

from __future__ import annotations

import asyncio
import ipaddress
import re
from enum import Enum
from typing import Any, Self
from urllib.parse import urlencode

from pydantic import Field, model_validator

from justflow.bounded_io import BoundedReadError, read_bounded_body
from justflow.resources.aws import DEFAULT_AWS_CLIENT_FACTORY, AwsResourceConfig
from justflow.resources.base import (
    AwsClientFactory,
    ResourceCapability,
    ResourceOperationError,
)
from justflow.resources.registry import ResourceProvider

S3_BUCKET_MIN_LENGTH = 3
S3_BUCKET_MAX_LENGTH = 63
S3_KEY_MAX_UTF8_BYTES = 1024
S3_BUCKET_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$")
S3_RESERVED_PREFIXES = ("xn--", "sthree-", "amzn_s3_demo_")
S3_RESERVED_SUFFIXES = ("-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3")
S3_RETENTION_TAG_KEY = "justflow-retention"
S3_TAG_VALUE_MAX_UTF8_BYTES = 256
S3_PREFIX_MAX_UTF8_BYTES = 768
DEFAULT_S3_OBJECT_BYTES = 10 * 1024 * 1024
MAX_S3_OBJECT_BYTES = 100 * 1024 * 1024


class S3ConfigurationError(ValueError):
    pass


class S3KeyError(ValueError):
    pass


class S3RetentionPolicyError(ValueError):
    pass


class S3Resource:
    """Thin S3 wrapper exposing the engine's archival ``write`` protocol.

    The boto3 client is created lazily so config validation and tests never
    need AWS credentials; pass ``client`` explicitly to inject a fake.
    """

    def __init__(
        self,
        bucket: str,
        retention_policies: dict[str, str],
        prefix: str = "",
        region: str | None = None,
        endpoint_url: str | None = None,
        expected_bucket_owner: str | None = None,
        server_side_encryption: str | None = None,
        kms_key_id: str | None = None,
        client: Any | None = None,
        client_factory: AwsClientFactory = DEFAULT_AWS_CLIENT_FACTORY,
    ):
        _validate_bucket(bucket)
        if not retention_policies:
            raise S3ConfigurationError("S3Resource requires at least one retention policy tag")
        if any(not policy for policy in retention_policies):
            raise S3ConfigurationError("S3 retention policy names must not be empty")
        for tag_value in retention_policies.values():
            _validate_retention_tag(tag_value)
        self.bucket = bucket
        self.prefix = _normalize_prefix(prefix)
        self._retention_policies = dict(retention_policies)
        self._region = region
        self._endpoint_url = endpoint_url
        self._expected_bucket_owner = expected_bucket_owner
        self._server_side_encryption = server_side_encryption
        self._kms_key_id = kms_key_id
        self._client = client
        self._client_factory = client_factory

    def _get_client(self) -> Any:
        if self._client is None:
            self._client = self._client_factory.client(
                "s3",
                region_name=self._region,
                endpoint_url=self._endpoint_url,
            )
        return self._client

    async def initialize(self) -> None:
        self._get_client()

    async def close(self) -> None:
        client = self._client
        self._client = None
        close = getattr(client, "close", None)
        if callable(close):
            await asyncio.to_thread(close)

    async def write(self, path: str, data: str, *, retention_policy: str) -> None:
        _validate_key(path)
        try:
            retention_tag = self._retention_policies[retention_policy]
        except KeyError as exc:
            raise S3RetentionPolicyError(
                f"Unknown S3 retention policy '{retention_policy}'"
            ) from exc
        request = self._request(
            Key=_scoped_key(self.prefix, path),
            Body=data.encode("utf-8"),
            Tagging=urlencode({S3_RETENTION_TAG_KEY: retention_tag}),
        )
        try:
            await asyncio.to_thread(self._get_client().put_object, **request)
        except Exception as exc:
            raise ResourceOperationError("S3 archive write failed", retryable=True) from exc

    async def delete(self, path: str) -> None:
        _validate_key(path)
        try:
            await asyncio.to_thread(
                self._get_client().delete_object,
                **self._request(Key=_scoped_key(self.prefix, path), include_encryption=False),
            )
        except Exception as exc:
            raise ResourceOperationError("S3 archive delete failed", retryable=True) from exc

    def _request(self, *, include_encryption: bool = True, **values: object) -> dict[str, object]:
        request: dict[str, object] = {"Bucket": self.bucket, **values}
        if self._expected_bucket_owner is not None:
            request["ExpectedBucketOwner"] = self._expected_bucket_owner
        if include_encryption and self._server_side_encryption is not None:
            request["ServerSideEncryption"] = self._server_side_encryption
        if include_encryption and self._kms_key_id is not None:
            request["SSEKMSKeyId"] = self._kms_key_id
        return request


class S3ObjectOperation(str, Enum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"


class S3ArchiveConfig(AwsResourceConfig):
    bucket: str = Field(min_length=S3_BUCKET_MIN_LENGTH, max_length=S3_BUCKET_MAX_LENGTH)
    prefix: str = Field(default="", max_length=S3_PREFIX_MAX_UTF8_BYTES)
    retention_policies: dict[str, str] = Field(min_length=1)
    expected_bucket_owner: str | None = Field(default=None, pattern=r"^[0-9]{12}$")
    server_side_encryption: str = Field(default="AES256", pattern=r"^(AES256|aws:kms)$")
    kms_key_id: str | None = Field(default=None, min_length=1, max_length=2048)

    @model_validator(mode="after")
    def validate_s3_archive(self) -> Self:
        _validate_bucket(self.bucket)
        _normalize_prefix(self.prefix)
        for policy, tag in self.retention_policies.items():
            if not policy:
                raise ValueError("retention policy names must not be empty")
            _validate_retention_tag(tag)
        if self.server_side_encryption == "aws:kms" and self.kms_key_id is None:
            raise ValueError("aws:kms encryption requires kms_key_id")
        if self.server_side_encryption != "aws:kms" and self.kms_key_id is not None:
            raise ValueError("kms_key_id requires aws:kms encryption")
        return self


class S3ObjectConfig(AwsResourceConfig):
    bucket: str = Field(min_length=S3_BUCKET_MIN_LENGTH, max_length=S3_BUCKET_MAX_LENGTH)
    prefix: str = Field(min_length=1, max_length=S3_PREFIX_MAX_UTF8_BYTES)
    allowed_operations: frozenset[S3ObjectOperation] = Field(min_length=1)
    max_object_bytes: int = Field(
        default=DEFAULT_S3_OBJECT_BYTES,
        ge=1,
        le=MAX_S3_OBJECT_BYTES,
    )
    expected_bucket_owner: str | None = Field(default=None, pattern=r"^[0-9]{12}$")
    server_side_encryption: str = Field(default="AES256", pattern=r"^(AES256|aws:kms)$")
    kms_key_id: str | None = Field(default=None, min_length=1, max_length=2048)

    @model_validator(mode="after")
    def validate_s3_object(self) -> Self:
        _validate_bucket(self.bucket)
        _normalize_prefix(self.prefix)
        if self.server_side_encryption == "aws:kms" and self.kms_key_id is None:
            raise ValueError("aws:kms encryption requires kms_key_id")
        if self.server_side_encryption != "aws:kms" and self.kms_key_id is not None:
            raise ValueError("kms_key_id requires aws:kms encryption")
        return self


class S3ObjectResource:
    def __init__(
        self,
        config: S3ObjectConfig,
        client_factory: AwsClientFactory,
    ) -> None:
        self._config = config
        self._prefix = _normalize_prefix(config.prefix)
        self._client_factory = client_factory
        self._client: Any | None = None

    async def initialize(self) -> None:
        self._client = self._client_factory.client(
            "s3",
            region_name=self._config.region,
            endpoint_url=self._config.endpoint_url,
        )

    async def close(self) -> None:
        client = self._client
        self._client = None
        close = getattr(client, "close", None)
        if callable(close):
            await asyncio.to_thread(close)

    async def read_object(self, key: str) -> bytes | None:
        self._require_operation(S3ObjectOperation.READ)
        return await asyncio.to_thread(self._read_object, key)

    def _read_object(self, key: str) -> bytes | None:
        try:
            response = self._require_client().get_object(
                **self._request(Key=_scoped_key(self._prefix, key), include_encryption=False),
            )
            return read_bounded_body(response.get("Body"), limit=self._config.max_object_bytes)
        except BoundedReadError as exc:
            raise ResourceOperationError(
                "S3 object body is invalid or exceeds its byte limit", retryable=False
            ) from exc
        except Exception as exc:
            if _aws_error_code(exc) in {"NoSuchKey", "NotFound"}:
                return None
            raise ResourceOperationError("S3 object read failed", retryable=True) from exc

    async def write_object(self, key: str, data: bytes) -> None:
        self._require_operation(S3ObjectOperation.WRITE)
        self._enforce_object_size(data)
        try:
            await asyncio.to_thread(
                self._require_client().put_object,
                **self._request(Key=_scoped_key(self._prefix, key), Body=data),
            )
        except Exception as exc:
            raise ResourceOperationError("S3 object write failed", retryable=True) from exc

    async def delete_object(self, key: str) -> None:
        self._require_operation(S3ObjectOperation.DELETE)
        try:
            await asyncio.to_thread(
                self._require_client().delete_object,
                **self._request(Key=_scoped_key(self._prefix, key), include_encryption=False),
            )
        except Exception as exc:
            raise ResourceOperationError("S3 object delete failed", retryable=True) from exc

    def _require_operation(self, operation: S3ObjectOperation) -> None:
        if operation not in self._config.allowed_operations:
            raise ResourceOperationError(
                f"S3 object operation '{operation.value}' is outside the configured authority",
                retryable=False,
            )

    def _require_client(self) -> Any:
        if self._client is None:
            raise ResourceOperationError("S3 object resource is not initialized", retryable=False)
        return self._client

    def _request(self, *, include_encryption: bool = True, **values: object) -> dict[str, object]:
        request: dict[str, object] = {"Bucket": self._config.bucket, **values}
        if self._config.expected_bucket_owner is not None:
            request["ExpectedBucketOwner"] = self._config.expected_bucket_owner
        if include_encryption:
            request["ServerSideEncryption"] = self._config.server_side_encryption
            if self._config.kms_key_id is not None:
                request["SSEKMSKeyId"] = self._config.kms_key_id
        return request

    def _enforce_object_size(self, data: bytes) -> None:
        if len(data) > self._config.max_object_bytes:
            raise ResourceOperationError(
                f"S3 object exceeds the configured {self._config.max_object_bytes}-byte limit",
                retryable=False,
            )


def s3_resource_providers() -> tuple[ResourceProvider[Any], ...]:
    return (
        ResourceProvider(
            name="s3",
            contract_version="1",
            config_model=S3ArchiveConfig,
            capabilities=frozenset({ResourceCapability.ARCHIVE}),
            factory=lambda config, context: S3Resource(
                bucket=config.bucket,
                prefix=config.prefix,
                retention_policies=config.retention_policies,
                region=config.region,
                endpoint_url=config.endpoint_url,
                expected_bucket_owner=config.expected_bucket_owner,
                server_side_encryption=config.server_side_encryption,
                kms_key_id=config.kms_key_id,
                client_factory=context.aws_client_factory or DEFAULT_AWS_CLIENT_FACTORY,
            ),
        ),
        ResourceProvider(
            name="s3_object",
            contract_version="1",
            config_model=S3ObjectConfig,
            capabilities=frozenset({ResourceCapability.OBJECT}),
            factory=lambda config, context: S3ObjectResource(
                config,
                context.aws_client_factory or DEFAULT_AWS_CLIENT_FACTORY,
            ),
        ),
    )


def _validate_bucket(bucket: str) -> None:
    if not S3_BUCKET_MIN_LENGTH <= len(bucket) <= S3_BUCKET_MAX_LENGTH:
        raise S3ConfigurationError(
            f"S3 bucket length must be {S3_BUCKET_MIN_LENGTH}-{S3_BUCKET_MAX_LENGTH} characters"
        )
    if S3_BUCKET_PATTERN.fullmatch(bucket) is None:
        raise S3ConfigurationError("S3 bucket contains unsupported characters or delimiters")
    if ".." in bucket or ".-" in bucket or "-." in bucket:
        raise S3ConfigurationError("S3 bucket contains adjacent invalid delimiters")
    if bucket.startswith(S3_RESERVED_PREFIXES) or bucket.endswith(S3_RESERVED_SUFFIXES):
        raise S3ConfigurationError("S3 bucket uses an AWS-reserved prefix or suffix")
    try:
        ipaddress.ip_address(bucket)
    except ValueError:
        return
    raise S3ConfigurationError("S3 bucket must not be formatted as an IP address")


def _validate_key(key: str) -> None:
    if not key:
        raise S3KeyError("S3 object key must not be empty")
    try:
        key_bytes = key.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise S3KeyError("S3 object key must contain valid UTF-8 text") from exc
    if len(key_bytes) > S3_KEY_MAX_UTF8_BYTES:
        raise S3KeyError(f"S3 object key must not exceed {S3_KEY_MAX_UTF8_BYTES} UTF-8 bytes")


def _normalize_prefix(prefix: str) -> str:
    if not prefix:
        return ""
    if prefix.startswith("/") or "\x00" in prefix:
        raise S3ConfigurationError("S3 prefix must be relative and contain no null bytes")
    if len(prefix.encode("utf-8")) > S3_PREFIX_MAX_UTF8_BYTES:
        raise S3ConfigurationError(
            f"S3 prefix must not exceed {S3_PREFIX_MAX_UTF8_BYTES} UTF-8 bytes"
        )
    return prefix if prefix.endswith("/") else f"{prefix}/"


def _scoped_key(prefix: str, key: str) -> str:
    if key.startswith("/"):
        raise S3KeyError("S3 object key must be relative to the configured prefix")
    scoped = f"{prefix}{key}"
    _validate_key(scoped)
    return scoped


def _aws_error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return None
    details = response.get("Error")
    if not isinstance(details, dict):
        return None
    code = details.get("Code")
    return code if isinstance(code, str) else None


def _validate_retention_tag(value: str) -> None:
    if not isinstance(value, str) or not value:
        raise S3ConfigurationError("S3 retention policy tag values must not be empty")
    if len(value.encode("utf-8")) > S3_TAG_VALUE_MAX_UTF8_BYTES:
        raise S3ConfigurationError(
            f"S3 retention policy tag values must not exceed "
            f"{S3_TAG_VALUE_MAX_UTF8_BYTES} UTF-8 bytes"
        )
