"""S3 storage backend for immutable definition catalogs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from justflow.bounded_io import read_bounded_body
from justflow.catalog_validation import (
    normalize_s3_catalog_prefix,
    validate_s3_bucket,
)
from justflow.definitions.catalog import (
    CatalogConflictError,
    CatalogStorageError,
    StoredCatalogObject,
)
from justflow.optional_dependencies import load_optional_dependency
from justflow.provenance import CatalogBackendIdentity, provenance_digest

S3_OBJECT_KEY_MAX_UTF8_BYTES = 1024
MAX_S3_CATALOG_OBJECT_BYTES = 64 * 1024 * 1024
CONDITIONAL_WRITE_ERROR_CODES = frozenset(
    {"409", "412", "ConditionalRequestConflict", "PreconditionFailed"}
)
NOT_FOUND_ERROR_CODES = frozenset({"404", "NoSuchKey", "NotFound"})
JSON_CONTENT_TYPE = "application/json"


class S3CatalogClient(Protocol):
    def get_object(self, **kwargs: object) -> Mapping[str, Any]: ...

    def list_objects_v2(self, **kwargs: object) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: object) -> Mapping[str, Any]: ...


class S3CatalogBackend:
    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        region: str | None = None,
        endpoint_url: str | None = None,
        expected_bucket_owner: str | None = None,
        server_side_encryption: str = "AES256",
        kms_key_id: str | None = None,
        client: S3CatalogClient | None = None,
    ) -> None:
        validate_s3_bucket(bucket)
        normalized_prefix = normalize_s3_catalog_prefix(prefix)
        if server_side_encryption not in {"AES256", "aws:kms"}:
            raise ValueError(f"Unsupported S3 catalog encryption '{server_side_encryption}'")
        if server_side_encryption == "aws:kms" and kms_key_id is None:
            raise ValueError("S3 catalog aws:kms encryption requires a KMS key id")
        if server_side_encryption != "aws:kms" and kms_key_id is not None:
            raise ValueError("S3 catalog KMS key id requires aws:kms encryption")
        self.bucket = bucket
        self.prefix = normalized_prefix
        self.region = region
        self.endpoint_url = endpoint_url
        self.expected_bucket_owner = expected_bucket_owner
        self.server_side_encryption = server_side_encryption
        self.kms_key_id = kms_key_id
        self._client = client
        self._identity = CatalogBackendIdentity(
            provider="s3",
            configuration_digest=provenance_digest(
                {
                    "bucket": bucket,
                    "endpoint_url": endpoint_url,
                    "expected_bucket_owner": expected_bucket_owner,
                    "kms_key_id": kms_key_id,
                    "prefix": normalized_prefix,
                    "region": region,
                    "server_side_encryption": server_side_encryption,
                }
            ),
        )

    @property
    def identity(self) -> CatalogBackendIdentity:
        return self._identity

    def read_object(self, key: str) -> StoredCatalogObject | None:
        object_key = self._object_key(key)
        try:
            response = self._get_client().get_object(**self._request_key(object_key))
        except Exception as exc:
            if _aws_error_code(exc) in NOT_FOUND_ERROR_CODES:
                return None
            raise CatalogStorageError(f"Cannot read S3 catalog object '{object_key}'") from exc
        payload = _read_response_body(response, object_key=object_key)
        etag = response.get("ETag")
        if not isinstance(etag, str) or not etag:
            raise CatalogStorageError(f"S3 catalog object '{object_key}' has no ETag")
        return StoredCatalogObject(key=key, payload=payload, version=etag)

    def list_objects(self, prefix: str) -> Sequence[StoredCatalogObject]:
        object_prefix = self._object_key(prefix, allow_trailing_separator=True)
        continuation_token: str | None = None
        keys: list[str] = []
        while True:
            request: dict[str, object] = {
                "Bucket": self.bucket,
                "Prefix": object_prefix,
            }
            if self.expected_bucket_owner is not None:
                request["ExpectedBucketOwner"] = self.expected_bucket_owner
            if continuation_token is not None:
                request["ContinuationToken"] = continuation_token
            try:
                response = self._get_client().list_objects_v2(**request)
            except Exception as exc:
                raise CatalogStorageError(
                    f"Cannot list S3 catalog objects below '{object_prefix}'"
                ) from exc
            keys.extend(
                _listed_keys(
                    response,
                    object_prefix=object_prefix,
                    catalog_prefix=self.prefix,
                )
            )
            if response.get("IsTruncated") is not True:
                break
            continuation_token = response.get("NextContinuationToken")
            if not isinstance(continuation_token, str) or not continuation_token:
                raise CatalogStorageError(
                    f"S3 catalog listing below '{object_prefix}' omitted its continuation token"
                )
        objects: list[StoredCatalogObject] = []
        for key in sorted(keys):
            stored = self.read_object(key)
            if stored is None:
                raise CatalogStorageError(f"S3 catalog object '{key}' disappeared while loading")
            objects.append(stored)
        return tuple(objects)

    def create_immutable(self, key: str, payload: bytes) -> StoredCatalogObject:
        return self._conditional_write(key, payload, condition={"IfNoneMatch": "*"})

    def compare_and_swap(
        self,
        key: str,
        payload: bytes,
        *,
        expected_version: str | None,
    ) -> StoredCatalogObject:
        current = self.read_object(key)
        if current is not None and current.payload == payload:
            return current
        condition = (
            {"IfNoneMatch": "*"} if expected_version is None else {"IfMatch": expected_version}
        )
        return self._conditional_write(key, payload, condition=condition)

    def _conditional_write(
        self,
        key: str,
        payload: bytes,
        *,
        condition: Mapping[str, str],
    ) -> StoredCatalogObject:
        object_key = self._object_key(key)
        request = {
            **self._request_key(object_key),
            **self._encryption_request(),
            **condition,
            "Body": payload,
            "ContentType": JSON_CONTENT_TYPE,
        }
        try:
            self._get_client().put_object(**request)
        except Exception as exc:
            if _aws_error_code(exc) not in CONDITIONAL_WRITE_ERROR_CODES:
                raise CatalogStorageError(f"Cannot write S3 catalog object '{object_key}'") from exc
            current = self.read_object(key)
            if current is not None and current.payload == payload:
                return current
            raise CatalogConflictError(
                f"S3 catalog object '{object_key}' changed concurrently"
            ) from exc
        stored = self.read_object(key)
        if stored is None or stored.payload != payload:
            raise CatalogStorageError(
                f"S3 catalog object '{object_key}' could not be verified after writing"
            )
        return stored

    def _request_key(self, object_key: str) -> dict[str, object]:
        request: dict[str, object] = {"Bucket": self.bucket, "Key": object_key}
        if self.expected_bucket_owner is not None:
            request["ExpectedBucketOwner"] = self.expected_bucket_owner
        return request

    def _encryption_request(self) -> dict[str, object]:
        request: dict[str, object] = {
            "ServerSideEncryption": self.server_side_encryption,
        }
        if self.kms_key_id is not None:
            request["SSEKMSKeyId"] = self.kms_key_id
        return request

    def _object_key(self, key: str, *, allow_trailing_separator: bool = False) -> str:
        normalized = key[:-1] if allow_trailing_separator and key.endswith("/") else key
        if (
            not normalized
            or normalized.startswith("/")
            or "\\" in normalized
            or "\0" in normalized
            or any(segment in {"", ".", ".."} for segment in normalized.split("/"))
        ):
            raise CatalogStorageError(f"Invalid S3 catalog object key '{key}'")
        object_key = f"{self.prefix}{normalized}"
        if allow_trailing_separator and key.endswith("/"):
            object_key = f"{object_key}/"
        if len(object_key.encode("utf-8")) > S3_OBJECT_KEY_MAX_UTF8_BYTES:
            raise CatalogStorageError(
                f"S3 catalog object key exceeds {S3_OBJECT_KEY_MAX_UTF8_BYTES} UTF-8 bytes"
            )
        return object_key

    def _get_client(self) -> S3CatalogClient:
        if self._client is None:
            boto3 = load_optional_dependency(
                "boto3",
                extra="aws",
                feature="the S3 definition catalog",
            )
            self._client = boto3.client(
                "s3",
                region_name=self.region,
                endpoint_url=self.endpoint_url,
            )
        return self._client


def _read_response_body(response: Mapping[str, Any], *, object_key: str) -> bytes:
    try:
        return read_bounded_body(response.get("Body"), limit=MAX_S3_CATALOG_OBJECT_BYTES)
    except Exception as exc:
        raise CatalogStorageError(
            f"Cannot read bounded S3 catalog object body '{object_key}'"
        ) from exc


def _listed_keys(
    response: Mapping[str, Any],
    *,
    object_prefix: str,
    catalog_prefix: str,
) -> tuple[str, ...]:
    contents = response.get("Contents", ())
    if not isinstance(contents, Sequence) or isinstance(contents, (str, bytes, bytearray)):
        raise CatalogStorageError(f"S3 catalog listing below '{object_prefix}' is malformed")
    keys: list[str] = []
    for item in contents:
        if not isinstance(item, Mapping):
            raise CatalogStorageError(f"S3 catalog listing below '{object_prefix}' is malformed")
        object_key = item.get("Key")
        if not isinstance(object_key, str) or not object_key.startswith(object_prefix):
            raise CatalogStorageError(
                f"S3 catalog listing below '{object_prefix}' has an invalid key"
            )
        keys.append(object_key.removeprefix(catalog_prefix))
    return tuple(keys)


def _aws_error_code(exc: Exception) -> str | None:
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return None
    error = response.get("Error")
    if isinstance(error, Mapping):
        code = error.get("Code")
        if isinstance(code, str):
            return code
    metadata = response.get("ResponseMetadata")
    if isinstance(metadata, Mapping):
        status = metadata.get("HTTPStatusCode")
        if isinstance(status, int):
            return str(status)
    return None
