"""Deterministic tests for S3 catalog immutability and alias concurrency."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypeAlias

import pytest
from pydantic import ValidationError

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.config.settings import S3CatalogSettings, Settings
from justflow.definitions.catalog import (
    ALIASES_FILENAME,
    CatalogConflictError,
    CatalogStorageError,
    DefinitionCatalogStore,
)
from justflow.definitions.configuration import configured_catalog_store
from justflow.definitions.manifest import build_definition_manifests
from justflow.definitions.s3 import S3CatalogBackend
from tests.settings import PRODUCTION_RUNTIME

BUCKET = "definition-catalog"
PREFIX = "service/definitions/"
EXPECTED_BUCKET_OWNER = "123456789012"
KMS_KEY_ID = "alias/catalog"
PAGE_SIZE = 1
PRECONDITION_FAILED = "PreconditionFailed"
ACCESS_DENIED = "AccessDenied"


@dataclass(frozen=True, kw_only=True)
class SettingsReturns:
    backend: str


@dataclass(frozen=True, kw_only=True)
class SettingsRaises:
    exc: type[Exception]
    match: str


SettingsOutcome: TypeAlias = SettingsReturns | SettingsRaises


@dataclass(frozen=True, kw_only=True)
class CatalogSettingsCase:
    id: str
    catalog: dict[str, object]
    outcome: SettingsOutcome


CATALOG_SETTINGS_CASES = [
    CatalogSettingsCase(
        id="valid",
        catalog={"backend": "s3", "bucket": BUCKET},
        outcome=SettingsReturns(backend="s3"),
    ),
    CatalogSettingsCase(
        id="credentials",
        catalog={"backend": "s3", "bucket": BUCKET, "access_key_id": "not-allowed"},
        outcome=SettingsRaises(exc=ValidationError, match="Extra inputs are not permitted"),
    ),
    CatalogSettingsCase(
        id="endpoint-userinfo",
        catalog={
            "backend": "s3",
            "bucket": BUCKET,
            "endpoint_url": "https://user@localhost",
        },
        outcome=SettingsRaises(exc=ValidationError, match="without credentials"),
    ),
    CatalogSettingsCase(
        id="kms-without-kms-encryption",
        catalog={"backend": "s3", "bucket": BUCKET, "kms_key_id": KMS_KEY_ID},
        outcome=SettingsRaises(exc=ValidationError, match="requires aws:kms"),
    ),
    CatalogSettingsCase(
        id="ip-bucket",
        catalog={"backend": "s3", "bucket": "192.168.5.4"},
        outcome=SettingsRaises(exc=ValidationError, match="must not be formatted as an IP"),
    ),
    CatalogSettingsCase(
        id="prefix-traversal",
        catalog={"backend": "s3", "bucket": BUCKET, "prefix": "catalog/../private"},
        outcome=SettingsRaises(exc=ValidationError, match="invalid path segment"),
    ),
]


class FakeS3Error(Exception):
    def __init__(self, code: str) -> None:
        super().__init__("fake S3 request failed")
        self.response = {"Error": {"Code": code}}


class FakeBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.closed = False

    def read(self, size: int) -> bytes:
        chunk = self._payload[:size]
        self._payload = self._payload[size:]
        return chunk

    def close(self) -> None:
        self.closed = True


class FakeS3Client:
    def __init__(self, *, page_size: int = PAGE_SIZE) -> None:
        self.objects: dict[str, bytes] = {}
        self.page_size = page_size
        self.put_requests: list[dict[str, object]] = []
        self.fail_next_put: str | None = None
        self.concurrent_alias_payload: bytes | None = None

    def get_object(self, **kwargs: object) -> Mapping[str, Any]:
        key = _request_key(kwargs)
        payload = self.objects.get(key)
        if payload is None:
            raise FakeS3Error("NoSuchKey")
        return {"Body": FakeBody(payload), "ETag": _etag(payload)}

    def list_objects_v2(self, **kwargs: object) -> Mapping[str, Any]:
        prefix = kwargs.get("Prefix")
        if not isinstance(prefix, str):
            raise TypeError("Prefix must be a string")
        token = kwargs.get("ContinuationToken", "0")
        if not isinstance(token, str) or not token.isdigit():
            raise AssertionError("ContinuationToken must be a numeric string")
        start = int(token)
        keys = sorted(key for key in self.objects if key.startswith(prefix))
        page = keys[start : start + self.page_size]
        next_start = start + len(page)
        response: dict[str, Any] = {
            "Contents": [{"Key": key} for key in page],
            "IsTruncated": next_start < len(keys),
        }
        if response["IsTruncated"]:
            response["NextContinuationToken"] = str(next_start)
        return response

    def put_object(self, **kwargs: object) -> Mapping[str, Any]:
        self.put_requests.append(dict(kwargs))
        key = _request_key(kwargs)
        body = kwargs.get("Body")
        if not isinstance(body, bytes):
            raise TypeError("Body must be bytes")
        if self.concurrent_alias_payload is not None and key.endswith(ALIASES_FILENAME):
            self.objects[key] = self.concurrent_alias_payload
            self.concurrent_alias_payload = None
        if self.fail_next_put is not None:
            code = self.fail_next_put
            self.fail_next_put = None
            raise FakeS3Error(code)
        current = self.objects.get(key)
        if kwargs.get("IfNoneMatch") == "*" and current is not None:
            raise FakeS3Error(PRECONDITION_FAILED)
        expected_etag = kwargs.get("IfMatch")
        if expected_etag is not None and (
            not isinstance(expected_etag, str) or current is None or _etag(current) != expected_etag
        ):
            raise FakeS3Error(PRECONDITION_FAILED)
        self.objects[key] = body
        return {"ETag": _etag(body)}


def test_s3_catalog_round_trip_uses_immutable_writes_and_paginated_reads() -> None:
    client = FakeS3Client()
    store = _store(client)
    manifests = _manifests()

    published = store.publish(manifests)
    loaded = store.load()

    assert loaded.manifests == published.manifests
    assert loaded.aliases == published.aliases
    manifest_request = next(
        request
        for request in client.put_requests
        if not _request_key(request).endswith(ALIASES_FILENAME)
    )
    alias_request = next(
        request
        for request in client.put_requests
        if _request_key(request).endswith(ALIASES_FILENAME)
    )
    assert manifest_request["IfNoneMatch"] == "*"
    assert alias_request["IfNoneMatch"] == "*"
    assert manifest_request["ServerSideEncryption"] == "AES256"
    assert manifest_request["ExpectedBucketOwner"] == EXPECTED_BUCKET_OWNER


def test_s3_catalog_alias_update_uses_the_observed_etag() -> None:
    client = FakeS3Client()
    store = _store(client)
    store.publish(_manifests())
    aliases_key = f"{PREFIX}{ALIASES_FILENAME}"
    previous_etag = _etag(client.objects[aliases_key])

    store.publish(_manifests(description="updated"))

    alias_request = client.put_requests[-1]
    assert alias_request["IfMatch"] == previous_etag
    assert len([key for key in client.objects if "/manifests/" in f"/{key}"]) == 2


def test_s3_catalog_rejects_concurrent_alias_change() -> None:
    client = FakeS3Client()
    store = _store(client)
    store.publish(_manifests())
    concurrent_payload = b'{"aliases":{},"format_version":1}'
    client.concurrent_alias_payload = concurrent_payload

    with pytest.raises(CatalogConflictError, match="changed concurrently"):
        store.publish(_manifests(description="updated"))

    assert client.objects[f"{PREFIX}{ALIASES_FILENAME}"] == concurrent_payload


def test_s3_catalog_treats_a_matching_conditional_conflict_as_a_safe_retry() -> None:
    client = FakeS3Client()
    backend = _backend(client)
    key = "manifests/example/digest.json"
    payload = b'{"value":"stable"}'
    client.objects[f"{PREFIX}{key}"] = payload

    stored = backend.create_immutable(key, payload)

    assert stored.payload == payload


def test_s3_catalog_rejects_divergent_immutable_content() -> None:
    client = FakeS3Client()
    backend = _backend(client)
    key = "manifests/example/digest.json"
    client.objects[f"{PREFIX}{key}"] = b'{"value":"old"}'

    with pytest.raises(CatalogConflictError, match="changed concurrently"):
        backend.create_immutable(key, b'{"value":"new"}')


def test_s3_catalog_translates_client_failures_without_exposing_client_details() -> None:
    client = FakeS3Client()
    client.fail_next_put = ACCESS_DENIED
    backend = _backend(client)

    with pytest.raises(CatalogStorageError, match="Cannot write S3 catalog object") as exc_info:
        backend.create_immutable("manifests/example/digest.json", b"{}")

    assert ACCESS_DENIED not in str(exc_info.value)


@pytest.mark.parametrize(
    ("encryption", "kms_key_id", "expected_kms_key_id"),
    [
        pytest.param("AES256", None, None, id="s3-managed"),
        pytest.param("aws:kms", KMS_KEY_ID, KMS_KEY_ID, id="kms"),
    ],
)
def test_s3_catalog_write_encryption_request(
    encryption: str,
    kms_key_id: str | None,
    expected_kms_key_id: str | None,
) -> None:
    client = FakeS3Client()
    backend = S3CatalogBackend(
        bucket=BUCKET,
        prefix=PREFIX,
        server_side_encryption=encryption,
        kms_key_id=kms_key_id,
        client=client,
    )

    backend.create_immutable("manifests/example/digest.json", b"{}")

    request = client.put_requests[-1]
    assert request["ServerSideEncryption"] == encryption
    assert request.get("SSEKMSKeyId") == expected_kms_key_id


def test_configured_catalog_store_accepts_an_injected_s3_client() -> None:
    client = FakeS3Client()
    settings = S3CatalogSettings(bucket=BUCKET, prefix=PREFIX)

    store = configured_catalog_store(settings, "unused", s3_client=client)

    assert store.backend_identity.provider == "s3"


@pytest.mark.parametrize("case", CATALOG_SETTINGS_CASES, ids=lambda case: case.id)
def test_s3_catalog_settings_are_strict_and_validated(case: CatalogSettingsCase) -> None:
    if isinstance(case.outcome, SettingsRaises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            Settings.model_validate({"catalog": case.catalog, "runtime": PRODUCTION_RUNTIME})
        return

    settings = Settings.model_validate({"catalog": case.catalog, "runtime": PRODUCTION_RUNTIME})
    assert settings.catalog.backend == case.outcome.backend


def _store(client: FakeS3Client) -> DefinitionCatalogStore:
    return DefinitionCatalogStore(_backend(client))


def _backend(client: FakeS3Client) -> S3CatalogBackend:
    return S3CatalogBackend(
        bucket=BUCKET,
        prefix=PREFIX,
        expected_bucket_owner=EXPECTED_BUCKET_OWNER,
        client=client,
    )


def _manifests(*, description: str = ""):
    workflow = WorkflowConfig(
        workflow="example",
        description=description,
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    return build_definition_manifests({"example": workflow}, {}, RuntimeLimits())


def _request_key(request: Mapping[str, object]) -> str:
    key = request.get("Key")
    if not isinstance(key, str):
        raise TypeError("Key must be a string")
    return key


def _etag(payload: bytes) -> str:
    return f'"{hashlib.md5(payload, usedforsecurity=False).hexdigest()}"'
