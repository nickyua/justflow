"""Tests for built-in resources and shipped-config loadability."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

import pytest

from justflow.config.loader import ConfigLoader
from justflow.config.validator import ConfigValidator
from justflow.resources.memory import MemoryCache, MemoryStore, StaticConfig
from justflow.resources.s3 import (
    S3_KEY_MAX_UTF8_BYTES,
    S3_RETENTION_TAG_KEY,
    S3ConfigurationError,
    S3KeyError,
    S3Resource,
)
from tests.workflow_fixtures.evaluators import has_allowed_item

RETENTION_POLICY = "audit-short"
RETENTION_TAG = "30-days"
RETENTION_SECONDS = 60.0


class FakeS3Client:
    def __init__(self):
        self.calls: list[dict] = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)

    def delete_object(self, **kwargs):
        self.calls.append(kwargs)


class TestS3Resource:
    async def test_write_puts_object_with_bucket_and_key(self):
        client = FakeS3Client()
        resource = S3Resource(
            bucket="audit-bucket",
            retention_policies={RETENTION_POLICY: RETENTION_TAG},
            client=client,
        )

        await resource.write(
            "audit/r1.json",
            '{"a": 1}',
            retention_policy=RETENTION_POLICY,
        )

        assert client.calls == [
            {
                "Bucket": "audit-bucket",
                "Key": "audit/r1.json",
                "Body": b'{"a": 1}',
                "Tagging": f"{S3_RETENTION_TAG_KEY}=30-days",
            }
        ]

    @pytest.mark.parametrize(
        "bucket",
        [
            "ab",
            "Uppercase-bucket",
            "bucket..name",
            "192.168.0.1",
            "bucket-s3alias",
        ],
        ids=["too-short", "uppercase", "adjacent-dots", "ip-address", "reserved-suffix"],
    )
    def test_rejects_invalid_bucket_names(self, bucket):
        with pytest.raises(S3ConfigurationError):
            S3Resource(
                bucket=bucket,
                retention_policies={RETENTION_POLICY: RETENTION_TAG},
                client=FakeS3Client(),
            )

    async def test_delete_removes_exact_object_key(self):
        client = FakeS3Client()
        resource = S3Resource(
            bucket="audit-bucket",
            retention_policies={RETENTION_POLICY: RETENTION_TAG},
            client=client,
        )

        await resource.delete("audit/r1.json")

        assert client.calls == [{"Bucket": "audit-bucket", "Key": "audit/r1.json"}]

    async def test_unknown_retention_policy_is_rejected_before_write(self):
        client = FakeS3Client()
        resource = S3Resource(
            bucket="audit-bucket",
            retention_policies={RETENTION_POLICY: RETENTION_TAG},
            client=client,
        )

        with pytest.raises(ValueError, match="Unknown S3 retention policy"):
            await resource.write("audit/r1.json", "data", retention_policy="unknown")

        assert client.calls == []


@dataclass(frozen=True, kw_only=True)
class WriteReturns:
    value: str


@dataclass(frozen=True, kw_only=True)
class WriteRaises:
    exc: type[Exception]
    match: str


WriteOutcome: TypeAlias = WriteReturns | WriteRaises


@dataclass(frozen=True, kw_only=True)
class S3WriteCase:
    id: str
    key: str
    outcome: WriteOutcome


S3_WRITE_CASES = [
    S3WriteCase(
        id="opaque-parent-segments",
        key="../records/result.json",
        outcome=WriteReturns(value="../records/result.json"),
    ),
    S3WriteCase(id="empty", key="", outcome=WriteRaises(exc=S3KeyError, match="empty")),
    S3WriteCase(
        id="utf8-byte-limit",
        key="é" * (S3_KEY_MAX_UTF8_BYTES // 2 + 1),
        outcome=WriteRaises(exc=S3KeyError, match="UTF-8 bytes"),
    ),
]


@pytest.mark.parametrize("case", S3_WRITE_CASES, ids=lambda case: case.id)
async def test_s3_key_validation_preserves_object_key_semantics(case: S3WriteCase) -> None:
    client = FakeS3Client()
    resource = S3Resource(
        bucket="audit-bucket",
        retention_policies={RETENTION_POLICY: RETENTION_TAG},
        client=client,
    )

    if isinstance(case.outcome, WriteRaises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            await resource.write(case.key, "data", retention_policy=RETENTION_POLICY)
        assert client.calls == []
        return

    await resource.write(case.key, "data", retention_policy=RETENTION_POLICY)
    assert client.calls[0]["Key"] == case.outcome.value


class TestMemoryResources:
    async def test_memory_store_write(self):
        store = MemoryStore(retention_policies={RETENTION_POLICY: RETENTION_SECONDS})
        await store.write("k1", "v1", retention_policy=RETENTION_POLICY)
        assert store.records == {"k1": "v1"}

    async def test_memory_store_expires_and_deletes_records(self):
        now = {"value": 100.0}
        store = MemoryStore(
            retention_policies={RETENTION_POLICY: RETENTION_SECONDS},
            clock=lambda: now["value"],
        )
        await store.write("expired", "v1", retention_policy=RETENTION_POLICY)
        await store.write("deleted", "v2", retention_policy=RETENTION_POLICY)

        await store.delete("deleted")
        now["value"] += RETENTION_SECONDS

        assert store.records == {}

    @pytest.mark.parametrize(
        "retention_seconds",
        [
            pytest.param(0.0, id="zero"),
            pytest.param(-1.0, id="negative"),
            pytest.param(float("nan"), id="not-finite"),
        ],
    )
    def test_memory_store_requires_finite_positive_retention(
        self,
        retention_seconds: float,
    ) -> None:
        with pytest.raises(ValueError, match="finite positive"):
            MemoryStore(retention_policies={RETENTION_POLICY: retention_seconds})

    def test_static_config_get_with_default(self):
        config = StaticConfig(supported=["a"])
        assert config.get("supported") == ["a"]
        assert config.get("missing", "fallback") == "fallback"


class TestMemoryCache:
    async def test_get_set_roundtrip(self):
        cache = MemoryCache()
        assert await cache.get("k") is None
        await cache.set("k", "v")
        assert await cache.get("k") == "v"

    async def test_ttl_expiry_with_injected_clock(self):
        now = {"t": 1000.0}
        cache = MemoryCache(clock=lambda: now["t"])
        await cache.set("k", "v", ttl_sec=60)

        now["t"] = 1059.0
        assert await cache.get("k") == "v"
        now["t"] = 1060.0
        assert await cache.get("k") is None

    async def test_no_ttl_never_expires(self):
        now = {"t": 0.0}
        cache = MemoryCache(clock=lambda: now["t"])
        await cache.set("k", "v")
        now["t"] = 10**9
        assert await cache.get("k") == "v"


class TestHasAllowedItem:
    @pytest.mark.parametrize(
        ("items", "expected"),
        [
            (["alpha", "unknown"], True),
            (["unknown"], False),
            ([], False),
        ],
    )
    def test_matches_against_runtime_config(self, items, expected):
        resources = {"runtime_config": StaticConfig(allowed_items=["alpha", "beta"])}
        assert has_allowed_item({"items": items}, resources) is expected

    def test_missing_runtime_config_raises(self):
        with pytest.raises(KeyError, match="runtime_config"):
            has_allowed_item({"items": ["alpha"]}, resources={})


class TestShippedConfigs:
    def test_shipped_configs_validate_with_import_checks(self, configs_dir):
        loader = ConfigLoader(configs_dir)
        resources, services, workflows = loader.load_all()

        result = ConfigValidator(resources, services, workflows).validate()

        assert result.is_valid, [str(e) for e in result.errors]
