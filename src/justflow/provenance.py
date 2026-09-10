"""Immutable execution provenance identities."""

from __future__ import annotations

import hashlib
import json
import re
from enum import Enum
from importlib.metadata import PackageNotFoundError, version
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from justflow.scope import SCOPE_DIGEST_LENGTH

ARTIFACT_DIGEST_ALGORITHM = "sha256"
ARTIFACT_DIGEST_HEX_LENGTH = 64
ARTIFACT_DIGEST_PATTERN = re.compile(
    rf"^{ARTIFACT_DIGEST_ALGORITHM}:[0-9a-f]{{{ARTIFACT_DIGEST_HEX_LENGTH}}}$"
)
LOCAL_ARTIFACT_DIGEST = "local-development"
JUSTFLOW_DISTRIBUTION = "justflow"
MAX_PROVENANCE_IDENTITY_LENGTH = 256
ENVIRONMENT_SNAPSHOT_FORMAT_VERSION = 1
SHA256_HEX_PATTERN = re.compile(rf"^[0-9a-f]{{{ARTIFACT_DIGEST_HEX_LENGTH}}}$")


class ProvenanceError(ValueError):
    """Execution provenance is missing or inconsistent."""


class RuntimeProfile(str, Enum):
    LOCAL = "local"
    PRODUCTION = "production"


class WorkerArtifactIdentity(BaseModel):
    """Immutable executable artifact selected for a workflow execution."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    deployment_name: str = Field(min_length=1, max_length=MAX_PROVENANCE_IDENTITY_LENGTH)
    build_id: str = Field(min_length=1, max_length=MAX_PROVENANCE_IDENTITY_LENGTH)
    artifact_digest: str = Field(min_length=1, max_length=MAX_PROVENANCE_IDENTITY_LENGTH)
    package_version: str = Field(min_length=1, max_length=MAX_PROVENANCE_IDENTITY_LENGTH)
    source_revision: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_PROVENANCE_IDENTITY_LENGTH,
    )

    @model_validator(mode="after")
    def validate_artifact_digest(self) -> Self:
        if (
            self.artifact_digest != LOCAL_ARTIFACT_DIGEST
            and ARTIFACT_DIGEST_PATTERN.fullmatch(self.artifact_digest) is None
        ):
            raise ValueError(
                "Artifact digest must be 'sha256:' followed by a full lowercase SHA-256 "
                f"digest, or '{LOCAL_ARTIFACT_DIGEST}'"
            )
        return self

    def validate_for_profile(self, profile: RuntimeProfile) -> None:
        is_local_identity = self.artifact_digest == LOCAL_ARTIFACT_DIGEST
        if profile is RuntimeProfile.LOCAL and not is_local_identity:
            raise ProvenanceError(
                f"The local runtime profile requires artifact digest '{LOCAL_ARTIFACT_DIGEST}'"
            )
        if profile is RuntimeProfile.PRODUCTION and is_local_identity:
            raise ProvenanceError(
                "The production runtime profile requires an immutable SHA-256 artifact digest"
            )


class ExecutionConfigurationIdentity(BaseModel):
    """Immutable, secret-free configuration selected for one execution."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    configuration_revision_id: str = Field(
        min_length=ARTIFACT_DIGEST_HEX_LENGTH,
        max_length=ARTIFACT_DIGEST_HEX_LENGTH,
        pattern=SHA256_HEX_PATTERN,
    )
    tenant_configuration_revision_id: str | None = Field(
        default=None,
        min_length=ARTIFACT_DIGEST_HEX_LENGTH,
        max_length=ARTIFACT_DIGEST_HEX_LENGTH,
        pattern=SHA256_HEX_PATTERN,
    )
    component_catalog_revision: str | None = Field(
        default=None,
        min_length=ARTIFACT_DIGEST_HEX_LENGTH,
        max_length=ARTIFACT_DIGEST_HEX_LENGTH,
        pattern=SHA256_HEX_PATTERN,
    )
    component_identity_digest: str | None = Field(
        default=None,
        min_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        max_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        pattern=ARTIFACT_DIGEST_PATTERN,
    )
    resolution_digest: str = Field(
        min_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        max_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        pattern=ARTIFACT_DIGEST_PATTERN,
    )

    @model_validator(mode="after")
    def validate_tenant_resolution(self) -> Self:
        tenant_values = (
            self.tenant_configuration_revision_id,
            self.component_catalog_revision,
            self.component_identity_digest,
        )
        if any(value is None for value in tenant_values) and any(
            value is not None for value in tenant_values
        ):
            raise ValueError("Tenant execution configuration identity is incomplete")
        return self


class CatalogBackendIdentity(BaseModel):
    """Secret-free identity of the catalog storage configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: str = Field(min_length=1, max_length=MAX_PROVENANCE_IDENTITY_LENGTH)
    configuration_digest: str = Field(
        min_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        max_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        pattern=ARTIFACT_DIGEST_PATTERN,
    )


class ProviderContractSnapshotIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=MAX_PROVENANCE_IDENTITY_LENGTH)
    version: str = Field(min_length=1, max_length=MAX_PROVENANCE_IDENTITY_LENGTH)


class BrokerProviderSnapshotIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    binding_identity: str = Field(
        min_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        max_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        pattern=ARTIFACT_DIGEST_PATTERN,
    )
    provider: str = Field(min_length=1, max_length=MAX_PROVENANCE_IDENTITY_LENGTH)


class SanitizedRuntimeConfigurationIdentity(BaseModel):
    """Allowlisted runtime settings that cannot contain provider configuration or secrets."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    temporal_namespace_identity: str = Field(
        min_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        max_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        pattern=ARTIFACT_DIGEST_PATTERN,
    )
    temporal_task_queue_identity: str = Field(
        min_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        max_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        pattern=ARTIFACT_DIGEST_PATTERN,
    )
    payload_protection_mode: Literal["plaintext", "codec"]
    broker_providers: tuple[BrokerProviderSnapshotIdentity, ...]
    runtime_limits_digest: str = Field(
        min_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        max_length=len(f"{ARTIFACT_DIGEST_ALGORITHM}:") + ARTIFACT_DIGEST_HEX_LENGTH,
        pattern=ARTIFACT_DIGEST_PATTERN,
    )


class ExecutionEnvironmentSnapshotContent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    format_version: int = ENVIRONMENT_SNAPSHOT_FORMAT_VERSION
    engine_version: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_PROVENANCE_IDENTITY_LENGTH,
    )
    worker_artifact: WorkerArtifactIdentity
    definition_digest: str = Field(
        min_length=ARTIFACT_DIGEST_HEX_LENGTH,
        max_length=ARTIFACT_DIGEST_HEX_LENGTH,
        pattern=SHA256_HEX_PATTERN,
    )
    provider_contracts: tuple[ProviderContractSnapshotIdentity, ...]
    catalog_backend: CatalogBackendIdentity
    runtime_profile: RuntimeProfile
    configuration: SanitizedRuntimeConfigurationIdentity
    execution_configuration: ExecutionConfigurationIdentity | None = None
    scope_digest: str | None = Field(
        default=None,
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=SHA256_HEX_PATTERN,
    )


class ExecutionEnvironmentSnapshot(ExecutionEnvironmentSnapshotContent):
    """Canonical, content-addressed, secret-free execution environment identity."""

    snapshot_digest: str = Field(
        min_length=ARTIFACT_DIGEST_HEX_LENGTH,
        max_length=ARTIFACT_DIGEST_HEX_LENGTH,
        pattern=SHA256_HEX_PATTERN,
    )

    @model_validator(mode="after")
    def verify_identity(self) -> Self:
        if self.format_version != ENVIRONMENT_SNAPSHOT_FORMAT_VERSION:
            raise ValueError(
                f"Unsupported execution environment format version {self.format_version}"
            )
        content = self.model_dump(mode="json", exclude={"snapshot_digest"})
        _drop_optional_snapshot_fields(content)
        expected = environment_snapshot_digest(content)
        if self.snapshot_digest != expected:
            raise ValueError(
                f"Execution environment snapshot digest mismatch: expected {expected}, "
                f"found {self.snapshot_digest}"
            )
        return self

    @classmethod
    def create(cls, **content: object) -> ExecutionEnvironmentSnapshot:
        normalized = ExecutionEnvironmentSnapshotContent.model_validate(content).model_dump(
            mode="json"
        )
        _drop_optional_snapshot_fields(normalized)
        return cls(
            **normalized,
            snapshot_digest=environment_snapshot_digest(normalized),
        )

    def canonical_bytes(self) -> bytes:
        value = self.model_dump(mode="json")
        _drop_optional_snapshot_fields(value)
        return canonical_provenance_bytes(value)


def provenance_digest(value: object) -> str:
    payload = canonical_provenance_bytes(value)
    return f"{ARTIFACT_DIGEST_ALGORITHM}:{hashlib.sha256(payload).hexdigest()}"


def environment_snapshot_digest(value: object) -> str:
    return hashlib.sha256(canonical_provenance_bytes(value)).hexdigest()


def _drop_optional_snapshot_fields(value: dict[str, object]) -> None:
    for field_name in ("execution_configuration", "scope_digest"):
        if value[field_name] is None:
            del value[field_name]


def canonical_provenance_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ProvenanceError(f"Execution provenance is not canonical JSON: {exc}") from exc


def installed_engine_version() -> str | None:
    try:
        return version(JUSTFLOW_DISTRIBUTION)
    except PackageNotFoundError:
        return None
