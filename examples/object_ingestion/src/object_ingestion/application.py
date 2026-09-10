"""Explicit local runtime composition for S3 object-created ingress."""

from __future__ import annotations

from pathlib import Path

from justflow.config.settings import (
    ControlSettings,
    DeploymentSettings,
    LocalTemporalConnectionSettings,
    PathSettings,
    RuntimeSettings,
    Settings,
    TemporalSettings,
)
from justflow.provenance import LOCAL_ARTIFACT_DIGEST, RuntimeProfile
from justflow.runtime import (
    CloudEventMappingRegistry,
    RuntimeApplication,
    S3ObjectCreatedEventMapper,
    S3ObjectKeyFilter,
)

MAPPING_NAME = "s3_object_created"
WORKFLOW_NAME = "object_ingestion"
CONFIG_ROOT = Path(__file__).resolve().parents[2] / "configs"
SOURCE_BUCKET = "example-ingestion"
SOURCE_PREFIX = "incoming/"
ARCHIVE_PREFIX = "archive/"
JSON_SUFFIX = ".json"


def create_event_registry() -> CloudEventMappingRegistry:
    registry = CloudEventMappingRegistry()
    registry.register(
        MAPPING_NAME,
        S3ObjectCreatedEventMapper(
            mapping_name=MAPPING_NAME,
            workflow_name=WORKFLOW_NAME,
            source=S3ObjectKeyFilter(
                bucket=SOURCE_BUCKET,
                prefix=SOURCE_PREFIX,
                suffix=JSON_SUFFIX,
            ),
            write_destinations=(
                S3ObjectKeyFilter(
                    bucket=SOURCE_BUCKET,
                    prefix=ARCHIVE_PREFIX,
                    suffix=JSON_SUFFIX,
                ),
            ),
        ),
    )
    return registry


def create_application() -> RuntimeApplication:
    settings = Settings(
        runtime=RuntimeSettings(profile=RuntimeProfile.LOCAL),
        temporal=TemporalSettings(
            address="127.0.0.1:7233",
            connection=LocalTemporalConnectionSettings(host="127.0.0.1"),
        ),
        control=ControlSettings(host="127.0.0.1", port=8323),
        paths=PathSettings(config_dir=str(CONFIG_ROOT)),
        deployment=DeploymentSettings(
            name="object-ingestion-local",
            build_id="development",
            artifact_digest=LOCAL_ARTIFACT_DIGEST,
            package_version="development",
        ),
    )
    return RuntimeApplication(settings, cloud_event_registry=create_event_registry())
