"""Smoke-test a built Justflow installation at its declared dependency floors."""

from __future__ import annotations

from importlib import import_module

from justflow.config.settings import RuntimeSettings, Settings
from justflow.openapi import build_openapi_document, load_bundled_openapi_document
from justflow.schemas import build_authoring_schemas, load_bundled_schemas
from justflow.scope import RuntimeScope

OPTIONAL_MODULES = (
    "asyncpg",
    "boto3",
    "grpc",
    "justflow_admin",
    "redis.asyncio",
    "uvicorn",
)


class MinimumEnvironmentError(RuntimeError):
    """The installed minimum dependency environment violates a release contract."""


def verify_minimum_environment() -> None:
    """Exercise imports and generated contracts without external I/O."""
    for module_name in OPTIONAL_MODULES:
        import_module(module_name)
    Settings(
        runtime=RuntimeSettings(
            scope=RuntimeScope.create(
                tenant="verification", application="minimum-dependencies", environment="production"
            )
        )
    )
    if load_bundled_schemas() != build_authoring_schemas():
        raise MinimumEnvironmentError("Bundled schemas differ from minimum-version generation")
    if load_bundled_openapi_document() != build_openapi_document():
        raise MinimumEnvironmentError("Bundled OpenAPI differs from minimum-version generation")


if __name__ == "__main__":
    verify_minimum_environment()
