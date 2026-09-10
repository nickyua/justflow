"""Typed construction of built-in definition catalog stores."""

from __future__ import annotations

from pathlib import Path

from justflow.config.settings import (
    CatalogSettings,
    LocalCatalogSettings,
    S3CatalogSettings,
)
from justflow.definitions.catalog import (
    CatalogStore,
    DefinitionCatalogStore,
    ScopedCatalogBackend,
)
from justflow.definitions.s3 import S3CatalogBackend, S3CatalogClient
from justflow.scope import RuntimeScope


def configured_catalog_store(
    settings: CatalogSettings,
    config_dir: str | Path,
    *,
    s3_client: S3CatalogClient | None = None,
    scope: RuntimeScope | None = None,
) -> DefinitionCatalogStore:
    if isinstance(settings, LocalCatalogSettings):
        if s3_client is not None:
            raise ValueError("An S3 client cannot be supplied for a local definition catalog")
        store = CatalogStore(config_dir)
        return (
            store
            if scope is None
            else DefinitionCatalogStore(ScopedCatalogBackend(store.backend, scope))
        )
    if isinstance(settings, S3CatalogSettings):
        backend = S3CatalogBackend(
            bucket=settings.bucket,
            prefix=settings.prefix,
            region=settings.region,
            endpoint_url=settings.endpoint_url,
            expected_bucket_owner=settings.expected_bucket_owner,
            server_side_encryption=settings.server_side_encryption,
            kms_key_id=settings.kms_key_id,
            client=s3_client,
        )
        return DefinitionCatalogStore(
            backend if scope is None else ScopedCatalogBackend(backend, scope)
        )
    raise TypeError(f"Unsupported definition catalog settings '{type(settings).__name__}'")
