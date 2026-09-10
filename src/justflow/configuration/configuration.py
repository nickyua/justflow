"""Typed construction of built-in configuration sources and stores."""

from __future__ import annotations

from pathlib import Path

from justflow.config.settings import (
    AwsConfigurationSettings,
    ConfigurationSettings,
    FileConfigurationSettings,
    SqliteConfigurationSettings,
)
from justflow.configuration.aws import (
    AwsConfigurationStore,
    DynamoConfigurationClient,
    S3ConfigurationClient,
)
from justflow.configuration.ports import ConfigurationSource, ConfigurationStore
from justflow.configuration.source import FileConfigurationSource, StoredConfigurationSource
from justflow.configuration.sqlite import SqliteConfigurationStore
from justflow.scope import RuntimeScope


def configured_configuration_store(
    settings: ConfigurationSettings,
    *,
    s3_client: S3ConfigurationClient | None = None,
    dynamodb_client: DynamoConfigurationClient | None = None,
) -> ConfigurationStore:
    if isinstance(settings, FileConfigurationSettings):
        raise TypeError("File-managed configuration has no writable store")
    if isinstance(settings, SqliteConfigurationSettings):
        if s3_client is not None or dynamodb_client is not None:
            raise ValueError("AWS clients cannot be supplied to a SQLite configuration store")
        return SqliteConfigurationStore(settings.path)
    if isinstance(settings, AwsConfigurationSettings):
        return AwsConfigurationStore(
            bucket=settings.bucket,
            table_name=settings.table_name,
            revision_index_name=settings.revision_index_name,
            prefix=settings.prefix,
            region=settings.region,
            endpoint_url=settings.endpoint_url,
            expected_bucket_owner=settings.expected_bucket_owner,
            server_side_encryption=settings.server_side_encryption,
            kms_key_id=settings.kms_key_id,
            s3_client=s3_client,
            dynamodb_client=dynamodb_client,
        )
    raise TypeError(f"Unsupported configuration backend '{type(settings).__name__}'")


def configured_configuration_source(
    settings: ConfigurationSettings,
    *,
    scope: RuntimeScope,
    config_dir: str | Path,
    store: ConfigurationStore | None = None,
) -> ConfigurationSource:
    if isinstance(settings, FileConfigurationSettings):
        if store is not None:
            raise ValueError("File-managed configuration cannot use a writable store")
        return FileConfigurationSource(config_dir, scope=scope)
    return StoredConfigurationSource(store or configured_configuration_store(settings))
