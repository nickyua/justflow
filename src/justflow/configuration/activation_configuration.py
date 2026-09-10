"""Typed construction of built-in activation stores."""

from __future__ import annotations

from justflow.config.settings import (
    AwsConfigurationSettings,
    ConfigurationSettings,
    FileConfigurationSettings,
    SqliteConfigurationSettings,
)
from justflow.configuration.activation_aws import DynamoDbActivationStore
from justflow.configuration.activation_sqlite import SqliteActivationStore
from justflow.configuration.activation_store import ActivationStore
from justflow.configuration.aws import DynamoConfigurationClient


def configured_activation_store(
    settings: ConfigurationSettings,
    *,
    dynamodb_client: DynamoConfigurationClient | None = None,
) -> ActivationStore:
    if isinstance(settings, FileConfigurationSettings):
        raise TypeError("File-managed configuration has no activation store")
    if isinstance(settings, SqliteConfigurationSettings):
        if dynamodb_client is not None:
            raise ValueError("A DynamoDB client cannot be supplied to a SQLite activation store")
        return SqliteActivationStore(settings.path)
    if isinstance(settings, AwsConfigurationSettings):
        return DynamoDbActivationStore(
            table_name=settings.table_name,
            activation_index_name=settings.activation_index_name,
            client=dynamodb_client,
            region=settings.region,
            endpoint_url=settings.endpoint_url,
        )
    raise TypeError(f"Unsupported activation backend '{type(settings).__name__}'")
