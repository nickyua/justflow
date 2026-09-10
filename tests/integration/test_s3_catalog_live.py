"""Opt-in verification against a caller-owned S3 catalog prefix."""

from __future__ import annotations

import pytest

from justflow.config.settings import S3CatalogSettings, load_settings
from justflow.definitions.catalog import CatalogConflictError
from justflow.definitions.s3 import S3CatalogBackend

IMMUTABLE_KEY = "live-verification/immutable.json"
ALIAS_KEY = "live-verification/alias.json"
INITIAL_PAYLOAD = b'{"state":"initial"}'
UPDATED_PAYLOAD = b'{"state":"updated"}'
CONFLICTING_PAYLOAD = b'{"state":"conflicting"}'
EMPTY_PREFIX_MESSAGE = "Live S3 catalog verification requires a new, empty JUSTFLOW_CATALOG__PREFIX"
RUN_AWS_INTEGRATION_OPTION = "--run-aws-integration"

pytestmark = pytest.mark.aws_integration


def test_live_s3_conditional_writes(run_aws_integration: bool) -> None:
    if not run_aws_integration:
        pytest.skip(f"enable with {RUN_AWS_INTEGRATION_OPTION}")

    catalog_settings = load_settings().catalog
    if not isinstance(catalog_settings, S3CatalogSettings):
        pytest.fail("Live S3 catalog verification requires JUSTFLOW_CATALOG__BACKEND=s3")
    backend = S3CatalogBackend(
        bucket=catalog_settings.bucket,
        prefix=catalog_settings.prefix,
        region=catalog_settings.region,
        endpoint_url=catalog_settings.endpoint_url,
        expected_bucket_owner=catalog_settings.expected_bucket_owner,
        server_side_encryption=catalog_settings.server_side_encryption,
        kms_key_id=catalog_settings.kms_key_id,
    )
    if backend.read_object(IMMUTABLE_KEY) is not None or backend.read_object(ALIAS_KEY) is not None:
        pytest.fail(EMPTY_PREFIX_MESSAGE)

    immutable = backend.create_immutable(IMMUTABLE_KEY, INITIAL_PAYLOAD)
    assert backend.create_immutable(IMMUTABLE_KEY, INITIAL_PAYLOAD) == immutable
    with pytest.raises(CatalogConflictError, match="changed concurrently"):
        backend.create_immutable(IMMUTABLE_KEY, CONFLICTING_PAYLOAD)

    initial_alias = backend.compare_and_swap(
        ALIAS_KEY,
        INITIAL_PAYLOAD,
        expected_version=None,
    )
    updated_alias = backend.compare_and_swap(
        ALIAS_KEY,
        UPDATED_PAYLOAD,
        expected_version=initial_alias.version,
    )
    assert backend.read_object(ALIAS_KEY) == updated_alias
    with pytest.raises(CatalogConflictError, match="changed concurrently"):
        backend.compare_and_swap(
            ALIAS_KEY,
            CONFLICTING_PAYLOAD,
            expected_version=initial_alias.version,
        )
