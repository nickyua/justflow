"""DynamoDB publication and activation state for shared controllers."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, cast

from pydantic import ValidationError

from justflow.configuration.activation import (
    MAX_ACTIVATION_PAGE_SIZE,
    MAX_ACTIVATION_RECORD_BYTES,
    ActivationPage,
    ActivationRecord,
    ActivationSummary,
    PublicationRecord,
)
from justflow.configuration.activation_errors import (
    ActivationConflictError,
    ActivationIntegrityError,
    ActivationLimitError,
    ActivationNotFoundError,
    ActivationUnavailableError,
)
from justflow.configuration.aws import (
    AWS_CONDITIONAL_ERROR_CODES,
    AWS_TABLE_NAME_PATTERN,
    DynamoConfigurationClient,
)
from justflow.configuration.lifecycle import ConfigurationDiscardRecord
from justflow.optional_dependencies import load_optional_dependency
from justflow.scope import RuntimeScope, decode_scope_cursor, encode_scope_cursor

RECORD_TYPE_PUBLICATION = "publication"
RECORD_TYPE_ACTIVATION = "activation"
RECORD_TYPE_DISCARD = "discard"
PUBLICATION_KEY_PREFIX = "PUB#"
ACTIVATION_KEY_PREFIX = "ACT#"
DISCARD_KEY_PREFIX = "DIS#"


class DynamoDbActivationStore:
    """Conditional activation records in one scope-partitioned DynamoDB table."""

    def __init__(
        self,
        *,
        table_name: str,
        activation_index_name: str,
        client: DynamoConfigurationClient | None = None,
        region: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        if AWS_TABLE_NAME_PATTERN.fullmatch(table_name) is None:
            raise ValueError("DynamoDB activation table name is invalid")
        if AWS_TABLE_NAME_PATTERN.fullmatch(activation_index_name) is None:
            raise ValueError("DynamoDB activation index name is invalid")
        self._table_name = table_name
        self._activation_index_name = activation_index_name
        self._client = client
        self._region = region
        self._endpoint_url = endpoint_url

    def create_discard(
        self,
        scope: RuntimeScope,
        record: ConfigurationDiscardRecord,
    ) -> ConfigurationDiscardRecord:
        self._require_scope(scope, record.scope_digest)
        try:
            self._dynamodb().put_item(
                TableName=self._table_name,
                Item=self._discard_item(scope, record),
                ConditionExpression="attribute_not_exists(metadata_key)",
            )
        except Exception as exc:
            if _aws_error_code(exc) not in AWS_CONDITIONAL_ERROR_CODES:
                raise ActivationUnavailableError(
                    "Cannot create the DynamoDB discard record"
                ) from exc
            existing = self.read_discard(scope, record.discard_id)
            if (
                existing.request_digest != record.request_digest
                or existing.idempotency_key_digest != record.idempotency_key_digest
            ):
                raise ActivationConflictError(record.discard_id) from exc
            return existing
        return record

    def read_discard(
        self,
        scope: RuntimeScope,
        discard_id: str,
    ) -> ConfigurationDiscardRecord:
        item = self._get(scope, self._discard_key(discard_id))
        if item is None:
            raise ActivationNotFoundError("Configuration discard was not found")
        self._require_record(item, scope, RECORD_TYPE_DISCARD)
        record = _discard(_binary(item, "payload"), scope)
        if record.discard_id != discard_id:
            raise ActivationIntegrityError("DynamoDB discard identity disagrees")
        return record

    def update_discard(
        self,
        scope: RuntimeScope,
        record: ConfigurationDiscardRecord,
        *,
        expected_version: int,
    ) -> ConfigurationDiscardRecord:
        self._require_scope(scope, record.scope_digest)
        if record.version != expected_version + 1:
            raise ActivationIntegrityError("Discard version transition is invalid")
        self._conditional_replace(
            self._discard_item(scope, record),
            expected_version=expected_version,
            conflict_identity=record.discard_id,
        )
        return record

    def create_publication(
        self,
        scope: RuntimeScope,
        record: PublicationRecord,
    ) -> PublicationRecord:
        self._require_scope(scope, record.scope_digest)
        try:
            self._dynamodb().put_item(
                TableName=self._table_name,
                Item=self._publication_item(scope, record),
                ConditionExpression="attribute_not_exists(metadata_key)",
            )
        except Exception as exc:
            if _aws_error_code(exc) not in AWS_CONDITIONAL_ERROR_CODES:
                raise ActivationUnavailableError(
                    "Cannot create the DynamoDB publication record"
                ) from exc
            existing = self.read_publication(scope, record.publication_id)
            if (
                existing.request_digest != record.request_digest
                or existing.idempotency_key_digest != record.idempotency_key_digest
            ):
                raise ActivationConflictError(record.publication_id) from exc
            return existing
        return record

    def read_publication(
        self,
        scope: RuntimeScope,
        publication_id: str,
    ) -> PublicationRecord:
        item = self._get(scope, self._publication_key(publication_id))
        if item is None:
            raise ActivationNotFoundError("Configuration publication was not found")
        self._require_record(item, scope, RECORD_TYPE_PUBLICATION)
        record = _publication(_binary(item, "payload"), scope)
        if record.publication_id != publication_id:
            raise ActivationIntegrityError("DynamoDB publication identity disagrees")
        return record

    def update_publication(
        self,
        scope: RuntimeScope,
        record: PublicationRecord,
        *,
        expected_version: int,
    ) -> PublicationRecord:
        self._require_scope(scope, record.scope_digest)
        if record.version != expected_version + 1:
            raise ActivationIntegrityError("Publication version transition is invalid")
        self._conditional_replace(
            self._publication_item(scope, record),
            expected_version=expected_version,
            conflict_identity=record.publication_id,
        )
        return record

    def create_activation(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
    ) -> ActivationRecord:
        self._require_scope(scope, record.scope_digest)
        try:
            self._dynamodb().put_item(
                TableName=self._table_name,
                Item=self._activation_item(scope, record),
                ConditionExpression="attribute_not_exists(metadata_key)",
            )
        except Exception as exc:
            if _aws_error_code(exc) not in AWS_CONDITIONAL_ERROR_CODES:
                raise ActivationUnavailableError(
                    "Cannot create the DynamoDB activation record"
                ) from exc
            existing = self.read_activation(scope, record.activation_id)
            if (
                existing.request_digest != record.request_digest
                or existing.idempotency_key_digest != record.idempotency_key_digest
            ):
                raise ActivationConflictError(record.activation_id) from exc
            return existing
        return record

    def read_activation(
        self,
        scope: RuntimeScope,
        activation_id: str,
    ) -> ActivationRecord:
        item = self._get(scope, self._activation_key(activation_id))
        if item is None:
            raise ActivationNotFoundError("Configuration activation was not found")
        self._require_record(item, scope, RECORD_TYPE_ACTIVATION)
        record = _activation(_binary(item, "payload"), scope)
        if record.activation_id != activation_id:
            raise ActivationIntegrityError("DynamoDB activation identity disagrees")
        return record

    def update_activation(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        *,
        expected_version: int,
    ) -> ActivationRecord:
        self._require_scope(scope, record.scope_digest)
        if record.version != expected_version + 1:
            raise ActivationIntegrityError("Activation version transition is invalid")
        self._conditional_replace(
            self._activation_item(scope, record),
            expected_version=expected_version,
            conflict_identity=record.activation_id,
        )
        return record

    def list_activations(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ActivationPage:
        if not 1 <= limit <= MAX_ACTIVATION_PAGE_SIZE:
            raise ActivationLimitError("Activation page size is invalid")
        request: dict[str, object] = {
            "TableName": self._table_name,
            "IndexName": self._activation_index_name,
            "KeyConditionExpression": "scope_digest = :scope_digest",
            "ExpressionAttributeValues": {":scope_digest": {"S": scope.digest}},
            "ScanIndexForward": False,
            "Limit": limit,
        }
        if cursor is not None:
            request["ExclusiveStartKey"] = _decode_cursor(scope, cursor)
        try:
            response = self._dynamodb().query(**request)
        except Exception as exc:
            raise ActivationUnavailableError("Cannot list DynamoDB activation records") from exc
        raw_items = response.get("Items", [])
        if not isinstance(raw_items, list) or len(raw_items) > limit:
            raise ActivationIntegrityError("DynamoDB activation page is invalid")
        records: list[ActivationRecord] = []
        for raw_item in raw_items:
            item = _checked_item(raw_item)
            self._require_record(item, scope, RECORD_TYPE_ACTIVATION)
            records.append(_activation(_binary(item, "payload"), scope))
        last_key = response.get("LastEvaluatedKey")
        next_cursor = (
            encode_scope_cursor(scope, _checked_item(last_key)) if last_key is not None else None
        )
        return ActivationPage(
            activations=tuple(
                ActivationSummary(
                    activation_id=record.activation_id,
                    source_revision_id=record.plan.source_revision_id,
                    target_revision_id=record.plan.target_revision_id,
                    state=record.state,
                    rollback_of_activation_id=record.rollback_of_activation_id,
                    created_at=record.created_at,
                    updated_at=record.updated_at,
                )
                for record in records
            ),
            next_cursor=next_cursor,
        )

    def _conditional_replace(
        self,
        item: Mapping[str, Any],
        *,
        expected_version: int,
        conflict_identity: str,
    ) -> None:
        try:
            self._dynamodb().put_item(
                TableName=self._table_name,
                Item=item,
                ConditionExpression="version = :expected_version",
                ExpressionAttributeValues={":expected_version": {"N": str(expected_version)}},
            )
        except Exception as exc:
            if _aws_error_code(exc) in AWS_CONDITIONAL_ERROR_CODES:
                raise ActivationConflictError(conflict_identity) from exc
            raise ActivationUnavailableError(
                "Cannot conditionally update DynamoDB activation state"
            ) from exc

    def _get(
        self,
        scope: RuntimeScope,
        metadata_key: str,
    ) -> Mapping[str, Any] | None:
        try:
            response = self._dynamodb().get_item(
                TableName=self._table_name,
                Key=self._metadata_key(scope, metadata_key),
                ConsistentRead=True,
            )
        except Exception as exc:
            raise ActivationUnavailableError("Cannot read DynamoDB activation state") from exc
        item = response.get("Item")
        return None if item is None else _checked_item(item)

    def _publication_item(
        self,
        scope: RuntimeScope,
        record: PublicationRecord,
    ) -> dict[str, dict[str, object]]:
        return {
            **self._metadata_key(scope, self._publication_key(record.publication_id)),
            "record_type": {"S": RECORD_TYPE_PUBLICATION},
            "scope_digest": {"S": scope.digest},
            "publication_id": {"S": record.publication_id},
            "version": {"N": str(record.version)},
            "created_at": {"S": record.created_at.isoformat()},
            "payload": {"B": _record_bytes(record)},
        }

    def _discard_item(
        self,
        scope: RuntimeScope,
        record: ConfigurationDiscardRecord,
    ) -> dict[str, dict[str, object]]:
        return {
            **self._metadata_key(scope, self._discard_key(record.discard_id)),
            "record_type": {"S": RECORD_TYPE_DISCARD},
            "scope_digest": {"S": scope.digest},
            "discard_id": {"S": record.discard_id},
            "version": {"N": str(record.version)},
            "created_at": {"S": record.created_at.isoformat()},
            "updated_at": {"S": record.updated_at.isoformat()},
            "payload": {"B": _record_bytes(record)},
        }

    def _activation_item(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
    ) -> dict[str, dict[str, object]]:
        return {
            **self._metadata_key(scope, self._activation_key(record.activation_id)),
            "record_type": {"S": RECORD_TYPE_ACTIVATION},
            "scope_digest": {"S": scope.digest},
            "activation_id": {"S": record.activation_id},
            "version": {"N": str(record.version)},
            "activation_order": {"S": f"{record.created_at.isoformat()}#{record.activation_id}"},
            "created_at": {"S": record.created_at.isoformat()},
            "updated_at": {"S": record.updated_at.isoformat()},
            "payload": {"B": _record_bytes(record)},
        }

    @staticmethod
    def _metadata_key(
        scope: RuntimeScope,
        metadata_key: str,
    ) -> dict[str, dict[str, object]]:
        return {
            "scope_key": {"S": f"scope#{scope.digest}"},
            "metadata_key": {"S": metadata_key},
        }

    @staticmethod
    def _publication_key(publication_id: str) -> str:
        return f"{PUBLICATION_KEY_PREFIX}{publication_id}"

    @staticmethod
    def _activation_key(activation_id: str) -> str:
        return f"{ACTIVATION_KEY_PREFIX}{activation_id}"

    @staticmethod
    def _discard_key(discard_id: str) -> str:
        return f"{DISCARD_KEY_PREFIX}{discard_id}"

    @staticmethod
    def _require_scope(scope: RuntimeScope, scope_digest: str) -> None:
        if scope_digest != scope.digest:
            raise ActivationIntegrityError("Activation record belongs to another runtime scope")

    @staticmethod
    def _require_record(
        item: Mapping[str, Any],
        scope: RuntimeScope,
        record_type: str,
    ) -> None:
        if (
            _string(item, "scope_digest") != scope.digest
            or _string(item, "record_type") != record_type
        ):
            raise ActivationIntegrityError("DynamoDB activation metadata is inconsistent")

    def _dynamodb(self) -> DynamoConfigurationClient:
        if self._client is None:
            boto3 = load_optional_dependency(
                "boto3",
                extra="aws",
                feature="AWS configuration activation",
            )
            self._client = cast(
                DynamoConfigurationClient,
                boto3.client(
                    "dynamodb",
                    region_name=self._region,
                    endpoint_url=self._endpoint_url,
                ),
            )
        return self._client


def _record_bytes(
    record: PublicationRecord | ActivationRecord | ConfigurationDiscardRecord,
) -> bytes:
    payload = json.dumps(
        record.model_dump(mode="json"),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(payload) > MAX_ACTIVATION_RECORD_BYTES:
        raise ActivationLimitError("Activation record exceeds its byte limit")
    return payload


def _publication(payload: bytes, scope: RuntimeScope) -> PublicationRecord:
    try:
        record = PublicationRecord.model_validate_json(payload)
    except (TypeError, ValidationError, ValueError) as exc:
        raise ActivationIntegrityError("DynamoDB publication record is invalid") from exc
    if record.scope_digest != scope.digest or payload != _record_bytes(record):
        raise ActivationIntegrityError("DynamoDB publication record is inconsistent")
    return record


def _activation(payload: bytes, scope: RuntimeScope) -> ActivationRecord:
    try:
        record = ActivationRecord.model_validate_json(payload)
    except (TypeError, ValidationError, ValueError) as exc:
        raise ActivationIntegrityError("DynamoDB activation record is invalid") from exc
    if record.scope_digest != scope.digest or payload != _record_bytes(record):
        raise ActivationIntegrityError("DynamoDB activation record is inconsistent")
    return record


def _discard(payload: bytes, scope: RuntimeScope) -> ConfigurationDiscardRecord:
    try:
        record = ConfigurationDiscardRecord.model_validate_json(payload)
    except (TypeError, ValidationError, ValueError) as exc:
        raise ActivationIntegrityError("DynamoDB discard record is invalid") from exc
    if record.scope_digest != scope.digest or payload != _record_bytes(record):
        raise ActivationIntegrityError("DynamoDB discard record is inconsistent")
    return record


def _decode_cursor(scope: RuntimeScope, cursor: str) -> Mapping[str, Any]:
    try:
        return _checked_item(decode_scope_cursor(scope, cursor))
    except ValueError as exc:
        raise ActivationIntegrityError("Activation cursor is invalid for this scope") from exc


def _checked_item(value: object) -> Mapping[str, Any]:
    if not isinstance(value, dict) or len(value) > 16:
        raise ActivationIntegrityError("DynamoDB activation item is invalid")
    return value


def _string(item: Mapping[str, Any], key: str) -> str:
    value = item.get(key)
    if not isinstance(value, dict) or set(value) != {"S"} or not isinstance(value["S"], str):
        raise ActivationIntegrityError("DynamoDB activation string is invalid")
    return value["S"]


def _binary(item: Mapping[str, Any], key: str) -> bytes:
    value = item.get(key)
    if not isinstance(value, dict) or set(value) != {"B"} or not isinstance(value["B"], bytes):
        raise ActivationIntegrityError("DynamoDB activation payload is invalid")
    return value["B"]


def _aws_error_code(exc: Exception) -> str | None:
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return None
    error = response.get("Error")
    if not isinstance(error, Mapping):
        return None
    code = error.get("Code")
    return str(code) if code is not None else None
