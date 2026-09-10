"""S3 and DynamoDB configuration store for shared production runtimes."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from justflow.bounded_io import BoundedReadError, BoundedReadLimitError, read_bounded_body
from justflow.catalog_validation import normalize_s3_catalog_prefix, validate_s3_bucket
from justflow.configuration.errors import (
    ConfigurationConflictError,
    ConfigurationIntegrityError,
    ConfigurationLimitError,
    ConfigurationNotFoundError,
    ConfigurationScopeError,
    ConfigurationUnavailableError,
)
from justflow.configuration.models import (
    MAX_CONFIGURATION_ENVELOPE_BYTES,
    MAX_RETENTION_DELETE_COUNT,
    MAX_REVISION_PAGE_SIZE,
    ActivePointer,
    ConfigurationDocument,
    ConfigurationEnvelope,
    DraftRecord,
    RetentionResult,
    RevisionIdentity,
    RevisionPage,
    RevisionRecord,
    RevisionSummary,
    configuration_revision_identity,
)
from justflow.optional_dependencies import load_optional_dependency
from justflow.scope import RuntimeScope, decode_scope_cursor, encode_scope_cursor

AWS_CONFIGURATION_CONTENT_TYPE = "application/json"
AWS_CONFIGURATION_STORE_MAX_QUERY_ITEMS = 10_000
AWS_CONFIGURATION_KEY_MAX_BYTES = 1_024
AWS_TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{3,255}$")
AWS_CONDITIONAL_ERROR_CODES = frozenset(
    {
        "409",
        "412",
        "ConditionalCheckFailedException",
        "ConditionalRequestConflict",
        "PreconditionFailed",
        "TransactionCanceledException",
    }
)
AWS_NOT_FOUND_ERROR_CODES = frozenset({"404", "NoSuchKey", "NotFound"})
RECORD_TYPE_DRAFT = "draft"
RECORD_TYPE_ACTIVE = "active"
RECORD_TYPE_REVISION = "revision"
RECORD_TYPE_REVISION_CLEANUP = "revision_cleanup"
CHILD_REFERENCE_COUNT_FIELD = "child_reference_count"
AWS_CONFIGURATION_BODY_SUFFIX_PATTERN = re.compile(r"^(drafts|revisions)/([0-9a-f]{64})\.json$")


class S3ConfigurationClient(Protocol):
    def get_object(self, **kwargs: object) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: object) -> Mapping[str, Any]: ...

    def delete_object(self, **kwargs: object) -> Mapping[str, Any]: ...


class DynamoConfigurationClient(Protocol):
    def get_item(self, **kwargs: object) -> Mapping[str, Any]: ...

    def put_item(self, **kwargs: object) -> Mapping[str, Any]: ...

    def query(self, **kwargs: object) -> Mapping[str, Any]: ...

    def delete_item(self, **kwargs: object) -> Mapping[str, Any]: ...

    def transact_write_items(self, **kwargs: object) -> Mapping[str, Any]: ...


class AwsConfigurationStore:
    """Immutable S3 bodies with DynamoDB conditional metadata and pointers."""

    def __init__(
        self,
        *,
        bucket: str,
        table_name: str,
        revision_index_name: str,
        prefix: str = "justflow/configuration/",
        region: str | None = None,
        endpoint_url: str | None = None,
        expected_bucket_owner: str | None = None,
        server_side_encryption: str = "AES256",
        kms_key_id: str | None = None,
        s3_client: S3ConfigurationClient | None = None,
        dynamodb_client: DynamoConfigurationClient | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        validate_s3_bucket(bucket)
        if AWS_TABLE_NAME_PATTERN.fullmatch(table_name) is None:
            raise ValueError("DynamoDB configuration table name is invalid")
        if AWS_TABLE_NAME_PATTERN.fullmatch(revision_index_name) is None:
            raise ValueError("DynamoDB configuration revision index name is invalid")
        if server_side_encryption not in {"AES256", "aws:kms"}:
            raise ValueError("S3 configuration encryption mode is invalid")
        if (server_side_encryption == "aws:kms") != (kms_key_id is not None):
            raise ValueError("S3 configuration KMS key requires aws:kms encryption")
        self._bucket = bucket
        self._table_name = table_name
        self._revision_index_name = revision_index_name
        self._prefix = normalize_s3_catalog_prefix(prefix)
        self._region = region
        self._endpoint_url = endpoint_url
        self._expected_bucket_owner = expected_bucket_owner
        self._server_side_encryption = server_side_encryption
        self._kms_key_id = kms_key_id
        self._s3_client = s3_client
        self._dynamodb_client = dynamodb_client
        self._clock = clock

    def read_draft(self, scope: RuntimeScope) -> DraftRecord | None:
        item = self._get_metadata(scope, self._draft_key())
        if item is None:
            return None
        self._require_record(item, scope, RECORD_TYPE_DRAFT)
        return DraftRecord(
            scope_digest=scope.digest,
            version=_number(item, "version"),
            bundle=self._read_bundle(scope, _string(item, "body_key")),
        )

    def compare_and_swap_draft(
        self,
        scope: RuntimeScope,
        bundle: ConfigurationDocument,
        *,
        expected_version: int | None,
    ) -> DraftRecord:
        body = ConfigurationEnvelope(scope_digest=scope.digest, bundle=bundle).canonical_bytes()
        body_key = self._draft_body_key(scope, body)
        self._put_immutable_body(body_key, body)
        version = 1 if expected_version is None else expected_version + 1
        item = {
            **self._metadata_key(scope, self._draft_key()),
            "record_type": {"S": RECORD_TYPE_DRAFT},
            "scope_digest": {"S": scope.digest},
            "version": {"N": str(version)},
            "body_key": {"S": body_key},
        }
        condition = (
            "attribute_not_exists(metadata_key)"
            if expected_version is None
            else "version = :expected_version"
        )
        values = (
            None
            if expected_version is None
            else {":expected_version": {"N": str(expected_version)}}
        )
        self._conditional_put(
            item,
            condition=condition,
            values=values,
            conflict_identity=scope.digest,
        )
        return DraftRecord(scope_digest=scope.digest, version=version, bundle=bundle)

    def create_revision(
        self,
        scope: RuntimeScope,
        bundle: ConfigurationDocument,
        *,
        parent_revision_id: RevisionIdentity | None,
    ) -> RevisionRecord:
        revision_id = configuration_revision_identity(scope.digest, bundle, parent_revision_id)
        body = ConfigurationEnvelope(scope_digest=scope.digest, bundle=bundle).canonical_bytes()
        body_key = self._revision_body_key(scope, revision_id)
        self._put_immutable_body(body_key, body)
        created_at = self._timestamp()
        item = {
            **self._metadata_key(scope, self._revision_key(revision_id)),
            "record_type": {"S": RECORD_TYPE_REVISION},
            "scope_digest": {"S": scope.digest},
            "revision_id": {"S": str(revision_id)},
            "revision_order": {"S": f"{created_at}#{revision_id}"},
            "created_at": {"S": created_at},
            "body_key": {"S": body_key},
            CHILD_REFERENCE_COUNT_FIELD: {"N": "0"},
        }
        if parent_revision_id is not None:
            item["parent_revision_id"] = {"S": str(parent_revision_id)}
        try:
            if parent_revision_id is None:
                self._conditional_put(
                    item,
                    condition="attribute_not_exists(metadata_key)",
                    values=None,
                    conflict_identity=str(revision_id),
                )
            else:
                self._create_child_revision_metadata(
                    scope,
                    item,
                    revision_id=revision_id,
                    parent_revision_id=parent_revision_id,
                )
        except ConfigurationConflictError:
            existing = self._get_metadata(scope, self._revision_key(revision_id))
            if existing is not None:
                if _string(existing, "record_type") == RECORD_TYPE_REVISION_CLEANUP:
                    raise
                return self.read_revision(scope, revision_id)
            if parent_revision_id is not None:
                self.read_revision(scope, parent_revision_id)
            raise
        return RevisionRecord(
            scope_digest=scope.digest,
            revision_id=revision_id,
            parent_revision_id=parent_revision_id,
            created_at=_datetime(created_at),
            bundle=bundle,
        )

    def read_revision(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
    ) -> RevisionRecord:
        item = self._get_metadata(scope, self._revision_key(revision_id))
        if item is None:
            raise ConfigurationNotFoundError("Configuration revision was not found")
        self._require_record(item, scope, RECORD_TYPE_REVISION)
        stored_revision_id = _revision(_string(item, "revision_id"))
        if stored_revision_id != revision_id:
            raise ConfigurationIntegrityError("Configuration revision identity disagrees")
        parent_revision_id = _optional_revision(item, "parent_revision_id")
        bundle = self._read_bundle(scope, _string(item, "body_key"))
        if configuration_revision_identity(scope.digest, bundle, parent_revision_id) != revision_id:
            raise ConfigurationIntegrityError("Configuration revision digest disagrees")
        return RevisionRecord(
            scope_digest=scope.digest,
            revision_id=revision_id,
            parent_revision_id=parent_revision_id,
            created_at=_datetime(_string(item, "created_at")),
            bundle=bundle,
        )

    def list_revisions(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> RevisionPage:
        if not 1 <= limit <= MAX_REVISION_PAGE_SIZE:
            raise ConfigurationLimitError("Configuration revision page size is invalid")
        exclusive_start_key = self._decode_dynamo_cursor(scope, cursor)
        request: dict[str, object] = {
            "TableName": self._table_name,
            "IndexName": self._revision_index_name,
            "KeyConditionExpression": "scope_digest = :scope_digest",
            "ExpressionAttributeValues": {":scope_digest": {"S": scope.digest}},
            "ScanIndexForward": False,
            "Limit": limit,
        }
        if exclusive_start_key is not None:
            request["ExclusiveStartKey"] = exclusive_start_key
        try:
            response = self._dynamodb().query(**request)
        except Exception as exc:
            raise ConfigurationUnavailableError(
                "Cannot list DynamoDB configuration revisions"
            ) from exc
        items = response.get("Items", [])
        if not isinstance(items, list) or len(items) > limit:
            raise ConfigurationIntegrityError("DynamoDB revision page is invalid")
        revisions = tuple(self._revision_summary(scope, item) for item in items)
        last_key = response.get("LastEvaluatedKey")
        next_cursor = self._encode_dynamo_cursor(scope, last_key) if last_key is not None else None
        return RevisionPage(revisions=revisions, next_cursor=next_cursor)

    def read_active(self, scope: RuntimeScope) -> ActivePointer | None:
        item = self._get_metadata(scope, self._active_key())
        if item is None:
            return None
        self._require_record(item, scope, RECORD_TYPE_ACTIVE)
        return ActivePointer(
            scope_digest=scope.digest,
            revision_id=_revision(_string(item, "revision_id")),
            version=_number(item, "version"),
        )

    def compare_and_swap_active(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
        *,
        expected_revision_id: RevisionIdentity | None,
        expected_version: int | None = None,
    ) -> ActivePointer:
        self.read_revision(scope, revision_id)
        current = self.read_active(scope)
        current_revision_id = current.revision_id if current is not None else None
        current_version = current.version if current is not None else None
        if current_revision_id != expected_revision_id or (
            expected_version is not None and current_version != expected_version
        ):
            raise ConfigurationConflictError(scope.digest)
        version = 1 if current is None else current.version + 1
        item = {
            **self._metadata_key(scope, self._active_key()),
            "record_type": {"S": RECORD_TYPE_ACTIVE},
            "scope_digest": {"S": scope.digest},
            "revision_id": {"S": str(revision_id)},
            "version": {"N": str(version)},
        }
        if expected_revision_id is None:
            condition = "attribute_not_exists(metadata_key)"
            values = None
        else:
            condition = "revision_id = :expected_revision_id"
            values = {":expected_revision_id": {"S": str(expected_revision_id)}}
            if expected_version is not None:
                condition += " AND version = :expected_version"
                values[":expected_version"] = {"N": str(expected_version)}
        put: dict[str, object] = {
            "TableName": self._table_name,
            "Item": item,
            "ConditionExpression": condition,
        }
        if values is not None:
            put["ExpressionAttributeValues"] = values
        self._transact_write(
            (
                {
                    "ConditionCheck": {
                        "TableName": self._table_name,
                        "Key": self._metadata_key(scope, self._revision_key(revision_id)),
                        "ConditionExpression": (
                            "attribute_exists(metadata_key) AND record_type = :revision_record"
                        ),
                        "ExpressionAttributeValues": {
                            ":revision_record": {"S": RECORD_TYPE_REVISION}
                        },
                    }
                },
                {"Put": put},
            ),
            conflict_identity=scope.digest,
        )
        return ActivePointer(
            scope_digest=scope.digest,
            revision_id=revision_id,
            version=version,
        )

    def retain_revisions(
        self,
        scope: RuntimeScope,
        *,
        keep_latest: int,
        delete_limit: int,
    ) -> RetentionResult:
        if keep_latest < 1 or not 1 <= delete_limit <= MAX_RETENTION_DELETE_COUNT:
            raise ConfigurationLimitError("Configuration retention bounds are invalid")
        items = self._all_revision_items(scope)
        referenced = self._validate_child_reference_counts(items)
        active = self.read_active(scope)
        protected = {str(active.revision_id)} if active is not None else set()
        protected.update(referenced)
        candidates = [
            item for item in items[keep_latest:] if _string(item, "revision_id") not in protected
        ][:delete_limit]
        deleted: list[RevisionIdentity] = []
        for item in candidates:
            revision_id = _revision(_string(item, "revision_id"))
            self._delete_revision_metadata(
                scope,
                revision_id,
                parent_revision_id=_optional_revision(item, "parent_revision_id"),
            )
            self.cleanup_orphaned_revision_body(scope, revision_id)
            deleted.append(revision_id)
        return RetentionResult(deleted_revision_ids=tuple(deleted))

    def cleanup_orphaned_revision_body(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
    ) -> bool:
        item = self._get_metadata(scope, self._revision_key(revision_id))
        if item is not None:
            self._require_record(
                item,
                scope,
                RECORD_TYPE_REVISION_CLEANUP,
            )
            if _revision(_string(item, "revision_id")) != revision_id:
                raise ConfigurationIntegrityError("Configuration cleanup marker identity disagrees")
        active = self.read_active(scope)
        if active is not None and active.revision_id == revision_id:
            return False
        if item is None and not self._acquire_cleanup_marker(scope, revision_id):
            return False
        try:
            self._s3().delete_object(**self._s3_key(self._revision_body_key(scope, revision_id)))
        except Exception as exc:
            self._release_cleanup_marker(scope, revision_id)
            raise ConfigurationUnavailableError(
                "Cannot clean up an orphaned S3 configuration body"
            ) from exc
        self._release_cleanup_marker(scope, revision_id)
        return True

    def _all_revision_items(self, scope: RuntimeScope) -> list[Mapping[str, Any]]:
        try:
            response = self._dynamodb().query(
                TableName=self._table_name,
                IndexName=self._revision_index_name,
                KeyConditionExpression="scope_digest = :scope_digest",
                ExpressionAttributeValues={":scope_digest": {"S": scope.digest}},
                ScanIndexForward=False,
                Limit=AWS_CONFIGURATION_STORE_MAX_QUERY_ITEMS + 1,
            )
        except Exception as exc:
            raise ConfigurationUnavailableError(
                "Cannot scan DynamoDB configuration retention candidates"
            ) from exc
        items = response.get("Items", [])
        if not isinstance(items, list) or len(items) > AWS_CONFIGURATION_STORE_MAX_QUERY_ITEMS:
            raise ConfigurationLimitError("Configuration retention scan exceeds its bound")
        if response.get("LastEvaluatedKey") is not None:
            raise ConfigurationLimitError("Configuration retention scan exceeds its bound")
        return [self._checked_item(item) for item in items]

    @staticmethod
    def _validate_child_reference_counts(
        items: list[Mapping[str, Any]],
    ) -> set[str]:
        counts = {_string(item, "revision_id"): 0 for item in items}
        for item in items:
            parent_revision_id = _optional_string(item, "parent_revision_id")
            if parent_revision_id is None:
                continue
            if parent_revision_id not in counts:
                raise ConfigurationIntegrityError(
                    "Configuration revision parent metadata is missing"
                )
            counts[parent_revision_id] += 1
        for item in items:
            revision_id = _string(item, "revision_id")
            if _nonnegative_number(item, CHILD_REFERENCE_COUNT_FIELD) != counts[revision_id]:
                raise ConfigurationIntegrityError(
                    "Configuration revision child reference count disagrees"
                )
        return {revision_id for revision_id, count in counts.items() if count > 0}

    def _create_child_revision_metadata(
        self,
        scope: RuntimeScope,
        item: Mapping[str, Any],
        *,
        revision_id: RevisionIdentity,
        parent_revision_id: RevisionIdentity,
    ) -> None:
        self._transact_write(
            (
                {
                    "Update": {
                        "TableName": self._table_name,
                        "Key": self._metadata_key(
                            scope,
                            self._revision_key(parent_revision_id),
                        ),
                        "UpdateExpression": f"ADD {CHILD_REFERENCE_COUNT_FIELD} :increment",
                        "ConditionExpression": (
                            "attribute_exists(metadata_key) AND record_type = :revision_record"
                        ),
                        "ExpressionAttributeValues": {
                            ":increment": {"N": "1"},
                            ":revision_record": {"S": RECORD_TYPE_REVISION},
                        },
                    }
                },
                {
                    "Put": {
                        "TableName": self._table_name,
                        "Item": item,
                        "ConditionExpression": "attribute_not_exists(metadata_key)",
                    }
                },
            ),
            conflict_identity=str(revision_id),
        )

    def _acquire_cleanup_marker(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
    ) -> bool:
        marker = {
            **self._metadata_key(scope, self._revision_key(revision_id)),
            "record_type": {"S": RECORD_TYPE_REVISION_CLEANUP},
            "scope_digest": {"S": scope.digest},
            "revision_id": {"S": str(revision_id)},
        }
        try:
            self._transact_write(
                (
                    {
                        "ConditionCheck": {
                            "TableName": self._table_name,
                            "Key": self._metadata_key(scope, self._active_key()),
                            "ConditionExpression": (
                                "attribute_not_exists(metadata_key) OR revision_id <> :revision_id"
                            ),
                            "ExpressionAttributeValues": {":revision_id": {"S": str(revision_id)}},
                        }
                    },
                    {
                        "Put": {
                            "TableName": self._table_name,
                            "Item": marker,
                            "ConditionExpression": "attribute_not_exists(metadata_key)",
                        }
                    },
                ),
                conflict_identity=str(revision_id),
            )
        except ConfigurationConflictError:
            return False
        return True

    def _release_cleanup_marker(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
    ) -> None:
        try:
            self._dynamodb().delete_item(
                TableName=self._table_name,
                Key=self._metadata_key(scope, self._revision_key(revision_id)),
                ConditionExpression="record_type = :cleanup_record",
                ExpressionAttributeValues={":cleanup_record": {"S": RECORD_TYPE_REVISION_CLEANUP}},
            )
        except Exception as exc:
            if _aws_error_code(exc) in AWS_CONDITIONAL_ERROR_CODES:
                if self._get_metadata(scope, self._revision_key(revision_id)) is None:
                    return
                raise ConfigurationConflictError(str(revision_id)) from exc
            raise ConfigurationUnavailableError(
                "Cannot release DynamoDB configuration cleanup marker"
            ) from exc

    def _delete_revision_metadata(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
        *,
        parent_revision_id: RevisionIdentity | None,
    ) -> None:
        operations: list[Mapping[str, object]] = [
            {
                "ConditionCheck": {
                    "TableName": self._table_name,
                    "Key": self._metadata_key(scope, self._active_key()),
                    "ConditionExpression": (
                        "attribute_not_exists(metadata_key) OR revision_id <> :revision_id"
                    ),
                    "ExpressionAttributeValues": {":revision_id": {"S": str(revision_id)}},
                }
            },
            {
                "Delete": {
                    "TableName": self._table_name,
                    "Key": self._metadata_key(scope, self._revision_key(revision_id)),
                    "ConditionExpression": (
                        "attribute_exists(metadata_key) "
                        "AND record_type = :revision_record "
                        f"AND {CHILD_REFERENCE_COUNT_FIELD} = :zero"
                    ),
                    "ExpressionAttributeValues": {
                        ":revision_record": {"S": RECORD_TYPE_REVISION},
                        ":zero": {"N": "0"},
                    },
                }
            },
        ]
        if parent_revision_id is not None:
            operations.append(
                {
                    "Update": {
                        "TableName": self._table_name,
                        "Key": self._metadata_key(
                            scope,
                            self._revision_key(parent_revision_id),
                        ),
                        "UpdateExpression": (f"ADD {CHILD_REFERENCE_COUNT_FIELD} :decrement"),
                        "ConditionExpression": (
                            "attribute_exists(metadata_key) "
                            "AND record_type = :revision_record "
                            f"AND {CHILD_REFERENCE_COUNT_FIELD} > :zero"
                        ),
                        "ExpressionAttributeValues": {
                            ":decrement": {"N": "-1"},
                            ":revision_record": {"S": RECORD_TYPE_REVISION},
                            ":zero": {"N": "0"},
                        },
                    }
                }
            )
        self._transact_write(operations, conflict_identity=str(revision_id))

    def _revision_summary(
        self,
        scope: RuntimeScope,
        raw_item: object,
    ) -> RevisionSummary:
        item = self._checked_item(raw_item)
        self._require_record(item, scope, RECORD_TYPE_REVISION)
        return RevisionSummary(
            scope_digest=scope.digest,
            revision_id=_revision(_string(item, "revision_id")),
            parent_revision_id=_optional_revision(item, "parent_revision_id"),
            created_at=_datetime(_string(item, "created_at")),
        )

    def _get_metadata(
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
            raise ConfigurationUnavailableError(
                "Cannot read DynamoDB configuration metadata"
            ) from exc
        item = response.get("Item")
        return None if item is None else self._checked_item(item)

    def _conditional_put(
        self,
        item: Mapping[str, Any],
        *,
        condition: str,
        values: Mapping[str, Any] | None,
        conflict_identity: str,
    ) -> None:
        request: dict[str, object] = {
            "TableName": self._table_name,
            "Item": item,
            "ConditionExpression": condition,
        }
        if values is not None:
            request["ExpressionAttributeValues"] = values
        try:
            self._dynamodb().put_item(**request)
        except Exception as exc:
            if _aws_error_code(exc) in AWS_CONDITIONAL_ERROR_CODES:
                raise ConfigurationConflictError(conflict_identity) from exc
            raise ConfigurationUnavailableError(
                "Cannot conditionally update DynamoDB configuration metadata"
            ) from exc

    def _transact_write(
        self,
        operations: tuple[Mapping[str, object], ...] | list[Mapping[str, object]],
        *,
        conflict_identity: str,
    ) -> None:
        try:
            self._dynamodb().transact_write_items(TransactItems=list(operations))
        except Exception as exc:
            if _aws_error_code(exc) in AWS_CONDITIONAL_ERROR_CODES:
                raise ConfigurationConflictError(conflict_identity) from exc
            raise ConfigurationUnavailableError(
                "Cannot atomically update DynamoDB configuration metadata"
            ) from exc

    def _put_immutable_body(self, key: str, body: bytes) -> None:
        request: dict[str, object] = {
            **self._s3_key(key),
            "Body": body,
            "ContentType": AWS_CONFIGURATION_CONTENT_TYPE,
            "IfNoneMatch": "*",
            "ServerSideEncryption": self._server_side_encryption,
        }
        if self._kms_key_id is not None:
            request["SSEKMSKeyId"] = self._kms_key_id
        try:
            self._s3().put_object(**request)
        except Exception as exc:
            if _aws_error_code(exc) in AWS_CONDITIONAL_ERROR_CODES:
                existing = self._read_body(key)
                if existing == body:
                    return
                raise ConfigurationIntegrityError(
                    "Immutable S3 configuration body disagrees"
                ) from exc
            raise ConfigurationUnavailableError(
                "Cannot create immutable S3 configuration body"
            ) from exc

    def _read_bundle(self, scope: RuntimeScope, key: str) -> ConfigurationDocument:
        body_kind, body_digest = self._validate_body_key(scope, key)
        payload = self._read_body(key)
        try:
            envelope = ConfigurationEnvelope.model_validate_json(payload)
        except (ValueError, TypeError) as exc:
            raise ConfigurationIntegrityError("S3 configuration body is invalid") from exc
        if envelope.scope_digest != scope.digest:
            raise ConfigurationIntegrityError("S3 configuration body has the wrong scope")
        if payload != envelope.canonical_bytes():
            raise ConfigurationIntegrityError("S3 configuration body is not canonical")
        if body_kind == "drafts" and hashlib.sha256(payload).hexdigest() != body_digest:
            raise ConfigurationIntegrityError("S3 configuration draft digest disagrees")
        return envelope.bundle

    def _read_body(self, key: str) -> bytes:
        try:
            response = self._s3().get_object(**self._s3_key(key))
        except Exception as exc:
            if _aws_error_code(exc) in AWS_NOT_FOUND_ERROR_CODES:
                raise ConfigurationNotFoundError(
                    "Configuration revision body was not found"
                ) from exc
            raise ConfigurationUnavailableError("Cannot read S3 configuration body") from exc
        try:
            return read_bounded_body(response.get("Body"), limit=MAX_CONFIGURATION_ENVELOPE_BYTES)
        except BoundedReadLimitError as exc:
            raise ConfigurationLimitError("S3 configuration body exceeds its byte limit") from exc
        except BoundedReadError as exc:
            raise ConfigurationIntegrityError("S3 configuration body has an invalid type") from exc
        except Exception as exc:
            raise ConfigurationUnavailableError(
                "Cannot read or close S3 configuration body"
            ) from exc

    def _require_record(
        self,
        item: Mapping[str, Any],
        scope: RuntimeScope,
        record_type: str,
    ) -> None:
        if (
            _string(item, "scope_digest") != scope.digest
            or _string(item, "record_type") != record_type
        ):
            raise ConfigurationIntegrityError("DynamoDB configuration metadata is inconsistent")

    def _metadata_key(self, scope: RuntimeScope, key: str) -> dict[str, dict[str, str]]:
        return {
            "scope_key": {"S": f"scope#{scope.digest}"},
            "metadata_key": {"S": key},
        }

    def _s3_key(self, key: str) -> dict[str, str]:
        request = {"Bucket": self._bucket, "Key": key}
        if self._expected_bucket_owner is not None:
            request["ExpectedBucketOwner"] = self._expected_bucket_owner
        return request

    def _draft_body_key(self, scope: RuntimeScope, body: bytes) -> str:
        digest = hashlib.sha256(body).hexdigest()
        return self._object_key(scope, f"drafts/{digest}.json")

    def _revision_body_key(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
    ) -> str:
        return self._object_key(scope, f"revisions/{revision_id}.json")

    def _object_key(self, scope: RuntimeScope, suffix: str) -> str:
        key = f"{self._prefix}scopes/{scope.digest}/{suffix}"
        if len(key.encode("utf-8")) > AWS_CONFIGURATION_KEY_MAX_BYTES:
            raise ConfigurationLimitError("S3 configuration object key exceeds its byte limit")
        return key

    def _validate_body_key(self, scope: RuntimeScope, key: str) -> tuple[str, str]:
        scope_prefix = self._object_key(scope, "")
        if not key.startswith(scope_prefix):
            raise ConfigurationIntegrityError("S3 configuration body key has the wrong scope")
        match = AWS_CONFIGURATION_BODY_SUFFIX_PATTERN.fullmatch(key.removeprefix(scope_prefix))
        if match is None:
            raise ConfigurationIntegrityError("S3 configuration body key is invalid")
        return match.group(1), match.group(2)

    @staticmethod
    def _draft_key() -> str:
        return "DRAFT"

    @staticmethod
    def _active_key() -> str:
        return "ACTIVE"

    @staticmethod
    def _revision_key(revision_id: RevisionIdentity) -> str:
        return f"REV#{revision_id}"

    def _timestamp(self) -> str:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ConfigurationIntegrityError("Configuration store clock must be timezone-aware")
        return value.astimezone(UTC).isoformat()

    def _encode_dynamo_cursor(self, scope: RuntimeScope, value: object) -> str:
        item = self._checked_item(value)
        return encode_scope_cursor(scope, item)

    def _decode_dynamo_cursor(
        self,
        scope: RuntimeScope,
        cursor: str | None,
    ) -> Mapping[str, Any] | None:
        if cursor is None:
            return None
        try:
            return self._checked_item(decode_scope_cursor(scope, cursor))
        except ValueError as exc:
            raise ConfigurationScopeError("Configuration cursor is invalid for this scope") from exc

    @staticmethod
    def _checked_item(value: object) -> Mapping[str, Any]:
        if not isinstance(value, dict) or len(value) > 16:
            raise ConfigurationIntegrityError("DynamoDB configuration item is invalid")
        return value

    def _s3(self) -> S3ConfigurationClient:
        if self._s3_client is None:
            boto3 = load_optional_dependency("boto3", extra="aws", feature="AWS configuration")
            self._s3_client = cast(
                S3ConfigurationClient,
                boto3.client("s3", region_name=self._region, endpoint_url=self._endpoint_url),
            )
        return self._s3_client

    def _dynamodb(self) -> DynamoConfigurationClient:
        if self._dynamodb_client is None:
            boto3 = load_optional_dependency("boto3", extra="aws", feature="AWS configuration")
            self._dynamodb_client = cast(
                DynamoConfigurationClient,
                boto3.client(
                    "dynamodb",
                    region_name=self._region,
                    endpoint_url=self._endpoint_url,
                ),
            )
        return self._dynamodb_client


def _string(item: Mapping[str, Any], key: str) -> str:
    value = item.get(key)
    if not isinstance(value, dict) or set(value) != {"S"} or not isinstance(value["S"], str):
        raise ConfigurationIntegrityError("DynamoDB configuration string is invalid")
    return value["S"]


def _optional_string(item: Mapping[str, Any], key: str) -> str | None:
    return None if key not in item else _string(item, key)


def _optional_revision(
    item: Mapping[str, Any],
    key: str,
) -> RevisionIdentity | None:
    value = _optional_string(item, key)
    return _revision(value) if value is not None else None


def _revision(value: str) -> RevisionIdentity:
    try:
        return RevisionIdentity(value)
    except ValueError as exc:
        raise ConfigurationIntegrityError(
            "DynamoDB configuration revision identity is invalid"
        ) from exc


def _datetime(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ConfigurationIntegrityError("DynamoDB configuration timestamp is invalid") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ConfigurationIntegrityError("DynamoDB configuration timestamp is invalid")
    return result


def _number(item: Mapping[str, Any], key: str) -> int:
    result = _nonnegative_number(item, key)
    if result < 1:
        raise ConfigurationIntegrityError("DynamoDB configuration number is invalid")
    return result


def _nonnegative_number(item: Mapping[str, Any], key: str) -> int:
    value = item.get(key)
    if not isinstance(value, dict) or set(value) != {"N"} or not isinstance(value["N"], str):
        raise ConfigurationIntegrityError("DynamoDB configuration number is invalid")
    try:
        result = int(value["N"])
    except ValueError as exc:
        raise ConfigurationIntegrityError("DynamoDB configuration number is invalid") from exc
    if result < 0:
        raise ConfigurationIntegrityError("DynamoDB configuration number is invalid")
    return result


def _aws_error_code(exc: Exception) -> str | None:
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return None
    error = response.get("Error")
    if not isinstance(error, Mapping):
        return None
    code = error.get("Code")
    return str(code) if code is not None else None
