"""SQLite publication and activation state for a coordinated local host."""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

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
from justflow.configuration.lifecycle import ConfigurationDiscardRecord
from justflow.scope import RuntimeScope, decode_scope_cursor, encode_scope_cursor


class SqliteActivationStore:
    """Conditional activation records sharing no mutable runtime state."""

    def __init__(self, path: str | Path) -> None:
        self._lock = threading.RLock()
        self._path = str(path)
        try:
            if self._path != ":memory:":
                Path(self._path).parent.mkdir(parents=True, exist_ok=True)
            self._connection = sqlite3.connect(
                self._path, isolation_level=None, check_same_thread=False
            )
            self._initialize()
        except (OSError, sqlite3.Error) as exc:
            raise ActivationUnavailableError(
                "Cannot initialize the SQLite activation store"
            ) from exc

    def close(self) -> None:
        with self._lock:
            try:
                self._connection.close()
            except sqlite3.Error as exc:
                raise ActivationUnavailableError(
                    "Cannot close the SQLite activation store"
                ) from exc

    def create_discard(
        self,
        scope: RuntimeScope,
        record: ConfigurationDiscardRecord,
    ) -> ConfigurationDiscardRecord:
        with self._lock:
            self._require_scope(scope, record.scope_digest)
            payload = _record_bytes(record)
            try:
                with self._transaction():
                    row = self._connection.execute(
                        """
                        SELECT payload FROM configuration_discards
                        WHERE scope_digest = ? AND discard_id = ?
                        """,
                        (scope.digest, record.discard_id),
                    ).fetchone()
                    if row is not None:
                        existing = _discard(row[0], scope)
                        if (
                            existing.request_digest != record.request_digest
                            or existing.idempotency_key_digest != record.idempotency_key_digest
                        ):
                            raise ActivationConflictError(record.discard_id)
                        return existing
                    self._connection.execute(
                        """
                        INSERT INTO configuration_discards(
                            scope_digest, discard_id, version, created_at, updated_at, payload
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            scope.digest,
                            record.discard_id,
                            record.version,
                            record.created_at.isoformat(),
                            record.updated_at.isoformat(),
                            payload,
                        ),
                    )
            except ActivationConflictError:
                raise
            except sqlite3.Error as exc:
                raise ActivationUnavailableError("Cannot create the SQLite discard record") from exc
            return record

    def read_discard(
        self,
        scope: RuntimeScope,
        discard_id: str,
    ) -> ConfigurationDiscardRecord:
        with self._lock:
            row = self._fetchone(
                """
                SELECT payload FROM configuration_discards
                WHERE scope_digest = ? AND discard_id = ?
                """,
                (scope.digest, discard_id),
            )
            if row is None:
                raise ActivationNotFoundError("Configuration discard was not found")
            record = _discard(row[0], scope)
            if record.discard_id != discard_id:
                raise ActivationIntegrityError("Stored discard identity disagrees")
            return record

    def update_discard(
        self,
        scope: RuntimeScope,
        record: ConfigurationDiscardRecord,
        *,
        expected_version: int,
    ) -> ConfigurationDiscardRecord:
        with self._lock:
            self._require_scope(scope, record.scope_digest)
            if record.version != expected_version + 1:
                raise ActivationIntegrityError("Discard version transition is invalid")
            try:
                cursor = self._connection.execute(
                    """
                    UPDATE configuration_discards SET version = ?, updated_at = ?, payload = ?
                    WHERE scope_digest = ? AND discard_id = ? AND version = ?
                    """,
                    (
                        record.version,
                        record.updated_at.isoformat(),
                        _record_bytes(record),
                        scope.digest,
                        record.discard_id,
                        expected_version,
                    ),
                )
            except sqlite3.Error as exc:
                raise ActivationUnavailableError("Cannot update the SQLite discard record") from exc
            if cursor.rowcount != 1:
                raise ActivationConflictError(record.discard_id)
            return record

    def create_publication(
        self,
        scope: RuntimeScope,
        record: PublicationRecord,
    ) -> PublicationRecord:
        with self._lock:
            self._require_scope(scope, record.scope_digest)
            payload = _record_bytes(record)
            try:
                with self._transaction():
                    row = self._connection.execute(
                        """
                        SELECT payload FROM activation_publications
                        WHERE scope_digest = ? AND publication_id = ?
                        """,
                        (scope.digest, record.publication_id),
                    ).fetchone()
                    if row is not None:
                        existing = _publication(row[0], scope)
                        if (
                            existing.request_digest != record.request_digest
                            or existing.idempotency_key_digest != record.idempotency_key_digest
                        ):
                            raise ActivationConflictError(record.publication_id)
                        return existing
                    self._connection.execute(
                        """
                        INSERT INTO activation_publications(
                            scope_digest, publication_id, version, created_at, payload
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            scope.digest,
                            record.publication_id,
                            record.version,
                            record.created_at.isoformat(),
                            payload,
                        ),
                    )
            except ActivationConflictError:
                raise
            except sqlite3.Error as exc:
                raise ActivationUnavailableError(
                    "Cannot create the SQLite publication record"
                ) from exc
            return record

    def read_publication(
        self,
        scope: RuntimeScope,
        publication_id: str,
    ) -> PublicationRecord:
        with self._lock:
            row = self._fetchone(
                """
                SELECT payload FROM activation_publications
                WHERE scope_digest = ? AND publication_id = ?
                """,
                (scope.digest, publication_id),
            )
            if row is None:
                raise ActivationNotFoundError("Configuration publication was not found")
            record = _publication(row[0], scope)
            if record.publication_id != publication_id:
                raise ActivationIntegrityError("Stored publication identity disagrees")
            return record

    def update_publication(
        self,
        scope: RuntimeScope,
        record: PublicationRecord,
        *,
        expected_version: int,
    ) -> PublicationRecord:
        with self._lock:
            self._require_scope(scope, record.scope_digest)
            if record.version != expected_version + 1:
                raise ActivationIntegrityError("Publication version transition is invalid")
            try:
                cursor = self._connection.execute(
                    """
                    UPDATE activation_publications SET version = ?, payload = ?
                    WHERE scope_digest = ? AND publication_id = ? AND version = ?
                    """,
                    (
                        record.version,
                        _record_bytes(record),
                        scope.digest,
                        record.publication_id,
                        expected_version,
                    ),
                )
            except sqlite3.Error as exc:
                raise ActivationUnavailableError(
                    "Cannot update the SQLite publication record"
                ) from exc
            if cursor.rowcount != 1:
                raise ActivationConflictError(record.publication_id)
            return record

    def create_activation(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
    ) -> ActivationRecord:
        with self._lock:
            self._require_scope(scope, record.scope_digest)
            payload = _record_bytes(record)
            try:
                with self._transaction():
                    row = self._connection.execute(
                        """
                        SELECT payload FROM activation_records
                        WHERE scope_digest = ? AND activation_id = ?
                        """,
                        (scope.digest, record.activation_id),
                    ).fetchone()
                    if row is not None:
                        existing = _activation(row[0], scope)
                        if (
                            existing.request_digest != record.request_digest
                            or existing.idempotency_key_digest != record.idempotency_key_digest
                        ):
                            raise ActivationConflictError(record.activation_id)
                        return existing
                    self._connection.execute(
                        """
                        INSERT INTO activation_records(
                            scope_digest, activation_id, version, created_at, updated_at, payload
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            scope.digest,
                            record.activation_id,
                            record.version,
                            record.created_at.isoformat(),
                            record.updated_at.isoformat(),
                            payload,
                        ),
                    )
            except ActivationConflictError:
                raise
            except sqlite3.Error as exc:
                raise ActivationUnavailableError(
                    "Cannot create the SQLite activation record"
                ) from exc
            return record

    def read_activation(
        self,
        scope: RuntimeScope,
        activation_id: str,
    ) -> ActivationRecord:
        with self._lock:
            row = self._fetchone(
                """
                SELECT payload FROM activation_records
                WHERE scope_digest = ? AND activation_id = ?
                """,
                (scope.digest, activation_id),
            )
            if row is None:
                raise ActivationNotFoundError("Configuration activation was not found")
            record = _activation(row[0], scope)
            if record.activation_id != activation_id:
                raise ActivationIntegrityError("Stored activation identity disagrees")
            return record

    def update_activation(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        *,
        expected_version: int,
    ) -> ActivationRecord:
        with self._lock:
            self._require_scope(scope, record.scope_digest)
            if record.version != expected_version + 1:
                raise ActivationIntegrityError("Activation version transition is invalid")
            try:
                cursor = self._connection.execute(
                    """
                    UPDATE activation_records SET version = ?, updated_at = ?, payload = ?
                    WHERE scope_digest = ? AND activation_id = ? AND version = ?
                    """,
                    (
                        record.version,
                        record.updated_at.isoformat(),
                        _record_bytes(record),
                        scope.digest,
                        record.activation_id,
                        expected_version,
                    ),
                )
            except sqlite3.Error as exc:
                raise ActivationUnavailableError(
                    "Cannot update the SQLite activation record"
                ) from exc
            if cursor.rowcount != 1:
                raise ActivationConflictError(record.activation_id)
            return record

    def list_activations(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ActivationPage:
        with self._lock:
            if not 1 <= limit <= MAX_ACTIVATION_PAGE_SIZE:
                raise ActivationLimitError("Activation page size is invalid")
            position = _activation_position(scope, cursor)
            parameters: tuple[object, ...]
            if position is None:
                where = "scope_digest = ?"
                parameters = (scope.digest, limit + 1)
            else:
                where = "scope_digest = ? AND (created_at < ? OR (created_at = ? AND activation_id < ?))"
                parameters = (scope.digest, position[0], position[0], position[1], limit + 1)
            rows = self._fetchall(
                f"""
                SELECT activation_id, created_at, payload FROM activation_records
                WHERE {where}
                ORDER BY created_at DESC, activation_id DESC
                LIMIT ?
                """,
                parameters,
            )
            has_more = len(rows) > limit
            selected = rows[:limit]
            records = tuple(_activation(row[2], scope) for row in selected)
            next_cursor = None
            if has_more and selected:
                next_cursor = encode_scope_cursor(
                    scope,
                    [_text(selected[-1][1]), _text(selected[-1][0])],
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

    def _initialize(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS activation_publications (
                scope_digest TEXT NOT NULL,
                publication_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY(scope_digest, publication_id)
            );
            CREATE TABLE IF NOT EXISTS configuration_discards (
                scope_digest TEXT NOT NULL,
                discard_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY(scope_digest, discard_id)
            );
            CREATE TABLE IF NOT EXISTS activation_records (
                scope_digest TEXT NOT NULL,
                activation_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY(scope_digest, activation_id)
            );
            CREATE INDEX IF NOT EXISTS activation_records_order
                ON activation_records(scope_digest, created_at DESC, activation_id DESC);
            """
        )

    def _fetchone(self, query: str, parameters: tuple[object, ...]) -> tuple[object, ...] | None:
        try:
            return self._connection.execute(query, parameters).fetchone()
        except sqlite3.Error as exc:
            raise ActivationUnavailableError("Cannot read SQLite activation state") from exc

    def _fetchall(
        self,
        query: str,
        parameters: tuple[object, ...],
    ) -> list[tuple[object, ...]]:
        try:
            return self._connection.execute(query, parameters).fetchall()
        except sqlite3.Error as exc:
            raise ActivationUnavailableError("Cannot list SQLite activation state") from exc

    @staticmethod
    def _require_scope(scope: RuntimeScope, scope_digest: str) -> None:
        if scope_digest != scope.digest:
            raise ActivationIntegrityError("Activation record belongs to another runtime scope")

    def _transaction(self) -> _SqliteTransaction:
        return _SqliteTransaction(self._connection)


class _SqliteTransaction:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def __enter__(self) -> None:
        self._connection.execute("BEGIN IMMEDIATE")

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._connection.execute("ROLLBACK" if exc_type is not None else "COMMIT")


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


def _publication(value: object, scope: RuntimeScope) -> PublicationRecord:
    try:
        record = PublicationRecord.model_validate_json(_bytes(value))
    except (TypeError, ValidationError, ValueError) as exc:
        raise ActivationIntegrityError("Stored publication record is invalid") from exc
    if record.scope_digest != scope.digest or _bytes(value) != _record_bytes(record):
        raise ActivationIntegrityError("Stored publication record is inconsistent")
    return record


def _activation(value: object, scope: RuntimeScope) -> ActivationRecord:
    try:
        record = ActivationRecord.model_validate_json(_bytes(value))
    except (TypeError, ValidationError, ValueError) as exc:
        raise ActivationIntegrityError("Stored activation record is invalid") from exc
    if record.scope_digest != scope.digest or _bytes(value) != _record_bytes(record):
        raise ActivationIntegrityError("Stored activation record is inconsistent")
    return record


def _discard(value: object, scope: RuntimeScope) -> ConfigurationDiscardRecord:
    try:
        record = ConfigurationDiscardRecord.model_validate_json(_bytes(value))
    except (TypeError, ValidationError, ValueError) as exc:
        raise ActivationIntegrityError("Stored discard record is invalid") from exc
    if record.scope_digest != scope.digest or _bytes(value) != _record_bytes(record):
        raise ActivationIntegrityError("Stored discard record is inconsistent")
    return record


def _activation_position(
    scope: RuntimeScope,
    cursor: str | None,
) -> tuple[str, str] | None:
    if cursor is None:
        return None
    try:
        value = decode_scope_cursor(scope, cursor)
    except ValueError as exc:
        raise ActivationIntegrityError("Activation cursor is invalid for this scope") from exc
    if (
        not isinstance(value, list)
        or len(value) != 2
        or not all(isinstance(item, str) for item in value)
    ):
        raise ActivationIntegrityError("Activation cursor position is invalid")
    return value[0], value[1]


def _bytes(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise ActivationIntegrityError("Stored activation payload has an invalid type")
    return value


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise ActivationIntegrityError("Stored activation metadata has an invalid type")
    return value
