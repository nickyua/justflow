"""Single-host SQLite configuration store."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from justflow.configuration.errors import (
    ConfigurationConflictError,
    ConfigurationIntegrityError,
    ConfigurationLimitError,
    ConfigurationNotFoundError,
    ConfigurationScopeError,
    ConfigurationUnavailableError,
)
from justflow.configuration.models import (
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
from justflow.scope import RuntimeScope, decode_scope_cursor, encode_scope_cursor

SQLITE_SCHEMA_VERSION = 1
MAX_RETENTION_SCAN_COUNT = 10_000


class SqliteConfigurationStore:
    """Deterministic writable store for a single local host."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._lock = threading.RLock()
        self._path = str(path)
        self._clock = clock
        try:
            if self._path != ":memory:":
                Path(self._path).parent.mkdir(parents=True, exist_ok=True)
            self._connection = sqlite3.connect(
                self._path, isolation_level=None, check_same_thread=False
            )
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._initialize()
        except ConfigurationIntegrityError:
            self._connection.close()
            raise
        except (OSError, sqlite3.Error) as exc:
            raise ConfigurationUnavailableError(
                "Cannot initialize the SQLite configuration store"
            ) from exc

    def close(self) -> None:
        with self._lock:
            try:
                self._connection.close()
            except sqlite3.Error as exc:
                raise ConfigurationUnavailableError(
                    "Cannot close the SQLite configuration store"
                ) from exc

    def read_draft(self, scope: RuntimeScope) -> DraftRecord | None:
        with self._lock:
            row = self._fetchone(
                "SELECT version, body FROM configuration_drafts WHERE scope_digest = ?",
                (scope.digest,),
            )
            if row is None:
                return None
            return DraftRecord(
                scope_digest=scope.digest,
                version=_integer(row[0]),
                bundle=_bundle(row[1], scope),
            )

    def compare_and_swap_draft(
        self,
        scope: RuntimeScope,
        bundle: ConfigurationDocument,
        *,
        expected_version: int | None,
    ) -> DraftRecord:
        with self._lock:
            body = ConfigurationEnvelope(scope_digest=scope.digest, bundle=bundle).canonical_bytes()
            try:
                with self._transaction():
                    row = self._connection.execute(
                        "SELECT version FROM configuration_drafts WHERE scope_digest = ?",
                        (scope.digest,),
                    ).fetchone()
                    current_version = _integer(row[0]) if row is not None else None
                    if current_version != expected_version:
                        raise ConfigurationConflictError(scope.digest)
                    version = 1 if current_version is None else current_version + 1
                    self._connection.execute(
                        """
                        INSERT INTO configuration_drafts(scope_digest, version, body)
                        VALUES (?, ?, ?)
                        ON CONFLICT(scope_digest) DO UPDATE SET
                            version = excluded.version,
                            body = excluded.body
                        """,
                        (scope.digest, version, body),
                    )
            except ConfigurationConflictError:
                raise
            except sqlite3.Error as exc:
                raise ConfigurationUnavailableError(
                    "Cannot update the SQLite configuration draft"
                ) from exc
            return DraftRecord(scope_digest=scope.digest, version=version, bundle=bundle)

    def create_revision(
        self,
        scope: RuntimeScope,
        bundle: ConfigurationDocument,
        *,
        parent_revision_id: RevisionIdentity | None,
    ) -> RevisionRecord:
        with self._lock:
            revision_id = configuration_revision_identity(scope.digest, bundle, parent_revision_id)
            body = ConfigurationEnvelope(scope_digest=scope.digest, bundle=bundle).canonical_bytes()
            created_at = self._timestamp()
            try:
                with self._transaction():
                    if parent_revision_id is not None:
                        self._require_revision(scope, parent_revision_id)
                    row = self._connection.execute(
                        """
                        SELECT parent_revision_id, created_at, body
                        FROM configuration_revisions
                        WHERE scope_digest = ? AND revision_id = ?
                        """,
                        (scope.digest, str(revision_id)),
                    ).fetchone()
                    if row is not None:
                        expected_parent = (
                            str(parent_revision_id) if parent_revision_id is not None else None
                        )
                        if row[0] != expected_parent or _bytes(row[2]) != body:
                            raise ConfigurationIntegrityError(
                                "Immutable configuration revision content disagrees"
                            )
                        created_at = _text(row[1])
                    else:
                        self._connection.execute(
                            """
                            INSERT INTO configuration_revisions(
                                scope_digest, revision_id, parent_revision_id, created_at, body
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                scope.digest,
                                str(revision_id),
                                str(parent_revision_id) if parent_revision_id is not None else None,
                                created_at,
                                body,
                            ),
                        )
            except (ConfigurationIntegrityError, ConfigurationNotFoundError):
                raise
            except sqlite3.Error as exc:
                raise ConfigurationUnavailableError(
                    "Cannot create the SQLite configuration revision"
                ) from exc
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
        with self._lock:
            row = self._fetchone(
                """
                SELECT parent_revision_id, created_at, body
                FROM configuration_revisions
                WHERE scope_digest = ? AND revision_id = ?
                """,
                (scope.digest, str(revision_id)),
            )
            if row is None:
                raise ConfigurationNotFoundError("Configuration revision was not found")
            parent_revision_id = _revision(_text(row[0])) if row[0] is not None else None
            bundle = _bundle(row[2], scope)
            if (
                configuration_revision_identity(scope.digest, bundle, parent_revision_id)
                != revision_id
            ):
                raise ConfigurationIntegrityError("Configuration revision digest disagrees")
            return RevisionRecord(
                scope_digest=scope.digest,
                revision_id=revision_id,
                parent_revision_id=parent_revision_id,
                created_at=_datetime(_text(row[1])),
                bundle=bundle,
            )

    def list_revisions(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> RevisionPage:
        with self._lock:
            if not 1 <= limit <= MAX_REVISION_PAGE_SIZE:
                raise ConfigurationLimitError("Configuration revision page size is invalid")
            position = _revision_position(scope, cursor)
            parameters: tuple[object, ...]
            where = "scope_digest = ?"
            parameters = (scope.digest,)
            if position is not None:
                where += " AND (created_at < ? OR (created_at = ? AND revision_id < ?))"
                parameters = (scope.digest, position[0], position[0], position[1])
            rows = self._fetchall(
                f"""
                SELECT revision_id, parent_revision_id, created_at
                FROM configuration_revisions
                WHERE {where}
                ORDER BY created_at DESC, revision_id DESC
                LIMIT ?
                """,
                (*parameters, limit + 1),
            )
            page_rows = rows[:limit]
            revisions = tuple(
                RevisionSummary(
                    scope_digest=scope.digest,
                    revision_id=_revision(_text(row[0])),
                    parent_revision_id=(_revision(_text(row[1])) if row[1] is not None else None),
                    created_at=_datetime(_text(row[2])),
                )
                for row in page_rows
            )
            next_cursor = None
            if len(rows) > limit and page_rows:
                last = page_rows[-1]
                next_cursor = encode_scope_cursor(scope, [_text(last[2]), _text(last[0])])
            return RevisionPage(revisions=revisions, next_cursor=next_cursor)

    def read_active(self, scope: RuntimeScope) -> ActivePointer | None:
        with self._lock:
            row = self._fetchone(
                """
                SELECT revision_id, version
                FROM configuration_active_pointers
                WHERE scope_digest = ?
                """,
                (scope.digest,),
            )
            if row is None:
                return None
            return ActivePointer(
                scope_digest=scope.digest,
                revision_id=_revision(_text(row[0])),
                version=_integer(row[1]),
            )

    def compare_and_swap_active(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
        *,
        expected_revision_id: RevisionIdentity | None,
        expected_version: int | None = None,
    ) -> ActivePointer:
        with self._lock:
            try:
                with self._transaction():
                    self._require_revision(scope, revision_id)
                    row = self._connection.execute(
                        """
                        SELECT revision_id, version FROM configuration_active_pointers
                        WHERE scope_digest = ?
                        """,
                        (scope.digest,),
                    ).fetchone()
                    current = _revision(_text(row[0])) if row is not None else None
                    current_version = _integer(row[1]) if row is not None else None
                    if current != expected_revision_id or (
                        expected_version is not None and current_version != expected_version
                    ):
                        raise ConfigurationConflictError(scope.digest)
                    version = 1 if current_version is None else current_version + 1
                    self._connection.execute(
                        """
                        INSERT INTO configuration_active_pointers(scope_digest, revision_id, version)
                        VALUES (?, ?, ?)
                        ON CONFLICT(scope_digest) DO UPDATE SET
                            revision_id = excluded.revision_id,
                            version = excluded.version
                        """,
                        (scope.digest, str(revision_id), version),
                    )
            except (ConfigurationConflictError, ConfigurationNotFoundError):
                raise
            except sqlite3.Error as exc:
                raise ConfigurationUnavailableError(
                    "Cannot update the SQLite active configuration pointer"
                ) from exc
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
        with self._lock:
            if keep_latest < 1 or not 1 <= delete_limit <= MAX_RETENTION_DELETE_COUNT:
                raise ConfigurationLimitError("Configuration retention bounds are invalid")
            rows = self._fetchall(
                """
                SELECT revision_id, parent_revision_id
                FROM configuration_revisions
                WHERE scope_digest = ?
                ORDER BY created_at DESC, revision_id DESC
                LIMIT ?
                """,
                (scope.digest, MAX_RETENTION_SCAN_COUNT + 1),
            )
            if len(rows) > MAX_RETENTION_SCAN_COUNT:
                raise ConfigurationLimitError("Configuration retention scan exceeds its bound")
            active = self.read_active(scope)
            protected = {str(active.revision_id)} if active is not None else set()
            protected.update(_text(row[1]) for row in rows if row[1] is not None)
            candidates = [
                _revision(_text(row[0]))
                for row in rows[keep_latest:]
                if _text(row[0]) not in protected
            ][:delete_limit]
            try:
                with self._transaction():
                    for revision_id in candidates:
                        self._connection.execute(
                            """
                            DELETE FROM configuration_revisions
                            WHERE scope_digest = ? AND revision_id = ?
                            """,
                            (scope.digest, str(revision_id)),
                        )
            except sqlite3.Error as exc:
                raise ConfigurationUnavailableError(
                    "Cannot apply SQLite configuration retention"
                ) from exc
            return RetentionResult(deleted_revision_ids=tuple(candidates))

    def _initialize(self) -> None:
        row = self._connection.execute("PRAGMA user_version").fetchone()
        version = _integer(row[0]) if row is not None else 0
        if version not in {0, SQLITE_SCHEMA_VERSION}:
            raise ConfigurationIntegrityError("SQLite configuration schema version is unsupported")
        self._connection.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS configuration_drafts (
                scope_digest TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                body BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS configuration_revisions (
                scope_digest TEXT NOT NULL,
                revision_id TEXT NOT NULL,
                parent_revision_id TEXT,
                created_at TEXT NOT NULL,
                body BLOB NOT NULL,
                PRIMARY KEY(scope_digest, revision_id),
                FOREIGN KEY(scope_digest, parent_revision_id)
                    REFERENCES configuration_revisions(scope_digest, revision_id)
            );
            CREATE INDEX IF NOT EXISTS configuration_revisions_order
                ON configuration_revisions(scope_digest, created_at DESC, revision_id DESC);
            CREATE TABLE IF NOT EXISTS configuration_active_pointers (
                scope_digest TEXT PRIMARY KEY,
                revision_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                FOREIGN KEY(scope_digest, revision_id)
                    REFERENCES configuration_revisions(scope_digest, revision_id)
            );
            PRAGMA user_version = {SQLITE_SCHEMA_VERSION};
            """
        )

    def _timestamp(self) -> str:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ConfigurationIntegrityError("Configuration store clock must be timezone-aware")
        return value.astimezone(UTC).isoformat()

    def _require_revision(self, scope: RuntimeScope, revision_id: RevisionIdentity) -> None:
        row = self._connection.execute(
            """
            SELECT 1 FROM configuration_revisions
            WHERE scope_digest = ? AND revision_id = ?
            """,
            (scope.digest, str(revision_id)),
        ).fetchone()
        if row is None:
            raise ConfigurationNotFoundError("Configuration revision was not found")

    def _fetchone(self, query: str, parameters: tuple[object, ...]) -> tuple[object, ...] | None:
        try:
            return self._connection.execute(query, parameters).fetchone()
        except sqlite3.Error as exc:
            raise ConfigurationUnavailableError(
                "Cannot read from the SQLite configuration store"
            ) from exc

    def _fetchall(
        self,
        query: str,
        parameters: tuple[object, ...],
    ) -> list[tuple[object, ...]]:
        try:
            return self._connection.execute(query, parameters).fetchall()
        except sqlite3.Error as exc:
            raise ConfigurationUnavailableError("Cannot list SQLite configuration state") from exc

    def _transaction(self) -> _SqliteTransaction:
        return _SqliteTransaction(self._connection)


class _SqliteTransaction:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def __enter__(self) -> None:
        self._connection.execute("BEGIN IMMEDIATE")

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._connection.execute("ROLLBACK" if exc_type is not None else "COMMIT")


def _bundle(value: object, scope: RuntimeScope) -> ConfigurationDocument:
    try:
        envelope = ConfigurationEnvelope.model_validate_json(_bytes(value))
    except (ValueError, TypeError) as exc:
        raise ConfigurationIntegrityError("Stored configuration body is invalid") from exc
    if envelope.scope_digest != scope.digest:
        raise ConfigurationIntegrityError("Stored configuration body has the wrong scope")
    if _bytes(value) != envelope.canonical_bytes():
        raise ConfigurationIntegrityError("Stored configuration body is not canonical")
    return envelope.bundle


def _revision_position(
    scope: RuntimeScope,
    cursor: str | None,
) -> tuple[str, str] | None:
    if cursor is None:
        return None
    try:
        value = decode_scope_cursor(scope, cursor)
    except ValueError as exc:
        raise ConfigurationScopeError("Configuration cursor is invalid for this scope") from exc
    if (
        not isinstance(value, list)
        or len(value) != 2
        or not all(isinstance(item, str) for item in value)
    ):
        raise ConfigurationScopeError("Configuration cursor position is invalid")
    return value[0], value[1]


def _bytes(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise ConfigurationIntegrityError("Stored configuration body has an invalid type")
    return value


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise ConfigurationIntegrityError("Stored configuration metadata has an invalid type")
    return value


def _integer(value: object) -> int:
    if not isinstance(value, int):
        raise ConfigurationIntegrityError("Stored configuration version has an invalid type")
    return value


def _revision(value: str) -> RevisionIdentity:
    try:
        return RevisionIdentity(value)
    except ValueError as exc:
        raise ConfigurationIntegrityError(
            "Stored configuration revision identity is invalid"
        ) from exc


def _datetime(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ConfigurationIntegrityError("Stored configuration timestamp is invalid") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ConfigurationIntegrityError("Stored configuration timestamp is invalid")
    return result
