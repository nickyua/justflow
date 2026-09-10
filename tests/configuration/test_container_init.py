"""Tests for the local container configuration initializer."""

from __future__ import annotations

from pathlib import Path

from host_application import initialize_local_configuration_database

from justflow.configuration import SqliteConfigurationStore
from justflow.scope import LOCAL_RUNTIME_SCOPE

EXAMPLE_CONFIG_DIR = Path("examples/host_application/src/host_application/configs")


def test_container_configuration_initialization_is_idempotent(tmp_path: Path) -> None:
    database = tmp_path / "configuration.sqlite3"

    initialize_local_configuration_database(database, EXAMPLE_CONFIG_DIR)
    initialize_local_configuration_database(database, EXAMPLE_CONFIG_DIR)

    store = SqliteConfigurationStore(database)
    try:
        active = store.read_active(LOCAL_RUNTIME_SCOPE)
        revisions = store.list_revisions(LOCAL_RUNTIME_SCOPE, limit=10)
    finally:
        store.close()

    assert active is not None
    assert len(revisions.revisions) == 1
    assert revisions.revisions[0].revision_id == active.revision_id
