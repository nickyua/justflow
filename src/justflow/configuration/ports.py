"""Typed configuration read and writable revision boundaries."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from justflow.configuration.models import (
    ActivePointer,
    ComponentCatalogRevision,
    ConfigurationDocument,
    ConfigurationSnapshot,
    DraftRecord,
    PlatformComponentCatalog,
    RetentionResult,
    RevisionIdentity,
    RevisionPage,
    RevisionRecord,
)
from justflow.scope import RuntimeScope


@runtime_checkable
class ConfigurationSource(Protocol):
    def read(self, scope: RuntimeScope) -> ConfigurationSnapshot: ...

    def read_triggers(self, scope: RuntimeScope) -> ConfigurationSnapshot: ...


@runtime_checkable
class PlatformComponentCatalogSource(Protocol):
    def read(self, revision_id: ComponentCatalogRevision) -> PlatformComponentCatalog: ...


@runtime_checkable
class ConfigurationStore(Protocol):
    def read_draft(self, scope: RuntimeScope) -> DraftRecord | None: ...

    def compare_and_swap_draft(
        self,
        scope: RuntimeScope,
        bundle: ConfigurationDocument,
        *,
        expected_version: int | None,
    ) -> DraftRecord: ...

    def create_revision(
        self,
        scope: RuntimeScope,
        bundle: ConfigurationDocument,
        *,
        parent_revision_id: RevisionIdentity | None,
    ) -> RevisionRecord: ...

    def read_revision(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
    ) -> RevisionRecord: ...

    def list_revisions(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> RevisionPage: ...

    def read_active(self, scope: RuntimeScope) -> ActivePointer | None: ...

    def compare_and_swap_active(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
        *,
        expected_revision_id: RevisionIdentity | None,
        expected_version: int | None = None,
    ) -> ActivePointer: ...

    def retain_revisions(
        self,
        scope: RuntimeScope,
        *,
        keep_latest: int,
        delete_limit: int,
    ) -> RetentionResult: ...
