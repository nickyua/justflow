from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias

import pytest

from justflow.configuration import (
    ConfigurationError,
    ConfigurationIntegrityError,
    ConfigurationLimitError,
    ConfigurationNotFoundError,
    ConfigurationUnavailableError,
    FilePlatformComponentCatalogSource,
    PlatformComponentCatalog,
)
from justflow.configuration.models import MAX_CONFIGURATION_BYTES

COMPONENT_CATALOG = PlatformComponentCatalog.create()


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: PlatformComponentCatalog


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


CatalogSourceOutcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class CatalogSourceCase:
    id: str
    revision_id: str
    payload: bytes | None
    outcome: CatalogSourceOutcome


CATALOG_SOURCE_CASES = [
    CatalogSourceCase(
        id="canonical",
        revision_id=COMPONENT_CATALOG.revision_id,
        payload=COMPONENT_CATALOG.canonical_bytes(),
        outcome=Returns(value=COMPONENT_CATALOG),
    ),
    CatalogSourceCase(
        id="missing",
        revision_id=COMPONENT_CATALOG.revision_id,
        payload=None,
        outcome=Raises(exc=ConfigurationNotFoundError, match="was not found"),
    ),
    CatalogSourceCase(
        id="non-canonical",
        revision_id=COMPONENT_CATALOG.revision_id,
        payload=json.dumps(
            COMPONENT_CATALOG.model_dump(mode="json"),
            indent=2,
        ).encode(),
        outcome=Raises(exc=ConfigurationIntegrityError, match="content is invalid"),
    ),
    CatalogSourceCase(
        id="wrong-revision",
        revision_id="b" * 64,
        payload=COMPONENT_CATALOG.canonical_bytes(),
        outcome=Raises(exc=ConfigurationIntegrityError, match="requested revision"),
    ),
    CatalogSourceCase(
        id="oversized",
        revision_id=COMPONENT_CATALOG.revision_id,
        payload=b"x" * (MAX_CONFIGURATION_BYTES + 1),
        outcome=Raises(exc=ConfigurationLimitError, match="byte limit"),
    ),
]


@pytest.mark.parametrize("case", CATALOG_SOURCE_CASES, ids=lambda case: case.id)
def test_file_platform_component_catalog_source(
    case: CatalogSourceCase,
    tmp_path: Path,
) -> None:
    if case.payload is not None:
        (tmp_path / f"{case.revision_id}.json").write_bytes(case.payload)
    source = FilePlatformComponentCatalogSource(tmp_path)

    if isinstance(case.outcome, Returns):
        assert source.read(case.revision_id) == case.outcome.value
        return

    with pytest.raises(case.outcome.exc, match=case.outcome.match):
        source.read(case.revision_id)


def test_file_platform_component_catalog_source_rejects_invalid_revision(
    tmp_path: Path,
) -> None:
    with pytest.raises(ConfigurationError, match="revision is invalid"):
        FilePlatformComponentCatalogSource(tmp_path).read("../catalog")


def test_file_platform_component_catalog_source_translates_unavailable_storage(
    tmp_path: Path,
) -> None:
    unavailable = tmp_path / "catalogs"
    unavailable.write_bytes(b"not-a-directory")

    with pytest.raises(ConfigurationUnavailableError, match="source is unavailable"):
        FilePlatformComponentCatalogSource(unavailable).read(COMPONENT_CATALOG.revision_id)
