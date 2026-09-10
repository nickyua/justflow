"""Immutable platform component-catalog sources."""

from __future__ import annotations

from pathlib import Path

from pydantic import TypeAdapter, ValidationError

from justflow.configuration.errors import (
    ConfigurationError,
    ConfigurationIntegrityError,
    ConfigurationLimitError,
    ConfigurationNotFoundError,
    ConfigurationUnavailableError,
)
from justflow.configuration.models import (
    MAX_CONFIGURATION_BYTES,
    ComponentCatalogRevision,
    PlatformComponentCatalog,
)

CATALOG_FILE_SUFFIX = ".json"
_REVISION_ADAPTER = TypeAdapter(ComponentCatalogRevision)


class FilePlatformComponentCatalogSource:
    """Read canonical immutable component catalogs packaged by exact revision."""

    def __init__(self, directory: str | Path) -> None:
        self._directory = Path(directory)

    def read(self, revision_id: ComponentCatalogRevision) -> PlatformComponentCatalog:
        try:
            validated_revision = _REVISION_ADAPTER.validate_python(revision_id)
        except ValidationError as exc:
            raise ConfigurationError("Platform component catalog revision is invalid") from exc
        path = self._directory / f"{validated_revision}{CATALOG_FILE_SUFFIX}"
        try:
            with path.open("rb") as catalog_file:
                payload = catalog_file.read(MAX_CONFIGURATION_BYTES + 1)
        except FileNotFoundError as exc:
            raise ConfigurationNotFoundError(
                "Platform component catalog revision was not found"
            ) from exc
        except OSError as exc:
            raise ConfigurationUnavailableError(
                "Platform component catalog source is unavailable"
            ) from exc
        if len(payload) > MAX_CONFIGURATION_BYTES:
            raise ConfigurationLimitError("Platform component catalog exceeds its byte limit")
        try:
            catalog = PlatformComponentCatalog.from_bytes(payload)
        except (TypeError, ValueError, UnicodeError) as exc:
            raise ConfigurationIntegrityError(
                "Platform component catalog content is invalid"
            ) from exc
        if catalog.revision_id != validated_revision:
            raise ConfigurationIntegrityError(
                "Platform component catalog does not match its requested revision"
            )
        return catalog
