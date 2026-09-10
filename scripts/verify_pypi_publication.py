"""Fail-closed PyPI artifact-collision preflight for release publication."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from scripts.verify_distribution import (
    ADMIN_DISTRIBUTION,
    CORE_DISTRIBUTION,
    SOURCE_SUFFIX,
    verify_distribution,
)

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
PROJECT_FILE = REPOSITORY_ROOT / "pyproject.toml"
PYPI_RELEASE_API = "https://pypi.org/pypi/{distribution}/{version}/json"
PYPI_REQUEST_TIMEOUT_SECONDS = 10
PYPI_SUCCESS_STATUS = 200
PYPI_NOT_FOUND_STATUS = 404
MAX_PYPI_RESPONSE_BYTES = 1_048_576
MAX_PYPI_RELEASE_FILES = 100
MAX_PROJECT_NAME_LENGTH = 200
MAX_VERSION_LENGTH = 128
MAX_ARTIFACT_FILENAME_LENGTH = 512
MAX_ARTIFACT_BYTES = 1_073_741_824
PROJECT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$")
VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_-]*$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
USER_AGENT = "justflow-release-preflight/1"
DISTRIBUTION_FILENAMES = {
    CORE_DISTRIBUTION: CORE_DISTRIBUTION,
    ADMIN_DISTRIBUTION: "justflow_admin",
}
ReleaseReader = Callable[[str, str], bytes | None]


class PublicationPreflightError(RuntimeError):
    """Local artifacts or remote publication state are unsafe or unavailable."""


class ArtifactPublicationState(StrEnum):
    UPLOAD_REQUIRED = "upload-required"
    ALREADY_PUBLISHED = "already-published"


@dataclass(frozen=True, kw_only=True)
class LocalArtifact:
    distribution: str
    version: str
    filename: str
    size: int
    sha256: str


@dataclass(frozen=True, kw_only=True)
class PublicationCheck:
    artifact: LocalArtifact
    state: ArtifactPublicationState


@dataclass(frozen=True, kw_only=True)
class PublishedArtifact:
    filename: str
    size: int
    sha256: str


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    document: dict[str, Any] = {}
    for key, value in pairs:
        if key in document:
            raise ValueError(f"PyPI response duplicates field {key!r}")
        document[key] = value
    return document


def _published_artifacts(payload: bytes | None) -> dict[str, PublishedArtifact]:
    if payload is None:
        return {}
    if len(payload) > MAX_PYPI_RESPONSE_BYTES:
        raise PublicationPreflightError("PyPI release response exceeds its byte bound")
    try:
        document = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (json.JSONDecodeError, UnicodeError, ValueError) as exc:
        raise PublicationPreflightError("PyPI returned an invalid release document") from exc
    if not isinstance(document, dict) or not isinstance(document.get("urls"), list):
        raise PublicationPreflightError("PyPI returned an unexpected release document shape")
    urls = document["urls"]
    if len(urls) > MAX_PYPI_RELEASE_FILES:
        raise PublicationPreflightError("PyPI release exceeds its file-count bound")

    artifacts: dict[str, PublishedArtifact] = {}
    for item in urls:
        if not isinstance(item, dict):
            raise PublicationPreflightError("PyPI release contains an invalid file entry")
        filename = item.get("filename")
        size = item.get("size")
        digests = item.get("digests")
        sha256 = digests.get("sha256") if isinstance(digests, dict) else None
        if (
            not isinstance(filename, str)
            or not filename
            or len(filename) > MAX_ARTIFACT_FILENAME_LENGTH
            or "/" in filename
            or "\\" in filename
            or type(size) is not int
            or size < 0
            or size > MAX_ARTIFACT_BYTES
            or not isinstance(sha256, str)
            or SHA256_PATTERN.fullmatch(sha256) is None
        ):
            raise PublicationPreflightError("PyPI release contains invalid artifact metadata")
        if filename in artifacts:
            raise PublicationPreflightError(f"PyPI release duplicates filename {filename!r}")
        artifacts[filename] = PublishedArtifact(filename=filename, size=size, sha256=sha256)
    return artifacts


def check_publication_state(
    artifacts: tuple[LocalArtifact, ...],
    read_release: ReleaseReader,
) -> tuple[PublicationCheck, ...]:
    """Compare local artifacts with their release-specific PyPI records."""
    grouped: dict[tuple[str, str], list[LocalArtifact]] = {}
    seen_filenames: set[str] = set()
    if not artifacts:
        raise PublicationPreflightError("No local release artifacts were provided")
    for artifact in artifacts:
        if (
            not artifact.distribution
            or len(artifact.distribution) > MAX_PROJECT_NAME_LENGTH
            or PROJECT_NAME_PATTERN.fullmatch(artifact.distribution) is None
            or not artifact.version
            or len(artifact.version) > MAX_VERSION_LENGTH
            or VERSION_PATTERN.fullmatch(artifact.version) is None
            or not artifact.filename
            or len(artifact.filename) > MAX_ARTIFACT_FILENAME_LENGTH
            or "/" in artifact.filename
            or "\\" in artifact.filename
            or artifact.filename in seen_filenames
            or type(artifact.size) is not int
            or artifact.size < 0
            or artifact.size > MAX_ARTIFACT_BYTES
            or SHA256_PATTERN.fullmatch(artifact.sha256) is None
        ):
            raise PublicationPreflightError("Local artifact metadata is invalid")
        seen_filenames.add(artifact.filename)
        grouped.setdefault((artifact.distribution, artifact.version), []).append(artifact)

    checks: list[PublicationCheck] = []
    for (distribution, version), local_artifacts in grouped.items():
        try:
            payload = read_release(distribution, version)
        except PublicationPreflightError:
            raise
        except (OSError, TimeoutError) as exc:
            raise PublicationPreflightError(
                f"Cannot inspect PyPI release {distribution} {version}"
            ) from exc
        published = _published_artifacts(payload)
        for artifact in local_artifacts:
            existing = published.get(artifact.filename)
            if existing is None:
                state = ArtifactPublicationState.UPLOAD_REQUIRED
            elif existing.size != artifact.size or existing.sha256 != artifact.sha256:
                raise PublicationPreflightError(
                    f"PyPI artifact {artifact.filename!r} differs from the local artifact"
                )
            else:
                state = ArtifactPublicationState.ALREADY_PUBLISHED
            checks.append(PublicationCheck(artifact=artifact, state=state))
    return tuple(checks)


def _read_pypi_release(distribution: str, version: str) -> bytes | None:
    url = PYPI_RELEASE_API.format(
        distribution=quote(distribution, safe=""),
        version=quote(version, safe=""),
    )
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=PYPI_REQUEST_TIMEOUT_SECONDS) as response:
            if response.status != PYPI_SUCCESS_STATUS:
                raise PublicationPreflightError(
                    f"PyPI returned unexpected HTTP status {response.status}"
                )
            payload = response.read(MAX_PYPI_RESPONSE_BYTES + 1)
    except HTTPError as exc:
        if exc.code == PYPI_NOT_FOUND_STATUS:
            return None
        raise PublicationPreflightError(f"PyPI returned unexpected HTTP status {exc.code}") from exc
    if len(payload) > MAX_PYPI_RESPONSE_BYTES:
        raise PublicationPreflightError("PyPI release response exceeds its byte bound")
    return payload


def _project_version() -> str:
    try:
        project = tomllib.loads(PROJECT_FILE.read_text(encoding="utf-8"))["project"]
        version = project["version"]
    except (OSError, KeyError, tomllib.TOMLDecodeError) as exc:
        raise PublicationPreflightError("Cannot load the release version") from exc
    if not isinstance(version, str) or not version:
        raise PublicationPreflightError("The release version is invalid")
    return version


def _local_artifacts(directory: Path, version: str) -> tuple[LocalArtifact, ...]:
    try:
        verify_distribution(directory, version)
        artifacts: list[LocalArtifact] = []
        for distribution, filename_prefix in DISTRIBUTION_FILENAMES.items():
            for filename in (
                f"{filename_prefix}-{version}-py3-none-any.whl",
                f"{filename_prefix}-{version}{SOURCE_SUFFIX}",
            ):
                path = directory / filename
                size = path.stat().st_size
                with path.open("rb") as stream:
                    digest = hashlib.file_digest(stream, "sha256").hexdigest()
                artifacts.append(
                    LocalArtifact(
                        distribution=distribution,
                        version=version,
                        filename=filename,
                        size=size,
                        sha256=digest,
                    )
                )
    except (OSError, ValueError) as exc:
        raise PublicationPreflightError("Local release artifacts are invalid") from exc
    return tuple(artifacts)


def verify_pypi_publication(directory: Path) -> tuple[PublicationCheck, ...]:
    """Verify that publishing or retrying every built artifact is collision-safe."""
    version = _project_version()
    return check_publication_state(_local_artifacts(directory, version), _read_pypi_release)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify PyPI release artifact collision state")
    parser.add_argument("distribution_directory", type=Path)
    args = parser.parse_args()
    try:
        checks = verify_pypi_publication(args.distribution_directory)
    except PublicationPreflightError as exc:
        raise SystemExit(str(exc)) from exc
    for check in checks:
        print(f"{check.artifact.filename}: {check.state.value}")


if __name__ == "__main__":
    main()
