"""Verify release metadata and publication boundaries without publishing."""

from __future__ import annotations

import re
import tomllib
import types
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, Union, get_args, get_origin

import yaml
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name
from pydantic import BaseModel

from justflow.config.settings import ENV_NESTED_DELIMITER, ENV_PREFIX, Settings
from justflow.openapi import OPENAPI_VERSION
from justflow.schemas import AUTHORING_SCHEMA_VERSION
from scripts.verify_dependency_contract import verify_dependency_contract

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
PROJECT_FILE = REPOSITORY_ROOT / "pyproject.toml"
ADMIN_PROJECT_FILE = REPOSITORY_ROOT / "packages" / "justflow-admin" / "pyproject.toml"
CHANGELOG_FILE = REPOSITORY_ROOT / "CHANGELOG.md"
MAKEFILE = REPOSITORY_ROOT / "Makefile"
DOCKERFILE = REPOSITORY_ROOT / "docker" / "Dockerfile"
COMPOSE_FILE = REPOSITORY_ROOT / "compose.yaml"
ENV_EXAMPLE_FILE = REPOSITORY_ROOT / ".env.example"
CI_WORKFLOW_FILE = REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml"
DOCS_WORKFLOW_FILE = REPOSITORY_ROOT / ".github" / "workflows" / "docs.yml"
RELEASE_WORKFLOW_FILE = REPOSITORY_ROOT / ".github" / "workflows" / "release.yml"
FULL_COMMIT_PATTERN = re.compile(r"uses:\s+[^\s@]+@[0-9a-f]{40}(?:\s+#.*)?$")
CHANGELOG_HEADING_PATTERN = re.compile(
    r"^## \[(?P<version>[^\]]+)\](?: — (?P<released_on>\d{4}-\d{2}-\d{2}))?$"
)
MAKEFILE_VERSION_PATTERN = re.compile(r"^EXPECTED_VERSION \?= (?P<version>\S+)$", re.MULTILINE)
DOCKER_VERSION_PATTERN = re.compile(r"^ARG JUSTFLOW_VERSION=(?P<version>\S+)$", re.MULTILINE)
UNRELEASED_CHANGELOG_SECTION = "Unreleased"
EXPECTED_DOCKER_VERSION_ARGUMENTS = 2
CORE_DISTRIBUTION = "justflow"
EXAMPLE_PROJECT_FILES = (
    REPOSITORY_ROOT / "examples" / "host_application" / "pyproject.toml",
    REPOSITORY_ROOT / "examples" / "object_ingestion" / "pyproject.toml",
    REPOSITORY_ROOT / "examples" / "prime_stats" / "pyproject.toml",
    REPOSITORY_ROOT / "examples" / "product_onboarding" / "pyproject.toml",
    REPOSITORY_ROOT / "examples" / "scheduled_reporting" / "pyproject.toml",
)
REQUIRED_PROJECT_URLS = frozenset({"Documentation", "Issues", "Repository", "Security"})
REQUIRED_SDIST_PATHS = frozenset(
    {
        ".env.example",
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "LICENSE",
        "NOTICE",
        "README.md",
        "SECURITY.md",
        "SUPPORT.md",
        "docs",
        "mkdocs.yml",
        "pyproject.toml",
        "src/justflow",
    }
)
REQUIRED_PUBLIC_FILES = (
    "CHANGELOG.md",
    "LICENSE",
    "NOTICE",
    "README.md",
    "SECURITY.md",
    "SUPPORT.md",
    "docs/index.md",
    "docs/requirements.txt",
    "mkdocs.yml",
)
REQUIRED_RELEASE_TOKENS = (
    "cosign sign",
    "--format spdx-json",
    "pypa/gh-action-pypi-publish@",
    'python scripts/verify_distribution.py dist "$EXPECTED_VERSION"',
    "python -m scripts.verify_pypi_publication dist",
    "skip-existing: true",
    "scripts/verify_release_tag.py",
)
REQUIRED_CI_TOKENS = (
    "minimum-deps:",
    "--requirement docs/requirements.txt",
    "npm --prefix ui/admin audit --audit-level=high",
    "make check PY=python",
    "scripts/export_installed_distributions.py",
    "scripts/verify_container_images.py",
    "--scanners vuln,secret,misconfig",
)


class ReleaseReadinessError(ValueError):
    """Release metadata or workflow boundaries are incomplete."""


@dataclass(frozen=True, kw_only=True)
class ChangelogRelease:
    version: str
    released_on: date
    content: str


def _project(path: Path) -> dict[str, object]:
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
        project = document["project"]
    except (OSError, KeyError, tomllib.TOMLDecodeError) as exc:
        raise ReleaseReadinessError(f"Cannot load project metadata from {path}") from exc
    if not isinstance(project, dict):
        raise ReleaseReadinessError(f"{path} must contain a project table")
    return project


def _required_string(value: object, *, location: str) -> str:
    if not isinstance(value, str) or not value:
        raise ReleaseReadinessError(f"{location} must be a non-empty string")
    return value


def changelog_releases(content: str) -> tuple[ChangelogRelease, ...]:
    """Parse ordered, dated changelog release sections after Unreleased."""
    lines = content.splitlines()
    headings: list[tuple[int, str, str | None]] = []
    for index, line in enumerate(lines):
        if not line.startswith("## ["):
            continue
        match = CHANGELOG_HEADING_PATTERN.fullmatch(line)
        if match is None:
            raise ReleaseReadinessError(f"Invalid changelog release heading: {line!r}")
        headings.append((index, match.group("version"), match.group("released_on")))

    if not headings or headings[0][1:] != (UNRELEASED_CHANGELOG_SECTION, None):
        raise ReleaseReadinessError("Changelog must begin with an undated Unreleased section")

    releases: list[ChangelogRelease] = []
    seen_versions: set[str] = set()
    for heading_index, (line_index, version, released_on_text) in enumerate(headings[1:], start=1):
        if version == UNRELEASED_CHANGELOG_SECTION or version in seen_versions:
            raise ReleaseReadinessError(f"Changelog duplicates release section {version!r}")
        if released_on_text is None:
            raise ReleaseReadinessError(f"Changelog release {version!r} has no date")
        try:
            released_on = date.fromisoformat(released_on_text)
        except ValueError as exc:
            raise ReleaseReadinessError(
                f"Changelog release {version!r} has an invalid date"
            ) from exc
        next_line_index = (
            headings[heading_index + 1][0] if heading_index + 1 < len(headings) else len(lines)
        )
        section_content = "\n".join(lines[line_index + 1 : next_line_index]).strip()
        if not section_content:
            raise ReleaseReadinessError(f"Changelog release {version!r} is empty")
        seen_versions.add(version)
        releases.append(
            ChangelogRelease(
                version=version,
                released_on=released_on,
                content=section_content,
            )
        )
    return tuple(releases)


def verify_changelog(content: str, expected_version: str) -> None:
    """Require the current version to be the first non-empty released section."""
    releases = changelog_releases(content)
    if not any(release.version == expected_version for release in releases):
        raise ReleaseReadinessError(
            f"Changelog does not contain release section {expected_version!r}"
        )
    if releases[0].version != expected_version:
        raise ReleaseReadinessError(
            f"Changelog release {expected_version!r} must be the first released section"
        )


def _model_variants(annotation: object) -> tuple[type[BaseModel], ...]:
    if get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return (annotation,)
    if get_origin(annotation) in (types.UnionType, Union):
        return tuple(
            item
            for item in get_args(annotation)
            if isinstance(item, type) and issubclass(item, BaseModel)
        )
    return ()


def _valid_environment_path(parts: tuple[str, ...]) -> bool:
    models: tuple[type[BaseModel], ...] = (Settings,)
    for index, part in enumerate(parts):
        matching_fields = [
            model.model_fields[part] for model in models if part in model.model_fields
        ]
        if not matching_fields:
            return False
        if index == len(parts) - 1:
            return True
        models = tuple(
            variant for field in matching_fields for variant in _model_variants(field.annotation)
        )
        if not models:
            return False
    return True


def _verify_environment_example() -> None:
    groups: set[str] = set()
    names: set[str] = set()
    for line_number, line in enumerate(
        ENV_EXAMPLE_FILE.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line.startswith(ENV_PREFIX):
            continue
        name, separator, value = line.partition("=")
        if separator != "=" or value:
            raise ReleaseReadinessError(
                f".env.example:{line_number} must declare a key with no value"
            )
        if name in names:
            raise ReleaseReadinessError(f".env.example duplicates {name}")
        names.add(name)
        parts = tuple(
            part.lower() for part in name.removeprefix(ENV_PREFIX).split(ENV_NESTED_DELIMITER)
        )
        if not _valid_environment_path(parts):
            raise ReleaseReadinessError(f".env.example declares unknown setting {name}")
        groups.add(parts[0])
    missing_groups = set(Settings.model_fields) - groups
    if missing_groups:
        raise ReleaseReadinessError(f".env.example omits settings groups: {sorted(missing_groups)}")


def _workflow(path: Path) -> dict[str, object]:
    value = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    if not isinstance(value, dict):
        raise ReleaseReadinessError(f"{path.name} must contain a workflow object")
    return value


def _verify_version_contract(version: str) -> None:
    admin_version = _required_string(
        _project(ADMIN_PROJECT_FILE).get("version"),
        location=f"{ADMIN_PROJECT_FILE}: project.version",
    )
    if admin_version != version:
        raise ReleaseReadinessError("Core and admin package versions must match")

    release_workflow = _workflow(RELEASE_WORKFLOW_FILE)
    release_environment = release_workflow.get("env")
    if not isinstance(release_environment, dict):
        raise ReleaseReadinessError("Release workflow must declare a global environment")
    if release_environment.get("EXPECTED_VERSION") != version:
        raise ReleaseReadinessError("Release workflow version must match the package version")

    makefile_versions = MAKEFILE_VERSION_PATTERN.findall(MAKEFILE.read_text(encoding="utf-8"))
    if makefile_versions != [version]:
        raise ReleaseReadinessError("Makefile expected version must match the package version")

    docker_versions = DOCKER_VERSION_PATTERN.findall(DOCKERFILE.read_text(encoding="utf-8"))
    if len(docker_versions) != EXPECTED_DOCKER_VERSION_ARGUMENTS or any(
        value != version for value in docker_versions
    ):
        raise ReleaseReadinessError("Docker image versions must match the package version")

    compose_document = _workflow(COMPOSE_FILE)
    runtime_environment = compose_document.get("x-runtime-environment")
    if not isinstance(runtime_environment, dict) or (
        runtime_environment.get("JUSTFLOW_DEPLOYMENT__PACKAGE_VERSION") != version
    ):
        raise ReleaseReadinessError("Compose package version must match the package version")

    expected_specifier = f"=={version}"
    for path in EXAMPLE_PROJECT_FILES:
        dependencies = _project(path).get("dependencies")
        if not isinstance(dependencies, list) or any(
            not isinstance(requirement, str) for requirement in dependencies
        ):
            raise ReleaseReadinessError(f"{path}: project.dependencies must be a string list")
        try:
            requirements = [Requirement(requirement) for requirement in dependencies]
        except InvalidRequirement as exc:
            raise ReleaseReadinessError(f"{path} contains an invalid dependency") from exc
        core_requirements = [
            requirement
            for requirement in requirements
            if canonicalize_name(requirement.name) == CORE_DISTRIBUTION
        ]
        if len(core_requirements) != 1 or str(core_requirements[0].specifier) != expected_specifier:
            raise ReleaseReadinessError(
                f"{path} must require the exact core package version {version}"
            )

    verify_changelog(CHANGELOG_FILE.read_text(encoding="utf-8"), version)


def _verify_documentation_workflow() -> None:
    workflow = _workflow(DOCS_WORKFLOW_FILE)
    triggers = workflow.get("on")
    if not isinstance(triggers, dict) or set(triggers) != {"release", "workflow_dispatch"}:
        raise ReleaseReadinessError(
            "documentation publication must be limited to release and explicit manual workflows"
        )
    release = triggers.get("release")
    if not isinstance(release, dict) or release.get("types") != ["published"]:
        raise ReleaseReadinessError("documentation release publication must require published")
    content = DOCS_WORKFLOW_FILE.read_text(encoding="utf-8")
    uses_lines = [line.strip() for line in content.splitlines() if "uses:" in line]
    if not uses_lines or any(FULL_COMMIT_PATTERN.search(line) is None for line in uses_lines):
        raise ReleaseReadinessError("documentation workflow actions must use full commit pins")
    for token in ("make docs-check PY=python", "actions/deploy-pages@", "path: site"):
        if token not in content:
            raise ReleaseReadinessError(f"documentation workflow is missing {token!r}")


def _verify_workflow_tokens(path: Path, tokens: tuple[str, ...]) -> None:
    content = path.read_text(encoding="utf-8")
    for token in tokens:
        if token not in content:
            raise ReleaseReadinessError(f"{path.name} is missing release gate {token!r}")


def verify_release_readiness() -> None:
    project_document = tomllib.loads(PROJECT_FILE.read_text(encoding="utf-8"))
    project = project_document["project"]
    version = _required_string(project.get("version"), location="project.version")
    _verify_version_contract(version)
    if OPENAPI_VERSION != AUTHORING_SCHEMA_VERSION:
        raise ReleaseReadinessError("schema and OpenAPI versions must remain aligned")
    if frozenset(project.get("urls", {})) != REQUIRED_PROJECT_URLS:
        raise ReleaseReadinessError("project metadata URLs are incomplete or unbounded")
    sdist_paths = frozenset(
        project_document["tool"]["hatch"]["build"]["targets"]["sdist"]["only-include"]
    )
    if not REQUIRED_SDIST_PATHS.issubset(sdist_paths):
        raise ReleaseReadinessError("sdist omits public release or documentation source")
    if "ENGINE_DECISIONS.md" in sdist_paths:
        raise ReleaseReadinessError("sdist must not publish the internal design record")
    for relative_path in REQUIRED_PUBLIC_FILES:
        path = REPOSITORY_ROOT / relative_path
        if not path.is_file() or not path.read_text(encoding="utf-8").strip():
            raise ReleaseReadinessError(f"public release file is missing or empty: {relative_path}")
    _verify_environment_example()
    verify_dependency_contract()
    _verify_documentation_workflow()
    _verify_workflow_tokens(CI_WORKFLOW_FILE, REQUIRED_CI_TOKENS)
    _verify_workflow_tokens(RELEASE_WORKFLOW_FILE, REQUIRED_RELEASE_TOKENS)


if __name__ == "__main__":
    verify_release_readiness()
