"""Validate public dependency ranges against the tested minimum constraints."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
PROJECT_FILE = REPOSITORY_ROOT / "pyproject.toml"
ADMIN_PROJECT_FILE = REPOSITORY_ROOT / "packages" / "justflow-admin" / "pyproject.toml"
MINIMUM_CONSTRAINTS_FILE = REPOSITORY_ROOT / "constraints" / "minimum.txt"
NON_PUBLIC_OPTIONAL_GROUPS = frozenset({"dev"})
INCLUSIVE_LOWER_BOUND_OPERATORS = frozenset({"==", ">=", "~="})
UPPER_BOUND_OPERATORS = frozenset({"==", "<", "<=", "~="})
EXACT_CONSTRAINT_OPERATOR = "=="


class DependencyContractError(ValueError):
    """Public dependency metadata and its executable minimum contract disagree."""


@dataclass(frozen=True, kw_only=True)
class ProjectMetadata:
    name: str
    version: Version
    requirements: tuple[Requirement, ...]


def _string(value: object, *, location: str) -> str:
    if not isinstance(value, str) or not value:
        raise DependencyContractError(f"{location} must be a non-empty string")
    return value


def _string_list(value: object, *, location: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise DependencyContractError(f"{location} must be a list of requirement strings")
    return tuple(value)


def _requirement(value: str, *, location: str) -> Requirement:
    try:
        requirement = Requirement(value)
    except InvalidRequirement as exc:
        raise DependencyContractError(
            f"{location} contains an invalid requirement: {value!r}"
        ) from exc
    if requirement.marker is not None or requirement.url is not None:
        raise DependencyContractError(
            f"{location} must use one unconditional version range: {requirement}"
        )
    return requirement


def _project_metadata(path: Path, *, include_optional_dependencies: bool) -> ProjectMetadata:
    try:
        document: dict[str, Any] = tomllib.loads(path.read_text(encoding="utf-8"))
        project = document["project"]
    except (OSError, KeyError, tomllib.TOMLDecodeError) as exc:
        raise DependencyContractError(f"Cannot load project metadata from {path}") from exc
    if not isinstance(project, dict):
        raise DependencyContractError(f"{path} must contain a project table")

    name = _string(project.get("name"), location=f"{path}: project.name")
    version_text = _string(project.get("version"), location=f"{path}: project.version")
    try:
        version = Version(version_text)
    except InvalidVersion as exc:
        raise DependencyContractError(
            f"{path}: project.version is invalid: {version_text!r}"
        ) from exc

    requirement_values = list(
        _string_list(project.get("dependencies", []), location=f"{path}: project.dependencies")
    )
    if include_optional_dependencies:
        optional_dependencies = project.get("optional-dependencies", {})
        if not isinstance(optional_dependencies, dict):
            raise DependencyContractError(f"{path}: project.optional-dependencies must be a table")
        for group_name, values in sorted(optional_dependencies.items()):
            if group_name in NON_PUBLIC_OPTIONAL_GROUPS:
                continue
            requirement_values.extend(
                _string_list(
                    values,
                    location=f"{path}: project.optional-dependencies.{group_name}",
                )
            )

    requirements = tuple(_requirement(value, location=str(path)) for value in requirement_values)
    return ProjectMetadata(name=name, version=version, requirements=requirements)


def _inclusive_lower_bound(requirement: Requirement) -> Version:
    candidates = [
        specifier
        for specifier in requirement.specifier
        if specifier.operator in INCLUSIVE_LOWER_BOUND_OPERATORS
    ]
    if not candidates:
        raise DependencyContractError(
            f"{requirement.name} must declare an inclusive lower version bound"
        )
    try:
        versions = [Version(specifier.version) for specifier in candidates]
    except InvalidVersion as exc:
        raise DependencyContractError(
            f"{requirement.name} has a lower bound that is not an exact version"
        ) from exc
    minimum = max(versions)
    if not requirement.specifier.contains(minimum, prereleases=True):
        raise DependencyContractError(
            f"{requirement.name} has no installable inclusive minimum version"
        )
    return minimum


def _verify_upper_bound(requirement: Requirement) -> None:
    if not any(specifier.operator in UPPER_BOUND_OPERATORS for specifier in requirement.specifier):
        raise DependencyContractError(f"{requirement.name} must declare an upper version bound")


def _minimum_constraints(path: Path) -> dict[str, Version]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise DependencyContractError(f"Cannot load minimum constraints from {path}") from exc

    constraints: dict[str, Version] = {}
    for line_number, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        requirement = _requirement(line, location=f"{path}:{line_number}")
        specifiers = tuple(requirement.specifier)
        if (
            requirement.extras
            or len(specifiers) != 1
            or specifiers[0].operator != EXACT_CONSTRAINT_OPERATOR
        ):
            raise DependencyContractError(
                f"{path}:{line_number} must contain one exact direct constraint"
            )
        try:
            version = Version(specifiers[0].version)
        except InvalidVersion as exc:
            raise DependencyContractError(
                f"{path}:{line_number} contains an invalid exact version"
            ) from exc
        name = canonicalize_name(requirement.name)
        if name in constraints:
            raise DependencyContractError(f"{path} duplicates minimum constraint {name}")
        constraints[name] = version
    return constraints


def _declared_minimums(projects: tuple[ProjectMetadata, ...]) -> dict[str, Version]:
    minimums: dict[str, Version] = {}
    for project in projects:
        for requirement in project.requirements:
            _verify_upper_bound(requirement)
            name = canonicalize_name(requirement.name)
            minimum = _inclusive_lower_bound(requirement)
            previous = minimums.setdefault(name, minimum)
            if previous != minimum:
                raise DependencyContractError(
                    f"{requirement.name} declares inconsistent minimums: {previous} and {minimum}"
                )
    return minimums


def _verify_sibling_dependency(project: ProjectMetadata, sibling: ProjectMetadata) -> None:
    sibling_name = canonicalize_name(sibling.name)
    requirements = [
        requirement
        for requirement in project.requirements
        if canonicalize_name(requirement.name) == sibling_name
    ]
    if not requirements:
        raise DependencyContractError(f"{project.name} must declare a dependency on {sibling.name}")
    if any(
        not requirement.specifier.contains(sibling.version, prereleases=True)
        for requirement in requirements
    ):
        raise DependencyContractError(
            f"{project.name} dependency range must admit {sibling.name} {sibling.version}"
        )


def verify_dependency_contract(
    project_file: Path = PROJECT_FILE,
    admin_project_file: Path = ADMIN_PROJECT_FILE,
    minimum_constraints_file: Path = MINIMUM_CONSTRAINTS_FILE,
) -> None:
    """Validate package metadata, public extras, sibling ranges, and exact floors."""
    core = _project_metadata(project_file, include_optional_dependencies=True)
    admin = _project_metadata(admin_project_file, include_optional_dependencies=False)
    projects = (core, admin)
    declared_minimums = _declared_minimums(projects)
    constrained_minimums = _minimum_constraints(minimum_constraints_file)

    missing = declared_minimums.keys() - constrained_minimums.keys()
    if missing:
        raise DependencyContractError(f"Minimum constraints omit: {sorted(missing)}")
    stale = constrained_minimums.keys() - declared_minimums.keys()
    if stale:
        raise DependencyContractError(f"Minimum constraints contain stale entries: {sorted(stale)}")
    mismatched = {
        name: (declared_minimums[name], constrained_minimums[name])
        for name in declared_minimums
        if declared_minimums[name] != constrained_minimums[name]
    }
    if mismatched:
        raise DependencyContractError(f"Minimum constraints disagree with metadata: {mismatched}")

    _verify_sibling_dependency(core, admin)
    _verify_sibling_dependency(admin, core)


if __name__ == "__main__":
    verify_dependency_contract()
