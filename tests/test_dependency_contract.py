"""Public dependency-range and minimum-constraint contracts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from scripts.verify_dependency_contract import (
    DependencyContractError,
    verify_dependency_contract,
)

CORE_REQUIREMENTS = ("package-a>=1.2,<2",)
PUBLIC_OPTIONAL_REQUIREMENTS = (
    "package-b>=3,<4",
    "justflow-admin>=0.1.0,<0.2",
)
ADMIN_REQUIREMENTS = ("justflow>=0.1.0,<0.2",)
MINIMUM_CONSTRAINTS = (
    "justflow==0.1.0",
    "justflow-admin==0.1.0",
    "package-a==1.2",
    "package-b==3",
)


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: None


@dataclass(frozen=True, kw_only=True)
class Raises:
    match: str


Outcome = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class DependencyContractCase:
    id: str
    core_requirements: tuple[str, ...]
    public_optional_requirements: tuple[str, ...]
    admin_requirements: tuple[str, ...]
    minimum_constraints: tuple[str, ...]
    outcome: Outcome


DEPENDENCY_CONTRACT_CASES = [
    DependencyContractCase(
        id="valid",
        core_requirements=CORE_REQUIREMENTS,
        public_optional_requirements=PUBLIC_OPTIONAL_REQUIREMENTS,
        admin_requirements=ADMIN_REQUIREMENTS,
        minimum_constraints=MINIMUM_CONSTRAINTS,
        outcome=Returns(value=None),
    ),
    DependencyContractCase(
        id="missing-lower-bound",
        core_requirements=("package-a<2",),
        public_optional_requirements=PUBLIC_OPTIONAL_REQUIREMENTS,
        admin_requirements=ADMIN_REQUIREMENTS,
        minimum_constraints=MINIMUM_CONSTRAINTS,
        outcome=Raises(match="package-a must declare an inclusive lower version bound"),
    ),
    DependencyContractCase(
        id="missing-upper-bound",
        core_requirements=("package-a>=1.2",),
        public_optional_requirements=PUBLIC_OPTIONAL_REQUIREMENTS,
        admin_requirements=ADMIN_REQUIREMENTS,
        minimum_constraints=MINIMUM_CONSTRAINTS,
        outcome=Raises(match="package-a must declare an upper version bound"),
    ),
    DependencyContractCase(
        id="missing-constraint",
        core_requirements=CORE_REQUIREMENTS,
        public_optional_requirements=PUBLIC_OPTIONAL_REQUIREMENTS,
        admin_requirements=ADMIN_REQUIREMENTS,
        minimum_constraints=MINIMUM_CONSTRAINTS[:-1],
        outcome=Raises(match="Minimum constraints omit: .*package-b"),
    ),
    DependencyContractCase(
        id="stale-constraint",
        core_requirements=CORE_REQUIREMENTS,
        public_optional_requirements=PUBLIC_OPTIONAL_REQUIREMENTS,
        admin_requirements=ADMIN_REQUIREMENTS,
        minimum_constraints=(*MINIMUM_CONSTRAINTS, "stale-package==1"),
        outcome=Raises(match="Minimum constraints contain stale entries: .*stale-package"),
    ),
    DependencyContractCase(
        id="constraint-is-not-exact",
        core_requirements=CORE_REQUIREMENTS,
        public_optional_requirements=PUBLIC_OPTIONAL_REQUIREMENTS,
        admin_requirements=ADMIN_REQUIREMENTS,
        minimum_constraints=(*MINIMUM_CONSTRAINTS[:-2], "package-a>=1.2", "package-b==3"),
        outcome=Raises(match="must contain one exact direct constraint"),
    ),
    DependencyContractCase(
        id="constraint-disagrees-with-floor",
        core_requirements=CORE_REQUIREMENTS,
        public_optional_requirements=PUBLIC_OPTIONAL_REQUIREMENTS,
        admin_requirements=ADMIN_REQUIREMENTS,
        minimum_constraints=(*MINIMUM_CONSTRAINTS[:-2], "package-a==1.3", "package-b==3"),
        outcome=Raises(match="Minimum constraints disagree with metadata: .*package-a"),
    ),
    DependencyContractCase(
        id="duplicate-declarations-have-different-floors",
        core_requirements=CORE_REQUIREMENTS,
        public_optional_requirements=(
            "package-a>=1.3,<2",
            *PUBLIC_OPTIONAL_REQUIREMENTS,
        ),
        admin_requirements=ADMIN_REQUIREMENTS,
        minimum_constraints=MINIMUM_CONSTRAINTS,
        outcome=Raises(match="package-a declares inconsistent minimums"),
    ),
    DependencyContractCase(
        id="core-rejects-built-admin-version",
        core_requirements=CORE_REQUIREMENTS,
        public_optional_requirements=("package-b>=3,<4", "justflow-admin>=0.2,<0.3"),
        admin_requirements=ADMIN_REQUIREMENTS,
        minimum_constraints=(
            "justflow==0.1.0",
            "justflow-admin==0.2",
            "package-a==1.2",
            "package-b==3",
        ),
        outcome=Raises(match="justflow dependency range must admit justflow-admin 0.1.0"),
    ),
    DependencyContractCase(
        id="admin-rejects-built-core-version",
        core_requirements=CORE_REQUIREMENTS,
        public_optional_requirements=PUBLIC_OPTIONAL_REQUIREMENTS,
        admin_requirements=("justflow>=0.2,<0.3",),
        minimum_constraints=(
            "justflow==0.2",
            "justflow-admin==0.1.0",
            "package-a==1.2",
            "package-b==3",
        ),
        outcome=Raises(match="justflow-admin dependency range must admit justflow 0.1.0"),
    ),
]


def _project_document(
    *,
    name: str,
    version: str,
    requirements: tuple[str, ...],
    public_optional_requirements: tuple[str, ...] = (),
) -> str:
    dependency_list = ", ".join(json.dumps(requirement) for requirement in requirements)
    lines = [
        "[project]",
        f"name = {json.dumps(name)}",
        f"version = {json.dumps(version)}",
        f"dependencies = [{dependency_list}]",
    ]
    if public_optional_requirements:
        optional_list = ", ".join(
            json.dumps(requirement) for requirement in public_optional_requirements
        )
        lines.extend(
            (
                "[project.optional-dependencies]",
                f"all = [{optional_list}]",
                'dev = ["dev-only>=1"]',
            )
        )
    return "\n".join(lines) + "\n"


@pytest.mark.parametrize(
    "case",
    DEPENDENCY_CONTRACT_CASES,
    ids=lambda case: case.id,
)
def test_dependency_contract_matrix(
    case: DependencyContractCase,
    tmp_path: Path,
) -> None:
    project_file = tmp_path / "pyproject.toml"
    admin_project_file = tmp_path / "admin-pyproject.toml"
    constraints_file = tmp_path / "minimum.txt"
    project_file.write_text(
        _project_document(
            name="justflow",
            version="0.1.0",
            requirements=case.core_requirements,
            public_optional_requirements=case.public_optional_requirements,
        ),
        encoding="utf-8",
    )
    admin_project_file.write_text(
        _project_document(
            name="justflow-admin",
            version="0.1.0",
            requirements=case.admin_requirements,
        ),
        encoding="utf-8",
    )
    constraints_file.write_text("\n".join(case.minimum_constraints) + "\n", encoding="utf-8")

    if isinstance(case.outcome, Raises):
        with pytest.raises(DependencyContractError, match=case.outcome.match):
            verify_dependency_contract(project_file, admin_project_file, constraints_file)
        return

    assert (
        verify_dependency_contract(project_file, admin_project_file, constraints_file)
        is case.outcome.value
    )


def test_repository_dependency_metadata_matches_minimum_constraints() -> None:
    verify_dependency_contract()
