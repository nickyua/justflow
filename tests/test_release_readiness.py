"""Static release-readiness contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

import pytest

from scripts.verify_release_readiness import (
    ReleaseReadinessError,
    verify_changelog,
    verify_release_readiness,
)

VALID_CHANGELOG = """# Changelog

## [Unreleased]

## [0.1.0] — 2026-09-02

### Added

- Initial release.
"""


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: None


@dataclass(frozen=True, kw_only=True)
class Raises:
    exception: type[Exception]
    match: str


Outcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class ChangelogCase:
    id: str
    content: str
    expected_version: str
    outcome: Outcome


CHANGELOG_CASES = [
    ChangelogCase(
        id="valid",
        content=VALID_CHANGELOG,
        expected_version="0.1.0",
        outcome=Returns(value=None),
    ),
    ChangelogCase(
        id="missing-release",
        content="# Changelog\n\n## [Unreleased]\n",
        expected_version="0.1.0",
        outcome=Raises(
            exception=ReleaseReadinessError,
            match="does not contain release section",
        ),
    ),
    ChangelogCase(
        id="empty-release",
        content="# Changelog\n\n## [Unreleased]\n\n## [0.1.0] — 2026-09-02\n",
        expected_version="0.1.0",
        outcome=Raises(exception=ReleaseReadinessError, match="release '0.1.0' is empty"),
    ),
    ChangelogCase(
        id="mismatched-version",
        content=VALID_CHANGELOG,
        expected_version="0.1.1",
        outcome=Raises(
            exception=ReleaseReadinessError,
            match="does not contain release section '0.1.1'",
        ),
    ),
    ChangelogCase(
        id="duplicate-release",
        content=(VALID_CHANGELOG + "\n## [0.1.0] — 2026-09-03\n\n### Fixed\n\n- Duplicate.\n"),
        expected_version="0.1.0",
        outcome=Raises(exception=ReleaseReadinessError, match="duplicates release section"),
    ),
    ChangelogCase(
        id="incorrect-order",
        content=(
            "# Changelog\n\n## [Unreleased]\n\n"
            "## [0.0.9] — 2026-09-02\n\n- Older.\n\n"
            "## [0.1.0] — 2026-09-01\n\n- Current.\n"
        ),
        expected_version="0.1.0",
        outcome=Raises(exception=ReleaseReadinessError, match="must be the first released section"),
    ),
]


@pytest.mark.parametrize("case", CHANGELOG_CASES, ids=lambda case: case.id)
def test_changelog_release_contract(case: ChangelogCase) -> None:
    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exception, match=case.outcome.match):
            verify_changelog(case.content, case.expected_version)
        return

    assert verify_changelog(case.content, case.expected_version) is case.outcome.value


def test_release_metadata_and_workflow_boundaries_are_complete() -> None:
    verify_release_readiness()
