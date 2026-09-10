from __future__ import annotations

import subprocess
from dataclasses import dataclass

import pytest

from scripts.promote_container_image import ImagePromotionError, promote_image

REPOSITORY = "registry.example/justflow"
CANDIDATE_DIGEST = f"sha256:{'a' * 64}"
OTHER_DIGEST = f"sha256:{'b' * 64}"
SOURCE = f"{REPOSITORY}@{CANDIDATE_DIGEST}"
DESTINATION = f"{REPOSITORY}:0.1.0"


@dataclass(frozen=True, kw_only=True)
class Returns:
    writes: int


@dataclass(frozen=True, kw_only=True)
class Raises:
    exception: type[Exception]
    match: str


@dataclass(frozen=True, kw_only=True)
class PromotionCase:
    id: str
    existing_digest: str | None
    inspection_error: str | None = None
    outcome: Returns | Raises


CASES = [
    PromotionCase(id="first-release", existing_digest=None, outcome=Returns(writes=1)),
    PromotionCase(
        id="same-release-retry", existing_digest=CANDIDATE_DIGEST, outcome=Returns(writes=0)
    ),
    PromotionCase(
        id="existing-different-release",
        existing_digest=OTHER_DIGEST,
        outcome=Raises(exception=ImagePromotionError, match="already names a different digest"),
    ),
    PromotionCase(
        id="registry-unavailable",
        existing_digest=None,
        inspection_error="connection refused",
        outcome=Raises(exception=ImagePromotionError, match="release state is unknown"),
    ),
    PromotionCase(
        id="unauthorized",
        existing_digest=None,
        inspection_error="unauthorized: authentication required",
        outcome=Raises(exception=ImagePromotionError, match="release state is unknown"),
    ),
]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.id)
def test_promotion_never_replaces_or_guesses_release_state(case: PromotionCase) -> None:
    current = case.existing_digest
    writes = 0

    def run(arguments: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        nonlocal current, writes
        if arguments[0] == "create":
            assert arguments == ("create", "--prefer-index=false", "--tag", DESTINATION, SOURCE)
            current = CANDIDATE_DIGEST
            writes += 1
            return subprocess.CompletedProcess(arguments, 0, "", "")
        if arguments[1] == SOURCE:
            return subprocess.CompletedProcess(arguments, 0, CANDIDATE_DIGEST, "")
        if case.inspection_error is not None:
            return subprocess.CompletedProcess(arguments, 1, "", case.inspection_error)
        if current is None:
            return subprocess.CompletedProcess(arguments, 1, "", f"ERROR: {DESTINATION}: not found")
        return subprocess.CompletedProcess(arguments, 0, current, "")

    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exception, match=case.outcome.match):
            promote_image(SOURCE, DESTINATION, run=run)
        assert writes == 0
    else:
        promote_image(SOURCE, DESTINATION, run=run)
        assert writes == case.outcome.writes
