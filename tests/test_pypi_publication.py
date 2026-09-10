"""Offline tests for collision-safe PyPI publication preflight."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TypeAlias

import pytest

from scripts.verify_pypi_publication import (
    MAX_PYPI_RESPONSE_BYTES,
    ArtifactPublicationState,
    LocalArtifact,
    PublicationPreflightError,
    check_publication_state,
)

VERSION = "0.1.0"


def _artifact(distribution: str, filename: str, content: bytes) -> LocalArtifact:
    return LocalArtifact(
        distribution=distribution,
        version=VERSION,
        filename=filename,
        size=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
    )


ARTIFACTS = (
    _artifact("justflow", "justflow-0.1.0-py3-none-any.whl", b"core-wheel"),
    _artifact("justflow", "justflow-0.1.0.tar.gz", b"core-source"),
    _artifact("justflow-admin", "justflow_admin-0.1.0-py3-none-any.whl", b"admin-wheel"),
    _artifact("justflow-admin", "justflow_admin-0.1.0.tar.gz", b"admin-source"),
)


def _release_payload(*artifacts: LocalArtifact) -> bytes:
    return json.dumps(
        {
            "urls": [
                {
                    "filename": artifact.filename,
                    "size": artifact.size,
                    "digests": {"sha256": artifact.sha256},
                }
                for artifact in artifacts
            ]
        }
    ).encode()


@dataclass(frozen=True, kw_only=True)
class Returns:
    states: tuple[ArtifactPublicationState, ...]


@dataclass(frozen=True, kw_only=True)
class Raises:
    exception: type[Exception]
    match: str


Outcome: TypeAlias = Returns | Raises
ReleaseResponse: TypeAlias = bytes | None | Exception


@dataclass(frozen=True, kw_only=True)
class PublicationCase:
    id: str
    responses: tuple[tuple[str, ReleaseResponse], ...]
    outcome: Outcome


UPLOAD_ALL = Returns(states=(ArtifactPublicationState.UPLOAD_REQUIRED,) * len(ARTIFACTS))
ALREADY_ALL = Returns(states=(ArtifactPublicationState.ALREADY_PUBLISHED,) * len(ARTIFACTS))
IDENTICAL_RESPONSES = (
    ("justflow", _release_payload(*ARTIFACTS[:2])),
    ("justflow-admin", _release_payload(*ARTIFACTS[2:])),
)
PUBLICATION_CASES = [
    PublicationCase(
        id="both-releases-absent",
        responses=(("justflow", None), ("justflow-admin", None)),
        outcome=UPLOAD_ALL,
    ),
    PublicationCase(
        id="both-releases-empty",
        responses=(("justflow", _release_payload()), ("justflow-admin", _release_payload())),
        outcome=UPLOAD_ALL,
    ),
    PublicationCase(
        id="all-identical",
        responses=IDENTICAL_RESPONSES,
        outcome=ALREADY_ALL,
    ),
    PublicationCase(
        id="partial-publication",
        responses=(
            ("justflow", _release_payload(ARTIFACTS[0])),
            ("justflow-admin", _release_payload(*ARTIFACTS[2:])),
        ),
        outcome=Returns(
            states=(
                ArtifactPublicationState.ALREADY_PUBLISHED,
                ArtifactPublicationState.UPLOAD_REQUIRED,
                ArtifactPublicationState.ALREADY_PUBLISHED,
                ArtifactPublicationState.ALREADY_PUBLISHED,
            )
        ),
    ),
    PublicationCase(
        id="digest-mismatch",
        responses=(
            (
                "justflow",
                _release_payload(
                    LocalArtifact(
                        distribution=ARTIFACTS[0].distribution,
                        version=VERSION,
                        filename=ARTIFACTS[0].filename,
                        size=ARTIFACTS[0].size,
                        sha256="0" * 64,
                    )
                ),
            ),
            IDENTICAL_RESPONSES[1],
        ),
        outcome=Raises(exception=PublicationPreflightError, match="differs from the local"),
    ),
    PublicationCase(
        id="size-mismatch",
        responses=(
            (
                "justflow",
                _release_payload(
                    LocalArtifact(
                        distribution=ARTIFACTS[0].distribution,
                        version=VERSION,
                        filename=ARTIFACTS[0].filename,
                        size=ARTIFACTS[0].size + 1,
                        sha256=ARTIFACTS[0].sha256,
                    )
                ),
            ),
            IDENTICAL_RESPONSES[1],
        ),
        outcome=Raises(exception=PublicationPreflightError, match="differs from the local"),
    ),
    PublicationCase(
        id="duplicate-server-filename",
        responses=(
            ("justflow", _release_payload(ARTIFACTS[0], ARTIFACTS[0])),
            IDENTICAL_RESPONSES[1],
        ),
        outcome=Raises(exception=PublicationPreflightError, match="duplicates filename"),
    ),
    PublicationCase(
        id="invalid-json",
        responses=(("justflow", b"{"), IDENTICAL_RESPONSES[1]),
        outcome=Raises(exception=PublicationPreflightError, match="invalid release document"),
    ),
    PublicationCase(
        id="invalid-response-shape",
        responses=(("justflow", b'{"urls": {}}'), IDENTICAL_RESPONSES[1]),
        outcome=Raises(exception=PublicationPreflightError, match="unexpected release document"),
    ),
    PublicationCase(
        id="oversized-response",
        responses=(("justflow", b"x" * (MAX_PYPI_RESPONSE_BYTES + 1)), IDENTICAL_RESPONSES[1]),
        outcome=Raises(exception=PublicationPreflightError, match="exceeds its byte bound"),
    ),
    PublicationCase(
        id="timeout",
        responses=(("justflow", TimeoutError("timed out")), IDENTICAL_RESPONSES[1]),
        outcome=Raises(exception=PublicationPreflightError, match="Cannot inspect PyPI release"),
    ),
    PublicationCase(
        id="unexpected-http-status",
        responses=(
            ("justflow", PublicationPreflightError("unexpected HTTP status 503")),
            IDENTICAL_RESPONSES[1],
        ),
        outcome=Raises(exception=PublicationPreflightError, match="unexpected HTTP status 503"),
    ),
]


@pytest.mark.parametrize("case", PUBLICATION_CASES, ids=lambda case: case.id)
def test_publication_collision_matrix(case: PublicationCase) -> None:
    responses = dict(case.responses)

    def read_release(distribution: str, version: str) -> bytes | None:
        assert version == VERSION
        response = responses[distribution]
        if isinstance(response, Exception):
            raise response
        return response

    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exception, match=case.outcome.match):
            check_publication_state(ARTIFACTS, read_release)
        return

    checks = check_publication_state(ARTIFACTS, read_release)
    assert tuple(check.state for check in checks) == case.outcome.states
