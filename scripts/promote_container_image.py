"""Promote a verified registry digest without rebuilding or replacing a release."""

from __future__ import annotations

import argparse
import re
import subprocess
from collections.abc import Callable

REGISTRY_COMMAND_TIMEOUT_SECONDS = 180
DIGEST_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")
VERSION_PATTERN = re.compile(r"\d+\.\d+\.\d+")
RunCommand = Callable[[tuple[str, ...]], subprocess.CompletedProcess[str]]


class ImagePromotionError(RuntimeError):
    """The registry cannot safely promote the verified candidate."""


def _run(arguments: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ("docker", "buildx", "imagetools", *arguments),
            check=False,
            capture_output=True,
            text=True,
            timeout=REGISTRY_COMMAND_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ImagePromotionError("Registry command could not complete") from exc


def _digest(reference: str, run: RunCommand, *, allow_missing: bool = False) -> str | None:
    result = run(("inspect", reference, "--format", "{{.Manifest.Digest}}"))
    if result.returncode != 0:
        missing = re.fullmatch(
            r"(?:ERROR: )?" + re.escape(reference) + r": not found", result.stderr.strip()
        )
        if allow_missing and missing is not None:
            return None
        raise ImagePromotionError("Registry inspection failed; release state is unknown")
    value = result.stdout.strip()
    if DIGEST_PATTERN.fullmatch(value) is None:
        raise ImagePromotionError("Registry returned an invalid digest")
    return value


def promote_image(source: str, destination: str, *, run: RunCommand = _run) -> None:
    repository, separator, expected = source.partition("@")
    target_repository, _, version = destination.rpartition(":")
    if (
        not separator
        or DIGEST_PATTERN.fullmatch(expected) is None
        or not repository
        or repository.startswith("-")
        or any(character.isspace() for character in repository)
        or repository != target_repository
        or VERSION_PATTERN.fullmatch(version) is None
    ):
        raise ImagePromotionError(
            "Promotion requires one repository, a digest and a release version"
        )
    if _digest(source, run) != expected:
        raise ImagePromotionError("Candidate digest does not match the verified source")
    existing = _digest(destination, run, allow_missing=True)
    if existing == expected:
        return
    if existing is not None:
        raise ImagePromotionError("Release version already names a different digest")
    result = run(("create", "--prefer-index=false", "--tag", destination, source))
    if result.returncode != 0:
        raise ImagePromotionError(
            "Registry promotion did not complete; inspect the destination before retrying"
        )
    if _digest(destination, run) != expected:
        raise ImagePromotionError("Promoted version does not match the verified candidate digest")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="Verified image repository@sha256:digest")
    parser.add_argument("destination", help="Same repository with a release version tag")
    arguments = parser.parse_args()
    promote_image(arguments.source, arguments.destination)


if __name__ == "__main__":
    main()
