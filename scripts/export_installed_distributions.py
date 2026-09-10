"""Export and verify a pip-independent installed-distribution inventory."""

from __future__ import annotations

import argparse
import re
from importlib.metadata import distributions
from pathlib import Path

MAX_CONSTRAINTS_BYTES = 65_536
MAX_DISTRIBUTIONS = 512
NORMALIZED_NAME_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_-]*$")


class DistributionInventoryError(RuntimeError):
    """The installed distribution closure differs from its release constraint."""


def normalize_distribution_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def installed_inventory(excluded: frozenset[str]) -> dict[str, str]:
    normalized_exclusions = {normalize_distribution_name(name) for name in excluded}
    inventory: dict[str, str] = {}
    for distribution in distributions():
        raw_name = distribution.metadata["Name"]
        version = distribution.version
        if not raw_name or not version:
            raise DistributionInventoryError("Installed distribution metadata is incomplete")
        name = normalize_distribution_name(raw_name)
        if name in normalized_exclusions:
            continue
        if (
            NORMALIZED_NAME_PATTERN.fullmatch(name) is None
            or VERSION_PATTERN.fullmatch(version) is None
        ):
            raise DistributionInventoryError("Installed distribution metadata is invalid")
        if name in inventory:
            raise DistributionInventoryError(f"Installed distribution {name!r} is duplicated")
        inventory[name] = version
    if len(inventory) > MAX_DISTRIBUTIONS:
        raise DistributionInventoryError("Installed distribution inventory exceeds its bound")
    return inventory


def expected_inventory(path: Path) -> dict[str, str]:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise DistributionInventoryError("Cannot load the expected distribution closure") from exc
    if len(payload) > MAX_CONSTRAINTS_BYTES:
        raise DistributionInventoryError("Expected distribution closure exceeds its byte bound")
    try:
        lines = payload.decode("utf-8").splitlines()
    except UnicodeError as exc:
        raise DistributionInventoryError("Expected distribution closure is not UTF-8") from exc

    inventory: dict[str, str] = {}
    for line_number, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name_text, separator, version = line.partition("==")
        name = normalize_distribution_name(name_text)
        if (
            separator != "=="
            or NORMALIZED_NAME_PATTERN.fullmatch(name) is None
            or VERSION_PATTERN.fullmatch(version) is None
        ):
            raise DistributionInventoryError(
                f"Expected distribution closure line {line_number} is not an exact pin"
            )
        if name in inventory:
            raise DistributionInventoryError(f"Expected distribution closure duplicates {name!r}")
        inventory[name] = version
    if len(inventory) > MAX_DISTRIBUTIONS:
        raise DistributionInventoryError("Expected distribution closure exceeds its bound")
    return inventory


def verify_inventory(installed: dict[str, str], expected: dict[str, str]) -> None:
    missing = expected.keys() - installed.keys()
    unexpected = installed.keys() - expected.keys()
    mismatched = {
        name: (expected[name], installed[name])
        for name in expected.keys() & installed.keys()
        if expected[name] != installed[name]
    }
    if missing or unexpected or mismatched:
        raise DistributionInventoryError(
            "Installed distribution closure differs from constraints: "
            f"missing={sorted(missing)}, unexpected={sorted(unexpected)}, "
            f"mismatched={mismatched}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export the installed Python distribution closure")
    parser.add_argument("--expected", required=True, type=Path)
    parser.add_argument("--exclude", action="append", default=[])
    args = parser.parse_args()
    try:
        inventory = installed_inventory(frozenset(args.exclude))
        verify_inventory(inventory, expected_inventory(args.expected))
    except DistributionInventoryError as exc:
        raise SystemExit(str(exc)) from exc
    for name, version in sorted(inventory.items()):
        print(f"{name}=={version}")


if __name__ == "__main__":
    main()
