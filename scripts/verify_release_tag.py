from __future__ import annotations

import sys
import tomllib
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
PROJECT_FILE = REPOSITORY_ROOT / "pyproject.toml"
TAG_PREFIX = "v"


def verify_release_tag(tag: str, expected_distribution: str) -> None:
    with PROJECT_FILE.open("rb") as project_file:
        project = tomllib.load(project_file)["project"]
    name = project["name"]
    version = project["version"]
    if name != expected_distribution:
        raise ValueError(
            f"Expected distribution '{expected_distribution}', pyproject declares '{name}'"
        )
    expected_tag = f"{TAG_PREFIX}{version}"
    if tag != expected_tag:
        raise ValueError(f"Release tag '{tag}' does not match version '{version}'")


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: verify_release_tag.py TAG EXPECTED_DISTRIBUTION")
    verify_release_tag(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()
