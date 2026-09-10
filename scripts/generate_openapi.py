"""Regenerate the bundled public OpenAPI document."""

from __future__ import annotations

from pathlib import Path

from justflow.openapi import OPENAPI_FILE_NAME, render_openapi_document

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
BUNDLED_DIRECTORY = REPOSITORY_ROOT / "src" / "justflow" / "openapi" / "bundled"


def main() -> None:
    BUNDLED_DIRECTORY.mkdir(parents=True, exist_ok=True)
    (BUNDLED_DIRECTORY / OPENAPI_FILE_NAME).write_bytes(render_openapi_document())


if __name__ == "__main__":
    main()
