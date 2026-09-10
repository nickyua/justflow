from __future__ import annotations

from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
BUILD_DIRECTORIES = (REPOSITORY_ROOT / "dist", REPOSITORY_ROOT / "example-dist")
BUILD_ARTIFACT_SUFFIXES = (".whl", ".tar.gz")


def prepare_build_directory(directory: Path) -> None:
    directory.mkdir(exist_ok=True)
    for path in directory.iterdir():
        if not path.is_file() or not path.name.endswith(BUILD_ARTIFACT_SUFFIXES):
            raise ValueError(f"Refusing to remove unexpected build output: {path}")
        path.unlink()


def main() -> None:
    for directory in BUILD_DIRECTORIES:
        prepare_build_directory(directory)


if __name__ == "__main__":
    main()
