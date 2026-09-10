from __future__ import annotations

import hashlib
import json
import sys
import tarfile
import zipfile
from email.message import Message
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name

WHEEL_SUFFIX = ".whl"
SOURCE_SUFFIX = ".tar.gz"
METADATA_SUFFIX = ".dist-info/METADATA"
SDIST_METADATA_NAME = "PKG-INFO"
CORE_DISTRIBUTION = "justflow"
ADMIN_DISTRIBUTION = "justflow-admin"
CORE_PACKAGE_PREFIX = "justflow/"
ADMIN_PACKAGE_PREFIX = "justflow_admin/"
ADMIN_ASSET_PREFIX = "justflow_admin/assets/dist/"
ADMIN_MANIFEST_PATH = f"{ADMIN_ASSET_PREFIX}justflow-manifest.json"
SOURCE_MAP_SUFFIX = ".map"
REQUIRED_CORE_FILES = frozenset(
    {
        "justflow/__init__.py",
        "justflow/openapi/bundled/justflow-openapi-0.1.0.json",
        "justflow/py.typed",
        "justflow/runtime/admin_panel.py",
        "justflow/schemas/bundled/configuration.schema.json",
        "justflow/visualization/static/mermaid.min.js",
    }
)
REQUIRED_ADMIN_FILES = frozenset(
    {
        "justflow_admin/__init__.py",
        "justflow_admin/panel.py",
        "justflow_admin/py.typed",
        "justflow_admin/assets/__init__.py",
        ADMIN_MANIFEST_PATH,
        f"{ADMIN_ASSET_PREFIX}index.html",
        f"{ADMIN_ASSET_PREFIX}graph-frame.html",
    }
)
REQUIRED_CORE_SDIST_FILES = frozenset(
    {
        "CHANGELOG.md",
        "LICENSE",
        "NOTICE",
        "README.md",
        "SECURITY.md",
        "SUPPORT.md",
        "docs/index.md",
        "docs/requirements.txt",
        "mkdocs.yml",
        "pyproject.toml",
        "src/justflow/runtime/admin_panel.py",
    }
)
FORBIDDEN_CORE_SDIST_FILES = frozenset(
    {
        "ENGINE_DECISIONS.md",
        "docs/scheduling-workflows-spec.md",
    }
)


def _distribution_filename(name: str) -> str:
    return name.replace("-", "_")


def _expected_artifacts(version: str) -> frozenset[str]:
    return frozenset(
        {
            f"{_distribution_filename(name)}-{version}-py3-none-any.whl"
            for name in (CORE_DISTRIBUTION, ADMIN_DISTRIBUTION)
        }
        | {
            f"{_distribution_filename(name)}-{version}{SOURCE_SUFFIX}"
            for name in (CORE_DISTRIBUTION, ADMIN_DISTRIBUTION)
        }
    )


def _validate_archive_path(name: str, *, artifact: Path) -> None:
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{artifact.name} contains unsafe path '{name}'")


def _metadata_fields(metadata: Message, *, artifact: Path) -> tuple[str, str]:
    name = metadata["Name"]
    version = metadata["Version"]
    if not name or not version:
        raise ValueError(f"{artifact.name} metadata has no distribution name or version")
    return name, version


def _wheel_metadata(path: Path) -> tuple[Message, str]:
    with zipfile.ZipFile(path) as archive:
        metadata_paths = [name for name in archive.namelist() if name.endswith(METADATA_SUFFIX)]
        if len(metadata_paths) != 1:
            raise ValueError(f"{path.name} contains {len(metadata_paths)} metadata files")
        metadata_path = metadata_paths[0]
        return BytesParser().parsebytes(archive.read(metadata_path)), metadata_path


def _sdist_metadata(path: Path) -> Message:
    with tarfile.open(path) as archive:
        members = [
            member
            for member in archive.getmembers()
            if member.name.endswith(f"/{SDIST_METADATA_NAME}")
        ]
        if len(members) != 1:
            raise ValueError(f"{path.name} contains {len(members)} metadata files")
        metadata_file = archive.extractfile(members[0])
        if metadata_file is None:
            raise ValueError(f"{path.name} metadata cannot be read")
        return BytesParser().parsebytes(metadata_file.read())


def _wheel_files(path: Path) -> frozenset[str]:
    with zipfile.ZipFile(path) as archive:
        names = frozenset(entry.filename for entry in archive.infolist() if not entry.is_dir())
    for name in names:
        _validate_archive_path(name, artifact=path)
    return names


def _verify_core_wheel(path: Path, names: frozenset[str]) -> None:
    missing = REQUIRED_CORE_FILES - names
    forbidden = {
        name
        for name in names
        if name.startswith(("justflow/runtime/admin_assets/", ADMIN_PACKAGE_PREFIX))
        or name.endswith(SOURCE_MAP_SUFFIX)
    }
    if missing or forbidden:
        raise ValueError(
            f"{path.name} core boundary mismatch: missing={sorted(missing)}, "
            f"forbidden={sorted(forbidden)}"
        )


def _verify_admin_wheel(path: Path, names: frozenset[str]) -> None:
    missing = REQUIRED_ADMIN_FILES - names
    forbidden = {
        name
        for name in names
        if name.startswith(CORE_PACKAGE_PREFIX) or name.endswith(SOURCE_MAP_SUFFIX)
    }
    if missing or forbidden:
        raise ValueError(
            f"{path.name} admin boundary mismatch: missing={sorted(missing)}, "
            f"forbidden={sorted(forbidden)}"
        )
    with zipfile.ZipFile(path) as archive:
        manifest = json.loads(archive.read(ADMIN_MANIFEST_PATH))
        entries = manifest.get("files")
        if not isinstance(entries, dict) or not entries:
            raise ValueError(f"{path.name} has an empty or invalid asset manifest")
        for relative_name, declaration in entries.items():
            asset_path = f"{ADMIN_ASSET_PREFIX}{relative_name}"
            if asset_path not in names or relative_name.endswith(SOURCE_MAP_SUFFIX):
                raise ValueError(f"{path.name} manifest references invalid asset '{relative_name}'")
            payload = archive.read(asset_path)
            if len(payload) != declaration.get("bytes"):
                raise ValueError(f"{path.name} asset byte count differs for '{relative_name}'")
            if hashlib.sha256(payload).hexdigest() != declaration.get("sha256"):
                raise ValueError(f"{path.name} asset digest differs for '{relative_name}'")


def _verify_sdist(path: Path, *, distribution: str) -> None:
    with tarfile.open(path) as archive:
        names = frozenset(member.name for member in archive.getmembers() if member.isfile())
    for name in names:
        _validate_archive_path(name, artifact=path)
    relative_names = frozenset(name.split("/", 1)[1] for name in names if "/" in name)
    if distribution == CORE_DISTRIBUTION:
        forbidden = {
            name
            for name in relative_names
            if name.startswith(("ui/admin/", "src/justflow/runtime/admin_assets/"))
        } | (relative_names & FORBIDDEN_CORE_SDIST_FILES)
        required = set(REQUIRED_CORE_SDIST_FILES)
    else:
        forbidden = {
            name
            for name in relative_names
            if name.startswith("src/justflow/") or name.endswith(SOURCE_MAP_SUFFIX)
        }
        required = {
            "src/justflow_admin/panel.py",
            "src/justflow_admin/assets/dist/index.html",
            "pyproject.toml",
        }
    missing = required - relative_names
    if missing or forbidden:
        raise ValueError(
            f"{path.name} source boundary mismatch: missing={sorted(missing)}, "
            f"forbidden={sorted(forbidden)}"
        )


def _requirements(metadata: Message, *, distribution: str) -> tuple[Requirement, ...]:
    try:
        return tuple(Requirement(value) for value in metadata.get_all("Requires-Dist", []))
    except InvalidRequirement as exc:
        raise ValueError(f"{distribution} metadata contains an invalid requirement") from exc


def _requirement_enabled_for_extra(requirement: Requirement, extra: str) -> bool:
    return requirement.marker is not None and requirement.marker.evaluate({"extra": extra})


def verify_distribution(directory: Path, expected_version: str) -> None:
    actual = frozenset(path.name for path in directory.iterdir() if path.is_file())
    expected = _expected_artifacts(expected_version)
    if actual != expected:
        raise ValueError(f"Expected artifacts {sorted(expected)}, found {sorted(actual)}")

    metadata_by_distribution: dict[str, Message] = {}
    for distribution in (CORE_DISTRIBUTION, ADMIN_DISTRIBUTION):
        filename = _distribution_filename(distribution)
        wheel = directory / f"{filename}-{expected_version}-py3-none-any.whl"
        sdist = directory / f"{filename}-{expected_version}{SOURCE_SUFFIX}"
        wheel_metadata, _ = _wheel_metadata(wheel)
        sdist_metadata = _sdist_metadata(sdist)
        expected_identity = (distribution, expected_version)
        if _metadata_fields(wheel_metadata, artifact=wheel) != expected_identity:
            raise ValueError(f"{wheel.name} metadata does not match {expected_identity}")
        if _metadata_fields(sdist_metadata, artifact=sdist) != expected_identity:
            raise ValueError(f"{sdist.name} metadata does not match {expected_identity}")
        metadata_by_distribution[distribution] = wheel_metadata
        names = _wheel_files(wheel)
        if distribution == CORE_DISTRIBUTION:
            _verify_core_wheel(wheel, names)
        else:
            _verify_admin_wheel(wheel, names)
        _verify_sdist(sdist, distribution=distribution)

    core_requirements = _requirements(
        metadata_by_distribution[CORE_DISTRIBUTION],
        distribution=CORE_DISTRIBUTION,
    )
    admin_requirements = _requirements(
        metadata_by_distribution[ADMIN_DISTRIBUTION],
        distribution=ADMIN_DISTRIBUTION,
    )
    expected_admin_specifier = SpecifierSet(f">={expected_version},<0.2")
    if not any(
        canonicalize_name(requirement.name) == ADMIN_DISTRIBUTION
        and requirement.specifier == expected_admin_specifier
        and _requirement_enabled_for_extra(requirement, "admin")
        for requirement in core_requirements
    ):
        raise ValueError("Core metadata has no justflow[admin] convenience dependency")
    expected_uvicorn_specifier = SpecifierSet(">=0.30,<1")
    if not any(
        canonicalize_name(requirement.name) == "uvicorn"
        and requirement.specifier == expected_uvicorn_specifier
        and _requirement_enabled_for_extra(requirement, "admin")
        for requirement in core_requirements
    ):
        raise ValueError("Core metadata has no justflow[admin] server dependency")
    expected_core_specifier = SpecifierSet(f">={expected_version},<0.2")
    if not any(
        canonicalize_name(requirement.name) == CORE_DISTRIBUTION
        and requirement.specifier == expected_core_specifier
        and requirement.marker is None
        for requirement in admin_requirements
    ):
        raise ValueError("Admin metadata has no bounded core compatibility dependency")


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: verify_distribution.py DIST_DIRECTORY EXPECTED_VERSION")
    verify_distribution(Path(sys.argv[1]), sys.argv[2])


if __name__ == "__main__":
    main()
