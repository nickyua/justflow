from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import venv
from pathlib import Path

CORE_WHEEL_PATTERN = "justflow-*.whl"
ADMIN_WHEEL_PATTERN = "justflow_admin-*.whl"
DEPENDENCY_DIRECTORY_NAMES = frozenset({"site-packages", "dist-packages"})
CORE_SMOKE_PROGRAM = """
import importlib.util
import sys
from pathlib import Path
from importlib.resources import files

environment_root = Path(sys.argv[1]).resolve()
for dependency_path in sys.argv[2:]:
    sys.path.append(str(Path(dependency_path).resolve()))

import justflow
from justflow.__main__ import ADMIN_INSTALL_MESSAGE, _configured_admin_panel
from justflow.config.settings import OperationsSettings, RuntimeSettings, Settings
from justflow.provenance import RuntimeProfile
from justflow.openapi import OPENAPI_FILE_NAME, load_bundled_openapi_document
from justflow.runtime import AdminPanel
from justflow.schemas import SCHEMA_FILE_NAMES, load_bundled_schemas
from justflow.transports import TransportProvider, builtin_transport_registry

module_path = Path(justflow.__file__).resolve()
assert module_path.is_relative_to(environment_root), module_path
assert (module_path.parent / "py.typed").is_file()
assert not (module_path.parent / "runtime" / "admin_assets").exists()
assert importlib.util.find_spec("justflow_admin") is None
assert AdminPanel is not None
runtime = RuntimeSettings(profile=RuntimeProfile.LOCAL)
assert _configured_admin_panel(Settings(runtime=runtime)) is None
try:
    _configured_admin_panel(
        Settings(runtime=runtime, operations=OperationsSettings(admin_panel_enabled=True))
    )
except SystemExit as exc:
    assert str(exc) == ADMIN_INSTALL_MESSAGE
else:
    raise AssertionError("Enabled administration assets accepted a missing distribution")
static = files("justflow.visualization").joinpath("static")
for name in ("MERMAID_ASSET.md", "MERMAID_LICENSE", "mermaid.min.js"):
    assert static.joinpath(name).is_file(), name
assert TransportProvider is not None
assert {"direct", "http"}.issubset(builtin_transport_registry().providers)
assert tuple(load_bundled_schemas()) == SCHEMA_FILE_NAMES
openapi = load_bundled_openapi_document()
assert openapi["openapi"] == "3.1.0"
assert OPENAPI_FILE_NAME == "justflow-openapi-0.1.0.json"
assert "/v1/scheduled-starts" in openapi["paths"]
"""
ADMIN_SMOKE_PROGRAM = """
import sys
from pathlib import Path

for dependency_path in sys.argv[1:]:
    sys.path.append(str(Path(dependency_path).resolve()))

from justflow.__main__ import _configured_admin_panel
from justflow.config.settings import OperationsSettings, RuntimeSettings, Settings
from justflow.provenance import RuntimeProfile

panel = _configured_admin_panel(
    Settings(
        runtime=RuntimeSettings(profile=RuntimeProfile.LOCAL),
        operations=OperationsSettings(admin_panel_enabled=True),
    )
)
assert panel is not None
index = panel.resolve(("admin",))
assert index is not None
assert b"Justflow operations" in index.body
assert panel.resolve(("admin", "assets", "source.ts")) is None
assert panel.resolve(("admin", "assets", "../justflow-manifest.json")) is None
"""


def _environment_python(environment: Path) -> Path:
    if os.name == "nt":
        return environment / "Scripts" / "python.exe"
    return environment / "bin" / "python"


def _dependency_paths() -> tuple[Path, ...]:
    paths = tuple(
        Path(value).resolve()
        for value in sys.path
        if value and DEPENDENCY_DIRECTORY_NAMES.intersection(Path(value).parts)
    )
    if not paths:
        raise RuntimeError("The active interpreter exposes no dependency directories")
    return paths


def _single_wheel(directory: Path, pattern: str) -> Path:
    wheels = sorted(directory.glob(pattern))
    if len(wheels) != 1:
        raise ValueError(
            f"Expected one wheel for {pattern}, found {[path.name for path in wheels]}"
        )
    return wheels[0]


def _install(python: Path, wheel: Path) -> None:
    subprocess.run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-deps",
            "--no-index",
            str(wheel.resolve()),
        ],
        check=True,
    )


def smoke_test_wheels(directory: Path) -> None:
    core_wheel = _single_wheel(directory, CORE_WHEEL_PATTERN)
    admin_wheel = _single_wheel(directory, ADMIN_WHEEL_PATTERN)
    with tempfile.TemporaryDirectory(prefix="justflow-wheel-smoke-") as temporary:
        environment = Path(temporary) / "environment"
        venv.EnvBuilder(with_pip=True).create(environment)
        python = _environment_python(environment)
        dependency_paths = _dependency_paths()
        _install(python, core_wheel)
        subprocess.run(
            [
                str(python),
                "-I",
                "-c",
                CORE_SMOKE_PROGRAM,
                str(environment),
                *(str(path) for path in dependency_paths),
            ],
            cwd=temporary,
            check=True,
        )
        _install(python, admin_wheel)
        subprocess.run(
            [
                str(python),
                "-I",
                "-c",
                ADMIN_SMOKE_PROGRAM,
                *(str(path) for path in dependency_paths),
            ],
            cwd=temporary,
            check=True,
        )

    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--dry-run",
            "--disable-pip-version-check",
            "--no-index",
            "--find-links",
            str(directory.resolve()),
            f"{core_wheel.resolve()}[admin]",
        ],
        check=True,
    )


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: smoke_test_wheel.py DIST_DIRECTORY")
    smoke_test_wheels(Path(sys.argv[1]))


if __name__ == "__main__":
    main()
