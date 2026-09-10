"""Fail-closed smoke and filesystem checks for built container images."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tarfile
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

MAX_COMMAND_OUTPUT_BYTES = 1_048_576
MAX_IMAGE_ARCHIVE_BYTES = 2_147_483_648
MAX_IMAGE_FILES = 200_000
COMMAND_TIMEOUT_SECONDS = 180
PYTHON_RUNTIME_VERSION = "3.14"
CONTAINER_ID_PATTERN = re.compile(r"^[0-9a-f]{64}$")
REQUIRED_LABELS = (
    "org.opencontainers.image.created",
    "org.opencontainers.image.revision",
    "org.opencontainers.image.source",
    "org.opencontainers.image.version",
)
FORBIDDEN_HISTORY_MARKERS = (
    "AWS_SECRET_ACCESS_KEY",
    "BEGIN PRIVATE KEY",
    "ghp_",
    "/Users/",
    "/home/runner/work/",
)
FORBIDDEN_IMAGE_PATH_PREFIXES = (
    ".git/",
    "build/",
    "repo/",
    "specs/",
    "workspace/",
)
FORBIDDEN_RUNTIME_PATHS = frozenset(
    {
        "opt/justflow/venv/bin/node",
        "opt/justflow/venv/bin/npm",
        "opt/justflow/venv/bin/pip",
        "opt/justflow/venv/bin/pip3",
        f"opt/justflow/venv/bin/pip{PYTHON_RUNTIME_VERSION}",
        "usr/bin/node",
        "usr/bin/npm",
        "usr/local/bin/node",
        "usr/local/bin/npm",
        "usr/local/bin/pip",
        "usr/local/bin/pip3",
        f"usr/local/bin/pip{PYTHON_RUNTIME_VERSION}",
    }
)
FORBIDDEN_RUNTIME_PATH_PREFIXES = (
    f"opt/justflow/venv/lib/python{PYTHON_RUNTIME_VERSION}/site-packages/pip",
    f"usr/local/lib/python{PYTHON_RUNTIME_VERSION}/ensurepip",
    f"usr/local/lib/python{PYTHON_RUNTIME_VERSION}/site-packages/pip",
)
BASE_IMPORT_SMOKE = (
    "import asyncpg,boto3,grpc,redis,uvicorn; "
    "from justflow_admin import create_admin_panel; "
    "from justflow.openapi import load_bundled_openapi_document; "
    "assert '/v1/workflows' in load_bundled_openapi_document()['paths']; "
    "assert create_admin_panel().resolve(('admin',)) is not None; "
    "import shutil; assert shutil.which('node') is None; assert shutil.which('npm') is None"
)
REFERENCE_IMPORT_SMOKE = (
    "from host_application import create_application; "
    "from host_application.resources import PROVIDER_NAME; "
    "assert PROVIDER_NAME in create_application().resource_registry.providers"
)
REFERENCE_CONFIGURATION_ENTRYPOINT = "host-application-config"


class ContainerVerificationError(Exception):
    pass


def verify_images(base_image: str, reference_image: str) -> None:
    for image in (base_image, reference_image):
        _validate_image_reference(image)
        document = _inspect_image(image)
        _verify_common_configuration(image, document)
        _verify_history(image)
        _verify_filesystem(image)
        _run_docker(
            (
                "run",
                "--rm",
                "--read-only",
                "--tmpfs",
                "/tmp:size=16m,mode=1777",
                image,
                "--help",
            )
        )
        _verify_pip_absent(image)
    _verify_healthcheck(reference_image, _inspect_image(reference_image))
    _run_python(base_image, BASE_IMPORT_SMOKE)
    _run_python(reference_image, REFERENCE_IMPORT_SMOKE)
    _run_docker(
        (
            "run",
            "--rm",
            "--read-only",
            "--tmpfs",
            "/tmp:size=16m,mode=1777",
            "--entrypoint",
            REFERENCE_CONFIGURATION_ENTRYPOINT,
            reference_image,
            "--help",
        )
    )


def _validate_image_reference(image: str) -> None:
    if not image or len(image) > 512 or image.startswith("-") or any(c.isspace() for c in image):
        raise ContainerVerificationError("Container image reference is invalid")


def _inspect_image(image: str) -> dict[str, Any]:
    payload = _run_docker(("image", "inspect", image))
    try:
        document = json.loads(payload)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise ContainerVerificationError("Docker returned invalid image metadata") from exc
    if not isinstance(document, list) or len(document) != 1 or not isinstance(document[0], dict):
        raise ContainerVerificationError("Docker returned an unexpected image metadata shape")
    return document[0]


def _verify_common_configuration(image: str, document: dict[str, Any]) -> None:
    config = _mapping_field(document, "Config")
    user = config.get("User")
    if not isinstance(user, str) or user in {"", "0", "root", "0:0", "root:root"}:
        raise ContainerVerificationError(f"Image '{image}' does not use a non-root user")
    labels = config.get("Labels")
    if not isinstance(labels, dict):
        raise ContainerVerificationError(f"Image '{image}' has no OCI labels")
    for label in REQUIRED_LABELS:
        value = labels.get(label)
        if not isinstance(value, str) or not value or value == "unknown":
            raise ContainerVerificationError(f"Image '{image}' has incomplete OCI provenance")


def _verify_healthcheck(image: str, document: dict[str, Any]) -> None:
    config = _mapping_field(document, "Config")
    healthcheck = config.get("Healthcheck")
    if not isinstance(healthcheck, dict) or not healthcheck.get("Test"):
        raise ContainerVerificationError(f"Image '{image}' has no health check")


def _verify_history(image: str) -> None:
    history = _run_docker(("history", "--no-trunc", "--format", "{{.CreatedBy}}", image))
    if any(marker in history for marker in FORBIDDEN_HISTORY_MARKERS):
        raise ContainerVerificationError(f"Image '{image}' history contains forbidden material")


def _verify_filesystem(image: str) -> None:
    container_id = _run_docker(("create", image, "--help")).strip()
    if CONTAINER_ID_PATTERN.fullmatch(container_id) is None:
        raise ContainerVerificationError("Docker returned an invalid temporary container identity")
    try:
        with tempfile.TemporaryDirectory(prefix="justflow-image-") as directory:
            archive = Path(directory) / "filesystem.tar"
            _run_docker(("export", "--output", str(archive), container_id))
            if archive.stat().st_size > MAX_IMAGE_ARCHIVE_BYTES:
                raise ContainerVerificationError(f"Image '{image}' exceeds its byte bound")
            with tarfile.open(archive, mode="r") as stream:
                members = stream.getmembers()
            if len(members) > MAX_IMAGE_FILES:
                raise ContainerVerificationError(f"Image '{image}' exceeds its file-count bound")
            for member in members:
                path = member.name.removeprefix("./").lstrip("/")
                if (
                    path in FORBIDDEN_RUNTIME_PATHS
                    or path.startswith(FORBIDDEN_IMAGE_PATH_PREFIXES)
                    or path.startswith(FORBIDDEN_RUNTIME_PATH_PREFIXES)
                ):
                    raise ContainerVerificationError(
                        f"Image '{image}' contains forbidden repository or runtime material"
                    )
    finally:
        _run_docker(("container", "rm", "--force", container_id))


def _run_python(image: str, program: str) -> None:
    _run_docker(
        (
            "run",
            "--rm",
            "--read-only",
            "--tmpfs",
            "/tmp:size=16m,mode=1777",
            "--entrypoint",
            "python",
            image,
            "-c",
            program,
        )
    )


def _verify_pip_absent(image: str) -> None:
    completed = _docker_command(
        (
            "run",
            "--rm",
            "--read-only",
            "--tmpfs",
            "/tmp:size=16m,mode=1777",
            "--entrypoint",
            "python",
            image,
            "-m",
            "pip",
            "--version",
        )
    )
    if completed.returncode == 0:
        raise ContainerVerificationError(f"Image '{image}' contains an executable pip module")


def _docker_command(arguments: Sequence[str]) -> subprocess.CompletedProcess[bytes]:
    try:
        completed = subprocess.run(
            ("docker", *arguments),
            check=False,
            capture_output=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ContainerVerificationError("Docker verification command is unavailable") from exc
    output = completed.stdout + completed.stderr
    if len(output) > MAX_COMMAND_OUTPUT_BYTES:
        raise ContainerVerificationError("Docker verification output exceeds its byte bound")
    return completed


def _run_docker(arguments: Sequence[str]) -> str:
    completed = _docker_command(arguments)
    if completed.returncode != 0:
        raise ContainerVerificationError("Docker verification command failed")
    try:
        return completed.stdout.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ContainerVerificationError("Docker verification output is not UTF-8") from exc


def _mapping_field(document: dict[str, Any], field: str) -> dict[str, Any]:
    value = document.get(field)
    if not isinstance(value, dict):
        raise ContainerVerificationError(f"Docker image metadata field '{field}' is invalid")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify built Justflow container images")
    parser.add_argument("base_image")
    parser.add_argument("reference_image")
    args = parser.parse_args()
    try:
        verify_images(args.base_image, args.reference_image)
    except ContainerVerificationError as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
