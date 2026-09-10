"""Contract tests for immutable container distribution artifacts."""

from __future__ import annotations

import stat
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
BASE_DOCKERFILE = ROOT / "docker/Dockerfile"
REFERENCE_DOCKERFILE = ROOT / "examples/host_application/Dockerfile"
DOCKERIGNORE = ROOT / ".dockerignore"
COMPOSE = ROOT / "compose.yaml"
LOCAL_STACK_RUNNER = ROOT / "scripts/run_local_stack.sh"
BASE_PYTHON_NAME_ARGUMENT = "PYTHON_BASE_NAME"
BASE_PYTHON_DIGEST_ARGUMENT = "PYTHON_BASE_DIGEST"
BASE_PYTHON_RUNTIME_VERSION_ARGUMENT = "PYTHON_RUNTIME_VERSION"
REFERENCE_PYTHON_NAME_ARGUMENT = "PYTHON_BUILD_NAME"
REFERENCE_PYTHON_DIGEST_ARGUMENT = "PYTHON_BUILD_DIGEST"
REFERENCE_PYTHON_RUNTIME_VERSION_ARGUMENT = "PYTHON_RUNTIME_VERSION"


@pytest.mark.parametrize("service", ["configuration-init", "catalog-init", "worker", "api"])
def test_compose_application_commands_receive_explicit_local_scope_policy(service: str) -> None:
    declaration = yaml.safe_load(COMPOSE.read_text())["services"][service]
    assert declaration.get("environment", {}).get("JUSTFLOW_RUNTIME__PROFILE") == "local"


@pytest.mark.parametrize(
    ("path", "required_fragments"),
    [
        pytest.param(
            BASE_DOCKERFILE,
            (
                "PYTHON_BASE_NAME=python:3.14.7-slim-trixie",
                "PYTHON_BASE_DIGEST=sha256:",
                "node:24.11.1-bookworm-slim@sha256:",
                "pip uninstall --yes pip",
                "USER ${RUNTIME_UID}:${RUNTIME_GID}",
                'ENTRYPOINT ["python", "-m", "justflow"]',
                'VOLUME ["/var/lib/justflow"]',
            ),
            id="base-image",
        ),
        pytest.param(
            REFERENCE_DOCKERFILE,
            (
                "FROM ${JUSTFLOW_BASE_IMAGE} AS runtime",
                'python -m venv --without-pip "${APPLICATION_VIRTUAL_ENV}"',
                'python -m pip --python "${APPLICATION_VIRTUAL_ENV}" install',
                "${APPLICATION_VIRTUAL_ENV}/lib/python${PYTHON_RUNTIME_VERSION}/site-packages/",
                "${APPLICATION_VIRTUAL_ENV}/bin/host-application-config",
                "COPY --chown=root:root examples/host_application/src/host_application/configs",
                "HEALTHCHECK",
                'ENTRYPOINT ["uvicorn"]',
                "USER 65532:65532",
            ),
            id="reference-image",
        ),
        pytest.param(
            COMPOSE,
            (
                "read_only: true",
                "service_completed_successfully",
                "JUSTFLOW_DEPLOYMENT__ARTIFACT_DIGEST: local-development",
                'entrypoint: ["uvicorn"]',
                "host_application.worker:create_worker_application",
            ),
            id="compose-runtime",
        ),
    ],
)
def test_container_artifact_preserves_required_runtime_contracts(
    path: Path,
    required_fragments: tuple[str, ...],
) -> None:
    content = path.read_text()

    assert all(fragment in content for fragment in required_fragments)


def test_docker_context_is_deny_by_default() -> None:
    patterns = DOCKERIGNORE.read_text().splitlines()

    assert patterns[0] == "**"
    assert not any(pattern.startswith("!.git") for pattern in patterns)
    assert not any(pattern.startswith("!.env") for pattern in patterns)
    assert not any(pattern.startswith("!specs") for pattern in patterns)


def test_python_base_image_pins_stay_aligned() -> None:
    base_content = BASE_DOCKERFILE.read_text()
    reference_content = REFERENCE_DOCKERFILE.read_text()
    base_name = _dockerfile_argument(base_content, BASE_PYTHON_NAME_ARGUMENT)
    base_digest = _dockerfile_argument(base_content, BASE_PYTHON_DIGEST_ARGUMENT)
    base_runtime_version = _dockerfile_argument(
        base_content,
        BASE_PYTHON_RUNTIME_VERSION_ARGUMENT,
    )
    reference_name = _dockerfile_argument(reference_content, REFERENCE_PYTHON_NAME_ARGUMENT)
    reference_digest = _dockerfile_argument(
        reference_content,
        REFERENCE_PYTHON_DIGEST_ARGUMENT,
    )
    reference_runtime_version = _dockerfile_argument(
        reference_content,
        REFERENCE_PYTHON_RUNTIME_VERSION_ARGUMENT,
    )

    assert reference_name == base_name
    assert reference_digest == base_digest
    assert reference_runtime_version == base_runtime_version
    assert 'org.opencontainers.image.base.name="${PYTHON_BASE_NAME}"' in base_content
    assert 'org.opencontainers.image.base.digest="${PYTHON_BASE_DIGEST}"' in base_content


def test_reference_runtime_adds_the_application_without_a_package_installer() -> None:
    content = REFERENCE_DOCKERFILE.read_text()
    _, runtime_stage = content.split("FROM ${JUSTFLOW_BASE_IMAGE} AS runtime", maxsplit=1)

    assert "pip install" not in runtime_stage
    assert (
        "${APPLICATION_VIRTUAL_ENV}/lib/python${PYTHON_RUNTIME_VERSION}/site-packages/"
        in runtime_stage
    )
    assert "${APPLICATION_VIRTUAL_ENV}/bin/host-application-config" in runtime_stage


def test_local_stack_runner_builds_images_and_starts_verified_compose() -> None:
    content = LOCAL_STACK_RUNNER.read_text()
    required_fragments = (
        'readonly BASE_IMAGE="justflow:local"',
        'readonly REFERENCE_IMAGE="justflow-host-application:local"',
        'source_revision="$(git -C "${REPOSITORY_ROOT}" rev-parse HEAD)"',
        "docker build \\",
        "docker compose config --quiet",
        "docker compose up \\",
        '--wait-timeout "${COMPOSE_WAIT_TIMEOUT_SECONDS}"',
    )

    assert LOCAL_STACK_RUNNER.stat().st_mode & stat.S_IXUSR
    assert all(fragment in content for fragment in required_fragments)


def _dockerfile_argument(content: str, name: str) -> str:
    prefix = f"ARG {name}="
    values = [line.removeprefix(prefix) for line in content.splitlines() if line.startswith(prefix)]
    assert len(values) == 1
    return values[0]
