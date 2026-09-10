"""Offline public documentation verification."""

from importlib.metadata import PackageNotFoundError
from pathlib import Path

import pytest
import yaml

from scripts.generate_docs_reference import (
    _normalize_cli_help,
    _symbol_summary,
    generated_pages,
    render_settings_reference,
)
from scripts.verify_docs import verify_documentation

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
PYTHON_313_HELP = """usage: justflow [-h]
                {worker,worker-server,api,serve,validate,graph,run,definitions,triggers,schema}
                ...

options:
  -o OUTPUT, --output OUTPUT
                        Output HTML path

usage: justflow definitions import [-h] [--config-dir CONFIG_DIR] --input
                                   INPUT [--dry-run]
"""
PYTHON_314_HELP = """usage: justflow [-h]
                {worker,worker-server,api,serve,validate,graph,run,definitions,triggers,schema} ...

options:
  -o, --output OUTPUT   Output HTML path

usage: justflow definitions import [-h] [--config-dir CONFIG_DIR]
                                   --input INPUT [--dry-run]
"""
NORMALIZED_HELP = """usage: justflow [-h]
                {worker,worker-server,api,serve,validate,graph,run,definitions,triggers,schema}
                ...

options:
  -o OUTPUT, --output OUTPUT
                        Output HTML path

usage: justflow definitions import [-h] [--config-dir CONFIG_DIR] --input
                                   INPUT [--dry-run]"""


def test_generated_documentation_is_current() -> None:
    for path, content in generated_pages().items():
        assert path.read_text(encoding="utf-8") == content.rstrip() + "\n"


@pytest.mark.parametrize(
    "help_text",
    (
        pytest.param(PYTHON_313_HELP, id="python-3.13"),
        pytest.param(PYTHON_314_HELP, id="python-3.14"),
    ),
)
def test_cli_reference_normalizes_python_argparse_output(help_text: str) -> None:
    assert _normalize_cli_help(help_text) == NORMALIZED_HELP


def test_python_reference_owns_union_type_summary() -> None:
    assert _symbol_summary(str | None) == "Represent a union type"


def test_settings_reference_is_independent_of_installed_package_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("justflow.provenance.version", lambda _distribution: "0.1.0")
    installed = render_settings_reference()

    def missing_version(distribution: str) -> str:
        raise PackageNotFoundError(distribution)

    monkeypatch.setattr("justflow.provenance.version", missing_version)
    missing = render_settings_reference()

    assert installed == missing
    assert (
        "| `JUSTFLOW_DEPLOYMENT__PACKAGE_VERSION` | `str \\| NoneType` | "
        "`installed version or null` | all modes |"
    ) in installed


def test_public_documentation_links_commands_and_boundaries() -> None:
    verify_documentation()


def test_downstream_ci_example_has_security_gate_shape() -> None:
    path = REPOSITORY_ROOT / "docs" / "examples" / "downstream-security.yml"
    workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
    jobs = workflow["jobs"]

    assert set(jobs) == {"application-security"}
    step_names = {step["name"] for step in jobs["application-security"]["steps"]}
    assert {
        "Audit resolved dependencies",
        "Scan repository history for secrets",
        "Scan the final application image",
        "Validate application configuration",
    }.issubset(step_names)
