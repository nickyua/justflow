"""Tests for optional dependency diagnostics."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from justflow.optional_dependencies import (
    OptionalDependencyError,
    load_optional_dependency,
)


def test_missing_optional_dependency_has_feature_and_install_hint() -> None:
    with (
        patch(
            "justflow.optional_dependencies.importlib.import_module",
            side_effect=ModuleNotFoundError("missing"),
        ),
        pytest.raises(OptionalDependencyError) as exc_info,
    ):
        load_optional_dependency("package", extra="feature", feature="sample support")

    assert exc_info.value.module_name == "package"
    assert "sample support" in str(exc_info.value)
    assert "pip install justflow[feature]" in str(exc_info.value)
