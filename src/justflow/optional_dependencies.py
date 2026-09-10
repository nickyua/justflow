"""Loading for dependencies supplied by optional package extras."""

from __future__ import annotations

import importlib
from types import ModuleType


class OptionalDependencyError(ModuleNotFoundError):
    def __init__(self, module_name: str, *, extra: str, feature: str) -> None:
        self.module_name = module_name
        self.extra = extra
        self.feature = feature
        super().__init__(
            f"'{module_name}' is required for {feature} but is not installed. "
            f"Install it with: pip install justflow[{extra}]"
        )


def load_optional_dependency(
    module_name: str,
    *,
    extra: str,
    feature: str,
) -> ModuleType:
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        raise OptionalDependencyError(
            module_name,
            extra=extra,
            feature=feature,
        ) from exc
