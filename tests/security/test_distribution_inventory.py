"""Tests for the pip-independent container distribution inventory."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

import pytest

from scripts.export_installed_distributions import (
    DistributionInventoryError,
    verify_inventory,
)


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: None


@dataclass(frozen=True, kw_only=True)
class Raises:
    exception: type[Exception]
    match: str


Outcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class InventoryCase:
    id: str
    installed: tuple[tuple[str, str], ...]
    expected: tuple[tuple[str, str], ...]
    outcome: Outcome


INVENTORY_CASES = [
    InventoryCase(
        id="exact",
        installed=(("package-a", "1.0"), ("package-b", "2.0")),
        expected=(("package-a", "1.0"), ("package-b", "2.0")),
        outcome=Returns(value=None),
    ),
    InventoryCase(
        id="missing",
        installed=(("package-a", "1.0"),),
        expected=(("package-a", "1.0"), ("package-b", "2.0")),
        outcome=Raises(exception=DistributionInventoryError, match="missing=.*package-b"),
    ),
    InventoryCase(
        id="unexpected",
        installed=(("package-a", "1.0"), ("package-b", "2.0")),
        expected=(("package-a", "1.0"),),
        outcome=Raises(exception=DistributionInventoryError, match="unexpected=.*package-b"),
    ),
    InventoryCase(
        id="version-mismatch",
        installed=(("package-a", "1.1"),),
        expected=(("package-a", "1.0"),),
        outcome=Raises(exception=DistributionInventoryError, match="mismatched=.*package-a"),
    ),
]


@pytest.mark.parametrize("case", INVENTORY_CASES, ids=lambda case: case.id)
def test_distribution_inventory_contract(case: InventoryCase) -> None:
    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exception, match=case.outcome.match):
            verify_inventory(dict(case.installed), dict(case.expected))
        return

    assert verify_inventory(dict(case.installed), dict(case.expected)) is case.outcome.value
