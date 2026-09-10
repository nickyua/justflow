"""Validation rules for open transport-provider identifiers."""

from __future__ import annotations

import re

PROVIDER_NAME_MAX_LENGTH = 64
PROVIDER_NAME_PATTERN_TEXT = r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$"
PROVIDER_CONTRACT_VERSION_MAX_LENGTH = 64
PROVIDER_CONTRACT_VERSION_PATTERN_TEXT = r"^[A-Za-z0-9]+(?:[._+-][A-Za-z0-9]+)*$"

PROVIDER_NAME_PATTERN = re.compile(PROVIDER_NAME_PATTERN_TEXT)
PROVIDER_CONTRACT_VERSION_PATTERN = re.compile(PROVIDER_CONTRACT_VERSION_PATTERN_TEXT)


def is_valid_provider_name(value: str) -> bool:
    return (
        len(value) <= PROVIDER_NAME_MAX_LENGTH
        and PROVIDER_NAME_PATTERN.fullmatch(value) is not None
    )


def is_valid_contract_version(value: str) -> bool:
    return (
        len(value) <= PROVIDER_CONTRACT_VERSION_MAX_LENGTH
        and PROVIDER_CONTRACT_VERSION_PATTERN.fullmatch(value) is not None
    )
