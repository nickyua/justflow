"""Backend-neutral configuration boundary failures."""

from __future__ import annotations

from justflow.scope import safe_identity_digest


class ConfigurationError(Exception):
    """Configuration content or an operation is invalid."""


class ConfigurationNotFoundError(ConfigurationError):
    """Requested configuration state does not exist in the scope."""


class ConfigurationConflictError(ConfigurationError):
    """Configuration state changed concurrently."""

    def __init__(self, identity: str) -> None:
        self.identity_digest = safe_identity_digest("configuration-conflict", identity)
        super().__init__("Configuration state changed concurrently")


class ConfigurationUnavailableError(ConfigurationError):
    """The configured backend could not complete the operation."""


class ConfigurationIntegrityError(ConfigurationError):
    """Stored configuration state is corrupt or internally inconsistent."""


class ConfigurationLimitError(ConfigurationError):
    """A configuration operation exceeds a documented bound."""


class ConfigurationScopeError(ConfigurationError):
    """A configuration lookup attempted to cross its trusted scope."""
