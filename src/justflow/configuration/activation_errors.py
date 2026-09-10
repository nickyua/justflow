"""Backend-neutral publication and activation failures."""

from __future__ import annotations

from justflow.scope import safe_identity_digest


class ActivationError(Exception):
    """A configuration publication or activation operation failed."""


class ActivationNotFoundError(ActivationError):
    """Requested activation control-plane state does not exist."""


class ActivationConflictError(ActivationError):
    """Activation control-plane state changed concurrently."""

    def __init__(self, identity: str) -> None:
        self.identity_digest = safe_identity_digest("activation-conflict", identity)
        super().__init__("Activation state changed concurrently")


class ActivationUnavailableError(ActivationError):
    """The activation backend or required external system is unavailable."""


class ActivationIntegrityError(ActivationError):
    """Persisted activation state is corrupt or internally inconsistent."""


class ActivationLimitError(ActivationError):
    """An activation operation exceeds a configured bound."""
