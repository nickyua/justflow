"""Explicit production identity for tests that exercise production policies."""

from justflow.config.settings import RuntimeSettings
from justflow.scope import RuntimeScope

PRODUCTION_SCOPE = RuntimeScope.create(
    tenant="test-tenant", application="test-application", environment="production"
)
PRODUCTION_RUNTIME = RuntimeSettings(scope=PRODUCTION_SCOPE)
