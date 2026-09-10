"""Optional beta administration console for Justflow."""

from justflow.runtime.admin_panel import AdminPanel, AdminPanelAsset
from justflow_admin.panel import BetaAdminPanel


def create_admin_panel() -> AdminPanel:
    """Create the explicitly composed adapter for the prebuilt beta console."""
    return BetaAdminPanel()


__all__ = ["AdminPanel", "AdminPanelAsset", "BetaAdminPanel", "create_admin_panel"]
