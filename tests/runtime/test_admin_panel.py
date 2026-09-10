"""Tests for the packaged administration panel boundary."""

from __future__ import annotations

import hashlib

import pytest
from justflow_admin.panel import (
    ADMIN_CONTENT_SECURITY_POLICY,
    CSS_CONTENT_TYPE,
    GRAPH_FRAME_CONTENT_SECURITY_POLICY,
    HASHED_ASSET_CACHE_CONTROL,
    HTML_CONTENT_TYPE,
    JAVASCRIPT_CONTENT_TYPE,
    MAX_ADMIN_ASSET_BYTES,
    BetaAdminPanel,
    _build_manifest,
)


@pytest.mark.parametrize(
    ("segments", "content_type", "content_marker", "policy"),
    [
        pytest.param(
            ("admin",),
            HTML_CONTENT_TYPE,
            b"Justflow operations",
            ADMIN_CONTENT_SECURITY_POLICY,
            id="document",
        ),
        pytest.param(
            ("admin", "graph-frame"),
            HTML_CONTENT_TYPE,
            b"Workflow graph",
            GRAPH_FRAME_CONTENT_SECURITY_POLICY,
            id="graph-frame-document",
        ),
        pytest.param(
            ("admin", "workflows", "example"),
            HTML_CONTENT_TYPE,
            b'id="view-workflow"',
            ADMIN_CONTENT_SECURITY_POLICY,
            id="workflow-route",
        ),
        pytest.param(
            ("admin", "editor", "workflows", "example"),
            HTML_CONTENT_TYPE,
            b'id="view-workflow-editor"',
            ADMIN_CONTENT_SECURITY_POLICY,
            id="fragment-editor-route",
        ),
        pytest.param(
            ("admin", "assets", "mermaid.js"),
            JAVASCRIPT_CONTENT_TYPE,
            b"mermaid",
            GRAPH_FRAME_CONTENT_SECURITY_POLICY,
            id="mermaid-vendored",
        ),
        pytest.param(
            ("admin", "assets", "graph-tooltips.js"),
            JAVASCRIPT_CONTENT_TYPE,
            b"JustflowGraphTooltips",
            GRAPH_FRAME_CONTENT_SECURITY_POLICY,
            id="tooltips-shared",
        ),
    ],
)
def test_admin_panel_serves_fixed_documents_and_shared_assets(
    segments: tuple[str, ...],
    content_type: bytes,
    content_marker: bytes,
    policy: bytes,
) -> None:
    asset = BetaAdminPanel().resolve(segments)

    assert asset is not None
    assert asset.content_type == content_type
    assert content_marker in asset.body
    assert (b"content-security-policy", policy) in asset.headers
    assert (b"cache-control", b"no-store") in asset.headers


def test_every_manifest_asset_serves_with_integrity_and_immutable_caching() -> None:
    manifest = _build_manifest()
    assert manifest, "the build manifest must list the packaged assets"
    panel = BetaAdminPanel()
    for path, (expected_bytes, expected_digest) in manifest.items():
        name = path.removeprefix("assets/")
        if path == name:
            continue  # non-asset outputs such as the HTML documents
        asset = panel.resolve(("admin", "assets", name))
        assert asset is not None, path
        assert len(asset.body) == expected_bytes
        assert len(asset.body) <= MAX_ADMIN_ASSET_BYTES
        assert hashlib.sha256(asset.body).hexdigest() == expected_digest
        expected_type = JAVASCRIPT_CONTENT_TYPE if name.endswith(".js") else CSS_CONTENT_TYPE
        assert asset.content_type == expected_type
        assert (b"cache-control", HASHED_ASSET_CACHE_CONTROL) in asset.headers
        assert (b"content-security-policy", ADMIN_CONTENT_SECURITY_POLICY) in asset.headers


@pytest.mark.parametrize(
    ("prefix", "marker"),
    [
        pytest.param("assets/app-", b"/v1/operations", id="application"),
        pytest.param("assets/editor-", b"EditorView", id="editor-chunk"),
        pytest.param("assets/graph-frame-", b"justflow-graph-bootstrap", id="graph-frame-script"),
    ],
)
def test_expected_hashed_bundles_are_present(prefix: str, marker: bytes) -> None:
    manifest = _build_manifest()
    matches = [path for path in manifest if path.startswith(prefix) and path.endswith(".js")]
    assert len(matches) == 1, f"expected exactly one bundle for {prefix}"
    name = matches[0].removeprefix("assets/")
    asset = BetaAdminPanel().resolve(("admin", "assets", name))
    assert asset is not None
    assert marker in asset.body


@pytest.mark.parametrize(
    "segments",
    [
        pytest.param(("admin", "../configuration.sqlite3"), id="traversal"),
        pytest.param(("admin", "assets", "source.ts"), id="unlisted-extension"),
        pytest.param(("admin", "assets", "app.js"), id="unhashed-legacy-name"),
        pytest.param(("admin", "assets", "../justflow-manifest.json"), id="manifest-escape"),
        pytest.param(("admin", "justflow-manifest.json"), id="manifest-direct"),
        pytest.param(("admin", "assets"), id="directory"),
        pytest.param(("admin", "editor", "workflows"), id="fragment-route-without-name"),
        pytest.param(("admin", "editor", "workflows", ""), id="fragment-route-empty-name"),
        pytest.param(
            ("admin", "editor", "schedules", "example"), id="fragment-route-unknown-section"
        ),
    ],
)
def test_admin_panel_rejects_every_non_allowlisted_path(segments: tuple[str, ...]) -> None:
    assert BetaAdminPanel().resolve(segments) is None


def test_shared_graph_tooltip_engine_is_single_sourced() -> None:
    """The panel serves the exact bytes the CLI export inlines — drift is impossible."""
    from importlib.resources import files

    from justflow.visualization.renderer import render_html

    served = BetaAdminPanel().resolve(("admin", "assets", "graph-tooltips.js"))
    assert served is not None
    shared = files("justflow.visualization").joinpath("static", "graph-tooltips.js").read_bytes()
    assert served.body == shared

    html = render_html("graph TD", "drift-check")
    assert shared.decode("utf-8") in html
    assert "graphTooltips.attach" in html
