"""Manifest-verified static asset adapter for the beta administration console."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files

from justflow.runtime.admin_panel import AdminPanelAsset

HTML_CONTENT_TYPE = b"text/html; charset=utf-8"
JAVASCRIPT_CONTENT_TYPE = b"text/javascript; charset=utf-8"
CSS_CONTENT_TYPE = b"text/css; charset=utf-8"
MAX_ADMIN_ASSET_BYTES = 524_288
MAX_MERMAID_ASSET_BYTES = 4_194_304
ADMIN_ASSET_PACKAGE = "justflow_admin.assets"
ADMIN_ASSET_DIRECTORY = "dist"
VISUALIZATION_ASSET_PACKAGE = "justflow.visualization"
VISUALIZATION_ASSET_DIRECTORY = "static"
ADMIN_CONTENT_SECURITY_POLICY = (
    b"default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
    b"connect-src 'self'; img-src 'self' data:; frame-src 'self'; base-uri 'none'; "
    b"form-action 'none'; frame-ancestors 'none'"
)
GRAPH_FRAME_CONTENT_SECURITY_POLICY = (
    b"default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
    b"img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'self'"
)
ADMIN_SECURITY_HEADERS = (
    (b"cache-control", b"no-store"),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"no-referrer"),
    (b"content-security-policy", ADMIN_CONTENT_SECURITY_POLICY),
)
CROSS_ORIGIN_STATIC_HEADER = (b"access-control-allow-origin", b"*")
GRAPH_FRAME_SECURITY_HEADERS = (
    (b"cache-control", b"no-store"),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"no-referrer"),
    (b"content-security-policy", GRAPH_FRAME_CONTENT_SECURITY_POLICY),
    CROSS_ORIGIN_STATIC_HEADER,
)
HASHED_ASSET_CACHE_CONTROL = b"private, max-age=31536000, immutable"
HASHED_ASSET_SECURITY_HEADERS = (
    (b"cache-control", HASHED_ASSET_CACHE_CONTROL),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"no-referrer"),
    (b"content-security-policy", ADMIN_CONTENT_SECURITY_POLICY),
    CROSS_ORIGIN_STATIC_HEADER,
)
MANIFEST_FILENAME = "justflow-manifest.json"
ASSET_CONTENT_TYPES = {
    ".js": JAVASCRIPT_CONTENT_TYPE,
    ".css": CSS_CONTENT_TYPE,
}
ADMIN_WORKFLOW_ROUTE_PREFIX = ("admin", "workflows")
ADMIN_WORKFLOW_ROUTE_SEGMENTS = 3
ADMIN_FRAGMENT_ROUTE_PREFIX = ("admin", "editor", "workflows")
ADMIN_FRAGMENT_ROUTE_SEGMENTS = 4


@dataclass(frozen=True, kw_only=True)
class AssetDeclaration:
    package: str
    directory: str
    parts: tuple[str, ...]
    content_type: bytes
    max_bytes: int = MAX_ADMIN_ASSET_BYTES
    headers: tuple[tuple[bytes, bytes], ...] = ADMIN_SECURITY_HEADERS


def _panel_asset(
    parts: tuple[str, ...],
    content_type: bytes,
    *,
    headers: tuple[tuple[bytes, bytes], ...] = ADMIN_SECURITY_HEADERS,
) -> AssetDeclaration:
    return AssetDeclaration(
        package=ADMIN_ASSET_PACKAGE,
        directory=ADMIN_ASSET_DIRECTORY,
        parts=parts,
        content_type=content_type,
        headers=headers,
    )


ADMIN_ASSETS: dict[tuple[str, ...], AssetDeclaration] = {
    ("admin",): _panel_asset(("index.html",), HTML_CONTENT_TYPE),
    ("admin", "graph-frame"): _panel_asset(
        ("graph-frame.html",),
        HTML_CONTENT_TYPE,
        headers=GRAPH_FRAME_SECURITY_HEADERS,
    ),
    ("admin", "assets", "graph-tooltips.js"): AssetDeclaration(
        package=VISUALIZATION_ASSET_PACKAGE,
        directory=VISUALIZATION_ASSET_DIRECTORY,
        parts=("graph-tooltips.js",),
        content_type=JAVASCRIPT_CONTENT_TYPE,
        headers=GRAPH_FRAME_SECURITY_HEADERS,
    ),
    ("admin", "assets", "graph-tooltips.css"): AssetDeclaration(
        package=VISUALIZATION_ASSET_PACKAGE,
        directory=VISUALIZATION_ASSET_DIRECTORY,
        parts=("graph-tooltips.css",),
        content_type=CSS_CONTENT_TYPE,
        headers=GRAPH_FRAME_SECURITY_HEADERS,
    ),
    ("admin", "assets", "mermaid.js"): AssetDeclaration(
        package=VISUALIZATION_ASSET_PACKAGE,
        directory=VISUALIZATION_ASSET_DIRECTORY,
        parts=("mermaid.min.js",),
        content_type=JAVASCRIPT_CONTENT_TYPE,
        max_bytes=MAX_MERMAID_ASSET_BYTES,
        headers=GRAPH_FRAME_SECURITY_HEADERS,
    ),
}


@lru_cache(maxsize=1)
def _build_manifest() -> dict[str, tuple[int, str]]:
    try:
        payload = (
            files(ADMIN_ASSET_PACKAGE)
            .joinpath(ADMIN_ASSET_DIRECTORY, MANIFEST_FILENAME)
            .read_text(encoding="utf-8")
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Administration panel assets are not built; run `make admin-build`"
        ) from exc
    try:
        parsed = json.loads(payload)
        return {
            str(name): (int(entry["bytes"]), str(entry["sha256"]))
            for name, entry in parsed["files"].items()
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("Administration panel build manifest is invalid") from exc


class BetaAdminPanel:
    """Resolve only fixed routes and build-manifest-listed immutable assets."""

    def has_route(self, segments: tuple[str, ...]) -> bool:
        if segments in ADMIN_ASSETS or self._is_workflow_route(segments):
            return True
        return self._manifest_entry(segments) is not None

    def resolve(self, segments: tuple[str, ...]) -> AdminPanelAsset | None:
        declaration = ADMIN_ASSETS.get(segments)
        if declaration is None and self._is_workflow_route(segments):
            declaration = ADMIN_ASSETS[("admin",)]
        if declaration is not None:
            body = (
                files(declaration.package)
                .joinpath(declaration.directory)
                .joinpath(*declaration.parts)
                .read_bytes()
            )
            if len(body) > declaration.max_bytes:
                raise RuntimeError("Administration panel asset exceeds its byte bound")
            return AdminPanelAsset(
                body=body,
                content_type=declaration.content_type,
                headers=declaration.headers,
            )
        entry = self._manifest_entry(segments)
        if entry is None:
            return None
        manifest_path, (expected_bytes, expected_digest), content_type = entry
        body = (
            files(ADMIN_ASSET_PACKAGE)
            .joinpath(ADMIN_ASSET_DIRECTORY, *manifest_path.split("/"))
            .read_bytes()
        )
        if len(body) > MAX_ADMIN_ASSET_BYTES:
            raise RuntimeError("Administration panel asset exceeds its byte bound")
        if len(body) != expected_bytes or hashlib.sha256(body).hexdigest() != expected_digest:
            raise RuntimeError("Administration panel asset failed its manifest integrity check")
        return AdminPanelAsset(
            body=body,
            content_type=content_type,
            headers=HASHED_ASSET_SECURITY_HEADERS,
        )

    @staticmethod
    def _manifest_entry(
        segments: tuple[str, ...],
    ) -> tuple[str, tuple[int, str], bytes] | None:
        if len(segments) != 3 or segments[:2] != ("admin", "assets"):
            return None
        name = segments[2]
        suffix = name[name.rfind(".") :] if "." in name else ""
        content_type = ASSET_CONTENT_TYPES.get(suffix)
        if content_type is None:
            return None
        manifest_path = f"assets/{name}"
        entry = _build_manifest().get(manifest_path)
        if entry is None:
            return None
        return manifest_path, entry, content_type

    @staticmethod
    def _is_workflow_route(segments: tuple[str, ...]) -> bool:
        if (
            len(segments) == ADMIN_WORKFLOW_ROUTE_SEGMENTS
            and segments[:2] == ADMIN_WORKFLOW_ROUTE_PREFIX
            and bool(segments[2])
        ):
            return True
        return (
            len(segments) == ADMIN_FRAGMENT_ROUTE_SEGMENTS
            and segments[:3] == ADMIN_FRAGMENT_ROUTE_PREFIX
            and bool(segments[3])
        )
