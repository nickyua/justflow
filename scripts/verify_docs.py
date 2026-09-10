"""Verify public Markdown links, command provenance, and disclosure boundaries."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
DOCS_ROOT = REPOSITORY_ROOT / "docs"
MARKDOWN_LINK_PATTERN = re.compile(r"(?<!!)\[[^]]+\]\(([^)]+)\)")
HEADING_PATTERN = re.compile(r"^#{1,6}\s+(.+?)\s*$")
EXPLICIT_ANCHOR_PATTERN = re.compile(r"\{#([A-Za-z0-9_-]+)\}\s*$")
SHELL_FENCE_PATTERN = re.compile(r"^```(?:bash|console|sh|shell)\s*$")
TESTED_MARKER_PATTERN = re.compile(r"^<!-- tested: ([^>]+) -->$")
OPT_IN_MARKERS = frozenset(
    {
        "<!-- opt-in: account mutation -->",
        "<!-- opt-in: cloud procedure -->",
    }
)
FORBIDDEN_PUBLIC_TERMS = (
    ".codegraph",
    "ENGINE_DECISIONS.md",
    "Public PR ",
    "sol-implementation-plan",
)
EXCLUDED_DOCUMENTATION_PATHS = frozenset({DOCS_ROOT / "scheduling-workflows-spec.md"})


def _slugify(heading: str) -> str:
    heading = re.sub(r"`([^`]*)`", r"\1", heading)
    heading = re.sub(r"<[^>]+>", "", heading)
    heading = heading.lower().strip()
    heading = re.sub(r"[^\w\- ]", "", heading)
    return re.sub(r"[\s-]+", "-", heading).strip("-")


def _anchors(path: Path) -> frozenset[str]:
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = HEADING_PATTERN.match(line)
        if match is None:
            continue
        heading = match.group(1)
        explicit = EXPLICIT_ANCHOR_PATTERN.search(heading)
        base = explicit.group(1) if explicit else _slugify(heading)
        count = counts.get(base, 0)
        counts[base] = count + 1
        anchors.add(base if count == 0 else f"{base}_{count}")
    return frozenset(anchors)


def _link_target(source: Path, raw_target: str) -> tuple[Path, str] | None:
    target = raw_target.split(maxsplit=1)[0].strip("<>")
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc or target.startswith("mailto:"):
        return None
    relative_path = unquote(parsed.path)
    resolved = source.parent / relative_path if relative_path else source
    if resolved.is_dir():
        index = resolved / "index.md"
        if index.exists():
            resolved = index
    return resolved.resolve(), unquote(parsed.fragment)


def _verify_links(paths: tuple[Path, ...]) -> list[str]:
    failures: list[str] = []
    anchor_cache: dict[Path, frozenset[str]] = {}
    for source in paths:
        content = source.read_text(encoding="utf-8")
        for raw_target in MARKDOWN_LINK_PATTERN.findall(content):
            target = _link_target(source, raw_target)
            if target is None:
                continue
            target_path, anchor = target
            if not target_path.exists():
                failures.append(
                    f"{source.relative_to(REPOSITORY_ROOT)}: missing link target {raw_target}"
                )
                continue
            if anchor and target_path.suffix == ".md":
                anchors = anchor_cache.setdefault(target_path, _anchors(target_path))
                if anchor not in anchors:
                    failures.append(
                        f"{source.relative_to(REPOSITORY_ROOT)}: missing anchor {raw_target}"
                    )
    return failures


def _previous_content_line(lines: list[str], index: int) -> str:
    for candidate in reversed(lines[:index]):
        if candidate.strip():
            return candidate.strip()
    return ""


def _verify_command_blocks(paths: tuple[Path, ...]) -> list[str]:
    failures: list[str] = []
    for source in paths:
        lines = source.read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(lines):
            if SHELL_FENCE_PATTERN.match(line) is None:
                continue
            marker = _previous_content_line(lines, index)
            if marker in OPT_IN_MARKERS:
                continue
            match = TESTED_MARKER_PATTERN.match(marker)
            if match is None:
                failures.append(
                    f"{source.relative_to(REPOSITORY_ROOT)}:{index + 1}: "
                    "shell block lacks a tested or opt-in marker"
                )
                continue
            test_path = REPOSITORY_ROOT / match.group(1)
            if not test_path.exists():
                failures.append(
                    f"{source.relative_to(REPOSITORY_ROOT)}:{index + 1}: "
                    f"test reference does not exist: {match.group(1)}"
                )
    return failures


def verify_documentation() -> None:
    markdown_paths = tuple(
        path for path in sorted(DOCS_ROOT.rglob("*.md")) if path not in EXCLUDED_DOCUMENTATION_PATHS
    ) + (REPOSITORY_ROOT / "README.md",)
    failures = _verify_links(markdown_paths)
    failures.extend(_verify_command_blocks(markdown_paths))
    for path in markdown_paths:
        content = path.read_text(encoding="utf-8")
        for term in FORBIDDEN_PUBLIC_TERMS:
            if term in content:
                failures.append(
                    f"{path.relative_to(REPOSITORY_ROOT)}: forbidden public term {term!r}"
                )
    if failures:
        raise SystemExit("Documentation verification failed:\n- " + "\n- ".join(failures))


if __name__ == "__main__":
    verify_documentation()
