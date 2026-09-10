"""CLI entry point: python -m justflow.visualization <yaml_path> -o <output.html>"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from justflow.config.loader import ConfigLoader
from justflow.visualization.graph import build_graph
from justflow.visualization.renderer import render_html, render_mermaid


def main() -> None:
    parser = argparse.ArgumentParser(description="Render workflow YAML as HTML diagram")
    parser.add_argument("yaml_path", help="Path to workflow YAML file")
    parser.add_argument("-o", "--output", default="workflow.html", help="Output HTML file path")
    parser.add_argument(
        "--config-dir",
        help="Config directory containing services.yaml and resources.yaml (enables Services/Resources tabs)",
    )
    parser.add_argument(
        "--mermaid-only", action="store_true", help="Output Mermaid text only (no HTML)"
    )
    args = parser.parse_args()

    yaml_path = Path(args.yaml_path)
    if not yaml_path.exists():
        print(f"Error: {yaml_path} not found", file=sys.stderr)
        sys.exit(1)

    config = ConfigLoader.inspect_workflow_file(yaml_path)
    graph = build_graph(config)
    mermaid_def = render_mermaid(graph)

    if args.mermaid_only:
        print(mermaid_def)
        return

    services_data = {}
    resources_data = {}

    if args.config_dir:
        config_dir = Path(args.config_dir)
        services_path = config_dir / "services.yaml"
        resources_path = config_dir / "resources.yaml"
        loader = ConfigLoader(config_dir)

        if services_path.exists():
            services_data = {
                name: service.model_dump(mode="json")
                for name, service in loader.load_services().services.items()
            }

        if resources_path.exists():
            resources_data = {
                name: resource.model_dump(mode="json", by_alias=True)
                for name, resource in loader.load_resources().resources.items()
            }

    html = render_html(
        mermaid_def,
        title=config.workflow,
        description=config.description,
        graph=graph,
        services=services_data,
        resources=resources_data,
    )
    output_path = Path(args.output)
    output_path.write_text(html)
    print(f"Written to {output_path}")


if __name__ == "__main__":
    main()
