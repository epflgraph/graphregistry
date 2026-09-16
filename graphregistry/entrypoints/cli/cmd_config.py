# graphregistry/entrypoints/cli/cmd_config.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import rich
import yaml
from rich import box
from rich.console import Console
from rich.panel import Panel

from graphregistry.common.config import APIConfig, GlobalConfig, IndexConfig, ScoresConfig
from graphregistry.common.paths import (
    CONFIG_AIRFLOW_PATH,
    CONFIG_API_PATH,
    CONFIG_GENAI_PATH,
    CONFIG_GRAPHAI_PATH,
    CONFIG_GRAPHDB_PATH,
    CONFIG_GRAPHES_PATH,
    CONFIG_INDEX_PATH,
    CONFIG_REGISTRY_PATH,
    CONFIG_SCORES_PATH,
)

console = Console()


#================================================================#
# Helpers for config validation                                  #
#================================================================#
def _check_file(path: Path) -> tuple[bool, str]:
    """Return (ok, message) for a required config file."""
    if not path.exists():
        return False, f"missing: {path}"
    if not path.is_file():
        return False, f"not a file: {path}"
    return True, "exists"


def _load_yaml(path: Path) -> dict[str, Any] | None:
    try:
        with open(path, "r", encoding="utf-8") as fp:
            return yaml.safe_load(fp) or {}
    except Exception:
        return None


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        with open(path, "r", encoding="utf-8") as fp:
            return json.load(fp)
    except Exception:
        return None


def _validate_required_keys(data: dict[str, Any], required: list[str], path: str) -> list[str]:
    """Return list of missing or invalid key errors."""
    errors: list[str] = []
    for key in required:
        parts = key.split(".")
        current = data
        for part in parts:
            if not isinstance(current, dict) or part not in current:
                errors.append(f"missing key '{key}' in {path}")
                break
            current = current[part]
    return errors


#================================================================#
# Handler: Show configuration                                    #
#================================================================#
def cmd_config_show(args):
    """
    Usage:
        graphregistry config show
        graphregistry config show --index
        graphregistry config show --scores
        graphregistry config show --api
        graphregistry config show --all
    """
    show_registry = args.registry
    show_index = args.index
    show_scores = args.scores
    show_api = args.api
    # Default is to show all sections.
    show_all = args.all or not any([show_registry, show_index, show_scores, show_api])

    # Ordered output for --all / default: registry, api, scores, index.
    if show_all or show_registry:
        _show_registry_summary(args.ctx.global_config)

    if show_all or show_api:
        _show_api_config(APIConfig())

    if show_all or show_scores:
        console.print("")
        console.print(Panel("Scores configuration", box=box.HEAVY, style="bold", expand=False))
        args.ctx.scores_config.print()

    if show_all or show_index:
        console.print("")
        console.print(Panel("Index configuration", box=box.HEAVY, style="bold", expand=False))
        args.ctx.index_config.print(compact=True)


def _show_registry_summary(glbcfg: GlobalConfig) -> None:
    console.print("")
    console.print(Panel("Registry configuration", box=box.HEAVY, style="bold", expand=False))
    console.print("")
    mode = glbcfg.mysql_execution_mode
    mode_colour = {"dev": "cyan", "prod": "red"}.get(mode, "white")
    console.print(f"  API title:          {glbcfg.api_title}")
    console.print(f"  API summary:        {glbcfg.api_summary}")
    console.print(f"  Execution mode:     [{mode_colour}]{mode}[/{mode_colour}]")
    console.print(f"  limit_per_type_max: {glbcfg.limit_per_type_max}")
    console.print("  Database schemas:")
    for key, name in glbcfg.schema_names.items():
        console.print(f"    - {key:24s} {name}")
    console.print("")


def _show_api_config(api_cfg: APIConfig) -> None:
    console.print("")
    console.print(Panel("API allowed types", box=box.HEAVY, style="bold", expand=False))
    console.print("")
    console.print("Nodes:")
    for node_type in api_cfg.allowed_node_types_list:
        console.print(f" - {node_type}")
    console.print("")
    console.print("Edges:")
    for edge_tuple in api_cfg.allowed_edge_tuples_list:
        console.print(f" - {edge_tuple[0]} --> {edge_tuple[1]} ({edge_tuple[2]})")
    console.print("")


#================================================================#
# Handler: Validate configuration files                          #
#================================================================#
def cmd_config_validate(args):
    """
    Usage:
        graphregistry config validate
        graphregistry config validate --strict
    """
    strict = args.strict
    all_ok = True
    warnings: list[str] = []
    errors: list[str] = []

    console.print("\n[bold]Validating GraphRegistry configuration[/bold]\n")

    #------------------------------------------------------------#
    # Required environment config files                          #
    #------------------------------------------------------------#
    required_env_files = {
        "Registry": CONFIG_REGISTRY_PATH,
        "GraphDB": CONFIG_GRAPHDB_PATH,
        "GraphES": CONFIG_GRAPHES_PATH,
    }
    optional_env_files = {
        "GraphAI": CONFIG_GRAPHAI_PATH,
        "GenAI": CONFIG_GENAI_PATH,
    }

    console.print("[bold]Environment configs[/bold]")
    for label, path in required_env_files.items():
        ok, msg = _check_file(path)
        if ok:
            console.print(f"  ✅ {label}: {path}")
        else:
            console.print(f"  ❌ {label}: {msg}")
            errors.append(f"{label}: {msg}")
            all_ok = False

    for label, path in optional_env_files.items():
        ok, msg = _check_file(path)
        if ok:
            console.print(f"  ✅ {label}: {path}")
        elif strict:
            console.print(f"  ⚠️  {label}: {msg}")
            warnings.append(f"{label}: {msg}")
        else:
            console.print(f"  ⚪ {label}: {msg} (optional, skipped)")

    #------------------------------------------------------------#
    # Required application config files                          #
    #------------------------------------------------------------#
    required_app_files = {
        "API": CONFIG_API_PATH,
        "Airflow": CONFIG_AIRFLOW_PATH,
        "Index": CONFIG_INDEX_PATH,
        "Scores": CONFIG_SCORES_PATH,
    }

    console.print("\n[bold]Application configs[/bold]")
    for label, path in required_app_files.items():
        ok, msg = _check_file(path)
        if ok:
            console.print(f"  ✅ {label}: {path}")
        else:
            console.print(f"  ❌ {label}: {msg}")
            errors.append(f"{label}: {msg}")
            all_ok = False

    #------------------------------------------------------------#
    # Parseability                                               #
    #------------------------------------------------------------#
    console.print("\n[bold]Parseability[/bold]")
    registry_data = _load_yaml(CONFIG_REGISTRY_PATH)
    graphdb_data = _load_yaml(CONFIG_GRAPHDB_PATH)
    graphes_data = _load_yaml(CONFIG_GRAPHES_PATH)
    graphai_data = _load_yaml(CONFIG_GRAPHAI_PATH) if CONFIG_GRAPHAI_PATH.exists() else {}
    genai_data = _load_yaml(CONFIG_GENAI_PATH) if CONFIG_GENAI_PATH.exists() else {}
    api_data = _load_json(CONFIG_API_PATH)
    index_data = _load_json(CONFIG_INDEX_PATH)
    scores_data = _load_json(CONFIG_SCORES_PATH)

    parse_results = [
        ("Registry", registry_data is not None),
        ("GraphDB", graphdb_data is not None),
        ("GraphES", graphes_data is not None),
        ("GraphAI", graphai_data is not None or not CONFIG_GRAPHAI_PATH.exists()),
        ("GenAI", genai_data is not None or not CONFIG_GENAI_PATH.exists()),
        ("API", api_data is not None),
        ("Index", index_data is not None),
        ("Scores", scores_data is not None),
    ]
    for label, ok in parse_results:
        if ok:
            console.print(f"  ✅ {label} parses")
        else:
            console.print(f"  ❌ {label} failed to parse")
            errors.append(f"{label} failed to parse")
            all_ok = False

    if not all_ok and registry_data is None:
        _print_result(all_ok, errors, warnings)
        return 1

    #------------------------------------------------------------#
    # Structural validation                                      #
    #------------------------------------------------------------#
    console.print("\n[bold]Structure[/bold]")
    structure_errors: list[str] = []

    if registry_data is not None:
        structure_errors.extend(
            _validate_required_keys(
                registry_data,
                ["api.title", "api.summary", "cli.limit_per_type_max", "database.schema_names", "database.mode"],
                str(CONFIG_REGISTRY_PATH),
            )
        )
        mode = registry_data.get("database", {}).get("mode")
        if mode is not None and mode not in ("dev", "prod"):
            structure_errors.append(f"database.mode must be 'dev' or 'prod', got '{mode}'")

    if graphdb_data is not None:
        structure_errors.extend(
            _validate_required_keys(
                graphdb_data,
                ["environments", "default_env"],
                str(CONFIG_GRAPHDB_PATH),
            )
        )
        envs = graphdb_data.get("environments", {})
        default_env = graphdb_data.get("default_env")
        if envs and default_env and default_env not in envs:
            structure_errors.append(f"graphdb default_env '{default_env}' not found in environments")

    if graphes_data is not None:
        structure_errors.extend(
            _validate_required_keys(
                graphes_data,
                ["environments", "default_env"],
                str(CONFIG_GRAPHES_PATH),
            )
        )
        envs = graphes_data.get("environments", {})
        default_env = graphes_data.get("default_env")
        if envs and default_env and default_env not in envs:
            structure_errors.append(f"graphes default_env '{default_env}' not found in environments")

    if graphai_data:
        structure_errors.extend(
            _validate_required_keys(
                graphai_data,
                ["graphai.host_address", "graphai.port", "graphai.username", "graphai.password"],
                str(CONFIG_GRAPHAI_PATH),
            )
        )

    if genai_data:
        structure_errors.extend(
            _validate_required_keys(
                genai_data,
                ["genai.api_key", "genai.inference_url", "genai.llm_model"],
                str(CONFIG_GENAI_PATH),
            )
        )

    if api_data is not None:
        if "allowed-types" not in api_data:
            structure_errors.append(f"missing key 'allowed-types' in {CONFIG_API_PATH}")
        else:
            if "nodes" not in api_data["allowed-types"]:
                structure_errors.append(f"missing key 'allowed-types.nodes' in {CONFIG_API_PATH}")
            if "edges" not in api_data["allowed-types"]:
                structure_errors.append(f"missing key 'allowed-types.edges' in {CONFIG_API_PATH}")

    if index_data is not None:
        structure_errors.extend(
            _validate_required_keys(
                index_data,
                ["object-selection.nodes", "object-selection.edges", "data-types"],
                str(CONFIG_INDEX_PATH),
            )
        )

    if scores_data is not None:
        structure_errors.extend(
            _validate_required_keys(
                scores_data,
                ["scored-edge-tuples"],
                str(CONFIG_SCORES_PATH),
            )
        )

    if structure_errors:
        for err in structure_errors:
            console.print(f"  ❌ {err}")
            errors.append(err)
        all_ok = False
    else:
        console.print("  ✅ All required keys present")

    #------------------------------------------------------------#
    # Cross-file consistency                                     #
    #------------------------------------------------------------#
    console.print("\n[bold]Cross-file consistency[/bold]")
    consistency_errors: list[str] = []

    if api_data and index_data:
        api_nodes = set(api_data.get("allowed-types", {}).get("nodes", []))
        index_nodes = set(index_data.get("object-selection", {}).get("nodes", []))
        unknown_api_nodes = api_nodes - index_nodes
        if unknown_api_nodes:
            consistency_errors.append(
                f"API allowed nodes not in index config: {sorted(unknown_api_nodes)}"
            )

    if api_data and scores_data:
        api_nodes = set(api_data.get("allowed-types", {}).get("nodes", []))
        scores_tuples = scores_data.get("scored-edge-tuples", {})
        all_scored = set()
        for cls, tuples in scores_tuples.items():
            for t in tuples:
                all_scored.update(t)
        unknown_scored = all_scored - api_nodes
        if unknown_scored:
            consistency_errors.append(
                f"Scored tuple types not in API allowed nodes: {sorted(unknown_scored)}"
            )

    if consistency_errors:
        for err in consistency_errors:
            console.print(f"  ⚠️  {err}")
            warnings.append(err)
    else:
        console.print("  ✅ No consistency issues")

    _print_result(all_ok, errors, warnings)
    return 0 if all_ok else 1


def _print_result(all_ok: bool, errors: list[str], warnings: list[str]) -> None:
    console.print("")
    if errors:
        console.print(f"[bold red]Validation failed with {len(errors)} error(s).[/bold red]")
    elif warnings:
        console.print(f"[bold yellow]Validation passed with {len(warnings)} warning(s).[/bold yellow]")
    else:
        console.print("[bold green]Configuration is valid.[/bold green]")
    console.print("")


# Backwards-compatible alias for the old command name.
cmd_config_index = cmd_config_show
