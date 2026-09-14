# graphregistry/entrypoints/cli/cmd_test.py
from __future__ import annotations

from pathlib import Path

from rich.console import Console

from graphregistry.adapters.clients.rcp_models import RCPModelsClient
from graphregistry.adapters.gateways.graphai.gtw_base import GraphAIBaseGateway
from graphregistry.common.paths import (
    CONFIG_GENAI_PATH,
    CONFIG_GRAPHAI_PATH,
    CONFIG_GRAPHDB_PATH,
    CONFIG_GRAPHES_PATH,
)

console = Console()


def cmd_test(args):
    """
    Usage:
        graphregistry test
        graphregistry test --env xaas_coresrv
        graphregistry test --skip-db --skip-es --skip-ai
    """
    skip_db = args.skip_db
    skip_es = args.skip_es
    skip_ai = args.skip_ai

    warnings: list[str] = []
    errors: list[str] = []

    console.print("\n[bold]GraphRegistry smoke test[/bold]\n")

    #------------------------------------------------------------#
    # 1. Config validation                                       #
    #------------------------------------------------------------#
    console.print("[bold]⚙️ : Configuration[/bold]")
    try:
        glbcfg = args.ctx.global_config
        idxcfg = args.ctx.index_config
        scores_cfg = args.ctx.scores_config
        api_cfg = None
        try:
            from graphregistry.common.config import APIConfig
            api_cfg = APIConfig()
        except Exception as exc:
            warnings.append(f"API config loaded with warnings: {exc}")
        console.print("  ✅ Configuration loads successfully")
    except Exception as exc:
        console.print(f"  ❌ Failed to load configuration: {exc}")
        errors.append(f"Configuration load failed: {exc}")
        _print_result(errors, warnings)
        return 1

    #------------------------------------------------------------#
    # 2. Service export folders                                  #
    #------------------------------------------------------------#
    console.print("\n[bold]📂: Referenced folders[/bold]")
    folders_to_check: list[tuple[str, Path]] = []

    try:
        from yaml import safe_load
        if CONFIG_GRAPHDB_PATH.exists():
            with open(CONFIG_GRAPHDB_PATH, "r", encoding="utf-8") as fp:
                graphdb_data = safe_load(fp) or {}
            export_path = graphdb_data.get("export_path")
            if export_path:
                folders_to_check.append(("graphdb export_path", Path(export_path)))

        if CONFIG_GRAPHES_PATH.exists():
            with open(CONFIG_GRAPHES_PATH, "r", encoding="utf-8") as fp:
                graphes_data = safe_load(fp) or {}
            export_path = graphes_data.get("export_path")
            if export_path:
                folders_to_check.append(("graphes export_path", Path(export_path)))
    except Exception as exc:
        warnings.append(f"Could not read service export paths: {exc}")

    for label, path in folders_to_check:
        if path.exists():
            console.print(f"  ✅ {label}: {path}")
        else:
            console.print(f"  ⚠️  {label}: {path} (does not exist)")
            warnings.append(f"{label} does not exist: {path}")

    #------------------------------------------------------------#
    # 3. MySQL connectivity                                      #
    #------------------------------------------------------------#
    if not skip_db:
        console.print("\n[bold]🐬: MySQL / MariaDB[/bold]")
        try:
            db = args.ctx.db
            result = db.execute_query(engine_name=args.env, query="SELECT 1 AS ok")
            console.print(f"  ✅ Connected to environment '{args.env}'")
        except Exception as exc:
            console.print(f"  ❌ Could not connect to environment '{args.env}': {exc}")
            errors.append(f"MySQL connectivity failed: {exc}")

    #------------------------------------------------------------#
    # 4. Elasticsearch connectivity                              #
    #------------------------------------------------------------#
    if not skip_es:
        console.print("\n[bold]⚡️: Elasticsearch[/bold]")
        es = args.ctx.es
        es_env = args.env
        try:
            if es_env not in es.engine:
                raise ValueError(
                    f"No Elasticsearch engine named '{es_env}'. "
                    f"Configured engines: {list(es.engine.keys())}"
                )
            if es.test(engine_name=es_env) is True:
                console.print(f"  ✅ Connected to environment '{es_env}'")
            else:
                console.print(f"  ❌ Could not connect to environment '{es_env}'")
                errors.append(f"ElasticSearch connectivity failed for env '{es_env}'")
        except Exception as exc:
            console.print(f"  ❌ Could not reach Elasticsearch: {exc}")
            errors.append(f"Elasticsearch connectivity failed: {exc}")

    #------------------------------------------------------------#
    # 5. GraphAI / GenAI connectivity                            #
    #------------------------------------------------------------#
    if not skip_ai:
        console.print("\n[bold]🤖: AI services[/bold]")

        if CONFIG_GRAPHAI_PATH.exists():
            try:
                gateway = GraphAIBaseGateway()
                gateway._ensure_login_info()
                console.print("  ✅ GraphAI authentication")
                gateway.close()
            except Exception as exc:
                console.print(f"  ❌ GraphAI authentication failed: {exc}")
                errors.append(f"GraphAI connectivity failed: {exc}")
        else:
            console.print("  ⚪ GraphAI config not present, skipping")

        if CONFIG_GENAI_PATH.exists():
            try:
                client = RCPModelsClient()
                console.print(f"  ✅ GenAI client configured (model: {client.llm_model})")
            except Exception as exc:
                console.print(f"  ❌ GenAI configuration failed: {exc}")
                errors.append(f"GenAI configuration failed: {exc}")
        else:
            console.print("  ⚪ GenAI config not present, skipping")

    _print_result(errors, warnings)
    return 0 if not errors else 1


def _print_result(errors: list[str], warnings: list[str]) -> None:
    console.print("")
    if errors:
        console.print(f"[bold red]Smoke test failed with {len(errors)} error(s).[/bold red]")
        for err in errors:
            console.print(f"  - {err}")
    elif warnings:
        console.print(f"[bold yellow]Smoke test passed with {len(warnings)} warning(s).[/bold yellow]")
        for warn in warnings:
            console.print(f"  - {warn}")
    else:
        console.print("[bold green]GraphRegistry is ready.[/bold green]")
    console.print("")
