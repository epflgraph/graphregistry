# graphregistry/entrypoints/cli/commands/cmd_test.py
from __future__ import annotations
from pathlib import Path
from typing import Annotated
import typer
from rich.console import Console
from graphregistry.adapters.clients.rcp_models import RCPModelsClient
from graphregistry.adapters.gateways.graphai.gtw_base import GraphAIBaseGateway
from graphregistry.common.paths import (
    CONFIG_GENAI_PATH,
    CONFIG_GRAPHAI_PATH,
    CONFIG_GRAPHDB_PATH,
    CONFIG_GRAPHES_PATH,
)
from graphregistry.entrypoints.cli.common import DEFAULT_ENV, EnvOption
from graphregistry.entrypoints.cli.context import CLIContext

# Shared Rich console for styled test output.
console = Console()

# Public Method: Run a safe smoke test of configuration and service connectivity.
def cmd_test(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    db: Annotated[bool, typer.Option("--db", help="Execute only/also database connectivity check.")] = False,
    es: Annotated[bool, typer.Option("--es", help="Execute only/also ElasticSearch connectivity check.")] = False,
    ai: Annotated[bool, typer.Option("--ai", help="Execute only/also GraphAI connectivity check.")] = False,
    gen: Annotated[bool, typer.Option("--gen", help="Execute only/also GenAI connectivity check.")] = False,
) -> None:
    """Run a safe smoke test of configuration and service connectivity."""
    cli_ctx: CLIContext = ctx.obj

    # If no specific check is requested, run all of them.
    run_all = not any([db, es, ai, gen])
    skip_db = not (run_all or db)
    skip_es = not (run_all or es)
    skip_ai = not (run_all or ai)
    skip_gen = not (run_all or gen)

    # Collect warnings and errors reported by each connectivity check.
    warnings: list[str] = []
    errors: list[str] = []

    # Print the smoke-test header.
    console.print("\n[bold]GraphRegistry smoke test[/bold]\n")

    #------------------------------------------------------------#
    # 1. Config validation                                       #
    #------------------------------------------------------------#
    console.print("[bold]⚙️ : Configuration[/bold]")
    try:
        _ = cli_ctx.global_config
        _ = cli_ctx.index_config
        _ = cli_ctx.scores_config
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
        raise typer.Exit(code=1)

    #------------------------------------------------------------#
    # 2. Service export folders                                  #
    #------------------------------------------------------------#
    console.print("\n[bold]📂: Referenced folders[/bold]")
    folders_to_check: list[tuple[str, Path]] = []

    # Read configured export paths from service config files.
    try:
        from yaml import safe_load
        if CONFIG_GRAPHDB_PATH.exists():
            with open(CONFIG_GRAPHDB_PATH, "r", encoding="utf-8") as fp:
                graphdb_data = safe_load(fp) or {}
            export_path = graphdb_data.get("export_path")
            if export_path:
                folders_to_check.append(("graphdb export_path", Path(export_path)))

        # Also collect the ElasticSearch export path when configured.
        if CONFIG_GRAPHES_PATH.exists():
            with open(CONFIG_GRAPHES_PATH, "r", encoding="utf-8") as fp:
                graphes_data = safe_load(fp) or {}
            export_path = graphes_data.get("export_path")
            if export_path:
                folders_to_check.append(("graphes export_path", Path(export_path)))
    except Exception as exc:
        warnings.append(f"Could not read service export paths: {exc}")

    # Report whether each configured export folder exists.
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
            db_client = cli_ctx.db
            db_client.execute_query(engine_name=env, query="SELECT 1 AS ok")
            console.print(f"  ✅ Connected to environment '{env}'")
        except Exception as exc:
            console.print(f"  ❌ Could not connect to environment '{env}': {exc}")
            errors.append(f"MySQL connectivity failed: {exc}")

    #------------------------------------------------------------#
    # 4. Elasticsearch connectivity                              #
    #------------------------------------------------------------#
    if not skip_es:
        console.print("\n[bold]⚡️: Elasticsearch[/bold]")
        es_client = cli_ctx.es
        try:
            if env not in es_client.engine:
                raise ValueError(
                    f"No Elasticsearch engine named '{env}'. "
                    f"Configured engines: {list(es_client.engine.keys())}"
                )
            if es_client.test(engine_name=env) is True:
                console.print(f"  ✅ Connected to environment '{env}'")
            else:
                console.print(f"  ❌ Could not connect to environment '{env}'")
                errors.append(f"ElasticSearch connectivity failed for env '{env}'")
        except Exception as exc:
            console.print(f"  ❌ Could not reach Elasticsearch: {exc}")
            errors.append(f"Elasticsearch connectivity failed: {exc}")

    #------------------------------------------------------------#
    # 5. GraphAI / GenAI connectivity                            #
    #------------------------------------------------------------#
    if not skip_ai:
        console.print("\n[bold]🤖: AI services[/bold]")

        # Authenticate against GraphAI when its config is present.
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

    # Validate GenAI client configuration when its config is present.
    if not skip_gen:
        if CONFIG_GENAI_PATH.exists():
            try:
                client = RCPModelsClient()
                console.print(f"  ✅ GenAI client configured (model: {client.llm_model})")
            except Exception as exc:
                console.print(f"  ❌ GenAI configuration failed: {exc}")
                errors.append(f"GenAI configuration failed: {exc}")
        else:
            console.print("  ⚪ GenAI config not present, skipping")

    # Print the summary and exit with an error code if any check failed.
    _print_result(errors, warnings)
    raise typer.Exit(code=0 if not errors else 1)

# Internal Function: Print the final smoke test result.
def _print_result(errors: list[str], warnings: list[str]) -> None:
    """Print the final smoke test result."""
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
