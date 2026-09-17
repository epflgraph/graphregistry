# graphregistry/entrypoints/cli/commands/cmd_airflow.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Annotated
import typer
from graphregistry.entrypoints.cli.common import DEFAULT_ENV, EnvOption, VerboseOption
from graphregistry.entrypoints.cli.context import CLIContext

# Create the Typer sub-app for Airflow orchestrator commands.
app = typer.Typer(help="Manage Airflow orchestrator operations.")

# Public Method: Sync new data (Registry -> Airflow).
@app.command(name="sync")
def cmd_airflow_sync(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    include_lectures: Annotated[bool, typer.Option("--include-lectures", help="Also sync lectures objects.")] = False,
    include_ontology: Annotated[bool, typer.Option("--include-ontology", help="Also sync ontology objects.")] = False,
) -> None:
    """Sync new data (Registry -> Airflow)."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    cli_ctx.registry.orchestrator.sync(
        include_lectures=include_lectures,
        include_ontology=include_ontology,
    )

# Public Method: Setup the object and content types to process.
@app.command(name="config")
def cmd_airflow_config(
    ctx: typer.Context,
    json_file: Annotated[Path, typer.Argument(help="Path to the typeflags JSON file.", exists=True)],
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
) -> None:
    """Setup the object and content types to process."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj

    # Resolve the typeflags input as either a file path or inline JSON.
    path_str = str(json_file)
    cfg_path = Path(path_str[1:]) if path_str.startswith("@") else Path(path_str)

    # Load typeflags from file or parse the inline JSON string.
    if cfg_path.is_file():
        with cfg_path.open("r", encoding="utf-8") as fp:
            cfg = json.load(fp)
    else:
        try:
            cfg = json.loads(path_str)
        except json.JSONDecodeError as e:
            raise typer.BadParameter(f"Invalid JSON passed to typeflags: {e}") from e

    # Apply the parsed typeflags configuration to the orchestrator.
    cli_ctx.registry.orchestrator.typeflags.config(config_json=cfg)

# Public Method: Mark objects as expired based on last cached date.
@app.command(name="expire")
def cmd_airflow_expire(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    fields: Annotated[bool, typer.Option("--fields", help="Include 'fields changed' Airflow tables.")] = False,
    scores: Annotated[bool, typer.Option("--scores", help="Include 'scores expired' Airflow table.")] = False,
    older_than: Annotated[int | None, typer.Option("--older-than", "-d", help="Expire objects last cached more than N days ago.")] = None,
) -> None:
    """Mark objects as expired based on last cached date."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj

    # Default to including both scopes when neither is explicitly selected.
    include_fields = fields or not scores
    include_scores = scores or not fields

    # Expire objects in the selected Airflow scopes.
    cli_ctx.registry.orchestrator.expire(
        include_nodes  = False,
        include_edges  = False,
        include_fields = include_fields,
        include_scores = include_scores,
        object_types   = None,
        older_than     = older_than,
        limit_per_type = None,
        count_only     = False,
        verbose        = verbose,
    )

# Public Method: Generate execution plan based on Airflow conditions.
@app.command(name="plan")
def cmd_airflow_plan(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    update_checksums: Annotated[bool, typer.Option("--update-checksums", "-c", help="Recompute and persist checksums before refreshing.")] = False,
    expire: Annotated[str | None, typer.Option("--expire", "-e", help="Comma-separated scopes to expire before refreshing: fields,scores.")] = None,
    older_than: Annotated[int | None, typer.Option("--older-than", "-d", help="Expire objects last cached more than N days ago.")] = None,
    limit_per_type: Annotated[int | None, typer.Option("--limit-per-type", "-l", help="Maximum number of objects to refresh per document type.")] = None,
    skip_hard_reset: Annotated[bool, typer.Option("--skip-hard-reset", "-shr", help="Reset only Airflow states; no full pipeline reset.")] = False,
) -> None:
    """Generate execution plan: reset, optionally update checksums, optionally expire, then refresh to_process flags."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    gr = cli_ctx.registry

    # Reset orchestrator flags. Deep reset includes cache/traversal tables unless skipped.
    reset_options = ("airflow",) if skip_hard_reset else ("airflow", "traversals", "cache")
    gr.orchestrator.reset(options=reset_options, doc_type=None, verbose=verbose)

    # Optionally recompute checksums before expiring stale objects.
    if update_checksums:
        gr.orchestrator.update_checksums_v2(actions=("commit",), verbose=verbose)

    # Optionally expire objects in the selected scopes.
    if expire:
        include_fields = "fields" in expire
        include_scores = "scores" in expire
        if not include_fields and not include_scores:
            include_fields = include_scores = True
        gr.orchestrator.expire(
            include_nodes  = False,
            include_edges  = False,
            include_fields = include_fields,
            include_scores = include_scores,
            object_types   = None,
            older_than     = older_than,
            limit_per_type = limit_per_type,
            count_only     = False,
            verbose        = verbose,
        )

    # Refresh to_process flags: this is the equivalent of the old 'airflow refresh' command.
    gr.orchestrator.refresh(
        doc_type       = None,
        limit_per_type = limit_per_type,
        verbose        = verbose,
    )

# Public Method: Display Airflow orchestrator status tables.
@app.command(name="status")
def cmd_airflow_status(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
) -> None:
    """Display Airflow orchestrator status tables."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    cli_ctx.registry.orchestrator.status()

# Public Method: Rollover states after a processing cycle.
@app.command(name="rollover")
def cmd_airflow_rollover(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    skip_hard_reset: Annotated[bool, typer.Option("--skip-hard-reset", "-shr", help="Reset only Airflow states; no full pipeline reset.")] = False,
) -> None:
    """Rollover states (new checksums and expiration dates) after a processing cycle."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    gr = cli_ctx.registry

    # Rollover checksums first.
    gr.orchestrator.rollover(actions=("commit",))

    # Update last cached dates while to_process flags are still set (update_dates
    # filters on to_process=1). Reset must happen after this step.
    gr.orchestrator.update_dates(actions=("commit",))

    # Reset orchestrator flags. Deep reset includes cache/traversal tables unless skipped.
    reset_options = ("airflow",) if skip_hard_reset else ("airflow", "traversals", "cache")
    gr.orchestrator.reset(options=reset_options, doc_type=None, verbose=verbose)
