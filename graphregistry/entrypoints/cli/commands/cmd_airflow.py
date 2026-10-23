# graphregistry/entrypoints/cli/commands/cmd_airflow.py
from __future__ import annotations
from datetime import timedelta
import json
from pathlib import Path
from typing import Annotated
import typer
from loguru import logger as sysmsg
from tabulate import tabulate
from graphregistry.common.auxfcn import print_colour
from graphregistry.domain.models.pipeline.mdl_policies import ExpirationPolicy
from graphregistry.domain.models.pipeline.mdl_typeflags import TypeFlagConfig
from graphregistry.entrypoints.cli.common import DEFAULT_ENV, EnvOption, VerboseOption
from graphregistry.entrypoints.cli.context import CLIContext
from graphregistry.entrypoints.cli.dependencies import (
    build_cacheprojection_repository,
    build_change_tracking_repository,
    build_processing_scope,
    build_typeflags_repository,
)

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

    # Insert tracking rows for new registry objects through the typed repository.
    repo = build_change_tracking_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, verbose=verbose)
    repo.sync_new_records(
        include_lectures = include_lectures,
        include_ontology = include_ontology,
    )

# Public Method: Reset orchestrator 'to_process' flags across Registry tables.
@app.command(name="reset")
def cmd_airflow_reset(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    options: Annotated[str, typer.Option("--options", help="Comma-separated options: typeflags,airflow,traversals,cache.")] = "typeflags,airflow",
) -> None:
    """Reset orchestrator 'to_process' flags across Registry tables."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    option_set = tuple(o.strip() for o in options.split(",") if o.strip())

    # Deactivate the typeflags through the typed repository when requested.
    if "typeflags" in option_set:
        typeflags_repo = build_typeflags_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, verbose=verbose)
        typeflags_repo.reset()

    # Clear the airflow tracking flags through the typed repository when requested.
    if "airflow" in option_set:
        repo = build_change_tracking_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, verbose=verbose)
        repo.clear_all_flags()

    # Clear the cache and traversals projection flags through the typed repository.
    cache_repo = build_cacheprojection_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, index_config=cli_ctx.index_config, verbose=verbose)
    cache_repo.reset_flags(
        include_cache      = "cache" in option_set,
        include_traversals = "traversals" in option_set,
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

    # Apply the parsed typeflags configuration through the typed repository,
    # which resets all flags and then activates the configured families.
    repo = build_typeflags_repository(
        db            = cli_ctx.db,
        global_config = cli_ctx.global_config,
        verbose       = verbose,
    )
    repo.save(config=TypeFlagConfig.from_json(cfg))

# Public Method: Update object checksums based on typeflag activation.
@app.command(name="update-checksums")
def cmd_airflow_update_checksums(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False
) -> None:
    """Update object checksums based on typeflag activation."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj

    # Checksum updates are scoped to the indexable edge families, mirroring
    # the legacy get_types_to_process intersection.
    repo = build_change_tracking_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, verbose=verbose)
    scope = build_processing_scope(
        db                    = cli_ctx.db,
        global_config         = cli_ctx.global_config,
        index_config          = cli_ctx.index_config,
        restrict_to_indexable = True,
    )
    repo.update_current_checksums(scope=scope, actions=("commit",))

# Public Method: Mark objects as expired based on last cached date.
@app.command(name="expire")
def cmd_airflow_expire(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    fields: Annotated[bool, typer.Option("--fields", help="Include 'fields changed' Airflow tables.")] = False,
    scores: Annotated[bool, typer.Option("--scores", help="Include 'scores expired' Airflow table.")] = False,
    older_than: Annotated[int | None, typer.Option("--older-than", "-d", help="Expire objects last cached more than N days ago.")] = None,
    types: Annotated[str | None, typer.Option("--types", "-t", help="Comma-separated object types to expire (e.g. Person,Publication).")] = None,
) -> None:
    """Mark objects as expired based on last cached date."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj

    # Default to including both scopes when neither is explicitly selected.
    include_fields = fields or not scores
    include_scores = scores or not fields

    # The "fields" scope covers both node and edge FieldsChanged tables; the
    # "scores" scope covers only the node ScoresExpired table.
    include_nodes = include_fields or include_scores
    include_edges = include_fields

    # Convert a comma-separated type list into a clean Python list, or leave it
    # as None when the caller wants to expire every active type.
    object_types = [t.strip() for t in types.split(",") if t.strip()] if types else None

    # Expire objects in the selected Airflow scopes through the typed repository.
    repo = build_change_tracking_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, verbose=verbose)
    scope = build_processing_scope(
        db            = cli_ctx.db,
        global_config = cli_ctx.global_config,
        index_config  = cli_ctx.index_config,
    )
    policy = ExpirationPolicy(
        older_than     = timedelta(days=older_than) if older_than else timedelta(0),
        limit_per_type = None,
        include_fields = include_fields,
        include_scores = include_scores,
        include_nodes  = include_nodes,
        include_edges  = include_edges,
        object_types   = object_types,
    )
    repo.apply_expiration(policy=policy, scope=scope, count_only=False)

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

    # Only clear has_expired when the plan itself will recompute expiration, so a
    # previous manual expire command is not accidentally discarded.
    will_expire = expire is not None or older_than is not None
    reset_options = ("airflow",) if skip_hard_reset else ("airflow", "traversals", "cache")

    # Reset the airflow tracking flags and the cache projection flags through
    # the typed repositories, with the legacy reset banners.
    sysmsg.info("🧹 📝 Reset 'to_process' flags to 0.")
    sysmsg.trace(f"Selected option(s): {reset_options}.")
    repo = build_change_tracking_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, verbose=verbose)
    if "airflow" in reset_options:
        repo.clear_all_flags(clear_has_expired=will_expire)
    cache_repo = build_cacheprojection_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, index_config=cli_ctx.index_config, verbose=verbose)
    cache_repo.reset_flags(
        include_cache      = "cache" in reset_options,
        include_traversals = "traversals" in reset_options,
    )
    sysmsg.success("🧹 ✅ Done resetting flags.")
    sysmsg.success("🧹 ✅ Done resetting flags.")

    # Optionally recompute checksums before expiring stale objects. Checksum
    # updates are scoped to the indexable edge families.
    if update_checksums:
        checksum_scope = build_processing_scope(
            db                    = cli_ctx.db,
            global_config         = cli_ctx.global_config,
            index_config          = cli_ctx.index_config,
            restrict_to_indexable = True,
        )
        repo.update_current_checksums(scope=checksum_scope, actions=("commit",))

    # Optionally expire objects in the selected scopes. Providing --older-than
    # without --expire implies both fields and scores scopes.
    if expire or older_than is not None:
        expire_value = expire or "fields,scores"
        include_fields = "fields" in expire_value
        include_scores = "scores" in expire_value
        if not include_fields and not include_scores:
            include_fields = include_scores = True

        # The "fields" scope covers both node and edge FieldsChanged tables; the
        # "scores" scope covers only the node ScoresExpired table.
        include_nodes = include_fields or include_scores
        include_edges = include_fields

        # Run expiration for the selected fields/scores scopes.
        expire_scope = build_processing_scope(
            db            = cli_ctx.db,
            global_config = cli_ctx.global_config,
            index_config  = cli_ctx.index_config,
        )
        policy = ExpirationPolicy(
            older_than     = timedelta(days=older_than) if older_than else timedelta(0),
            limit_per_type = limit_per_type,
            include_fields = include_fields,
            include_scores = include_scores,
            include_nodes  = include_nodes,
            include_edges  = include_edges,
        )
        repo.apply_expiration(policy=policy, scope=expire_scope, count_only=False)

    # Refresh to_process flags: this is the equivalent of the old 'airflow refresh' command.
    refresh_scope = build_processing_scope(
        db            = cli_ctx.db,
        global_config = cli_ctx.global_config,
        index_config  = cli_ctx.index_config,
    )
    repo.refresh_flags(scope=refresh_scope, limit_per_type=limit_per_type)

# Internal Function: Render one status table in the legacy format: a blank
# line, a bold white title, and a fancy-grid tabulate table, skipped entirely
# when there are no rows — mirroring the legacy print_dataframe behaviour.
def _print_status_table(rows: list[list], headers: list[str], title: str) -> None:
    if not rows:
        return
    print('')
    print_colour(title, colour='white', background='black', style='bold')
    print(tabulate(rows, headers=headers, tablefmt='fancy_grid', showindex=False))
    print('')

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

    # Render the type flags from the loaded configuration, in the legacy
    # tabulate format: one fancy-grid table per family, skipped when empty.
    typeflags_repo = build_typeflags_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, verbose=verbose)
    config = typeflags_repo.load()
    node_flag_rows = [
        [flag.object_type, flag.flag_type, int(flag.to_process)]
        for flag in sorted(config.nodes, key=lambda f: (f.object_type, f.flag_type))
        if flag.to_process
    ]
    _print_status_table(
        rows    = node_flag_rows,
        headers = ['object_type', 'flag_type', 'to_process'],
        title   = '⛳️ TYPE FLAGS: Object',
    )
    edge_flag_rows = [
        [pair.from_object_type, pair.to_object_type, 1]
        for pair in sorted(config.active_edge_pairs(), key=lambda p: (p.from_object_type, p.to_object_type))
    ]
    _print_status_table(
        rows    = edge_flag_rows,
        headers = ['from_object_type', 'to_object_type', 'to_process'],
        title   = '⛳️ TYPE FLAGS: Object-to-Object',
    )

    # Render the tracking stats from the change-tracking repository, in the
    # same legacy tabulate format.
    change_repo = build_change_tracking_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, verbose=verbose)
    status = change_repo.get_status()
    _print_status_table(
        rows    = [list(row) for row in status.node_fields_counts],
        headers = ['object_type', 'n_to_process'],
        title   = '🪪  FIELDS CHANGED: Object [stats]',
    )
    _print_status_table(
        rows    = [list(row) for row in status.edge_fields_counts],
        headers = ['from_object_type', 'to_object_type', 'n_to_process'],
        title   = '🪪  FIELDS CHANGED: Object-to-Object [stats]',
    )
    _print_status_table(
        rows    = [list(row) for row in status.scores_counts],
        headers = ['object_type', 'n_to_process'],
        title   = '🧮 SCORES EXPIRED: Object [stats]',
    )

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

    # Rollover checksums first, then update the cache dates while to_process
    # flags are still set, through the typed repository.
    repo = build_change_tracking_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, verbose=verbose)
    scope = build_processing_scope(
        db            = cli_ctx.db,
        global_config = cli_ctx.global_config,
        index_config  = cli_ctx.index_config,
    )
    repo.rollover_checksums(scope=scope, actions=("commit",))
    repo.update_cache_dates(scope=scope, actions=("commit",))

    # Reset the airflow tracking flags and the cache projection flags through
    # the typed repositories, with the legacy reset banners.
    reset_options = ("airflow",) if skip_hard_reset else ("airflow", "traversals", "cache")
    sysmsg.info("🧹 📝 Reset 'to_process' flags to 0.")
    sysmsg.trace(f"Selected option(s): {reset_options}.")
    if "airflow" in reset_options:
        repo.clear_all_flags()
    cache_repo = build_cacheprojection_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, index_config=cli_ctx.index_config, verbose=verbose)
    cache_repo.reset_flags(
        include_cache      = "cache" in reset_options,
        include_traversals = "traversals" in reset_options,
    )
    sysmsg.success("🧹 ✅ Done resetting flags.")
