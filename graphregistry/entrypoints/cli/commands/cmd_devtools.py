# graphregistry/entrypoints/cli/commands/cmd_devtools.py
from __future__ import annotations
from datetime import date
from typing import Annotated
import typer
from graphregistry.entrypoints.cli.commands.cmd_data import _load_json_input
from graphregistry.entrypoints.cli.common import DEFAULT_ENV, EnvOption, VerboseOption
from graphregistry.entrypoints.cli.context import CLIContext
from graphregistry.entrypoints.cli.dependencies import build_registry_operations_from_cli
from graphregistry.entrypoints.mappers import SpecMapper

# Create the Typer sub-app for advanced developer commands.
app = typer.Typer(help="Low-level building blocks for advanced users.")

#================================================================#
# Function Group: Actions parsing helper                         #
#================================================================#

# Internal Function: Parse and validate the --actions option.
def _parse_actions(actions: str) -> tuple[str, ...]:
    """Parse and validate the --actions option into a tuple of action names."""
    action_set = tuple(a.strip() for a in actions.split(",") if a.strip())
    valid = {"print", "eval", "commit"}
    invalid = set(action_set) - valid
    if invalid:
        raise typer.BadParameter(f"Invalid actions: {sorted(invalid)}. Valid: print, eval, commit.")
    return action_set

#================================================================#
# Function Group: data-* commands                                #
#================================================================#

# Public Method: Save node(s) or edge(s) from a JSON file.
@app.command(name="data-save")
def cmd_devtools_data_save(
    ctx: typer.Context,
    json_file: Annotated[str, typer.Argument(help="Path to the JSON save file.")],
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    actions: Annotated[str, typer.Option("--actions", help="Comma-separated actions: print,eval,commit.")] = "print,eval,commit",
) -> None:
    """Save node(s) or edge(s) from a JSON file."""
    from graphregistry.entrypoints.cli.commands.cmd_data import (
        _is_edge,
        _is_edge_list,
        _is_node,
        _is_node_list,
        _unwrap_edge,
        _unwrap_edge_list,
        _unwrap_node,
        _unwrap_node_list,
    )

    # Declare the cli_ctx data structure.
    cli_ctx: CLIContext = ctx.obj
    payload = _load_json_input(json_file)
    node_ops, edge_ops = build_registry_operations_from_cli(ctx=cli_ctx, env=env, verbose=verbose)
    action_set = _parse_actions(actions)

    # Handle the conditional case.
    if _is_node(payload):
        node = SpecMapper.from_node_spec(_unwrap_node(payload))
        node_ops.save(node, actions=action_set)
    elif _is_edge(payload):
        edge = SpecMapper.from_edge_spec(_unwrap_edge(payload))
        edge_ops.save(edge, actions=action_set)
    elif _is_node_list(payload):
        node_list = SpecMapper.from_node_list_spec(_unwrap_node_list(payload))
        node_ops.save_many(node_list, actions=action_set)
    elif _is_edge_list(payload):
        edge_list = SpecMapper.from_edge_list_spec(_unwrap_edge_list(payload))
        edge_ops.save_many(edge_list, actions=action_set)
    else:
        raise typer.BadParameter("JSON file must contain a node, edge, node_list, or edge_list.")

# Public Method: Delete node(s) or edge(s) from a JSON file.
@app.command(name="data-delete")
def cmd_devtools_data_delete(
    ctx: typer.Context,
    json_file: Annotated[str, typer.Argument(help="Path to the JSON key file.")],
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    actions: Annotated[str, typer.Option("--actions", help="Comma-separated actions: print,eval,commit.")] = "print,eval,commit",
) -> None:
    """Delete node(s) or edge(s) from a JSON file."""
    from graphregistry.entrypoints.cli.commands.cmd_data import (
        _is_edge_key,
        _is_edge_key_list,
        _is_node_key,
        _is_node_key_list,
        _unwrap_key,
        _unwrap_key_list,
    )

    # Declare the cli_ctx data structure.
    cli_ctx: CLIContext = ctx.obj
    payload = _load_json_input(json_file)
    node_ops, edge_ops = build_registry_operations_from_cli(ctx=cli_ctx, env=env, verbose=verbose)
    action_set = _parse_actions(actions)

    # Handle the conditional case.
    if _is_node_key(payload):
        node_key = SpecMapper.from_node_key_spec(_unwrap_key(payload))
        node_ops.delete(node_key, actions=action_set)
    elif _is_edge_key(payload):
        edge_key = SpecMapper.from_edge_key_spec(_unwrap_key(payload))
        edge_ops.delete(edge_key, actions=action_set)
    elif _is_node_key_list(payload):
        node_key_list = SpecMapper.from_node_key_list_spec(_unwrap_key_list(payload))
        node_ops.delete_many(node_key_list, actions=action_set)
    elif _is_edge_key_list(payload):
        edge_key_list = SpecMapper.from_edge_key_list_spec(_unwrap_key_list(payload))
        edge_ops.delete_many(edge_key_list, actions=action_set)
    else:
        raise typer.BadParameter("JSON file must contain a node key, edge key, or key list.")

#================================================================#
# Function Group: airflow-* commands                             #
#================================================================#

# Public Method: Inspect or reset 'to_process' flags across Airflow tables.
@app.command(name="airflow-inspect-to-process")
def cmd_devtools_airflow_inspect_to_process(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    count: Annotated[bool, typer.Option("--count", help="Count rows with to_process=1 in each Airflow table.")] = False,
    reset: Annotated[bool, typer.Option("--reset", help="Set to_process=0 for all rows currently marked to_process=1.")] = False,
) -> None:
    """Inspect or reset the Airflow 'to_process' flags across Airflow tables."""
    cli_ctx: CLIContext = ctx.obj
    glbcfg = cli_ctx.global_config
    db = cli_ctx.db

    # Prepare list_of_tables for the following steps.
    list_of_tables = [
        table_name
        for table_name in db.get_tables_in_schema(engine_name=env, schema_name=glbcfg.schema_airflow)
        if "to_process" in db.get_column_names(engine_name=env, schema_name=glbcfg.schema_airflow, table_name=table_name)
    ]

    # Handle the conditional case.
    if count:
        print(f"\n{'-'*78}\ntable_name{' '*(64 - len('table_name') + 2)}n_to_process\n{'-'*78}")
        for table_name in list_of_tables:
            n = db.count_rows_in_table(
                engine_name  = env,
                schema_name  = glbcfg.schema_airflow,
                table_name   = table_name,
                where_clause = "to_process = 1",
            )
            print(f"{table_name} {'.'*(64 - len(table_name))} {n}")
        print("-" * 78 + "\n")

    # Continue with the next step.
    elif reset:
        for table_name in list_of_tables:
            print(f"⚙️  Processing table: {table_name} ...")
            db.set_cells(
                engine_name = env,
                schema_name = glbcfg.schema_airflow,
                table_name  = table_name,
                set         = [("to_process", 0)],
                where       = [("to_process", 1)],
                verbose     = False,
            )

# Public Method: Reset orchestrator 'to_process' flags across Registry tables.
@app.command(name="airflow-reset")
def cmd_devtools_airflow_reset(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    options: Annotated[str, typer.Option("--options", help="Comma-separated options: typeflags,airflow,traversals,cache.")] = "typeflags,airflow",
) -> None:
    """Reset orchestrator 'to_process' flags across Registry tables."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    option_set = tuple(o.strip() for o in options.split(",") if o.strip())
    cli_ctx.registry.orchestrator.reset(options=option_set, doc_type=None, verbose=verbose)

# Public Method: Update object checksums based on typeflag activation.
@app.command(name="airflow-update-checksums")
def cmd_devtools_airflow_update_checksums(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    actions: Annotated[str, typer.Option("--actions", help="Comma-separated actions: print,eval,commit.")] = "commit",
) -> None:
    """Update object checksums based on typeflag activation."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    cli_ctx.registry.orchestrator.update_checksums_v2(actions=_parse_actions(actions), verbose=verbose)

# Public Method: Mark objects as expired based on last cached date.
@app.command(name="airflow-expire")
def cmd_devtools_airflow_expire(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    fields: Annotated[bool, typer.Option("--fields", help="Include 'fields changed' Airflow tables.")] = False,
    scores: Annotated[bool, typer.Option("--scores", help="Include 'scores expired' Airflow table.")] = False,
    types: Annotated[str | None, typer.Option("--types", help="Comma-separated object types to restrict expiration to.")] = "Exercise,Notebook",
    older_than: Annotated[int, typer.Option("--older-than", help="Expire objects last cached more than N days ago.")] = 300,
    actions: Annotated[str, typer.Option("--actions", help="Comma-separated actions: print,eval,commit.")] = "commit",
) -> None:
    """Mark objects as expired (has_expired=1) based on when they were last cached."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj

    # Prepare include_fields for the following steps.
    include_fields = fields or not scores
    include_scores = scores or not fields
    object_types = [t.strip() for t in types.split(",") if t.strip()] if types else None

    # Continue with the next step.
    cli_ctx.registry.orchestrator.expire(
        include_nodes  = False,
        include_edges  = False,
        include_fields = include_fields,
        include_scores = include_scores,
        object_types   = object_types,
        older_than     = older_than,
        limit_per_type = None,
        count_only     = False,
        verbose        = verbose,
    )

# Public Method: Refresh 'to_process' flags.
@app.command(name="airflow-refresh")
def cmd_devtools_airflow_refresh(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    limit_per_type: Annotated[int, typer.Option("--limit-per-type", help="Maximum number of objects to refresh per document type.")] = 1000,
) -> None:
    """Refresh 'to_process' flags based on changed checksums, expired dates, and/or new objects."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    cli_ctx.registry.orchestrator.refresh(
        doc_type       = None,
        limit_per_type = limit_per_type,
        verbose        = verbose,
    )

# Public Method: Propagate 'to_process' flags to cache tables.
@app.command(name="airflow-propagate")
def cmd_devtools_airflow_propagate(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    fields: Annotated[bool, typer.Option("--fields", help="Propagate fields-changed flags only.")] = False,
    scores: Annotated[bool, typer.Option("--scores", help="Propagate scores-expired flags only.")] = False,
    actions: Annotated[str, typer.Option("--actions", help="Comma-separated actions: print,eval,commit.")] = "print,eval,commit",
) -> None:
    """Propagate Airflow 'to_process' flags to graph_cache tables."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj

    # Prepare include_fields for the following steps.
    include_fields = fields or not scores
    include_scores = scores or not fields

    # Continue with the next step.
    cli_ctx.registry.orchestrator.propagate(
        actions        = _parse_actions(actions),
        include_fields = include_fields,
        include_scores = include_scores,
        verbose        = verbose,
    )

#================================================================#
# Function Group: cache-* commands                               #
#================================================================#

# Public Method: Execute cache SQL formulas.
@app.command(name="cache-formulas-update")
def cmd_devtools_cache_formulas_update(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    formulas: Annotated[str, typer.Option("--formulas", help="Comma-separated formulas: reset,fields,views,traversals,scores.")] = "reset,fields,views,traversals,scores",
    actions: Annotated[str, typer.Option("--actions", help="Comma-separated actions: print,eval,commit.")] = "print,eval,commit",
) -> None:
    """Execute cache SQL formulas."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    cachemanager = cli_ctx.registry.cachemanager
    action_set = _parse_actions(actions)
    formula_set = tuple(f.strip() for f in formulas.split(",") if f.strip())

    # Handle the conditional case.
    if "reset" in formula_set and "commit" in action_set:
        cachemanager.apply_data_reset_formulas(verbose="print" in action_set, actions=action_set)
    if "fields" in formula_set and "commit" in action_set:
        cachemanager.apply_calculated_field_formulas(verbose="print" in action_set, actions=action_set)
    if "views" in formula_set:
        cachemanager.materialize_views(actions=action_set)
    if "traversals" in formula_set and "commit" in action_set:
        cachemanager.apply_traversals(verbose="print" in action_set, actions=action_set)
    if "scores" in formula_set and "commit" in action_set:
        cachemanager.apply_scoring_formulas(verbose="print" in action_set, actions=action_set)

# Public Method: Recalculate the scores matrix.
@app.command(name="cache-scores-matrix")
def cmd_devtools_cache_scores_matrix(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    actions: Annotated[str, typer.Option("--actions", help="Comma-separated actions: print,eval,commit.")] = "print,eval,commit",
) -> None:
    """Recalculate the scores matrix."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    cli_ctx.registry.cachemanager.update_scores_matrix(
        score_thr = 0.1,
        actions   = _parse_actions(actions),
    )

#================================================================#
# Function Group: index-* commands                               #
#================================================================#

# Public Method: Build up and/or update index field tables.
@app.command(name="index-build")
def cmd_devtools_index_build(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    actions: Annotated[str, typer.Option("--actions", help="Comma-separated actions: print,eval,commit.")] = "print,eval,commit",
) -> None:
    """Build up and/or update index field tables."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    cli_ctx.registry.indexdb.build(actions=_parse_actions(actions))

# Public Method: Apply vertical and horizontal patching to index tables.
@app.command(name="index-patch")
def cmd_devtools_index_patch(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    actions: Annotated[str, typer.Option("--actions", help="Comma-separated actions: print,eval,commit.")] = "print,eval,commit",
) -> None:
    """Apply vertical and horizontal patching to all index tables."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    cli_ctx.registry.indexdb.patch(actions=_parse_actions(actions))

# Public Method: Delete loose ends from cache/graphsearch tables.
@app.command(name="index-delete-loose-ends")
def cmd_devtools_index_delete_loose_ends(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    use_cache: Annotated[bool, typer.Option("--use-cache", help="Use the cached largest connected graph instead of recalculating.")] = False,
    actions: Annotated[str, typer.Option("--actions", help="Comma-separated actions: print,eval,commit.")] = "print,eval,commit",
) -> None:
    """Delete loose ends from cache and graphsearch index tables."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    cli_ctx.registry.indexdb.delete_loose_ends(
        refresh_graph = not use_cache,
        actions       = _parse_actions(actions),
    )

# Public Method: Generate ElasticSearch index files from MySQL.
@app.command(name="index-es-generate")
def cmd_devtools_index_es_generate(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    index_date: Annotated[str, typer.Option("--index-date", help="Date of the ElasticSearch index (YYYY-MM-DD).")] = date.today().isoformat(),
    ignore_warnings: Annotated[bool, typer.Option("--ignore-warnings", "-i", help="Ignore warning messages.")] = False,
    replace_existing: Annotated[bool, typer.Option("--replace-existing", "-r", help="Replace existing local cache and index files.")] = False,
    force_replace: Annotated[bool, typer.Option("--force-replace", "-f", help="Force replace without prompting.")] = False,
    local_cache_only: Annotated[bool, typer.Option("--local-cache-only", "-lco", help="Generate local cache only.")] = False,
    index_file_only: Annotated[bool, typer.Option("--index-file-only", "-ifo", help="Generate index file only (from existing local cache).")] = False,
    actions: Annotated[str, typer.Option("--actions", help="Comma-separated actions: print,eval,commit.")] = "commit",
) -> None:
    """Generate ElasticSearch index files from MySQL."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    indexes = cli_ctx.registry.indexes

    # Handle the conditional case.
    if not index_file_only:
        indexes.generate_local_cache_streaming(
            index_date       = index_date,
            ignore_warnings  = ignore_warnings,
            replace_existing = replace_existing,
            force_replace    = force_replace,
        )

    # Handle the conditional case.
    if not local_cache_only:
        indexes.generate_index_from_local_cache(
            index_date       = index_date,
            ignore_warnings  = ignore_warnings,
            replace_existing = replace_existing,
            force_replace    = force_replace,
        )

# Public Method: Import an ElasticSearch index from a local folder.
@app.command(name="index-es-import")
def cmd_devtools_index_es_import(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    input_folder: Annotated[str | None, typer.Option("--input-folder", help="Input folder containing the exported index.")] = None,
    rename_to: Annotated[str | None, typer.Option("--rename-to", help="Rename index to this name on target server.")] = None,
    chunk_size: Annotated[int, typer.Option("--chunk-size", help="Number of documents to import per batch.")] = 1000000,
    replace_existing: Annotated[bool, typer.Option("--replace-existing", "-r", help="Replace existing files in the output folder.")] = False,
    force: Annotated[bool, typer.Option("--force", "-f", help="Force replace without prompting for confirmation.")] = False,
    actions: Annotated[str, typer.Option("--actions", help="Comma-separated actions: print,eval,commit.")] = "commit",
) -> None:
    """Import an ElasticSearch index from a local folder."""
    cli_ctx: CLIContext = ctx.obj
    es = cli_ctx.es

    # Handle the conditional case.
    if input_folder is None:
        raise typer.BadParameter("--input-folder is required.")

    # Handle the conditional case.
    if rename_to is None:
        rename_to = __import__("os").path.basename(input_folder)

    # Continue with the next step.
    es.import_index_from_folder(
        engine_name      = env,
        input_folder     = input_folder,
        rename_to        = rename_to,
        chunk_size       = chunk_size,
        replace_existing = replace_existing,
        force            = force,
    )
