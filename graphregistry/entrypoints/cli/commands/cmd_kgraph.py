# graphregistry/entrypoints/cli/commands/cmd_kgraph.py
from __future__ import annotations
from datetime import date
from typing import Annotated
import typer
from graphregistry.application.core.cor_registry import ELASTICSEARCH_DATA_EXPORT_PATH
from graphregistry.entrypoints.cli.common import DEFAULT_ENV, EnvOption, VerboseOption
from graphregistry.entrypoints.cli.context import CLIContext

# Create the Typer sub-app for high-level knowledge-graph workflows.
app = typer.Typer(help="Knowledge Graph construction workflows.")

# Public Method: Execute graph (re)computation formulas.
@app.command(name="compute")
def cmd_kgraph_compute(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
) -> None:
    """Execute cache formulas and update the scores matrix."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    cachemanager = cli_ctx.registry.cachemanager

    # Run the full set of cache computation formulas.
    # cachemanager.apply_data_reset_formulas(verbose=verbose, actions=("commit",))
    cachemanager.apply_calculated_field_formulas(verbose=verbose, actions=("commit",))
    cachemanager.materialize_views(actions=("commit",))
    cachemanager.apply_traversals(verbose=verbose, actions=("commit",))
    cachemanager.apply_scoring_formulas(verbose=verbose, actions=("commit",))

    # Update the scores matrix.
    cachemanager.update_scores_matrix(score_thr=0.1, actions=("commit",))

# Public Method: (Re)build and patch database for Graph Search.
@app.command(name="patch")
def cmd_kgraph_patch(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
) -> None:
    """Build and patch index field tables."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    indexdb = cli_ctx.registry.indexdb

    # Build and then patch the index field tables.
    indexdb.build(actions=("commit", ""))
    indexdb.patch(actions=("commit", ""))

# Public Method: Prune orphan nodes, loose edges, and small islands from the knowledge graph.
@app.command(name="prune")
def cmd_kgraph_prune(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
) -> None:
    """Prune the knowledge graph."""
    del env  # Registry uses the configured environment internally.
    cli_ctx: CLIContext = ctx.obj
    cli_ctx.registry.indexdb.delete_loose_ends(
        refresh_graph = True,
        actions       = ("commit",),
    )

# Public Method: Generate graph index for ElasticSearch.
@app.command(name="index")
def cmd_kgraph_index(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    index_name: Annotated[str, typer.Option("--index-name", "-n", help="Index name on ElasticSearch.")] = f"graphsearch_test_{date.today().isoformat()}",
    replace_existing: Annotated[bool, typer.Option("--replace-existing", "-r", help="Replace existing local cache and index files.")] = False,
    force_replace: Annotated[bool, typer.Option("--force-replace", "-f", help="Force replace without prompting.")] = False,
    from_cache: Annotated[bool, typer.Option("--from-cache", "-c", help="Import index file from existing local cache.")] = False,
    ignore_warnings: Annotated[bool, typer.Option("--ignore-warnings", "-i", help="Ignore warning messages.")] = False,
) -> None:
    """Generate graph index for ElasticSearch and import it."""
    cli_ctx: CLIContext = ctx.obj
    indexes = cli_ctx.registry.indexes
    index_date = index_name.split("_")[-1]

    # Generate local cache unless the caller only wants the index file.
    if not from_cache:
        indexes.generate_local_cache_streaming(
            index_date       = index_date,
            ignore_warnings  = ignore_warnings,
            replace_existing = replace_existing,
            force_replace    = force_replace,
        )

    # Generate the ElasticSearch index file from local cache.
    indexes.generate_index_from_local_cache(
        index_date       = index_date,
        ignore_warnings  = ignore_warnings,
        replace_existing = replace_existing,
        force_replace    = force_replace,
    )

    # Import the generated index folder into ElasticSearch.
    input_folder = f"{ELASTICSEARCH_DATA_EXPORT_PATH}/{index_date}/es_fullindex_{index_date}"
    cli_ctx.es.import_index_from_folder(
        engine_name      = env,
        input_folder     = input_folder,
        rename_to        = index_name,
        replace_existing = replace_existing,
        force            = force_replace,
        chunk_size       = 1000,
    )
