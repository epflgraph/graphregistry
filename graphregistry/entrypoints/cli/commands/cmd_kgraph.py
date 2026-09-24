# graphregistry/entrypoints/cli/commands/cmd_kgraph.py
from __future__ import annotations
from datetime import date
from typing import Annotated
import typer
from graphregistry.application.core.cor_registry import ELASTICSEARCH_DATA_EXPORT_PATH
from graphregistry.domain.models.pipeline.mdl_scores import ScoreConsolidationParams
from graphregistry.entrypoints.cli.common import DEFAULT_ENV, EnvOption, VerboseOption
from graphregistry.entrypoints.cli.context import CLIContext
from graphregistry.entrypoints.cli.dependencies import (
    build_formula_repository,
    build_index_integrity_repository,
    build_index_patch_operations,
    build_processing_scope,
    build_scoresmatrix_repository,
    build_searchindex_export_repository,
    build_typeflags_repository,
)

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

    # Run the full set of cache computation formulas through the typed
    # repository.
    formula_repo = build_formula_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, verbose=verbose)
    formula_repo.apply_calculated_field_formulas(actions=("commit",))
    formula_repo.materialize_views(actions=("commit",))
    formula_repo.apply_traversals(actions=("commit",))
    formula_repo.apply_scoring_formulas(actions=("commit",))

    # Update the scores matrix through the typed repository; the cache
    # formulas above remain on the legacy cache manager until the formulas
    # family is migrated.
    scores_repo = build_scoresmatrix_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, scores_config=cli_ctx.scores_config, verbose=verbose)
    scope = build_processing_scope(
        db            = cli_ctx.db,
        global_config = cli_ctx.global_config,
        index_config  = cli_ctx.index_config,
    )
    scores_repo.update_matrix(
        scope   = scope,
        params  = ScoreConsolidationParams(),
        actions = ("commit",),
    )

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

    # Build the staging tables, then patch, through the typed operation.
    patch_ops = build_index_patch_operations(
        db            = cli_ctx.db,
        global_config = cli_ctx.global_config,
        index_config  = cli_ctx.index_config,
        scores_config = cli_ctx.scores_config,
        verbose       = verbose,
    )
    typeflags_repo = build_typeflags_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, verbose=verbose)
    flags = typeflags_repo.load()
    patch_ops.build(flags=flags, actions=("commit",))
    patch_ops.patch(flags=flags, actions=("commit",))

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
    integrity_repo = build_index_integrity_repository(db=cli_ctx.db, global_config=cli_ctx.global_config, verbose=verbose)
    integrity_repo.delete_loose_ends(
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
    index_date = index_name.split("_")[-1]

    # Generate through the typed export repository: the local cache unless
    # the caller only wants the index file, then the import folder.
    export_repo = build_searchindex_export_repository(
        db            = cli_ctx.db,
        global_config = cli_ctx.global_config,
        index_config  = cli_ctx.index_config,
        verbose       = verbose,
    )
    if not from_cache:
        export_repo.generate_local_cache(
            index_date       = index_date,
            ignore_warnings  = ignore_warnings,
            replace_existing = replace_existing,
            force_replace    = force_replace,
        )
    export_repo.generate_index_folder(
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
