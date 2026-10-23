# graphregistry/entrypoints/cli/commands/cmd_ai.py
from __future__ import annotations
from typing import Annotated
import typer
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn
from graphregistry.common.config import GlobalConfig
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_node import Node, NodeKey
from graphregistry.entrypoints.cli.common import DEFAULT_ENV, EnvOption, VerboseOption
from graphregistry.entrypoints.cli.context import CLIContext
from graphregistry.entrypoints.cli.dependencies import build_node_operations_with_concept_detection_from_cli, build_processing_scope

# Create the Typer sub-app for GraphAI commands.
app = typer.Typer(help="Execute GraphAI operations.")

# Internal Function: Initialize node operations with the repository and concept-detection gateway.
def _get_node_ops(ctx: CLIContext, env: str, verbose: bool) -> "NodeOperations":
    """Initialize node operations with the repository and concept-detection gateway."""
    return build_node_operations_with_concept_detection_from_cli(ctx=ctx, env=env, verbose=verbose)

# Internal Function: Build a rich Progress instance for the CLI.
def _make_progress() -> Progress:
    """Build a rich Progress instance for the CLI."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        transient=True,
    )

# Internal Function: Return a compact, human-readable label for a node key.
def _node_key_label(key: NodeKey) -> str:
    """Return a compact, human-readable label for a node key."""
    return f"{key.object_type}/{key.object_id}"

# Public Method: Detect concepts for objects active on Airflow.
@app.command(name="detect-concepts")
def cmd_ai_detect_concepts(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    object_types: Annotated[str | None, typer.Option("--object-types", help="Comma-separated object types to detect concepts for.")] = None,
) -> None:
    """Detect concepts for objects active on Airflow."""
    cli_ctx: CLIContext = ctx.obj
    node_ops = _get_node_ops(cli_ctx, env, verbose)

    # Resolve the object types to process. When not provided, inherit the node
    # types marked for field processing in the Airflow typeflags configuration.
    if object_types:
        resolved_types = [t.strip() for t in object_types.split(",") if t.strip()]
    else:
        scope = build_processing_scope(
            db            = cli_ctx.db,
            global_config = cli_ctx.global_config,
            index_config  = cli_ctx.index_config,
        )
        resolved_types = scope.node_fields_types

    # Concept detection attaches concepts to content nodes; ontology nodes such as
    # Category and Concept are the targets of those edges, not the sources.
    glbcfg = GlobalConfig()
    resolved_types = [
        t for t in resolved_types
        if glbcfg.object_type_to_schema.get(t) != glbcfg.schema_ontology
    ]

    # Abort early when there are no eligible object types to process.
    if not resolved_types:
        typer.echo("No object types to process (typeflags config has no nodes with field processing enabled).")
        raise typer.Exit(code=0)

    # Manage the resource context.
    with _make_progress() as progress:
        progress.console.print(f"Detecting concepts for object types: {', '.join(resolved_types)}")

        # Fetch all candidate keys in a single query using an IN-list predicate.
        key_list = node_ops.find_keys_with_no_concepts(object_types=resolved_types)
        candidate_keys: list[NodeKey] = list(key_list.item_list)

        # Handle the conditional case.
        if not candidate_keys:
            progress.console.print("No candidate nodes found.")
            return

        # Prepare total for the following steps.
        total = len(candidate_keys)
        enrich_task = progress.add_task("Detecting concepts...", total=total)
        for i, key in enumerate(candidate_keys, start=1):
            label = _node_key_label(key)
            progress.update(enrich_task, description=f"[{i}/{total}] Loading {label}")
            node = node_ops.get(key)
            if node is None:
                progress.advance(enrich_task)
                continue

            # Continue with the next step.
            progress.update(
                enrich_task,
                advance     = 1,
                description = f"[{i}/{total}] Detecting concepts for {label}",
            )
            enriched_node = node_ops.enrich_with_concepts(node)
            concept_count = (
                len(enriched_node.concepts.detected.item_list)
                if enriched_node.concepts.detected is not None
                else 0
            )
            progress.update(
                enrich_task,
                description=f"[{i}/{total}] Saving {label} ({concept_count} concept(s))",
            )
            node_ops.save(enriched_node)

        # Continue with the next step.
        progress.console.print(f"Concept detection complete for {total} node(s).")
