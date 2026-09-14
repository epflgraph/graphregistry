# graphregistry/entrypoints/cli/cmd_ai.py
from __future__ import annotations
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_node import Node, NodeKey
from graphregistry.entrypoints.cli.dependencies import build_node_operations_with_concept_detection_from_args

# Internal Function: Initialize node operations with the repository and concept-detection gateway.
def _get_node_ops(args):
    return build_node_operations_with_concept_detection_from_args(args)

# Internal Function: Build a rich Progress instance for the CLI.
def _make_progress() -> Progress:
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
    return f"{key.object_type}/{key.object_id}"

#-----------------------------------#
# Handler: Detect concepts in nodes #
#-----------------------------------#
# Public Method: Detect concepts in nodes without detected concepts.
def cmd_ai_detect_concepts(args) -> None:
    # Initialize node operations with the repository and gateways
    node_ops = _get_node_ops(args)
    # Resolve the object types to process. Default to Course for backward compatibility.
    object_types = [t.strip() for t in args.types.split(',') if t.strip()] if args.types else ["Course"]
    # Drive the workflow through a rich progress display so the user sees per-node status.
    with _make_progress() as progress:
        # Show which object types will be processed.
        progress.console.print(f"Detecting concepts for object types: {', '.join(object_types)}")
        # Collect candidate keys first so the progress bar can advance per node loaded.
        candidate_keys: list[NodeKey] = []
        for object_type in object_types:
            key_list = node_ops.find_keys_with_no_concepts(object_type=object_type)
            candidate_keys.extend(key_list.item_list)
        # Short-circuit when no candidates were found so we don't render an empty task.
        if not candidate_keys:
            progress.console.print("No candidate nodes found.")
            return
        # Process each candidate one at a time: load, detect concepts, save.
        total = len(candidate_keys)
        enrich_task = progress.add_task("Detecting concepts...", total=total)
        for i, key in enumerate(candidate_keys, start=1):
            label = _node_key_label(key)
            progress.update(enrich_task, description=f"[{i}/{total}] Loading {label}")
            node = node_ops.get(key)
            if node is None:
                progress.advance(enrich_task)
                continue
            # Advance the counter before the slow API call so the bar moves while waiting.
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
                description = f"[{i}/{total}] Saving {label} ({concept_count} concept(s))",
            )
            # Save the enriched node back to the repository
            node_ops.save(enriched_node)
        # Report completion once every candidate has been enriched and persisted.
        progress.console.print(f"Concept detection complete for {total} node(s).")

#--------------------------------------------------#
# Handler: Launch interation of lecture processing #
#--------------------------------------------------#
# def cmd_ai_process_lectures(args) -> None:
