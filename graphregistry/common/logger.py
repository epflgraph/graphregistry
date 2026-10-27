# graphregistry/common/logger.py
from __future__ import annotations
import rich
from graphregistry.domain.models.entities.mdl_edge import EdgeKey
from graphregistry.domain.models.entities.mdl_node import NodeKey

# Action icons shared by the single-item and batch log formatters.
_ACTION_ICONS: dict[str, str] = {
    'exists'             : '✅',
    'not found'          : '❌',
    'saved'              : '💾',
    'deleted'            : '🗑️ ',
    'concepts detected'  : '🧬',
    'concepts validated' : '🧬👍',
    'translated'         : '🌐',
    'ai enriched'        : '🤖✨',
    'airflow modified'   : '♻️ ',
}

# Internal Function: Render the colored (type, id) tuple of a node key.
def _node_tuple(key: NodeKey) -> str:
    return f"([cyan]{key.object_type}[/cyan], [bold][cyan]{key.object_id}[/cyan][/bold])"

# Internal Function: Render the colored (from, to, context) tuple of an edge key.
def _edge_tuple(key: EdgeKey) -> str:
    return f"([cyan]{key.from_object_type}[/cyan], [bold][cyan]{key.from_object_id}[/cyan][/bold], [cyan]{key.to_object_type}[/cyan], [bold][cyan]{key.to_object_id}[/cyan][/bold], [cyan]{key.context}[/cyan])"

# Internal Function: Render the single-item log line for a node or edge action.
def _node_or_edge_action(key: NodeKey | EdgeKey, action: str) -> str:
    icon = _ACTION_ICONS[action]
    return f"{icon} [green]{action.capitalize()}:[/green] [yellow]{'Node' if isinstance(key, NodeKey) else 'Edge'}[/yellow] [cyan]~[/cyan] {_node_tuple(key) if isinstance(key, NodeKey) else _edge_tuple(key)}"

# Internal Function: Format one aggregated batch line, grouping the keys by
# entity type so a bulk operation logs a single line instead of one per item.
def _batch_action(keys: list[NodeKey | EdgeKey], action: str) -> str:
    icon = _ACTION_ICONS[action]
    entity = "Node" if isinstance(keys[0], NodeKey) else "Edge"

    # Count the keys per type label: the object type for nodes, and the
    # from/to/context triple for edges.
    counts: dict[str, int] = {}
    for key in keys:
        label = key.object_type if isinstance(key, NodeKey) else f"{key.from_object_type}, {key.to_object_type}, {key.context}"
        counts[label] = counts.get(label, 0) + 1

    # Show the bare type for single-type batches and a count-sorted breakdown
    # otherwise, keeping the line compact for the common bulk-ingestion case.
    if len(counts) == 1:
        breakdown = next(iter(counts))
    else:
        breakdown = ", ".join(f"{label} ×{count}" for label, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])))
    return f"{icon} [green]{action.capitalize()}:[/green] [yellow]{len(keys)} {entity}s[/yellow] [cyan]~[/cyan] ({breakdown})"

#==================#
# Class Definition #
#==================#
class GraphLogger:

    # Public Method: Initialize the logger.
    def __init__(self) -> None:
        pass

    # Public Method: Log that a node or edge exists in the database.
    def exists(self, key: NodeKey | EdgeKey) -> None:
        rich.print(_node_or_edge_action(key, 'exists'))

    # Public Method: Log that a node or edge was not found in the database.
    def not_found(self, key: NodeKey | EdgeKey) -> None:
        rich.print(_node_or_edge_action(key, 'not found'))

    # Public Method: Log that a single node or edge was saved.
    def saved(self, key: NodeKey | EdgeKey) -> None:
        rich.print(_node_or_edge_action(key, 'saved'))

    # Public Method: Log that a single node or edge was deleted.
    def deleted(self, key: NodeKey | EdgeKey) -> None:
        rich.print(_node_or_edge_action(key, 'deleted'))

    # Public Method: Log one aggregated line for a batch of saved nodes or edges.
    def saved_many(self, keys: list[NodeKey | EdgeKey]) -> None:
        if keys:
            rich.print(_batch_action(keys, 'saved'))

    # Public Method: Log one aggregated line for a batch of deleted nodes or edges.
    def deleted_many(self, keys: list[NodeKey | EdgeKey]) -> None:
        if keys:
            rich.print(_batch_action(keys, 'deleted'))

    # Public Method: Log that concepts were detected for a node.
    def concepts_detected(self, key: NodeKey) -> None:
        rich.print(_node_or_edge_action(key, 'concepts detected'))

    # Public Method: Log that concepts were validated for a node.
    def concepts_validated(self, key: NodeKey) -> None:
        rich.print(_node_or_edge_action(key, 'concepts validated'))

    # Public Method: Log that a node was translated.
    def translated(self, key: NodeKey) -> None:
        rich.print(_node_or_edge_action(key, 'translated'))

    # Public Method: Log that a node was enriched through GenAI.
    def enriched(self, key: NodeKey) -> None:
        rich.print(_node_or_edge_action(key, 'ai enriched'))

    # Public Method: Log that an Airflow modification was applied.
    def airflow_saved(self, key: NodeKey) -> None:
        rich.print(_node_or_edge_action(key, 'airflow modified'))
