# graphregistry/common/logger.py
from graphregistry.domain.models.entities.mdl_node import NodeKey
from graphregistry.domain.models.entities.mdl_edge import EdgeKey
import rich

#==================#
# Helper functions #
#==================#
def _node_tuple(key: NodeKey) -> str:
    return f"([cyan]{key.institution_id}[/cyan], [cyan]{key.object_type}[/cyan], [bold][cyan]{key.object_id}[/cyan][/bold])"

def _edge_tuple(key: EdgeKey) -> str:
    return f"([cyan]{key.from_institution_id}[/cyan], [cyan]{key.from_object_type}[/cyan], [bold][cyan]{key.from_object_id}[/cyan][/bold], [cyan]{key.to_institution_id}[/cyan], [cyan]{key.to_object_type}[/cyan], [bold][cyan]{key.to_object_id}[/cyan][/bold], [cyan]{key.context}[/cyan])"

def _node_or_edge_action(key: NodeKey | EdgeKey, action) -> str:
    icon = {
        'exists'    : '✅',
        'not found' : '❌',
        'saved'     : '💾',
        'deleted'   : '🗑️ ',
        'concepts detected' : '🧬',
        'translated'        : '🌐',
    }[action]
    return f"{icon} [green]{action.capitalize()}:[/green] [yellow]{'Node' if isinstance(key, NodeKey) else 'Edge'}[/yellow] [cyan]~[/cyan] {_node_tuple(key) if isinstance(key, NodeKey) else _edge_tuple(key)}"


# Class definition
class GraphLogger:

    # Initialisation function
    def __init__(self):
        pass

    # Print method: Node or Edge exists in database
    def exists(self, key: NodeKey | EdgeKey) -> None:
        rich.print(_node_or_edge_action(key, 'exists'))

    # Print method: Node or Edge not found in database
    def not_found(self, key: NodeKey | EdgeKey) -> None:
        rich.print(_node_or_edge_action(key, 'not found'))

    # Print method: Node or Edge saved in database
    def saved(self, key: NodeKey | EdgeKey) -> None:
        rich.print(_node_or_edge_action(key, 'saved'))

    # Print method: Node or Edge deleted from database
    def deleted(self, key: NodeKey | EdgeKey) -> None:
        rich.print(_node_or_edge_action(key, 'deleted'))

    # Print method: Concepts detected for a Node
    def concepts_detected(self, key: NodeKey) -> None:
        rich.print(_node_or_edge_action(key, 'concepts detected'))

    # Print method: Node translated
    def translated(self, key: NodeKey) -> None:
        rich.print(_node_or_edge_action(key, 'translated'))