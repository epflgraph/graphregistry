# graphregistry/application/ports/repositories/prt_node.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_node import Node, NodeKey, NodeList
from graphregistry.domain.types import ActionSet


#================================================================#
# Class Definition                                               #
#================================================================#
@runtime_checkable
class NodeRepository(Protocol):
    """Port for node persistence operations.

    Implementations must provide atomic save/save_many/delete/delete_many
    semantics when 'commit' is in actions.
    """

    def list(self, object_type: str, id_pattern: str | None) -> list[tuple[str, str]]:
        ...

    def exists(self, key: NodeKey) -> bool:
        ...

    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        ...

    def get(self, key: NodeKey) -> Node | None:
        ...

    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> NodeList:
        ...

    def save(self, node: Node, actions: ActionSet = ('commit',)) -> Node:
        """Save a single node.

        Implementations must execute all related inserts/updates atomically
        within the same transaction when 'commit' is in actions.
        """
        ...

    def save_many(self, node_list: NodeList | list[Node], actions: ActionSet = ('commit',)) -> NodeList:
        """Save many nodes efficiently.

        Implementations must provide a first-class batch implementation; they
        must not simply loop over save(). All writes for the whole batch should
        share one transaction when 'commit' is in actions.
        """
        ...

    def delete(self, key: NodeKey, actions: ActionSet = ('commit',)) -> bool | None:
        ...

    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ('commit',)) -> list[bool | None]:
        """Delete many nodes efficiently.

        Implementations must provide a first-class batch implementation; they
        must not simply loop over delete(). All deletes for the whole batch
        should share one transaction when 'commit' is in actions.
        """
        ...

    def get_with_no_concepts(self, object_type: str | None = None, id_pattern: str | None = None) -> NodeList:
        ...
