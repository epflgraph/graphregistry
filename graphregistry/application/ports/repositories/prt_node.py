# graphregistry/application/ports/repositories/prt_node.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_node import Node, NodeKey, NodeList
from graphregistry.domain.types import ActionSet

#==================#
# Class Definition #
#==================#
@runtime_checkable
class NodeRepository(Protocol):

    # Public Method: List nodes of a given object type and optional ID pattern.
    def list(self, object_type: str, id_pattern: str | None) -> list[tuple[str, str]]:
        ...

    # Public Method: Check whether a single node exists.
    def exists(self, key: NodeKey) -> bool:
        ...

    # Public Method: Check whether a list of nodes exist.
    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        ...

    # Public Method: Retrieve a single node by key.
    def get(self, key: NodeKey) -> Node | None:
        ...

    # Public Method: Retrieve a list of nodes by key.
    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> NodeList:
        ...

    # Public Method: Save a single node.
    def save(self, node: Node, actions: ActionSet = ('commit',)) -> Node:
        ...

    # Public Method: Save a list of nodes.
    def save_many(self, node_list: NodeList | list[Node], actions: ActionSet = ('commit',)) -> NodeList:
        ...

    # Public Method: Delete a single node.
    def delete(self, key: NodeKey, actions: ActionSet = ('commit',)) -> bool | None:
        ...

    # Public Method: Delete a list of nodes.
    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ('commit',)) -> list[bool | None]:
        ...

    # Public Method: Return nodes that have no concepts attached.
    def get_with_no_concepts(self, object_type: str | None = None, id_pattern: str | None = None) -> NodeList:
        ...
