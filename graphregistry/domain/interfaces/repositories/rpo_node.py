from __future__ import annotations
from typing import Any, Protocol, runtime_checkable
from graphregistry.domain.models.mdl_node import Node, NodeKey, NodeList

@runtime_checkable
class NodeRepository(Protocol):

    # Check if object exists
    def exists(self, key: NodeKey) -> bool:
        ...

    # Check if objects in list exist
    def exists_many(self, key_list: list[NodeKey]) -> list[bool]:
        ...

    # Load one node from persistence
    def get(self, key: NodeKey) -> Node | None:
        ...

    # Load many nodes from persistence
    def get_many(self, key_list: list[NodeKey]) -> NodeList:
        ...

    # Save one node to persistence
    def save(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> Any:
        ...

    # Save many nodes
    def save_many(self, node_list: NodeList, actions: tuple[str, ...] = ("eval",)) -> list[Any]:
        ...

    # Optional delete operations
    def delete(self, key: NodeKey, actions: tuple[str, ...] = ("eval",)) -> bool:
        ...

    def delete_many(self, key_list: list[NodeKey], actions: tuple[str, ...] = ("eval",)) -> list[bool]:
        ...
