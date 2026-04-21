# graphregistry/domain/interfaces/repositories/rpo_node.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.interfaces.types import ActionSet
from graphregistry.domain.models.entities.mdl_node import Node, NodeKey, NodeList

# Class definition
@runtime_checkable
class NodeRepository(Protocol):
    def exists(self, key: NodeKey) -> bool:
        ...

    def exists_many(self, key_list: list[NodeKey]) -> list[bool]:
        ...

    def get(self, key: NodeKey) -> Node | None:
        ...

    def get_many(self, key_list: list[NodeKey]) -> NodeList:
        ...

    def save(self, node: Node, actions: ActionSet = ("eval",)) -> Node:
        ...

    def save_many(self, node_list: NodeList, actions: ActionSet = ("eval",)) -> list[Node]:
        ...

    def delete(self, key: NodeKey, actions: ActionSet = ("eval",)) -> bool | None:
        ...

    def delete_many(self, key_list: list[NodeKey], actions: ActionSet = ("eval",)) -> list[bool | None]:
        ...