# graphregistry/domain/interfaces/repositories/rpo_node.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_node import Node, NodeKey, NodeList
from graphregistry.domain.types import ActionSet

# Class definition
@runtime_checkable
class NodeRepository(Protocol):

    def list(self, object_type: str, id_pattern: str | None) -> list[tuple[str, str, str]]:
        ...

    def exists(self, key: NodeKey) -> bool:
        ...

    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        ...

    def get(self, key: NodeKey) -> Node | None:
        ...

    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> NodeList:
        ...

    def save(self, node: Node, actions: ActionSet = ("eval",)) -> Node:
        ...

    def save_many(self, node_list: NodeList | list[Node], actions: ActionSet = ("eval",)) -> NodeList:
        ...

    def delete(self, key: NodeKey, actions: ActionSet = ("eval",)) -> bool | None:
        ...

    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ("eval",)) -> list[bool | None]:
        ...