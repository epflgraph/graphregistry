from __future__ import annotations

from graphregistry.domain.interfaces.repositories.rpo_node import NodeRepository
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_node import Node, NodeList


class DummyNodeRepo:
    def exists(self, key: NodeKey) -> bool:
        return False

    def exists_many(self, key_list: list[NodeKey]) -> list[bool]:
        return [False for _ in key_list]

    def get(self, key: NodeKey) -> Node | None:
        return None

    def get_many(self, key_list: list[NodeKey]) -> NodeList:
        return NodeList(node_list=[])

    def save(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> Node:
        return node

    def save_many(self, node_list: NodeList, actions: tuple[str, ...] = ("eval",)) -> list[Node]:
        return node_list.node_list

    def delete(self, key: NodeKey, actions: tuple[str, ...] = ("eval",)) -> bool | None:
        return True

    def delete_many(self, key_list: list[NodeKey], actions: tuple[str, ...] = ("eval",)) -> list[bool | None]:
        return [True for _ in key_list]


def test_node_repository_protocol_is_runtime_checkable() -> None:
    assert isinstance(DummyNodeRepo(), NodeRepository)
