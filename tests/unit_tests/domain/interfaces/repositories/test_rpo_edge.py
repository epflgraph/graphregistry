from __future__ import annotations

from graphregistry.domain.interfaces.repositories.rpo_edge import EdgeRepository
from graphregistry.domain.models.mdl_base import EdgeKey
from graphregistry.domain.models.mdl_edge import Edge, EdgeList


class DummyEdgeRepo:
    def exists(self, key: EdgeKey) -> bool:
        return False

    def exists_many(self, key_list: list[EdgeKey]) -> list[bool]:
        return [False for _ in key_list]

    def get(self, key: EdgeKey) -> Edge | None:
        return None

    def get_many(self, key_list: list[EdgeKey]) -> EdgeList:
        return EdgeList(edge_list=[])

    def save(self, edge: Edge, actions: tuple[str, ...] = ("eval",)) -> Edge:
        return edge

    def save_many(self, edge_list: EdgeList, actions: tuple[str, ...] = ("eval",)) -> list[Edge]:
        return edge_list.edge_list

    def delete(self, key: EdgeKey, actions: tuple[str, ...] = ("eval",)) -> bool | None:
        return True

    def delete_many(self, key_list: list[EdgeKey], actions: tuple[str, ...] = ("eval",)) -> list[bool | None]:
        return [True for _ in key_list]


def test_edge_repository_protocol_is_runtime_checkable() -> None:
    assert isinstance(DummyEdgeRepo(), EdgeRepository)
