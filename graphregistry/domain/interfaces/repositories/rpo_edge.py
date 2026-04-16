# graphregistry/domain/interfaces/repositories/rpo_edge.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.mdl_edge import Edge, EdgeKey, EdgeList
from graphregistry.domain.interfaces.types import ActionSet

# Class definition
@runtime_checkable
class EdgeRepository(Protocol):

    def exists(self, key: EdgeKey) -> bool:
        ...

    def exists_many(self, key_list: list[EdgeKey]) -> list[bool]:
        ...

    def get(self, key: EdgeKey) -> Edge | None:
        ...

    def get_many(self, key_list: list[EdgeKey]) -> EdgeList:
        ...

    def save(self, edge: Edge, actions: ActionSet = ("eval",)) -> Edge:
        ...

    def save_many(self, edge_list: EdgeList, actions: ActionSet = ("eval",)) -> list[Edge]:
        ...

    def delete(self, key: EdgeKey, actions: ActionSet = ("eval",)) -> bool | None:
        ...

    def delete_many(self, key_list: list[EdgeKey], actions: ActionSet = ("eval",)) -> list[bool | None]:
        ...