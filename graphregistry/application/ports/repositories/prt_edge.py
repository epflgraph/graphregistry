# graphregistry/application/ports/repositories/prt_edge.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.entities.mdl_base import EdgeKeyList
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeKey, EdgeList
from graphregistry.domain.types import ActionSet

# Class definition
@runtime_checkable
class EdgeRepository(Protocol):

    def list(self, object_type: tuple[str, str], id_pattern: str | None) -> list[tuple[str, str, str, str, str]]:
        ...

    def exists(self, key: EdgeKey) -> bool:
        ...

    def exists_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> list[bool]:
        ...

    def get(self, key: EdgeKey) -> Edge | None:
        ...

    def get_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> EdgeList:
        ...

    def save(self, edge: Edge, actions: ActionSet = ('commit',)) -> Edge:
        ...

    def save_many(self, edge_list: EdgeList | list[Edge], actions: ActionSet = ('commit',)) -> EdgeList:
        ...

    def delete(self, key: EdgeKey, actions: ActionSet = ('commit',)) -> bool | None:
        ...

    def delete_many(self, key_list: EdgeKeyList | list[EdgeKey], actions: ActionSet = ('commit',)) -> list[bool | None]:
        ...