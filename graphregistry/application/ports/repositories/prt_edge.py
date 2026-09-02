# graphregistry/application/ports/repositories/prt_edge.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.entities.mdl_base import EdgeKeyList
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeKey, EdgeList
from graphregistry.domain.types import ActionSet

#==================#
# Class Definition #
#==================#
@runtime_checkable
class EdgeRepository(Protocol):

    # Public Method: List edges of a given object type and optional ID pattern.
    def list(self, object_type: tuple[str, str], id_pattern: str | None) -> list[tuple[str, str, str, str, str]]:
        ...

    # Public Method: Check whether a single edge exists.
    def exists(self, key: EdgeKey) -> bool:
        ...

    # Public Method: Check whether a list of edges exist.
    def exists_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> list[bool]:
        ...

    # Public Method: Retrieve a single edge by key.
    def get(self, key: EdgeKey) -> Edge | None:
        ...

    # Public Method: Retrieve a list of edges by key.
    def get_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> EdgeList:
        ...

    # Public Method: Save a single edge.
    def save(self, edge: Edge, actions: ActionSet = ('commit',)) -> Edge:
        ...

    # Public Method: Save a list of edges.
    def save_many(self, edge_list: EdgeList | list[Edge], actions: ActionSet = ('commit',)) -> EdgeList:
        ...

    # Public Method: Delete a single edge.
    def delete(self, key: EdgeKey, actions: ActionSet = ('commit',)) -> bool | None:
        ...

    # Public Method: Delete a list of edges.
    def delete_many(self, key_list: EdgeKeyList | list[EdgeKey], actions: ActionSet = ('commit',)) -> list[bool | None]:
        ...
