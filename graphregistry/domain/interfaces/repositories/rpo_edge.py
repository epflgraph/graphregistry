from __future__ import annotations
from typing import Any, Protocol, runtime_checkable
from graphregistry.domain.models.mdl_edge import Edge, EdgeKey, EdgeList

@runtime_checkable
class EdgeRepository(Protocol):

    # Check if object exists
    def exists(self, key: EdgeKey) -> bool:
        ...

    # Check if objects in list exist
    def exists_many(self, key_list: list[EdgeKey]) -> list[bool]:
        ...

    # Load one edge from persistence
    def get_by_key(self, key: EdgeKey) -> Edge | None:
        ...

    # Load many edges from persistence
    def get_by_keys(self, key_list: list[EdgeKey]) -> EdgeList:
        ...

    # Save one edge to persistence
    def save(self, edge: Edge, actions: tuple[str, ...] = ("eval",)) -> Any:
        ...

    # Save many edges
    def save_many(self, edge_list: EdgeList, actions: tuple[str, ...] = ("eval",)) -> list[Any]:
        ...

    # Optional delete operations
    def delete(self, key: EdgeKey, actions: tuple[str, ...] = ("eval",)) -> bool:
        ...

    def delete_many(self, key_list: list[EdgeKey], actions: tuple[str, ...] = ("eval",)) -> list[bool]:
        ...
