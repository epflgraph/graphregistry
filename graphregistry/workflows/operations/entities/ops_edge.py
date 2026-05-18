# graphregistry/workflows/operations/entities/ops_edge.py
from __future__ import annotations
from dataclasses import dataclass
from graphregistry.domain.types import ActionSet
from graphregistry.domain.interfaces.repositories.rpo_edge import EdgeRepository
from graphregistry.domain.models.entities.mdl_base import EdgeKeyList
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeKey, EdgeList

# Class definition
@dataclass(frozen=True)
class EdgeUpsertResult:
    success: bool
    created: bool

# Class definition
class EdgeOperations:

    def __init__(self, repo: EdgeRepository):
        self.repo = repo

    def list(self, object_type: tuple[str, str], id_pattern: str | None = None) -> list[tuple[str, str, str, str, str, str, str]]:
        return self.repo.list(object_type=object_type, id_pattern=id_pattern)

    def exists(self, key: EdgeKey) -> bool:
        return self.repo.exists(key)

    def exists_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> list[bool]:
        return self.repo.exists_many(key_list)

    def get(self, key: EdgeKey) -> Edge | None:
        return self.repo.get(key)

    def get_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> EdgeList:
        return self.repo.get_many(key_list)

    def save(self, edge: Edge, actions: ActionSet = ("eval",)) -> Edge:
        return self.repo.save(edge, actions=actions)

    def save_many(self, edge_list: EdgeList | list[Edge], actions: ActionSet = ("eval",)) -> EdgeList:
        return self.repo.save_many(edge_list, actions=actions)

    def insert(self, edge: Edge, actions: ActionSet = ("eval",)) -> bool:
        """
        Backward-compatible alias for save/upsert semantics.
        """
        return bool(self.repo.save(edge, actions=actions))

    def update(self, edge: Edge, actions: ActionSet = ("eval",)) -> bool:
        """
        Backward-compatible alias for save/upsert semantics.
        """
        return bool(self.repo.save(edge, actions=actions))

    def upsert(self, edge: Edge, actions: ActionSet = ("eval",)) -> EdgeUpsertResult:
        created = not self.repo.exists(edge.key)
        success = bool(self.repo.save(edge, actions=actions))
        return EdgeUpsertResult(success=success, created=created)

    def delete(self, key: EdgeKey, actions: ActionSet = ("eval",)) -> bool | None:
        return self.repo.delete(key, actions=actions)

    def delete_many(self, key_list: EdgeKeyList | list[EdgeKey], actions: ActionSet = ("eval",)) -> list[bool | None]:
        return self.repo.delete_many(key_list, actions=actions)