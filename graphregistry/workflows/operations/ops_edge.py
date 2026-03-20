from dataclasses import dataclass
from typing import Any

from graphregistry.domain.models.mdl_edge import Edge, EdgeKey, EdgeList
from graphregistry.domain.interfaces.repositories.rpo_edge import EdgeRepository


@dataclass(frozen=True)
class UpsertResult:
    success: bool
    created: bool


class EdgeOperations:

    def __init__(self, repo: EdgeRepository):
        self.repo = repo

    def exists(self, key: EdgeKey) -> bool:
        return self.repo.exists(key)

    def insert(self, edge: Edge, actions: tuple[str, ...] = ("eval",)) -> bool:
        if self.repo.exists(edge.key):
            raise ValueError("Edge already exists")
        return bool(self.repo.save(edge, actions=actions))

    def update(self, edge: Edge, actions: tuple[str, ...] = ("eval",)) -> bool:
        if not self.repo.exists(edge.key):
            raise ValueError("Edge does not exist")
        return bool(self.repo.save(edge, actions=actions))

    def upsert(self, edge: Edge, actions: tuple[str, ...] = ("eval",)) -> UpsertResult:
        created = not self.repo.exists(edge.key)
        success = bool(self.repo.save(edge, actions=actions))
        return UpsertResult(success=success, created=created)

    def delete(self, key: EdgeKey) -> bool:
        return bool(self.repo.delete(key))

    # Draft lifecycle/use-case methods migrated from legacy model-centric flow.
    def get_by_key(self, key: EdgeKey) -> Edge | None:
        return self.repo.get_by_key(key)

    def get_by_keys(self, key_list: list[EdgeKey]) -> EdgeList:
        return self.repo.get_by_keys(key_list)

    def save_many(self, edge_list: EdgeList, actions: tuple[str, ...] = ("eval",)) -> list[Any]:
        return self.repo.save_many(edge_list, actions=actions)

    def commit_edge_object(self, edge: Edge, actions: tuple[str, ...] = ("eval",)) -> Any:
        raise NotImplementedError("Use-case draft: map legacy edge-object commit to repository save semantics.")

    def commit_custom_fields(self, edge: Edge, actions: tuple[str, ...] = ("eval",)) -> Any:
        raise NotImplementedError("Use-case draft: map legacy custom-fields commit to repository save semantics.")
