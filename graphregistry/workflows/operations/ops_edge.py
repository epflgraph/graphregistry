from dataclasses import dataclass

from graphregistry.domain.models.mdl_edge import Edge, EdgeKey
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
