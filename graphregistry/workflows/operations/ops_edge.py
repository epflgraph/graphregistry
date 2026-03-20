from graphregistry.domain.models.edge import EdgeKey
from graphregistry.domain.interfaces.repositories.rpo_edge import EdgeRepository


class EdgeOperations:

    def __init__(self, repo: EdgeRepository):
        self.repo = repo

    def exists(self, key: EdgeKey) -> bool:
        return self.repo.exists(key)

    def insert(self, key: EdgeKey) -> bool:
        if self.repo.exists(key):
            raise ValueError("Edge already exists")
        return self.repo.insert(key)

    def update(self, key: EdgeKey) -> bool:
        return self.repo.update(key)

    def upsert(self, key: EdgeKey) -> bool:
        if self.repo.exists(key):
            return self.repo.update(key)
        return self.repo.insert(key)

    def delete(self, key: EdgeKey) -> bool:
        return self.repo.delete(key)